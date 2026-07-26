use std::num::{NonZeroU32, NonZeroU64};

use s3_types::{
    AclGrants, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId,
    ObjectLockDefaultRetention, ObjectLockMode, ObjectLockState, ObjectRetention, RetentionPeriod,
    StoredLegalHoldStatus,
};

use crate::types::{
    AbortMultipartUploadCleanup, BucketAclSummary, BucketEncryptionConfig, BucketName,
    BucketObjectOwnership, BucketOwnershipControls, BucketState, BucketSubresourceAux,
    BucketSubresourceKind, BucketWriteReservationRecord, ChecksumAlgorithm, ChecksumBytes,
    ChecksumType, ClusterEpoch, CreateBucketConfig, CreateMultipartUploadReq,
    CreateStreamUploadReq, EcShape, EtagKind, GenerationId, LiveObjectRecord,
    ManagedEncryptionAlgorithm, MultipartChecksumConfig, MultipartCompletionFingerprint,
    MultipartPartRecord, MultipartPartSegmentRecord, MultipartReclaimPartRecord,
    MultipartReclaimPartSegmentRecord, MultipartReclaimRecord, MultipartUploadIdKey,
    MultipartUploadRecord, ObjectEncryption, ObjectEncryptionType, ObjectEtag, ObjectKey,
    ObjectLayout, ObjectPartRecord, ObjectPayloadReclaimClaimRecord, ObjectPayloadReclaimKind,
    ObjectSegmentRecord, ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord,
    OwnerIdentity, PgId, PublicAccessBlockConfig, PutLiveObjectReq, SerializedMetadataBlob,
    SerializedSystemMetadataBlob, SerializedTagSet, SessionId, StorageClass,
    StreamUploadCommandRecord, StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget,
    TerminalStreamCleanupRecord, UploadId, UploadState, VersionId, MULTIPART_UPLOAD_ID_KEY_LEN,
};

const METADATA_COMMAND_MAGIC: &[u8] = b"argmin-metadata-command";
const METADATA_COMMAND_ENCODING_VERSION: u16 = 4;
const ABANDONED_METADATA_COMMAND_MAGIC: &[u8] = b"argmin-metadata-command-abandoned";
const ABANDONED_METADATA_COMMAND_ENCODING_VERSION: u16 = 1;
const METADATA_COMMAND_CREATE_BUCKET: u16 = 1;
const METADATA_COMMAND_PUT_BUCKET_VERSIONING: u16 = 2;
const METADATA_COMMAND_PUT_BUCKET_ACL: u16 = 3;
const METADATA_COMMAND_PUT_BUCKET_PROPERTY: u16 = 4;
const METADATA_COMMAND_PUT_BUCKET_SUBRESOURCE: u16 = 5;
const METADATA_COMMAND_RESERVE_OBJECT_GENERATION: u16 = 6;
const METADATA_COMMAND_RELEASE_OBJECT_GENERATION: u16 = 7;
const METADATA_COMMAND_COMMIT_DIRECT_PUT_OBJECT: u16 = 8;
const METADATA_COMMAND_DELETE_OBJECT_VERSION: u16 = 9;
const METADATA_COMMAND_INSERT_DELETE_MARKER: u16 = 10;
const METADATA_COMMAND_COMMIT_MULTIPART_OBJECT: u16 = 11;
const METADATA_COMMAND_PUT_OBJECT_METADATA: u16 = 12;
const METADATA_COMMAND_CREATE_STREAM_UPLOAD: u16 = 13;
const METADATA_COMMAND_APPEND_STREAM_SEGMENT: u16 = 14;
const METADATA_COMMAND_ABORT_STREAM_UPLOAD: u16 = 15;
const METADATA_COMMAND_CREATE_MULTIPART_UPLOAD: u16 = 16;
const METADATA_COMMAND_ABORT_MULTIPART_UPLOAD: u16 = 17;
const METADATA_COMMAND_COMMIT_STREAM_PART: u16 = 18;
const METADATA_COMMAND_DELETE_OBJECT_PAYLOAD_RECLAIM: u16 = 19;
const METADATA_COMMAND_ADVANCE_MULTIPART_COMPLETION_BARRIER: u16 = 21;
const METADATA_COMMAND_RESERVE_OBJECT_VERSION: u16 = 22;
const METADATA_COMMAND_MARK_BUCKET_DELETING: u16 = 23;
const METADATA_COMMAND_DELETE_FINALIZED_BUCKET: u16 = 24;

/// The retry/convergence contract owned by a metadata-command publisher.
///
/// This is the authoritative transitional classification registry. The typed
/// publisher APIs planned in `storage-boundary-compiler-enforcement-plan.md`
/// will eventually make these classes part of the callable API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MetadataCommandPublisherClass {
    SnapshotSensitive,
    ApplyValidated,
    AllocatorCleanup,
    TerminalSessionRetry,
    MatchingOutcomeRetry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetadataCommandPublisherDescriptor {
    pub(crate) canonical_name: &'static str,
    pub(crate) command_kind: &'static str,
    pub(crate) class: MetadataCommandPublisherClass,
}

macro_rules! define_metadata_command_publishers {
    (
        $(
            $id:ident => ($canonical_name:literal, $command_kind:literal, $class:ident)
        ),+ $(,)?
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub(crate) enum MetadataCommandPublisherId {
            $($id),+
        }

        impl MetadataCommandPublisherId {
            pub(crate) const ALL: &'static [Self] = &[$(Self::$id),+];

            pub(crate) const fn descriptor(self) -> MetadataCommandPublisherDescriptor {
                match self {
                    $(
                        Self::$id => MetadataCommandPublisherDescriptor {
                            canonical_name: $canonical_name,
                            command_kind: $command_kind,
                            class: MetadataCommandPublisherClass::$class,
                        },
                    )+
                }
            }
        }
    };
}

define_metadata_command_publishers! {
    CreateBucket => (
        "create_bucket_with_config_and_load_info",
        "CreateBucket",
        ApplyValidated
    ),
    BeginBucketDelete => (
        "begin_bucket_delete",
        "MarkBucketDeleting",
        SnapshotSensitive
    ),
    DeleteBucketFromActingSet => (
        "delete_bucket_from_acting_set",
        "DeleteFinalizedBucket",
        SnapshotSensitive
    ),
    PutBucketVersioning => (
        "put_bucket_versioning_and_load_info",
        "PutBucketVersioning",
        SnapshotSensitive
    ),
    PutBucketAcl => (
        "put_bucket_acl_and_load_info",
        "PutBucketAcl",
        SnapshotSensitive
    ),
    PutBucketProperty => (
        "put_bucket_property_command_and_load_info",
        "PutBucketProperty",
        SnapshotSensitive
    ),
    PutBucketSubresource => (
        "put_bucket_subresource_command_and_load_info_with_route_validation",
        "PutBucketSubresource",
        SnapshotSensitive
    ),
    ReservePutObjectGeneration => (
        "reserve_put_object_generation",
        "ReserveObjectGeneration",
        AllocatorCleanup
    ),
    ReserveNextObjectVersion => (
        "reserve_next_object_version",
        "ReserveObjectVersion",
        AllocatorCleanup
    ),
    ReleaseObjectGenerationReservationCommandRequired => (
        "release_object_generation_reservation_command_required",
        "ReleaseObjectGeneration",
        AllocatorCleanup
    ),
    ReleaseObjectGenerationReservation => (
        "release_object_generation_reservation",
        "ReleaseObjectGeneration",
        AllocatorCleanup
    ),
    CommitDirectPutObjectFromPayloadShards => (
        "commit_direct_put_object_from_payload_shards",
        "CommitDirectPutObject",
        SnapshotSensitive
    ),
    CreatePutObjectStreamSessionRecordUnderReservation => (
        "create_put_object_stream_session_record_under_reservation",
        "CreateStreamUpload",
        SnapshotSensitive
    ),
    CommitStreamSegmentAppend => (
        "commit_stream_segment_append",
        "AppendStreamSegment",
        ApplyValidated
    ),
    AbortStreamUploadSession => (
        "abort_stream_upload_session",
        "AbortStreamUpload",
        TerminalSessionRetry
    ),
    PutObjectMetadataIf => (
        "put_object_metadata_if_with_route_validation",
        "PutObjectMetadata",
        SnapshotSensitive
    ),
    DeleteSpecificObjectVersionIf => (
        "delete_specific_object_version_if_with_route_validation",
        "DeleteObjectVersion",
        SnapshotSensitive
    ),
    DeleteCurrentObjectIf => (
        "delete_current_object_if_with_route_validation",
        "DeleteObjectVersion",
        SnapshotSensitive
    ),
    InsertCurrentDeleteMarkerIf => (
        "insert_current_delete_marker_if_with_route_validation",
        "InsertDeleteMarker",
        SnapshotSensitive
    ),
    ExpireCurrentObjectIfDue => (
        "expire_current_object_if_due",
        "DeleteObjectVersion/InsertDeleteMarker",
        SnapshotSensitive
    ),
    DeleteNoncurrentLiveVersionsIfDue => (
        "delete_noncurrent_live_versions_if_due",
        "DeleteObjectVersion",
        SnapshotSensitive
    ),
    DeleteExpiredDeleteMarkerIfDue => (
        "delete_expired_delete_marker_if_due",
        "DeleteObjectVersion",
        SnapshotSensitive
    ),
    ReclaimObjectPayloadIfUnleased => (
        "reclaim_object_payload_if_unleased",
        "DeleteObjectPayloadReclaim",
        SnapshotSensitive
    ),
    CreatePutObjectStreamSession => (
        "create_put_object_stream_session_with_cleanup_deadline",
        "CreateStreamUpload",
        SnapshotSensitive
    ),
    FinalizePutObjectStream => (
        "finalize_put_object_stream",
        "CommitDirectPutObject",
        TerminalSessionRetry
    ),
    CreateMultipartUpload => (
        "create_multipart_upload",
        "CreateMultipartUpload",
        SnapshotSensitive
    ),
    BeginUploadPartStreamSession => (
        "begin_upload_part_stream_session_with_cleanup_deadline",
        "CreateStreamUpload",
        SnapshotSensitive
    ),
    CreateUploadPartStreamSession => (
        "create_upload_part_stream_session",
        "CreateStreamUpload",
        SnapshotSensitive
    ),
    EstablishMultipartCompletionBarrier => (
        "establish_multipart_completion_barrier",
        "AdvanceMultipartCompletionBarrier",
        AllocatorCleanup
    ),
    CompleteMultipartUploadCommitSerialized => (
        "complete_multipart_upload_commit_serialized",
        "CommitMultipartObject",
        MatchingOutcomeRetry
    ),
    FinalizeUploadPartStream => (
        "finalize_upload_part_stream",
        "CommitStreamPart",
        TerminalSessionRetry
    ),
    AbortMultipartUploadLocked => (
        "abort_multipart_upload_locked",
        "AbortMultipartUpload",
        TerminalSessionRetry
    ),
    AbortAuthorizedMultipartUploadLocked => (
        "abort_authorized_multipart_upload_locked",
        "AbortMultipartUpload",
        TerminalSessionRetry
    ),
}

/// Mark a production entry point as the owner of one registered publisher ID.
///
/// The boundary check mechanically pairs these markers with discovered
/// pending-slot installation calls and rejects unregistered or dead entries.
macro_rules! metadata_command_publisher {
    ($id:ident) => {
        const _: (
            &'static [crate::metadata_command::MetadataCommandPublisherId],
            crate::metadata_command::MetadataCommandPublisherDescriptor,
        ) = (
            crate::metadata_command::MetadataCommandPublisherId::ALL,
            crate::metadata_command::MetadataCommandPublisherId::$id.descriptor(),
        );
    };
}

pub(crate) use metadata_command_publisher;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MetadataCommandLogIndex(NonZeroU64);

impl MetadataCommandLogIndex {
    pub(crate) fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    pub(crate) fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetadataCommandAcceptance {
    Apply,
    AlreadyApplied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetadataCommandLogEntryKind {
    Applied,
    Abandoned { original_command_checksum: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetadataCommandLogEntryHeader {
    id: MetadataCommandId,
    kind: MetadataCommandLogEntryKind,
    command_kind_name: Option<&'static str>,
}

impl MetadataCommandLogEntryHeader {
    pub(crate) fn id(self) -> MetadataCommandId {
        self.id
    }

    pub(crate) fn kind(self) -> MetadataCommandLogEntryKind {
        self.kind
    }

    pub(crate) fn command_kind_name(self) -> Option<&'static str> {
        self.command_kind_name
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetadataCommandReplicaState {
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) applied_log_index: u64,
    pub(crate) applied_log_hash: u64,
    pub(crate) state_digest: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetadataCommandLogHashRangeEntry {
    pub(crate) log_index: u64,
    pub(crate) previous_log_hash: u64,
    pub(crate) log_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataCommandLogRangeEntry {
    pub(crate) log_index: u64,
    pub(crate) previous_log_hash: u64,
    pub(crate) log_hash: u64,
    pub(crate) pre_state_digest: Option<u64>,
    pub(crate) post_state_digest: Option<u64>,
    pub(crate) kind: MetadataCommandLogRangeEntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MetadataCommandLogRangeEntryKind {
    Applied(Box<MetadataCommandEnvelope>),
    Abandoned { original_command_checksum: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataTransferCommand {
    pub(crate) command: MetadataCommandEnvelope,
    pub(crate) pre_state_digest: u64,
    pub(crate) post_state_digest: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MetadataCommandId {
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    log_index: MetadataCommandLogIndex,
}

impl MetadataCommandId {
    pub(crate) fn new(
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        log_index: MetadataCommandLogIndex,
    ) -> Self {
        Self {
            cluster_epoch,
            pg_id,
            log_index,
        }
    }

    pub(crate) fn cluster_epoch(self) -> ClusterEpoch {
        self.cluster_epoch
    }

    pub(crate) fn pg_id(self) -> PgId {
        self.pg_id
    }

    pub(crate) fn log_index(self) -> MetadataCommandLogIndex {
        self.log_index
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketRecord {
    pub(crate) name: BucketName,
    pub(crate) owner_principal: String,
    pub(crate) owner_canonical_id: CanonicalUserId,
    pub(crate) created_at: u64,
    pub(crate) region: u16,
    pub(crate) state: BucketState,
    pub(crate) versioning: BucketVersioningState,
    pub(crate) object_lock: BucketObjectLockConfig,
    pub(crate) acl_grants: AclGrants,
    pub(crate) public_read: bool,
    pub(crate) public_write: bool,
    pub(crate) public_access_block: Option<PublicAccessBlockConfig>,
    pub(crate) ownership_controls: Option<BucketOwnershipControls>,
    pub(crate) bucket_policy_public: bool,
    pub(crate) bucket_policy_generation: u64,
    pub(crate) bucket_lifecycle_generation: u64,
    pub(crate) bucket_execution_generation: u64,
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) multipart_upload_id_key: MultipartUploadIdKey,
    /// Fixed-size synchronization state advanced before each object-PG multipart commit.
    /// It does not identify or retain completed uploads.
    pub(crate) multipart_completion_barrier_sequence: u64,
    pub(crate) bucket_abac_enabled: bool,
    pub(crate) encryption: BucketEncryptionConfig,
}

impl BucketRecord {
    pub(crate) fn from_create_config(
        config: &CreateBucketConfig<'_>,
        created_at_millis: u64,
        bucket_execution_generation: u64,
    ) -> Result<Self, String> {
        let name = BucketName::try_from(config.name.to_string())
            .map_err(|reason| format!("invalid bucket name in create bucket command: {reason}"))?;
        let multipart_upload_id_key = MultipartUploadIdKey::generate()?;
        Ok(Self {
            name,
            owner_principal: config.owner_principal.to_string(),
            owner_canonical_id: config.owner_canonical_id.clone(),
            created_at: created_at_millis,
            region: 0,
            state: BucketState::Active,
            versioning: config.versioning,
            object_lock: config.object_lock,
            acl_grants: config.acl_grants.clone(),
            public_read: config.public_read,
            public_write: config.public_write,
            public_access_block: None,
            ownership_controls: Some(config.ownership_controls),
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            bucket_lifecycle_generation: 0,
            bucket_execution_generation,
            bucket_incarnation_generation: bucket_execution_generation,
            multipart_upload_id_key,
            multipart_completion_barrier_sequence: 0,
            bucket_abac_enabled: false,
            encryption: BucketEncryptionConfig {
                default_encryption: None,
                sse_c_blocked: true,
            },
        })
    }

    pub(crate) fn matches_create_config(&self, config: &CreateBucketConfig<'_>) -> bool {
        self.name.as_str() == config.name
            && self.owner_principal == config.owner_principal
            && self.owner_canonical_id == *config.owner_canonical_id
            && self.acl_grants == *config.acl_grants
            && self.public_read == config.public_read
            && self.public_write == config.public_write
            && self.versioning == config.versioning
            && self.object_lock == config.object_lock
            && self.ownership_controls == Some(config.ownership_controls)
    }

    pub(crate) fn with_execution_generation(mut self, generation: u64) -> Self {
        self.bucket_execution_generation = generation;
        self
    }

    pub(crate) fn command_metadata_projection(self) -> Self {
        self
    }

    pub(crate) fn command_metadata_eq(&self, other: &Self) -> bool {
        self.clone().command_metadata_projection() == other.clone().command_metadata_projection()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateBucketCommand {
    pub(crate) bucket: BucketRecord,
}

impl CreateBucketCommand {
    pub(crate) fn from_config(
        config: &CreateBucketConfig<'_>,
        created_at_millis: u64,
        bucket_execution_generation: u64,
    ) -> Result<Self, String> {
        Ok(Self {
            bucket: BucketRecord::from_create_config(
                config,
                created_at_millis,
                bucket_execution_generation,
            )?,
        })
    }

    pub(crate) fn matches_create_config(&self, config: &CreateBucketConfig<'_>) -> bool {
        self.bucket.matches_create_config(config)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MetadataCommandPayload {
    CreateBucket(CreateBucketCommand),
    PutBucketVersioning(PutBucketVersioningCommand),
    PutBucketAcl(PutBucketAclCommand),
    PutBucketProperty(PutBucketPropertyCommand),
    PutBucketSubresource(PutBucketSubresourceCommand),
    MarkBucketDeleting(MarkBucketDeletingCommand),
    DeleteFinalizedBucket(DeleteFinalizedBucketCommand),
    ReserveObjectGeneration(ReserveObjectGenerationCommand),
    ReleaseObjectGeneration(ReleaseObjectGenerationCommand),
    ReserveObjectVersion(ReserveObjectVersionCommand),
    CommitDirectPutObject(Box<CommitDirectPutObjectCommand>),
    CommitMultipartObject(Box<CommitMultipartObjectCommand>),
    DeleteObjectVersion(Box<DeleteObjectVersionCommand>),
    InsertDeleteMarker(InsertDeleteMarkerCommand),
    PutObjectMetadata(Box<PutObjectMetadataCommand>),
    CreateStreamUpload(Box<CreateStreamUploadCommand>),
    AppendStreamSegment(Box<AppendStreamSegmentCommand>),
    AbortStreamUpload(Box<AbortStreamUploadCommand>),
    CommitStreamPart(Box<CommitStreamPartCommand>),
    CreateMultipartUpload(Box<CreateMultipartUploadCommand>),
    AbortMultipartUpload(Box<AbortMultipartUploadCommand>),
    DeleteObjectPayloadReclaim(Box<DeleteObjectPayloadReclaimCommand>),
    AdvanceMultipartCompletionBarrier(AdvanceMultipartCompletionBarrierCommand),
}

impl MetadataCommandPayload {
    pub(crate) fn kind_name(&self) -> &'static str {
        metadata_command_payload_kind_name(self.kind_id())
            .expect("every metadata command payload kind must have a diagnostic name")
    }

    fn kind_id(&self) -> u16 {
        match self {
            Self::CreateBucket(_) => METADATA_COMMAND_CREATE_BUCKET,
            Self::PutBucketVersioning(_) => METADATA_COMMAND_PUT_BUCKET_VERSIONING,
            Self::PutBucketAcl(_) => METADATA_COMMAND_PUT_BUCKET_ACL,
            Self::PutBucketProperty(_) => METADATA_COMMAND_PUT_BUCKET_PROPERTY,
            Self::PutBucketSubresource(_) => METADATA_COMMAND_PUT_BUCKET_SUBRESOURCE,
            Self::MarkBucketDeleting(_) => METADATA_COMMAND_MARK_BUCKET_DELETING,
            Self::DeleteFinalizedBucket(_) => METADATA_COMMAND_DELETE_FINALIZED_BUCKET,
            Self::ReserveObjectGeneration(_) => METADATA_COMMAND_RESERVE_OBJECT_GENERATION,
            Self::ReleaseObjectGeneration(_) => METADATA_COMMAND_RELEASE_OBJECT_GENERATION,
            Self::ReserveObjectVersion(_) => METADATA_COMMAND_RESERVE_OBJECT_VERSION,
            Self::CommitDirectPutObject(_) => METADATA_COMMAND_COMMIT_DIRECT_PUT_OBJECT,
            Self::CommitMultipartObject(_) => METADATA_COMMAND_COMMIT_MULTIPART_OBJECT,
            Self::DeleteObjectVersion(_) => METADATA_COMMAND_DELETE_OBJECT_VERSION,
            Self::InsertDeleteMarker(_) => METADATA_COMMAND_INSERT_DELETE_MARKER,
            Self::PutObjectMetadata(_) => METADATA_COMMAND_PUT_OBJECT_METADATA,
            Self::CreateStreamUpload(_) => METADATA_COMMAND_CREATE_STREAM_UPLOAD,
            Self::AppendStreamSegment(_) => METADATA_COMMAND_APPEND_STREAM_SEGMENT,
            Self::AbortStreamUpload(_) => METADATA_COMMAND_ABORT_STREAM_UPLOAD,
            Self::CommitStreamPart(_) => METADATA_COMMAND_COMMIT_STREAM_PART,
            Self::CreateMultipartUpload(_) => METADATA_COMMAND_CREATE_MULTIPART_UPLOAD,
            Self::AbortMultipartUpload(_) => METADATA_COMMAND_ABORT_MULTIPART_UPLOAD,
            Self::DeleteObjectPayloadReclaim(_) => METADATA_COMMAND_DELETE_OBJECT_PAYLOAD_RECLAIM,
            Self::AdvanceMultipartCompletionBarrier(_) => {
                METADATA_COMMAND_ADVANCE_MULTIPART_COMPLETION_BARRIER
            }
        }
    }

    pub(crate) fn bucket_name(&self) -> &BucketName {
        match self {
            Self::CreateBucket(create) => &create.bucket.name,
            Self::PutBucketVersioning(versioning) => versioning.bucket_name(),
            Self::PutBucketAcl(acl) => acl.bucket_name(),
            Self::PutBucketProperty(property) => property.bucket_name(),
            Self::PutBucketSubresource(subresource) => &subresource.name,
            Self::MarkBucketDeleting(mark) => mark.bucket_name(),
            Self::DeleteFinalizedBucket(delete) => &delete.bucket,
            Self::ReserveObjectGeneration(reservation) => &reservation.bucket,
            Self::ReleaseObjectGeneration(release) => &release.bucket,
            Self::ReserveObjectVersion(reservation) => &reservation.bucket,
            Self::CommitDirectPutObject(commit) => &commit.object.bucket,
            Self::CommitMultipartObject(commit) => &commit.object.bucket,
            Self::DeleteObjectVersion(delete) => &delete.bucket,
            Self::InsertDeleteMarker(insert) => &insert.bucket,
            Self::PutObjectMetadata(metadata) => &metadata.object.bucket,
            Self::CreateStreamUpload(create) => &create.session.bucket,
            Self::AppendStreamSegment(append) => &append.bucket,
            Self::AbortStreamUpload(abort) => &abort.bucket,
            Self::CommitStreamPart(commit) => &commit.bucket,
            Self::CreateMultipartUpload(create) => &create.upload.bucket,
            Self::AbortMultipartUpload(abort) => &abort.bucket,
            Self::DeleteObjectPayloadReclaim(reclaim) => &reclaim.bucket,
            Self::AdvanceMultipartCompletionBarrier(advance) => &advance.bucket,
        }
    }

    pub(crate) fn abandoned_recovery_follow_up(&self) -> Option<Self> {
        let release = match self {
            Self::CommitDirectPutObject(commit) => ReleaseObjectGenerationCommand::new(
                commit.object.bucket.clone(),
                commit.object.key.clone(),
                commit.generation_reservation_id.clone(),
            ),
            Self::CreateStreamUpload(create)
                if create.session.target == StreamUploadTarget::PutObject =>
            {
                ReleaseObjectGenerationCommand::new(
                    create.session.bucket.clone(),
                    create.session.key.clone(),
                    create.session.session_id.clone(),
                )
            }
            _ => return None,
        };
        Some(Self::ReleaseObjectGeneration(release))
    }

    pub(crate) fn is_authorized_recovery_derivative_of(&self, source: &Self) -> bool {
        self == source || source.abandoned_recovery_follow_up().as_ref() == Some(self)
    }
}

fn metadata_command_payload_kind_name(kind_id: u16) -> Option<&'static str> {
    match kind_id {
        METADATA_COMMAND_CREATE_BUCKET => Some("CreateBucket"),
        METADATA_COMMAND_PUT_BUCKET_VERSIONING => Some("PutBucketVersioning"),
        METADATA_COMMAND_PUT_BUCKET_ACL => Some("PutBucketAcl"),
        METADATA_COMMAND_PUT_BUCKET_PROPERTY => Some("PutBucketProperty"),
        METADATA_COMMAND_PUT_BUCKET_SUBRESOURCE => Some("PutBucketSubresource"),
        METADATA_COMMAND_MARK_BUCKET_DELETING => Some("MarkBucketDeleting"),
        METADATA_COMMAND_DELETE_FINALIZED_BUCKET => Some("DeleteFinalizedBucket"),
        METADATA_COMMAND_RESERVE_OBJECT_GENERATION => Some("ReserveObjectGeneration"),
        METADATA_COMMAND_RELEASE_OBJECT_GENERATION => Some("ReleaseObjectGeneration"),
        METADATA_COMMAND_RESERVE_OBJECT_VERSION => Some("ReserveObjectVersion"),
        METADATA_COMMAND_COMMIT_DIRECT_PUT_OBJECT => Some("CommitDirectPutObject"),
        METADATA_COMMAND_COMMIT_MULTIPART_OBJECT => Some("CommitMultipartObject"),
        METADATA_COMMAND_DELETE_OBJECT_VERSION => Some("DeleteObjectVersion"),
        METADATA_COMMAND_INSERT_DELETE_MARKER => Some("InsertDeleteMarker"),
        METADATA_COMMAND_PUT_OBJECT_METADATA => Some("PutObjectMetadata"),
        METADATA_COMMAND_CREATE_STREAM_UPLOAD => Some("CreateStreamUpload"),
        METADATA_COMMAND_APPEND_STREAM_SEGMENT => Some("AppendStreamSegment"),
        METADATA_COMMAND_ABORT_STREAM_UPLOAD => Some("AbortStreamUpload"),
        METADATA_COMMAND_COMMIT_STREAM_PART => Some("CommitStreamPart"),
        METADATA_COMMAND_CREATE_MULTIPART_UPLOAD => Some("CreateMultipartUpload"),
        METADATA_COMMAND_ABORT_MULTIPART_UPLOAD => Some("AbortMultipartUpload"),
        METADATA_COMMAND_DELETE_OBJECT_PAYLOAD_RECLAIM => Some("DeleteObjectPayloadReclaim"),
        METADATA_COMMAND_ADVANCE_MULTIPART_COMPLETION_BARRIER => {
            Some("AdvanceMultipartCompletionBarrier")
        }
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MarkBucketDeletingCommand {
    pub(crate) bucket: BucketRecord,
}

impl MarkBucketDeletingCommand {
    pub(crate) fn from_bucket(mut bucket: BucketRecord) -> Self {
        bucket.state = BucketState::Deleting;
        bucket = bucket.command_metadata_projection();
        Self { bucket }
    }

    pub(crate) fn bucket_name(&self) -> &BucketName {
        &self.bucket.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeleteFinalizedBucketCommand {
    pub(crate) bucket: BucketName,
    pub(crate) bucket_execution_generation: u64,
    pub(crate) bucket_incarnation_generation: u64,
}

impl DeleteFinalizedBucketCommand {
    pub(crate) fn new(
        bucket: BucketName,
        bucket_execution_generation: u64,
        bucket_incarnation_generation: u64,
    ) -> Self {
        Self {
            bucket,
            bucket_execution_generation,
            bucket_incarnation_generation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutBucketVersioningCommand {
    pub(crate) bucket: BucketRecord,
}

impl PutBucketVersioningCommand {
    pub(crate) fn from_bucket(mut bucket: BucketRecord, state: BucketVersioningState) -> Self {
        bucket.versioning = state;
        bucket = bucket.command_metadata_projection();
        Self { bucket }
    }

    pub(crate) fn bucket_name(&self) -> &BucketName {
        &self.bucket.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutBucketAclCommand {
    pub(crate) bucket: BucketRecord,
}

impl PutBucketAclCommand {
    pub(crate) fn from_bucket(
        mut bucket: BucketRecord,
        acl_grants: AclGrants,
        summary: BucketAclSummary,
    ) -> Self {
        bucket.acl_grants = acl_grants;
        bucket.public_read = summary.public_read;
        bucket.public_write = summary.public_write;
        bucket = bucket.command_metadata_projection();
        Self { bucket }
    }

    pub(crate) fn bucket_name(&self) -> &BucketName {
        &self.bucket.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutBucketPropertyCommand {
    pub(crate) bucket: BucketRecord,
    pub(crate) effect: BucketPropertyEffect,
}

impl PutBucketPropertyCommand {
    pub(crate) fn from_bucket_and_mutation(
        mut bucket: BucketRecord,
        mutation: BucketPropertyMutation,
    ) -> Self {
        let effect = mutation.effect();
        mutation.apply_to_bucket(&mut bucket);
        bucket = bucket.command_metadata_projection();
        Self { bucket, effect }
    }

    pub(crate) fn bucket_name(&self) -> &BucketName {
        &self.bucket.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BucketPropertyMutation {
    ObjectLock(BucketObjectLockConfig),
    Encryption(BucketEncryptionConfig),
    PublicAccessBlock(Option<PublicAccessBlockConfig>),
    OwnershipControls(Option<BucketOwnershipControls>),
    AbacEnabled(bool),
}

impl BucketPropertyMutation {
    pub(crate) fn effect(&self) -> BucketPropertyEffect {
        match self {
            Self::ObjectLock(_) => BucketPropertyEffect::ObjectLock,
            Self::Encryption(_) => BucketPropertyEffect::Encryption,
            Self::PublicAccessBlock(_) => BucketPropertyEffect::PublicAccessBlock,
            Self::OwnershipControls(_) => BucketPropertyEffect::OwnershipControls,
            Self::AbacEnabled(_) => BucketPropertyEffect::AbacEnabled,
        }
    }

    pub(crate) fn apply_to_bucket(self, bucket: &mut BucketRecord) {
        match self {
            Self::ObjectLock(config) => {
                bucket.object_lock = config;
            }
            Self::Encryption(config) => {
                bucket.encryption = config;
            }
            Self::PublicAccessBlock(config) => {
                bucket.public_access_block = config;
            }
            Self::OwnershipControls(config) => {
                bucket.ownership_controls = config;
            }
            Self::AbacEnabled(enabled) => {
                bucket.bucket_abac_enabled = enabled;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum BucketPropertyEffect {
    ObjectLock = 0,
    Encryption = 1,
    PublicAccessBlock = 2,
    OwnershipControls = 3,
    AbacEnabled = 4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutBucketSubresourceCommand {
    pub(crate) name: BucketName,
    pub(crate) mutation: BucketSubresourceMutation,
    pub(crate) bucket_execution_generation: u64,
}

impl PutBucketSubresourceCommand {
    pub(crate) fn new(
        name: BucketName,
        mutation: BucketSubresourceMutation,
        bucket_execution_generation: u64,
    ) -> Self {
        Self {
            name,
            mutation,
            bucket_execution_generation,
        }
    }

    pub(crate) fn matches_mutation(
        &self,
        bucket: &BucketName,
        mutation: &BucketSubresourceMutation,
    ) -> bool {
        self.name == *bucket && self.mutation == *mutation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BucketSubresourceMutation {
    Put {
        kind: BucketSubresourceKind,
        body: String,
        aux: BucketSubresourceAux,
    },
    Delete {
        kind: BucketSubresourceKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReserveObjectGenerationCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) reservation_id: SessionId,
    pub(crate) generation_id: GenerationId,
    pub(crate) created_at_millis: u64,
}

impl ReserveObjectGenerationCommand {
    pub(crate) fn new(
        bucket: BucketName,
        key: ObjectKey,
        reservation_id: SessionId,
        generation_id: GenerationId,
        created_at_millis: u64,
    ) -> Self {
        Self {
            bucket,
            key,
            reservation_id,
            generation_id,
            created_at_millis,
        }
    }

    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> bool {
        self.bucket == *bucket && self.key == *key && self.reservation_id == *reservation_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReleaseObjectGenerationCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) reservation_id: SessionId,
}

impl ReleaseObjectGenerationCommand {
    pub(crate) fn new(bucket: BucketName, key: ObjectKey, reservation_id: SessionId) -> Self {
        Self {
            bucket,
            key,
            reservation_id,
        }
    }

    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> bool {
        self.bucket == *bucket && self.key == *key && self.reservation_id == *reservation_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReserveObjectVersionCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) version_id: VersionId,
}

impl ReserveObjectVersionCommand {
    pub(crate) fn new(bucket: BucketName, key: ObjectKey, version_id: VersionId) -> Self {
        Self {
            bucket,
            key,
            version_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitDirectPutObjectCommand {
    pub(crate) object: PutLiveObjectReq,
    pub(crate) segments: Vec<ObjectSegmentRecord>,
    pub(crate) generation_reservation_id: SessionId,
    pub(crate) write_sequence: u64,
    pub(crate) last_modified_millis: u64,
    pub(crate) stale_payload: Option<ObjectPayloadReclaimCommand>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

impl CommitDirectPutObjectCommand {
    pub(crate) fn matches_stream_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> bool {
        self.object.bucket == *bucket
            && self.object.key == *key
            && self.generation_reservation_id == *session_id
    }

    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        generation_id: GenerationId,
    ) -> bool {
        self.object.bucket == *bucket
            && self.object.key == *key
            && self.generation_reservation_id == *reservation_id
            && self.object.generation_id == generation_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitMultipartObjectCommand {
    pub(crate) upload_id: UploadId,
    pub(crate) completion_fingerprint: MultipartCompletionFingerprint,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
    pub(crate) object: PutLiveObjectReq,
    pub(crate) parts: Vec<ObjectPartRecord>,
    pub(crate) selected_streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub(crate) omitted_parts: Vec<MultipartPartRecord>,
    pub(crate) omitted_streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub(crate) stream_uploads: Vec<TerminalStreamCleanupRecord>,
    pub(crate) stream_upload_segments: Vec<StreamUploadSegmentRecord>,
    pub(crate) write_sequence: u64,
    pub(crate) last_modified_millis: u64,
    pub(crate) stale_payload: Option<ObjectPayloadReclaimCommand>,
}

impl CommitMultipartObjectCommand {
    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        generation_id: GenerationId,
        completion_fingerprint: MultipartCompletionFingerprint,
        parts: &[MultipartPartRecord],
    ) -> bool {
        self.object.bucket == *bucket
            && self.object.key == *key
            && self.upload_id == *upload_id
            && self.object.generation_id == generation_id
            && self.completion_fingerprint == completion_fingerprint
            && self
                .parts
                .iter()
                .zip(parts.iter())
                .all(|(stored, requested)| {
                    stored.part_number == requested.part_number
                        && stored.size == requested.size
                        && stored.etag == requested.etag
                        && stored.etag_kind == requested.etag_kind
                        && stored.part_okh == requested.part_okh
                        && stored.part_vid == requested.part_vid
                        && stored.ec_k == requested.ec_k
                        && stored.ec_m == requested.ec_m
                        && stored.checksum == requested.checksum
                })
            && self.parts.len() == parts.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ObjectPayloadReclaimCommand {
    Segments(ObjectSegmentsReclaimRecord),
    Multipart(MultipartReclaimRecord),
}

impl ObjectPayloadReclaimCommand {
    pub(crate) fn kind(&self) -> ObjectPayloadReclaimKind {
        match self {
            Self::Segments(_) => ObjectPayloadReclaimKind::ObjectSegments,
            Self::Multipart(_) => ObjectPayloadReclaimKind::Multipart,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectPayloadReclaimClaimProof {
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) reclaim_kind: ObjectPayloadReclaimKind,
    pub(crate) claim_id: String,
    pub(crate) owner_token: String,
    pub(crate) cluster_epoch: ClusterEpoch,
}

impl From<&ObjectPayloadReclaimClaimRecord> for ObjectPayloadReclaimClaimProof {
    fn from(claim: &ObjectPayloadReclaimClaimRecord) -> Self {
        Self {
            bucket_incarnation_generation: claim.bucket_incarnation_generation,
            reclaim_kind: claim.reclaim_kind,
            claim_id: claim.claim_id.clone(),
            owner_token: claim.owner_token.clone(),
            cluster_epoch: claim.cluster_epoch,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeleteObjectPayloadReclaimCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) generation_id: GenerationId,
    pub(crate) payload: ObjectPayloadReclaimCommand,
    pub(crate) reclaim_claim: ObjectPayloadReclaimClaimProof,
}

impl DeleteObjectPayloadReclaimCommand {
    pub(crate) fn new(
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
        payload: ObjectPayloadReclaimCommand,
        reclaim_claim: ObjectPayloadReclaimClaimProof,
    ) -> Self {
        Self {
            bucket,
            key,
            generation_id,
            payload,
            reclaim_claim,
        }
    }

    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.bucket == *bucket && self.key == *key && self.generation_id == generation_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeleteObjectVersionTarget {
    DeleteMarker {
        write_sequence: u64,
    },
    Live {
        generation_id: GenerationId,
        layout: ObjectLayout,
        payload: ObjectPayloadReclaimCommand,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeleteObjectVersionCommand {
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) version_id: VersionId,
    pub(crate) target: DeleteObjectVersionTarget,
}

impl DeleteObjectVersionCommand {
    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> bool {
        self.bucket == *bucket && self.key == *key && self.version_id == version_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InsertDeleteMarkerCommand {
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) version_id: VersionId,
    pub(crate) owner: OwnerIdentity,
    pub(crate) write_sequence: u64,
    pub(crate) last_modified_millis: u64,
    pub(crate) stale_payload: Option<ObjectPayloadReclaimCommand>,
}

impl InsertDeleteMarkerCommand {
    pub(crate) fn matches_request(&self, bucket: &BucketName, key: &ObjectKey) -> bool {
        self.bucket == *bucket && self.key == *key
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PutObjectMetadataMutation {
    PutTags(String),
    DeleteTags,
    PutRetention(ObjectRetention),
    PutLegalHold(StoredLegalHoldStatus),
    PutAcl {
        acl_grants: AclGrants,
        public_read: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutObjectMetadataCommand {
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
    pub(crate) object: LiveObjectRecord,
}

impl PutObjectMetadataCommand {
    pub(crate) fn from_live_object_and_mutation(
        mut object: LiveObjectRecord,
        mutation: PutObjectMetadataMutation,
        bucket_write_reservation: BucketWriteReservationProof,
    ) -> Self {
        match mutation {
            PutObjectMetadataMutation::PutTags(tags) => {
                object.tags = Some(SerializedTagSet::new(tags));
            }
            PutObjectMetadataMutation::DeleteTags => {
                object.tags = None;
            }
            PutObjectMetadataMutation::PutRetention(retention) => {
                object.object_lock.retention = Some(retention);
            }
            PutObjectMetadataMutation::PutLegalHold(legal_hold) => {
                object.object_lock.legal_hold = legal_hold;
            }
            PutObjectMetadataMutation::PutAcl {
                acl_grants,
                public_read,
            } => {
                object.acl_grants = acl_grants;
                object.public_read = public_read;
            }
        }
        Self {
            bucket_write_reservation,
            object,
        }
    }

    pub(crate) fn matches_object(&self, object: &LiveObjectRecord) -> bool {
        self.object == *object
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketWriteReservationProof {
    pub(crate) bucket: BucketName,
    pub(crate) reservation_id: String,
    pub(crate) owner_token: String,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) bucket_execution_generation: u64,
    pub(crate) bucket_incarnation_generation: u64,
    pub(crate) operation_kind: String,
    pub(crate) created_at: u64,
    pub(crate) lease_deadline: u64,
    pub(crate) target_context: Option<String>,
}

pub(crate) const COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND: &str =
    "complete-multipart-upload";
pub(crate) const ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND: &str =
    "abort-multipart-upload";
pub(crate) const CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND: &str =
    "create-multipart-upload";
pub(crate) const PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND: &str =
    "put-object-stream-create";
pub(crate) const UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND: &str =
    "upload-part-stream-create";
pub(crate) const UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND: &str =
    "upload-part-stream-finalize";
pub(crate) const PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND: &str = "put-object-metadata";
pub(crate) const DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND: &str = "delete-current-object";
pub(crate) const DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND: &str = "delete-object-version";
pub(crate) const INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND: &str = "insert-delete-marker";

pub(crate) fn is_stream_create_bucket_write_operation_kind(operation_kind: &str) -> bool {
    matches!(
        operation_kind,
        PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            | UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
    )
}

impl From<&BucketWriteReservationRecord> for BucketWriteReservationProof {
    fn from(record: &BucketWriteReservationRecord) -> Self {
        Self {
            bucket: record.bucket.clone(),
            reservation_id: record.reservation_id.clone(),
            owner_token: record.owner_token.clone(),
            cluster_epoch: record.cluster_epoch,
            bucket_execution_generation: record.bucket_execution_generation,
            bucket_incarnation_generation: record.bucket_incarnation_generation,
            operation_kind: record.operation_kind.clone(),
            created_at: record.created_at,
            lease_deadline: record.lease_deadline,
            target_context: record.target_context.clone(),
        }
    }
}

impl BucketWriteReservationProof {
    pub(crate) fn has_same_stable_identity(&self, other: &Self) -> bool {
        // lease_deadline is renewable state. Every other field identifies the
        // reservation and its authorized mutation target.
        self.bucket == other.bucket
            && self.reservation_id == other.reservation_id
            && self.owner_token == other.owner_token
            && self.cluster_epoch == other.cluster_epoch
            && self.bucket_execution_generation == other.bucket_execution_generation
            && self.bucket_incarnation_generation == other.bucket_incarnation_generation
            && self.operation_kind == other.operation_kind
            && self.created_at == other.created_at
            && self.target_context == other.target_context
    }

    pub(crate) fn matches_record(&self, record: &BucketWriteReservationRecord) -> bool {
        // The lease deadline is mutable heartbeat state, not stable proof
        // identity. Callers that need freshness validate the deadline
        // separately after matching the stable reservation fields.
        self.bucket == record.bucket
            && self.reservation_id == record.reservation_id
            && self.owner_token == record.owner_token
            && self.cluster_epoch == record.cluster_epoch
            && self.bucket_execution_generation == record.bucket_execution_generation
            && self.bucket_incarnation_generation == record.bucket_incarnation_generation
            && self.operation_kind == record.operation_kind
            && self.created_at == record.created_at
            && self.target_context == record.target_context
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateStreamUploadCommand {
    pub(crate) session: StreamUploadCommandRecord,
    pub(crate) initial_next_segment_vid: GenerationId,
    pub(crate) cleanup_after: Option<u64>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

impl CreateStreamUploadCommand {
    #[cfg(test)]
    pub(crate) fn from_request_with_bucket_write_reservation(
        request: CreateStreamUploadReq,
        created_at_millis: u64,
        bucket_write_reservation: BucketWriteReservationProof,
    ) -> Self {
        Self::from_request_with_bucket_write_reservation_and_cleanup_deadline(
            request,
            created_at_millis,
            None,
            bucket_write_reservation,
        )
    }

    pub(crate) fn from_request_with_bucket_write_reservation_and_cleanup_deadline(
        request: CreateStreamUploadReq,
        created_at_millis: u64,
        cleanup_after: Option<u64>,
        bucket_write_reservation: BucketWriteReservationProof,
    ) -> Self {
        Self {
            session: StreamUploadCommandRecord {
                session_id: request.session_id,
                bucket: request.bucket,
                key: request.key,
                target: request.target,
                state: StreamUploadState::InProgress,
                created_at: created_at_millis,
                encryption: request.encryption,
            },
            initial_next_segment_vid: GenerationId::MIN,
            cleanup_after,
            bucket_write_reservation,
        }
    }
}

/// Authorized by the existing stream session; PutObject sessions carry their bucket write proof on the session row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendStreamSegmentCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) segment: StreamUploadSegmentRecord,
}

/// Authorized by the existing stream session; the optional proof releases a PutObject stream-create reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbortStreamUploadCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) session_id: SessionId,
    pub(crate) staged_segments: Vec<StreamUploadSegmentRecord>,
    pub(crate) stream_create_bucket_write_reservation: Option<BucketWriteReservationProof>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitStreamPartCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) session_id: SessionId,
    pub(crate) upload: MultipartUploadRecord,
    pub(crate) part: MultipartPartRecord,
    pub(crate) segments: Vec<MultipartPartSegmentRecord>,
    pub(crate) existing_part: Option<MultipartPartRecord>,
    pub(crate) displaced_segments: Vec<MultipartPartSegmentRecord>,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

impl CommitStreamPartCommand {
    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
    ) -> bool {
        self.bucket == *bucket
            && self.key == *key
            && self.upload.upload_id == *upload_id
            && self.session_id == *session_id
            && self.part.part_number == part_number
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateMultipartUploadCommand {
    pub(crate) upload: MultipartUploadRecord,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

impl CreateMultipartUploadCommand {
    pub(crate) fn from_request_with_bucket_write_reservation(
        request: CreateMultipartUploadReq,
        object_generation_id: GenerationId,
        initiated_object_identity: Option<crate::MultipartObjectIdentity>,
        initiated_at_millis: u64,
        bucket_write_reservation: BucketWriteReservationProof,
    ) -> Self {
        Self {
            upload: MultipartUploadRecord {
                upload_id: request.upload_id,
                bucket: request.bucket,
                key: request.key,
                initiated_at: initiated_at_millis,
                state: crate::UploadState::InProgress,
                tags: request.tags,
                metadata_blob: request.metadata_blob,
                system_metadata_blob: request.system_metadata_blob,
                initiator: request.initiator,
                owner: request.owner,
                acl_grants: request.acl_grants,
                public_read: request.public_read,
                object_generation_id,
                initiated_object_identity,
                object_lock: request.object_lock,
                checksum: request.checksum,
                encryption: request.encryption,
            },
            bucket_write_reservation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbortMultipartUploadCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) upload_id: UploadId,
    pub(crate) cleanup: AbortMultipartUploadCleanup,
    pub(crate) bucket_write_reservation: BucketWriteReservationProof,
}

/// Bucket-PG barrier authorized by CompleteMultipartUpload's bucket write reservation proof.
///
/// The monotonic sequence makes replicated application idempotent without retaining any
/// per-upload terminal state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdvanceMultipartCompletionBarrierCommand {
    pub(crate) bucket: BucketName,
    pub(crate) barrier_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataCommandEnvelope {
    id: MetadataCommandId,
    payload: MetadataCommandPayload,
    checksum_crc64: u64,
}

impl MetadataCommandEnvelope {
    pub(crate) fn new(id: MetadataCommandId, payload: MetadataCommandPayload) -> Self {
        let checksum_crc64 = checksum::crc64::checksum(&canonical_command_bytes(id, &payload));
        Self {
            id,
            payload,
            checksum_crc64,
        }
    }

    pub(crate) fn id(&self) -> MetadataCommandId {
        self.id
    }

    pub(crate) fn payload(&self) -> &MetadataCommandPayload {
        &self.payload
    }

    pub(crate) fn bucket_name(&self) -> &BucketName {
        self.payload.bucket_name()
    }

    pub(crate) fn checksum_crc64(&self) -> u64 {
        self.checksum_crc64
    }

    pub(crate) fn command_bytes(&self) -> Vec<u8> {
        canonical_command_bytes(self.id, &self.payload)
    }

    pub(crate) fn abandoned_log_bytes(&self) -> Vec<u8> {
        abandoned_command_log_bytes(self.id, self.checksum_crc64)
    }

    pub(crate) fn abandoned_log_checksum_crc64(&self) -> u64 {
        checksum::crc64::checksum(&self.abandoned_log_bytes())
    }

    #[cfg(test)]
    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = self.command_bytes();
        put_u64(&mut out, self.checksum_crc64);
        out
    }

    pub(crate) fn verify_checksum(&self) -> bool {
        checksum::crc64::checksum(&self.command_bytes()) == self.checksum_crc64
    }
}

pub(crate) fn metadata_command_log_hash(
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    log_index: MetadataCommandLogIndex,
    previous_log_hash: u64,
    command_checksum: u64,
) -> u64 {
    let mut out = Vec::new();
    put_bytes(&mut out, b"ARGMIN-METADATA-COMMAND-LOG-V1");
    put_u64(&mut out, cluster_epoch.get());
    put_u32(&mut out, pg_id.get());
    put_u64(&mut out, log_index.get());
    put_u64(&mut out, previous_log_hash);
    put_u64(&mut out, command_checksum);
    checksum::crc64::checksum(&out)
}

pub(crate) fn abandoned_command_log_bytes(id: MetadataCommandId, command_checksum: u64) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, ABANDONED_METADATA_COMMAND_MAGIC);
    put_u16(&mut out, ABANDONED_METADATA_COMMAND_ENCODING_VERSION);
    put_u64(&mut out, id.cluster_epoch().get());
    put_u32(&mut out, id.pg_id().get());
    put_u64(&mut out, id.log_index().get());
    put_u64(&mut out, command_checksum);
    out
}

pub(crate) fn decode_metadata_command_log_entry_header(
    bytes: &[u8],
) -> Result<MetadataCommandLogEntryHeader, String> {
    let mut decoder = MetadataCommandLogEntryDecoder::new(bytes);
    let magic = decoder.read_bytes()?;
    if magic == METADATA_COMMAND_MAGIC {
        let version = decoder.read_u16()?;
        if version != METADATA_COMMAND_ENCODING_VERSION {
            return Err(format!(
                "unsupported metadata command encoding version {version}"
            ));
        }
        let id = decoder.read_command_id()?;
        let payload_kind = decoder.read_u16()?;
        decoder.skip_metadata_command_payload(payload_kind)?;
        decoder.finish()?;
        return Ok(MetadataCommandLogEntryHeader {
            id,
            kind: MetadataCommandLogEntryKind::Applied,
            command_kind_name: metadata_command_payload_kind_name(payload_kind),
        });
    }
    if magic == ABANDONED_METADATA_COMMAND_MAGIC {
        let version = decoder.read_u16()?;
        if version != ABANDONED_METADATA_COMMAND_ENCODING_VERSION {
            return Err(format!(
                "unsupported abandoned metadata command encoding version {version}"
            ));
        }
        let id = decoder.read_command_id()?;
        let original_command_checksum = decoder.read_u64()?;
        decoder.finish()?;
        return Ok(MetadataCommandLogEntryHeader {
            id,
            kind: MetadataCommandLogEntryKind::Abandoned {
                original_command_checksum,
            },
            command_kind_name: None,
        });
    }
    Err("unknown metadata command log entry magic".to_string())
}

pub(crate) fn decode_metadata_command_envelope(
    bytes: &[u8],
) -> Result<MetadataCommandEnvelope, String> {
    let mut decoder = MetadataCommandLogEntryDecoder::new(bytes);
    let magic = decoder.read_bytes()?;
    if magic != METADATA_COMMAND_MAGIC {
        return Err(
            "pending metadata command slot does not contain an applied command".to_string(),
        );
    }
    let version = decoder.read_u16()?;
    if version != METADATA_COMMAND_ENCODING_VERSION {
        return Err(format!(
            "unsupported metadata command encoding version {version}"
        ));
    }
    let id = decoder.read_command_id()?;
    let payload_kind = decoder.read_u16()?;
    let payload = decoder.read_metadata_command_payload(payload_kind)?;
    decoder.finish()?;
    let envelope = MetadataCommandEnvelope::new(id, payload);
    if envelope.command_bytes() != bytes {
        return Err("decoded metadata command did not round-trip canonical bytes".to_string());
    }
    Ok(envelope)
}

fn canonical_command_bytes(id: MetadataCommandId, payload: &MetadataCommandPayload) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, METADATA_COMMAND_MAGIC);
    put_u16(&mut out, METADATA_COMMAND_ENCODING_VERSION);
    put_u64(&mut out, id.cluster_epoch().get());
    put_u32(&mut out, id.pg_id().get());
    put_u64(&mut out, id.log_index().get());
    put_u16(&mut out, payload.kind_id());
    match payload {
        MetadataCommandPayload::CreateBucket(command) => encode_create_bucket(&mut out, command),
        MetadataCommandPayload::PutBucketVersioning(command) => {
            encode_put_bucket_versioning(&mut out, command);
        }
        MetadataCommandPayload::PutBucketAcl(command) => {
            encode_put_bucket_acl(&mut out, command);
        }
        MetadataCommandPayload::PutBucketProperty(command) => {
            encode_put_bucket_property(&mut out, command);
        }
        MetadataCommandPayload::PutBucketSubresource(command) => {
            encode_put_bucket_subresource(&mut out, command);
        }
        MetadataCommandPayload::MarkBucketDeleting(command) => {
            encode_mark_bucket_deleting(&mut out, command);
        }
        MetadataCommandPayload::DeleteFinalizedBucket(command) => {
            encode_delete_finalized_bucket(&mut out, command);
        }
        MetadataCommandPayload::ReserveObjectGeneration(command) => {
            encode_reserve_object_generation(&mut out, command);
        }
        MetadataCommandPayload::ReleaseObjectGeneration(command) => {
            encode_release_object_generation(&mut out, command);
        }
        MetadataCommandPayload::ReserveObjectVersion(command) => {
            encode_reserve_object_version(&mut out, command);
        }
        MetadataCommandPayload::CommitDirectPutObject(command) => {
            encode_commit_direct_put_object(&mut out, command);
        }
        MetadataCommandPayload::CommitMultipartObject(command) => {
            encode_commit_multipart_object(&mut out, command);
        }
        MetadataCommandPayload::DeleteObjectVersion(command) => {
            encode_delete_object_version(&mut out, command);
        }
        MetadataCommandPayload::InsertDeleteMarker(command) => {
            encode_insert_delete_marker(&mut out, command);
        }
        MetadataCommandPayload::PutObjectMetadata(command) => {
            encode_put_object_metadata(&mut out, command);
        }
        MetadataCommandPayload::CreateStreamUpload(command) => {
            encode_create_stream_upload(&mut out, command);
        }
        MetadataCommandPayload::AppendStreamSegment(command) => {
            encode_append_stream_segment(&mut out, command);
        }
        MetadataCommandPayload::AbortStreamUpload(command) => {
            encode_abort_stream_upload(&mut out, command);
        }
        MetadataCommandPayload::CommitStreamPart(command) => {
            encode_commit_stream_part(&mut out, command);
        }
        MetadataCommandPayload::CreateMultipartUpload(command) => {
            encode_create_multipart_upload(&mut out, command);
        }
        MetadataCommandPayload::AbortMultipartUpload(command) => {
            encode_abort_multipart_upload(&mut out, command);
        }
        MetadataCommandPayload::DeleteObjectPayloadReclaim(command) => {
            encode_delete_object_payload_reclaim(&mut out, command);
        }
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(command) => {
            encode_advance_multipart_completion_barrier(&mut out, command);
        }
    }
    out
}

struct MetadataCommandLogEntryDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> MetadataCommandLogEntryDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| "metadata command log entry offset overflowed".to_string())?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| "truncated metadata command log entry".to_string())?;
        self.offset = end;
        Ok(slice)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], String> {
        let len = self.read_u32()? as usize;
        self.read_exact(len)
    }

    fn read_u16(&mut self) -> Result<u16, String> {
        let bytes: [u8; 2] = self
            .read_exact(2)?
            .try_into()
            .expect("read_exact returned two bytes");
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u8(&mut self) -> Result<u8, String> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, String> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .expect("read_exact returned four bytes");
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, String> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .expect("read_exact returned eight bytes");
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_command_id(&mut self) -> Result<MetadataCommandId, String> {
        let cluster_epoch = ClusterEpoch::new(self.read_u64()?)
            .ok_or_else(|| "metadata command log entry stores zero cluster epoch".to_string())?;
        let pg_id = PgId::new(self.read_u32()?);
        let log_index = MetadataCommandLogIndex::new(self.read_u64()?)
            .ok_or_else(|| "metadata command log entry stores zero log index".to_string())?;
        Ok(MetadataCommandId::new(cluster_epoch, pg_id, log_index))
    }

    fn skip_metadata_command_payload(&mut self, kind_id: u16) -> Result<(), String> {
        match kind_id {
            METADATA_COMMAND_CREATE_BUCKET
            | METADATA_COMMAND_PUT_BUCKET_VERSIONING
            | METADATA_COMMAND_PUT_BUCKET_ACL
            | METADATA_COMMAND_MARK_BUCKET_DELETING => self.skip_bucket_record(),
            METADATA_COMMAND_DELETE_FINALIZED_BUCKET => {
                self.skip_str()?;
                self.read_u64()?;
                self.read_u64()?;
                Ok(())
            }
            METADATA_COMMAND_PUT_BUCKET_PROPERTY => {
                self.skip_bucket_record()?;
                self.read_valid_u8("bucket property effect", 0..=4)
            }
            METADATA_COMMAND_PUT_BUCKET_SUBRESOURCE => {
                self.skip_str()?;
                self.skip_bucket_subresource_mutation()?;
                self.read_u64()?;
                Ok(())
            }
            METADATA_COMMAND_RESERVE_OBJECT_GENERATION => {
                self.skip_str()?;
                self.skip_str()?;
                self.skip_str()?;
                self.read_nonzero_u64("reserved object generation")?;
                self.read_u64()?;
                Ok(())
            }
            METADATA_COMMAND_RELEASE_OBJECT_GENERATION => {
                self.skip_str()?;
                self.skip_str()?;
                self.skip_str()
            }
            METADATA_COMMAND_RESERVE_OBJECT_VERSION => {
                self.skip_str()?;
                self.skip_str()?;
                self.read_u64()?;
                Ok(())
            }
            METADATA_COMMAND_COMMIT_DIRECT_PUT_OBJECT => {
                self.skip_put_live_object()?;
                self.skip_repeated(Self::skip_object_segment)?;
                self.skip_str()?;
                self.read_u64()?;
                self.read_u64()?;
                self.skip_optional_stale_payload()?;
                self.skip_required_bucket_write_reservation_proof()
            }
            METADATA_COMMAND_COMMIT_MULTIPART_OBJECT => {
                self.skip_str()?;
                self.read_bytes()?;
                self.skip_put_live_object()?;
                self.skip_repeated(Self::skip_object_part)?;
                self.skip_repeated(Self::skip_multipart_part_segment)?;
                self.skip_repeated(Self::skip_multipart_part)?;
                self.skip_repeated(Self::skip_multipart_part_segment)?;
                self.skip_repeated(Self::skip_terminal_stream_cleanup)?;
                self.skip_repeated(Self::skip_stream_upload_segment)?;
                self.read_u64()?;
                self.read_u64()?;
                self.skip_optional_stale_payload()?;
                self.skip_required_bucket_write_reservation_proof()
            }
            METADATA_COMMAND_DELETE_OBJECT_VERSION => {
                self.skip_str()?;
                self.skip_str()?;
                self.read_u64()?;
                match self.read_u8()? {
                    1 => self.read_u64().map(|_| ()),
                    2 => {
                        self.read_nonzero_u64("deleted object generation")?;
                        self.skip_object_layout()?;
                        self.skip_live_payload_reclaim()
                    }
                    tag => Err(format!("invalid delete object target tag {tag}")),
                }?;
                self.skip_required_bucket_write_reservation_proof()
            }
            METADATA_COMMAND_INSERT_DELETE_MARKER => {
                self.skip_str()?;
                self.skip_str()?;
                self.read_u64()?;
                self.skip_owner_identity()?;
                self.read_u64()?;
                self.read_u64()?;
                self.skip_optional_stale_payload()?;
                self.skip_required_bucket_write_reservation_proof()
            }
            METADATA_COMMAND_PUT_OBJECT_METADATA => {
                self.skip_live_object_record()?;
                self.skip_required_bucket_write_reservation_proof()
            }
            METADATA_COMMAND_CREATE_STREAM_UPLOAD => self.skip_create_stream_upload(),
            METADATA_COMMAND_APPEND_STREAM_SEGMENT => {
                self.skip_str()?;
                self.skip_str()?;
                self.skip_stream_upload_segment()
            }
            METADATA_COMMAND_ABORT_STREAM_UPLOAD => {
                self.skip_str()?;
                self.skip_str()?;
                self.skip_str()?;
                self.skip_repeated(Self::skip_stream_upload_segment)?;
                self.skip_optional(Self::skip_required_bucket_write_reservation_proof)
            }
            METADATA_COMMAND_COMMIT_STREAM_PART => {
                self.skip_str()?;
                self.skip_str()?;
                self.skip_str()?;
                self.skip_multipart_upload()?;
                self.skip_multipart_part()?;
                self.skip_repeated(Self::skip_multipart_part_segment)?;
                self.skip_optional_multipart_part()?;
                self.skip_repeated(Self::skip_multipart_part_segment)?;
                self.skip_required_bucket_write_reservation_proof()
            }
            METADATA_COMMAND_CREATE_MULTIPART_UPLOAD => {
                self.skip_multipart_upload()?;
                self.skip_required_bucket_write_reservation_proof()
            }
            METADATA_COMMAND_ABORT_MULTIPART_UPLOAD => {
                self.skip_str()?;
                self.skip_str()?;
                self.skip_str()?;
                self.skip_abort_multipart_upload_cleanup()?;
                self.skip_required_bucket_write_reservation_proof()
            }
            METADATA_COMMAND_DELETE_OBJECT_PAYLOAD_RECLAIM => {
                self.skip_str()?;
                self.skip_str()?;
                self.read_nonzero_u64("payload reclaim generation")?;
                self.skip_object_payload_reclaim()?;
                self.skip_object_payload_reclaim_claim_proof()
            }
            METADATA_COMMAND_ADVANCE_MULTIPART_COMPLETION_BARRIER => {
                self.skip_str()?;
                self.read_u64()?;
                Ok(())
            }
            _ => Err(format!("unknown metadata command payload kind {kind_id}")),
        }
    }

    fn read_metadata_command_payload(
        &mut self,
        kind_id: u16,
    ) -> Result<MetadataCommandPayload, String> {
        match kind_id {
            METADATA_COMMAND_CREATE_BUCKET => {
                Ok(MetadataCommandPayload::CreateBucket(CreateBucketCommand {
                    bucket: self.read_bucket_record()?,
                }))
            }
            METADATA_COMMAND_PUT_BUCKET_VERSIONING => Ok(
                MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand {
                    bucket: self.read_bucket_record()?,
                }),
            ),
            METADATA_COMMAND_PUT_BUCKET_ACL => {
                Ok(MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand {
                    bucket: self.read_bucket_record()?,
                }))
            }
            METADATA_COMMAND_PUT_BUCKET_PROPERTY => {
                let bucket = self.read_bucket_record()?;
                let effect = match self.read_u8()? {
                    0 => BucketPropertyEffect::ObjectLock,
                    1 => BucketPropertyEffect::Encryption,
                    2 => BucketPropertyEffect::PublicAccessBlock,
                    3 => BucketPropertyEffect::OwnershipControls,
                    4 => BucketPropertyEffect::AbacEnabled,
                    effect => return Err(format!("invalid bucket property effect {effect}")),
                };
                Ok(MetadataCommandPayload::PutBucketProperty(
                    PutBucketPropertyCommand { bucket, effect },
                ))
            }
            METADATA_COMMAND_PUT_BUCKET_SUBRESOURCE => Ok(
                MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand {
                    name: self.read_bucket_name()?,
                    mutation: self.read_bucket_subresource_mutation()?,
                    bucket_execution_generation: self.read_u64()?,
                }),
            ),
            METADATA_COMMAND_MARK_BUCKET_DELETING => Ok(
                MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand {
                    bucket: self.read_bucket_record()?,
                }),
            ),
            METADATA_COMMAND_DELETE_FINALIZED_BUCKET => Ok(
                MetadataCommandPayload::DeleteFinalizedBucket(DeleteFinalizedBucketCommand::new(
                    self.read_bucket_name()?,
                    self.read_u64()?,
                    self.read_u64()?,
                )),
            ),
            METADATA_COMMAND_RESERVE_OBJECT_GENERATION => {
                Ok(MetadataCommandPayload::ReserveObjectGeneration(
                    ReserveObjectGenerationCommand::new(
                        self.read_bucket_name()?,
                        self.read_object_key()?,
                        self.read_session_id()?,
                        self.read_generation_id("reserved object generation")?,
                        self.read_u64()?,
                    ),
                ))
            }
            METADATA_COMMAND_RELEASE_OBJECT_GENERATION => {
                Ok(MetadataCommandPayload::ReleaseObjectGeneration(
                    ReleaseObjectGenerationCommand::new(
                        self.read_bucket_name()?,
                        self.read_object_key()?,
                        self.read_session_id()?,
                    ),
                ))
            }
            METADATA_COMMAND_RESERVE_OBJECT_VERSION => Ok(
                MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                    self.read_bucket_name()?,
                    self.read_object_key()?,
                    self.read_version_id()?,
                )),
            ),
            METADATA_COMMAND_COMMIT_DIRECT_PUT_OBJECT => {
                Ok(MetadataCommandPayload::CommitDirectPutObject(Box::new(
                    CommitDirectPutObjectCommand {
                        object: self.read_put_live_object()?,
                        segments: self.read_repeated(Self::read_object_segment)?,
                        generation_reservation_id: self.read_session_id()?,
                        write_sequence: self.read_u64()?,
                        last_modified_millis: self.read_u64()?,
                        stale_payload: self.read_optional_stale_payload()?,
                        bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
                    },
                )))
            }
            METADATA_COMMAND_COMMIT_MULTIPART_OBJECT => {
                Ok(MetadataCommandPayload::CommitMultipartObject(Box::new(
                    CommitMultipartObjectCommand {
                        upload_id: self.read_upload_id()?,
                        completion_fingerprint: MultipartCompletionFingerprint::from_bytes(
                            self.read_fixed_bytes("multipart completion fingerprint")?,
                        ),
                        object: self.read_put_live_object()?,
                        parts: self.read_repeated(Self::read_object_part)?,
                        selected_streaming_segments: self
                            .read_repeated(Self::read_multipart_part_segment)?,
                        omitted_parts: self.read_repeated(Self::read_multipart_part)?,
                        omitted_streaming_segments: self
                            .read_repeated(Self::read_multipart_part_segment)?,
                        stream_uploads: self.read_repeated(Self::read_terminal_stream_cleanup)?,
                        stream_upload_segments: self
                            .read_repeated(Self::read_stream_upload_segment)?,
                        write_sequence: self.read_u64()?,
                        last_modified_millis: self.read_u64()?,
                        stale_payload: self.read_optional_stale_payload()?,
                        bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
                    },
                )))
            }
            METADATA_COMMAND_DELETE_OBJECT_VERSION => Ok(
                MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                    bucket: self.read_bucket_name()?,
                    key: self.read_object_key()?,
                    version_id: self.read_version_id()?,
                    target: match self.read_u8()? {
                        1 => DeleteObjectVersionTarget::DeleteMarker {
                            write_sequence: self.read_u64()?,
                        },
                        2 => DeleteObjectVersionTarget::Live {
                            generation_id: self.read_generation_id("deleted object generation")?,
                            layout: self.read_object_layout()?,
                            payload: self.read_live_payload_reclaim()?,
                        },
                        tag => return Err(format!("invalid delete object target tag {tag}")),
                    },
                    bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
                })),
            ),
            METADATA_COMMAND_INSERT_DELETE_MARKER => Ok(
                MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                    bucket: self.read_bucket_name()?,
                    key: self.read_object_key()?,
                    version_id: self.read_version_id()?,
                    owner: self.read_owner_identity()?,
                    write_sequence: self.read_u64()?,
                    last_modified_millis: self.read_u64()?,
                    stale_payload: self.read_optional_stale_payload()?,
                    bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
                }),
            ),
            METADATA_COMMAND_PUT_OBJECT_METADATA => Ok(MetadataCommandPayload::PutObjectMetadata(
                Box::new(PutObjectMetadataCommand {
                    object: self.read_live_object_record()?,
                    bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
                }),
            )),
            METADATA_COMMAND_CREATE_STREAM_UPLOAD => Ok(
                MetadataCommandPayload::CreateStreamUpload(Box::new(CreateStreamUploadCommand {
                    session: self.read_stream_upload_command_record()?,
                    initial_next_segment_vid: self
                        .read_generation_id("initial next stream segment VID")?,
                    cleanup_after: self.read_optional_u64_value()?,
                    bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
                })),
            ),
            METADATA_COMMAND_APPEND_STREAM_SEGMENT => Ok(
                MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
                    bucket: self.read_bucket_name()?,
                    key: self.read_object_key()?,
                    segment: self.read_stream_upload_segment()?,
                })),
            ),
            METADATA_COMMAND_ABORT_STREAM_UPLOAD => Ok(MetadataCommandPayload::AbortStreamUpload(
                Box::new(AbortStreamUploadCommand {
                    bucket: self.read_bucket_name()?,
                    key: self.read_object_key()?,
                    session_id: self.read_session_id()?,
                    staged_segments: self.read_repeated(Self::read_stream_upload_segment)?,
                    stream_create_bucket_write_reservation: self
                        .read_optional_bucket_write_reservation_proof()?,
                }),
            )),
            METADATA_COMMAND_COMMIT_STREAM_PART => Ok(MetadataCommandPayload::CommitStreamPart(
                Box::new(CommitStreamPartCommand {
                    bucket: self.read_bucket_name()?,
                    key: self.read_object_key()?,
                    session_id: self.read_session_id()?,
                    upload: self.read_multipart_upload()?,
                    part: self.read_multipart_part()?,
                    segments: self.read_repeated(Self::read_multipart_part_segment)?,
                    existing_part: self.read_optional(Self::read_multipart_part)?,
                    displaced_segments: self.read_repeated(Self::read_multipart_part_segment)?,
                    bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
                }),
            )),
            METADATA_COMMAND_CREATE_MULTIPART_UPLOAD => {
                Ok(MetadataCommandPayload::CreateMultipartUpload(Box::new(
                    CreateMultipartUploadCommand {
                        upload: self.read_multipart_upload()?,
                        bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
                    },
                )))
            }
            METADATA_COMMAND_ABORT_MULTIPART_UPLOAD => {
                Ok(MetadataCommandPayload::AbortMultipartUpload(Box::new(
                    AbortMultipartUploadCommand {
                        bucket: self.read_bucket_name()?,
                        key: self.read_object_key()?,
                        upload_id: self.read_upload_id()?,
                        cleanup: self.read_abort_multipart_upload_cleanup()?,
                        bucket_write_reservation: self.read_bucket_write_reservation_proof()?,
                    },
                )))
            }
            METADATA_COMMAND_DELETE_OBJECT_PAYLOAD_RECLAIM => {
                Ok(MetadataCommandPayload::DeleteObjectPayloadReclaim(
                    Box::new(DeleteObjectPayloadReclaimCommand::new(
                        self.read_bucket_name()?,
                        self.read_object_key()?,
                        self.read_generation_id("payload reclaim generation")?,
                        self.read_object_payload_reclaim()?,
                        self.read_object_payload_reclaim_claim_proof()?,
                    )),
                ))
            }
            METADATA_COMMAND_ADVANCE_MULTIPART_COMPLETION_BARRIER => {
                Ok(MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
                    AdvanceMultipartCompletionBarrierCommand {
                        bucket: self.read_bucket_name()?,
                        barrier_sequence: self.read_u64()?,
                    },
                ))
            }
            _ => Err(format!(
                "pending metadata command payload kind {kind_id} does not have a typed decoder yet"
            )),
        }
    }

    fn read_repeated<T>(
        &mut self,
        mut read_item: impl FnMut(&mut Self) -> Result<T, String>,
    ) -> Result<Vec<T>, String> {
        let count = self.read_u32()? as usize;
        let remaining = self.remaining();
        if count > remaining {
            return Err(format!(
                "metadata command repeated item count {count} exceeds remaining encoded bytes {remaining}"
            ));
        }
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(read_item(self)?);
        }
        Ok(items)
    }

    fn skip_repeated(
        &mut self,
        mut skip_item: impl FnMut(&mut Self) -> Result<(), String>,
    ) -> Result<(), String> {
        let count = self.read_u32()? as usize;
        let remaining = self.remaining();
        if count > remaining {
            return Err(format!(
                "metadata command repeated item count {count} exceeds remaining encoded bytes {remaining}"
            ));
        }
        for _ in 0..count {
            skip_item(self)?;
        }
        Ok(())
    }

    fn skip_optional(
        &mut self,
        skip_value: impl FnOnce(&mut Self) -> Result<(), String>,
    ) -> Result<(), String> {
        match self.read_u8()? {
            0 => Ok(()),
            1 => skip_value(self),
            tag => Err(format!("invalid optional tag {tag}")),
        }
    }

    fn read_optional<T>(
        &mut self,
        read_value: impl FnOnce(&mut Self) -> Result<T, String>,
    ) -> Result<Option<T>, String> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => read_value(self).map(Some),
            tag => Err(format!("invalid optional tag {tag}")),
        }
    }

    fn skip_str(&mut self) -> Result<(), String> {
        self.read_bytes().map(|_| ())
    }

    fn read_string(&mut self, field: &'static str) -> Result<String, String> {
        std::str::from_utf8(self.read_bytes()?)
            .map(str::to_owned)
            .map_err(|error| format!("{field} is not UTF-8: {error}"))
    }

    fn read_bucket_name(&mut self) -> Result<BucketName, String> {
        BucketName::try_from(self.read_string("bucket name")?)
            .map_err(|reason| format!("invalid bucket name in metadata command: {reason}"))
    }

    fn read_object_key(&mut self) -> Result<ObjectKey, String> {
        ObjectKey::try_from(self.read_string("object key")?)
            .map_err(|reason| format!("invalid object key in metadata command: {reason}"))
    }

    fn read_upload_id(&mut self) -> Result<UploadId, String> {
        UploadId::try_from(self.read_string("upload id")?)
            .map_err(|reason| format!("invalid upload id in metadata command: {reason}"))
    }

    fn read_session_id(&mut self) -> Result<SessionId, String> {
        SessionId::try_from(self.read_string("session id")?)
            .map_err(|reason| format!("invalid session id in metadata command: {reason}"))
    }

    fn read_canonical_user_id(&mut self) -> Result<CanonicalUserId, String> {
        let value = self.read_string("canonical user id")?;
        CanonicalUserId::parse_stored(&value)
            .ok_or_else(|| format!("invalid canonical user id in metadata command: {value}"))
    }

    fn read_acl_grants(&mut self) -> Result<AclGrants, String> {
        AclGrants::parse(&self.read_string("ACL grants")?)
            .map_err(|reason| format!("invalid ACL grants in metadata command: {reason}"))
    }

    fn read_bool(&mut self) -> Result<bool, String> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            tag => Err(format!("invalid bool value {tag}")),
        }
    }

    fn read_optional_string(&mut self, field: &'static str) -> Result<Option<String>, String> {
        self.read_optional(|decoder| decoder.read_string(field))
    }

    fn read_optional_bytes_value(&mut self) -> Result<Option<Vec<u8>>, String> {
        self.read_optional(|decoder| decoder.read_bytes().map(<[u8]>::to_vec))
    }

    fn read_optional_u64_value(&mut self) -> Result<Option<u64>, String> {
        self.read_optional(Self::read_u64)
    }

    fn read_fixed_bytes<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], String> {
        let bytes = self.read_bytes()?;
        bytes
            .try_into()
            .map_err(|_| format!("{field} must be exactly {N} bytes"))
    }

    fn read_generation_id(&mut self, field: &'static str) -> Result<GenerationId, String> {
        GenerationId::new(self.read_u64()?).ok_or_else(|| format!("{field} must be non-zero"))
    }

    fn read_cluster_epoch(&mut self, field: &'static str) -> Result<ClusterEpoch, String> {
        ClusterEpoch::new(self.read_u64()?).ok_or_else(|| format!("{field} must be non-zero"))
    }

    fn read_version_id(&mut self) -> Result<VersionId, String> {
        Ok(VersionId::from_u64(self.read_u64()?))
    }

    fn read_ec_shape(&mut self) -> Result<EcShape, String> {
        Ok(EcShape {
            k: self.read_u8()?,
            m: self.read_u8()?,
        })
    }

    fn skip_optional_str(&mut self) -> Result<(), String> {
        self.skip_optional(Self::skip_str)
    }

    fn skip_optional_bytes(&mut self) -> Result<(), String> {
        self.skip_optional(|decoder| decoder.read_bytes().map(|_| ()))
    }

    fn skip_optional_u64(&mut self) -> Result<(), String> {
        self.skip_optional(|decoder| decoder.read_u64().map(|_| ()))
    }

    fn skip_bool(&mut self) -> Result<(), String> {
        self.read_valid_u8("bool", 0..=1)
    }

    fn read_valid_u8(
        &mut self,
        field: &'static str,
        valid: std::ops::RangeInclusive<u8>,
    ) -> Result<(), String> {
        let value = self.read_u8()?;
        if valid.contains(&value) {
            Ok(())
        } else {
            Err(format!("invalid {field} value {value}"))
        }
    }

    fn read_nonzero_u32(&mut self, field: &'static str) -> Result<(), String> {
        let value = self.read_u32()?;
        if value == 0 {
            Err(format!("{field} must be non-zero"))
        } else {
            Ok(())
        }
    }

    fn read_nonzero_u64(&mut self, field: &'static str) -> Result<(), String> {
        let value = self.read_u64()?;
        if value == 0 {
            Err(format!("{field} must be non-zero"))
        } else {
            Ok(())
        }
    }

    fn skip_bucket_record(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.skip_str()?;
        self.read_u64()?;
        self.read_u16()?;
        self.read_valid_u8("bucket state", 0..=1)?;
        self.read_valid_u8("bucket versioning state", 0..=2)?;
        self.skip_bucket_object_lock()?;
        self.skip_str()?;
        self.skip_bool()?;
        self.skip_bool()?;
        self.skip_public_access_block()?;
        self.skip_ownership_controls()?;
        self.skip_bool()?;
        self.read_u64()?;
        self.read_u64()?;
        self.read_u64()?;
        self.read_u64()?;
        self.read_bytes()?;
        self.read_u64()?;
        self.skip_bool()?;
        self.skip_bucket_encryption()
    }

    fn read_bucket_record(&mut self) -> Result<BucketRecord, String> {
        Ok(BucketRecord {
            name: self.read_bucket_name()?,
            owner_principal: self.read_string("bucket owner principal")?,
            owner_canonical_id: self.read_canonical_user_id()?,
            created_at: self.read_u64()?,
            region: self.read_u16()?,
            state: BucketState::from_u8(self.read_u8()?)
                .ok_or_else(|| "invalid bucket state in metadata command".to_string())?,
            versioning: BucketVersioningState::from_u8(self.read_u8()?)
                .ok_or_else(|| "invalid bucket versioning state in metadata command".to_string())?,
            object_lock: self.read_bucket_object_lock()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            public_write: self.read_bool()?,
            public_access_block: self.read_public_access_block()?,
            ownership_controls: self.read_ownership_controls()?,
            bucket_policy_public: self.read_bool()?,
            bucket_policy_generation: self.read_u64()?,
            bucket_lifecycle_generation: self.read_u64()?,
            bucket_execution_generation: self.read_u64()?,
            bucket_incarnation_generation: self.read_u64()?,
            multipart_upload_id_key: MultipartUploadIdKey::from_bytes(
                self.read_fixed_bytes::<MULTIPART_UPLOAD_ID_KEY_LEN>("multipart upload ID key")?,
            ),
            multipart_completion_barrier_sequence: self.read_u64()?,
            bucket_abac_enabled: self.read_bool()?,
            encryption: self.read_bucket_encryption()?,
        })
    }

    fn read_put_live_object(&mut self) -> Result<PutLiveObjectReq, String> {
        Ok(PutLiveObjectReq {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: self.read_version_id()?,
            owner: self.read_owner_identity()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            generation_id: self.read_generation_id("object generation")?,
            size: self.read_u64()?,
            etag: self.read_object_etag()?,
            ec: self.read_ec_shape()?,
            layout: self.read_object_layout()?,
            tags: self
                .read_optional_string("object tags")?
                .map(SerializedTagSet::new),
            metadata_blob: self
                .read_optional_bytes_value()?
                .map(SerializedMetadataBlob::new),
            system_metadata_blob: self
                .read_optional_bytes_value()?
                .map(SerializedSystemMetadataBlob::new),
            object_lock: self.read_object_lock_state()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn skip_put_live_object(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.read_u64()?;
        self.skip_owner_identity()?;
        self.skip_str()?;
        self.skip_bool()?;
        self.read_nonzero_u64("object generation")?;
        self.read_u64()?;
        self.skip_object_etag()?;
        self.read_u8()?;
        self.read_u8()?;
        self.skip_object_layout()?;
        self.skip_optional_str()?;
        self.skip_optional_bytes()?;
        self.skip_optional_bytes()?;
        self.skip_object_lock_state()?;
        self.skip_object_encryption()
    }

    fn read_live_object_record(&mut self) -> Result<LiveObjectRecord, String> {
        Ok(LiveObjectRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: self.read_version_id()?,
            owner: self.read_owner_identity()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            generation_id: self.read_generation_id("object generation")?,
            size: self.read_u64()?,
            etag: self.read_object_etag()?,
            last_modified: self.read_u64()?,
            became_noncurrent_at: self.read_optional_u64_value()?,
            storage_class: StorageClass::from_u8(self.read_u8()?)
                .ok_or_else(|| "invalid storage class".to_string())?,
            ec: self.read_ec_shape()?,
            layout: self.read_object_layout()?,
            tags: self
                .read_optional_string("object tags")?
                .map(SerializedTagSet::new),
            metadata_blob: self
                .read_optional_bytes_value()?
                .map(SerializedMetadataBlob::new),
            system_metadata_blob: self
                .read_optional_bytes_value()?
                .map(SerializedSystemMetadataBlob::new),
            object_lock: self.read_object_lock_state()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn skip_live_object_record(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.read_u64()?;
        self.skip_owner_identity()?;
        self.skip_str()?;
        self.skip_bool()?;
        self.read_nonzero_u64("object generation")?;
        self.read_u64()?;
        self.skip_object_etag()?;
        self.read_u64()?;
        self.skip_optional_u64()?;
        self.read_valid_u8("storage class", 0..=0)?;
        self.read_u8()?;
        self.read_u8()?;
        self.skip_object_layout()?;
        self.skip_optional_str()?;
        self.skip_optional_bytes()?;
        self.skip_optional_bytes()?;
        self.skip_object_lock_state()?;
        self.skip_object_encryption()
    }

    fn skip_object_segment(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.read_u64()?;
        self.read_u32()?;
        self.read_u64()?;
        self.read_u64()?;
        self.read_bytes()?;
        self.read_nonzero_u64("object segment VID")?;
        self.read_u32()?;
        self.read_cluster_epoch("object segment placement epoch")?;
        self.read_u8()?;
        self.read_u8()?;
        Ok(())
    }

    fn read_object_segment(&mut self) -> Result<ObjectSegmentRecord, String> {
        Ok(ObjectSegmentRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: self.read_version_id()?,
            segment_index: self.read_u32()?,
            size: self.read_u64()?,
            segment_crc64: self.read_u64()?,
            segment_okh: self.read_fixed_bytes("object segment OKH")?,
            segment_vid: self.read_generation_id("object segment VID")?,
            data_pg_id: self.read_u32()?,
            placement_cluster_epoch: self.read_cluster_epoch("object segment placement epoch")?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
        })
    }

    fn skip_object_part(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.read_u64()?;
        self.read_u32()?;
        self.read_u64()?;
        self.read_u64()?;
        self.read_bytes()?;
        self.read_valid_u8("etag kind", 0..=1)?;
        self.read_bytes()?;
        self.read_nonzero_u64("object part VID")?;
        self.read_cluster_epoch("object part placement epoch")?;
        self.read_u8()?;
        self.read_u8()?;
        self.read_u32()?;
        self.skip_optional_bytes()
    }

    fn read_object_part(&mut self) -> Result<ObjectPartRecord, String> {
        Ok(ObjectPartRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            version_id: self.read_version_id()?,
            part_number: self.read_u32()?,
            size: self.read_u64()?,
            payload_crc64: self.read_u64()?,
            etag: self.read_bytes()?.to_vec(),
            etag_kind: EtagKind::from_u8(self.read_u8()?)
                .ok_or_else(|| "invalid etag kind".to_string())?,
            part_okh: self.read_fixed_bytes("object part OKH")?,
            part_vid: self.read_generation_id("object part VID")?,
            placement_cluster_epoch: self.read_cluster_epoch("object part placement epoch")?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
            data_pg_id: self.read_u32()?,
            checksum: self.read_optional_checksum_bytes()?,
        })
    }

    fn skip_multipart_part(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.read_u32()?;
        self.read_u32()?;
        self.read_u64()?;
        self.read_u64()?;
        self.read_bytes()?;
        self.read_valid_u8("etag kind", 0..=1)?;
        self.read_bytes()?;
        self.read_nonzero_u64("multipart part VID")?;
        self.read_cluster_epoch("multipart part placement epoch")?;
        self.read_u8()?;
        self.read_u8()?;
        self.read_u64()?;
        self.skip_optional_bytes()
    }

    fn read_multipart_part(&mut self) -> Result<MultipartPartRecord, String> {
        Ok(MultipartPartRecord {
            upload_id: self.read_upload_id()?,
            part_number: self.read_u32()?,
            generation: self.read_u32()?,
            size: self.read_u64()?,
            payload_crc64: self.read_u64()?,
            etag: self.read_bytes()?.to_vec(),
            etag_kind: EtagKind::from_u8(self.read_u8()?)
                .ok_or_else(|| "invalid etag kind".to_string())?,
            part_okh: self.read_fixed_bytes("multipart part OKH")?,
            part_vid: self.read_generation_id("multipart part VID")?,
            placement_cluster_epoch: self.read_cluster_epoch("multipart part placement epoch")?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
            last_modified: self.read_u64()?,
            checksum: self.read_optional_checksum_bytes()?,
        })
    }

    fn skip_optional_multipart_part(&mut self) -> Result<(), String> {
        self.skip_optional(Self::skip_multipart_part)
    }

    fn skip_multipart_part_segment(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.skip_str()?;
        self.read_u64()?;
        self.read_u32()?;
        self.read_u32()?;
        self.read_u64()?;
        self.read_u64()?;
        self.read_bytes()?;
        self.read_nonzero_u64("multipart part segment VID")?;
        self.read_u32()?;
        self.read_cluster_epoch("multipart part segment placement epoch")?;
        self.read_u8()?;
        self.read_u8()?;
        Ok(())
    }

    fn read_multipart_part_segment(&mut self) -> Result<MultipartPartSegmentRecord, String> {
        Ok(MultipartPartSegmentRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            upload_id: self.read_upload_id()?,
            version_id: self.read_u64()?,
            part_number: self.read_u32()?,
            segment_index: self.read_u32()?,
            size: self.read_u64()?,
            segment_crc64: self.read_u64()?,
            segment_okh: self.read_fixed_bytes("multipart part segment OKH")?,
            segment_vid: self.read_generation_id("multipart part segment VID")?,
            data_pg_id: self.read_u32()?,
            placement_cluster_epoch: self
                .read_cluster_epoch("multipart part segment placement epoch")?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
        })
    }

    fn skip_create_stream_upload(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.skip_str()?;
        self.skip_stream_upload_target()?;
        self.read_valid_u8("stream upload state", 0..=3)?;
        self.read_u64()?;
        self.skip_object_encryption()?;
        self.read_nonzero_u64("next stream segment VID")?;
        self.skip_optional_u64()?;
        self.skip_required_bucket_write_reservation_proof()
    }

    fn skip_required_bucket_write_reservation_proof(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.skip_str()?;
        self.read_u64()?;
        self.read_u64()?;
        self.read_u64()?;
        self.skip_str()?;
        self.read_u64()?;
        self.read_u64()?;
        self.skip_optional(Self::skip_str)
    }

    fn skip_terminal_stream_cleanup(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.skip_str()?;
        self.skip_stream_upload_target()?;
        self.read_valid_u8("stream upload state", 0..=3)?;
        self.read_u64()?;
        self.skip_object_encryption()
    }

    fn read_stream_upload_command_record(&mut self) -> Result<StreamUploadCommandRecord, String> {
        Ok(StreamUploadCommandRecord {
            session_id: self.read_session_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            target: self.read_stream_upload_target()?,
            state: StreamUploadState::from_u8(self.read_u8()?)
                .ok_or_else(|| "invalid stream upload state".to_string())?,
            created_at: self.read_u64()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn read_terminal_stream_cleanup(&mut self) -> Result<TerminalStreamCleanupRecord, String> {
        Ok(TerminalStreamCleanupRecord {
            session_id: self.read_session_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            target: self.read_stream_upload_target()?,
            state: StreamUploadState::from_u8(self.read_u8()?)
                .ok_or_else(|| "invalid stream upload state".to_string())?,
            created_at: self.read_u64()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn skip_stream_upload_target(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            0 => Ok(()),
            1 => {
                self.skip_str()?;
                self.read_u32()?;
                Ok(())
            }
            tag => Err(format!("invalid stream upload target tag {tag}")),
        }
    }

    fn read_stream_upload_target(&mut self) -> Result<StreamUploadTarget, String> {
        match self.read_u8()? {
            0 => Ok(StreamUploadTarget::PutObject),
            1 => Ok(StreamUploadTarget::UploadPart {
                upload_id: self.read_upload_id()?,
                part_number: self.read_u32()?,
            }),
            tag => Err(format!("invalid stream upload target tag {tag}")),
        }
    }

    fn skip_stream_upload_segment(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.read_u32()?;
        self.read_u64()?;
        self.read_u64()?;
        self.read_u64()?;
        self.read_bytes()?;
        self.read_nonzero_u64("stream upload segment VID")?;
        self.read_u32()?;
        self.read_cluster_epoch("stream upload segment placement epoch")?;
        self.read_u8()?;
        self.read_u8()?;
        Ok(())
    }

    fn read_stream_upload_segment(&mut self) -> Result<StreamUploadSegmentRecord, String> {
        Ok(StreamUploadSegmentRecord {
            session_id: self.read_session_id()?,
            segment_index: self.read_u32()?,
            size: self.read_u64()?,
            segment_crc64: self.read_u64()?,
            payload_crc64: self.read_u64()?,
            segment_okh: self.read_fixed_bytes("stream upload segment OKH")?,
            segment_vid: self.read_generation_id("stream upload segment VID")?,
            data_pg_id: self.read_u32()?,
            placement_cluster_epoch: self
                .read_cluster_epoch("stream upload segment placement epoch")?,
            ec_k: self.read_u8()?,
            ec_m: self.read_u8()?,
        })
    }

    fn skip_multipart_upload(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.skip_str()?;
        self.read_u64()?;
        self.read_valid_u8("multipart upload state", 0..=2)?;
        self.skip_optional_str()?;
        self.read_bytes()?;
        self.read_bytes()?;
        self.skip_owner_identity()?;
        self.skip_owner_identity()?;
        self.skip_str()?;
        self.skip_bool()?;
        self.read_nonzero_u64("multipart upload object generation")?;
        self.skip_multipart_object_identity()?;
        self.skip_object_lock_state()?;
        self.skip_optional_multipart_checksum_config()?;
        self.skip_object_encryption()
    }

    fn read_multipart_upload(&mut self) -> Result<MultipartUploadRecord, String> {
        Ok(MultipartUploadRecord {
            upload_id: self.read_upload_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            initiated_at: self.read_u64()?,
            state: UploadState::from_u8(self.read_u8()?)
                .ok_or_else(|| "invalid multipart upload state".to_string())?,
            tags: self
                .read_optional_string("multipart upload tags")?
                .map(SerializedTagSet::new),
            metadata_blob: SerializedMetadataBlob::new(self.read_bytes()?.to_vec()),
            system_metadata_blob: SerializedSystemMetadataBlob::new(self.read_bytes()?.to_vec()),
            initiator: self.read_owner_identity()?,
            owner: self.read_owner_identity()?,
            acl_grants: self.read_acl_grants()?,
            public_read: self.read_bool()?,
            object_generation_id: self.read_generation_id("multipart upload object generation")?,
            initiated_object_identity: self.read_multipart_object_identity()?,
            object_lock: self.read_object_lock_state()?,
            checksum: self.read_optional_multipart_checksum_config()?,
            encryption: self.read_object_encryption()?,
        })
    }

    fn skip_multipart_object_identity(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            0 => Ok(()),
            1 => {
                self.read_version_id()?;
                self.read_nonzero_u64("multipart initiation live generation")?;
                Ok(())
            }
            2 => {
                self.read_version_id()?;
                self.read_u64()?;
                Ok(())
            }
            tag => Err(format!("invalid multipart object identity tag {tag}")),
        }
    }

    fn read_multipart_object_identity(
        &mut self,
    ) -> Result<Option<crate::MultipartObjectIdentity>, String> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(crate::MultipartObjectIdentity::Live {
                version_id: self.read_version_id()?,
                generation_id: self.read_generation_id("multipart initiation live generation")?,
            })),
            2 => Ok(Some(crate::MultipartObjectIdentity::DeleteMarker {
                version_id: self.read_version_id()?,
                write_sequence: self.read_u64()?,
            })),
            tag => Err(format!("invalid multipart object identity tag {tag}")),
        }
    }

    fn skip_object_payload_reclaim(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            0 => self.skip_object_segments_reclaim(),
            1 => self.skip_multipart_reclaim(),
            tag => Err(format!("invalid object payload reclaim tag {tag}")),
        }
    }

    fn read_object_payload_reclaim(&mut self) -> Result<ObjectPayloadReclaimCommand, String> {
        match self.read_u8()? {
            0 => self
                .read_object_segments_reclaim()
                .map(ObjectPayloadReclaimCommand::Segments),
            1 => self
                .read_multipart_reclaim()
                .map(ObjectPayloadReclaimCommand::Multipart),
            tag => Err(format!("invalid object payload reclaim tag {tag}")),
        }
    }

    fn skip_live_payload_reclaim(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            1 => self.skip_object_segments_reclaim(),
            2 => self.skip_multipart_reclaim(),
            tag => Err(format!("invalid live payload reclaim tag {tag}")),
        }
    }

    fn read_live_payload_reclaim(&mut self) -> Result<ObjectPayloadReclaimCommand, String> {
        match self.read_u8()? {
            1 => self
                .read_object_segments_reclaim()
                .map(ObjectPayloadReclaimCommand::Segments),
            2 => self
                .read_multipart_reclaim()
                .map(ObjectPayloadReclaimCommand::Multipart),
            tag => Err(format!("invalid live payload reclaim tag {tag}")),
        }
    }

    fn skip_optional_stale_payload(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            0 => Ok(()),
            1 => self.skip_object_segments_reclaim(),
            2 => self.skip_multipart_reclaim(),
            tag => Err(format!("invalid stale payload tag {tag}")),
        }
    }

    fn read_optional_stale_payload(
        &mut self,
    ) -> Result<Option<ObjectPayloadReclaimCommand>, String> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self
                .read_object_segments_reclaim()
                .map(ObjectPayloadReclaimCommand::Segments)
                .map(Some),
            2 => self
                .read_multipart_reclaim()
                .map(ObjectPayloadReclaimCommand::Multipart)
                .map(Some),
            tag => Err(format!("invalid stale payload tag {tag}")),
        }
    }

    fn skip_object_segments_reclaim(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.read_nonzero_u64("object segments reclaim generation")?;
        self.read_u64()?;
        self.skip_repeated(|decoder| {
            decoder.read_u32()?;
            decoder.read_bytes()?;
            decoder.read_nonzero_u64("object segments reclaim segment VID")?;
            decoder.read_u32()?;
            decoder.read_u8()?;
            decoder.read_u8()?;
            Ok(())
        })
    }

    fn read_object_segments_reclaim(&mut self) -> Result<ObjectSegmentsReclaimRecord, String> {
        Ok(ObjectSegmentsReclaimRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            generation_id: self.read_generation_id("object segments reclaim generation")?,
            created_at: self.read_u64()?,
            segments: self.read_repeated(|decoder| {
                Ok(ObjectSegmentsReclaimSegmentRecord {
                    segment_index: decoder.read_u32()?,
                    segment_okh: decoder.read_fixed_bytes("object segments reclaim segment OKH")?,
                    segment_vid: decoder
                        .read_generation_id("object segments reclaim segment VID")?,
                    data_pg_id: decoder.read_u32()?,
                    ec: decoder.read_ec_shape()?,
                })
            })?,
        })
    }

    fn skip_multipart_reclaim(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.read_nonzero_u64("multipart reclaim generation")?;
        self.read_u64()?;
        self.skip_repeated(|decoder| match decoder.read_u8()? {
            1 => {
                decoder.read_u32()?;
                decoder.read_bytes()?;
                decoder.read_nonzero_u64("multipart reclaim part VID")?;
                decoder.read_u32()?;
                decoder.read_u8()?;
                decoder.read_u8()?;
                Ok(())
            }
            2 => {
                decoder.read_u32()?;
                decoder.skip_repeated(|decoder| {
                    decoder.read_u32()?;
                    decoder.read_u32()?;
                    decoder.read_bytes()?;
                    decoder.read_nonzero_u64("multipart reclaim segment VID")?;
                    decoder.read_u32()?;
                    decoder.read_u8()?;
                    decoder.read_u8()?;
                    Ok(())
                })
            }
            tag => Err(format!("invalid multipart reclaim part tag {tag}")),
        })
    }

    fn read_multipart_reclaim(&mut self) -> Result<MultipartReclaimRecord, String> {
        Ok(MultipartReclaimRecord {
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            generation_id: self.read_generation_id("multipart reclaim generation")?,
            created_at: self.read_u64()?,
            parts: self.read_repeated(|decoder| match decoder.read_u8()? {
                1 => Ok(MultipartReclaimPartRecord::ShardSet {
                    part_number: decoder.read_u32()?,
                    part_okh: decoder.read_fixed_bytes("multipart reclaim part OKH")?,
                    part_vid: decoder.read_generation_id("multipart reclaim part VID")?,
                    data_pg_id: decoder.read_u32()?,
                    ec: decoder.read_ec_shape()?,
                }),
                2 => Ok(MultipartReclaimPartRecord::Segments {
                    part_number: decoder.read_u32()?,
                    segments: decoder.read_repeated(|decoder| {
                        Ok(MultipartReclaimPartSegmentRecord {
                            part_number: decoder.read_u32()?,
                            segment_index: decoder.read_u32()?,
                            segment_okh: decoder
                                .read_fixed_bytes("multipart reclaim segment OKH")?,
                            segment_vid: decoder
                                .read_generation_id("multipart reclaim segment VID")?,
                            data_pg_id: decoder.read_u32()?,
                            ec: decoder.read_ec_shape()?,
                        })
                    })?,
                }),
                tag => Err(format!("invalid multipart reclaim part tag {tag}")),
            })?,
        })
    }

    fn skip_abort_multipart_upload_cleanup(&mut self) -> Result<(), String> {
        self.skip_multipart_upload()?;
        self.skip_repeated(Self::skip_multipart_part)?;
        self.skip_repeated(Self::skip_multipart_part_segment)?;
        self.skip_repeated(Self::skip_terminal_stream_cleanup)?;
        self.skip_repeated(Self::skip_stream_upload_segment)
    }

    fn read_abort_multipart_upload_cleanup(
        &mut self,
    ) -> Result<AbortMultipartUploadCleanup, String> {
        Ok(AbortMultipartUploadCleanup {
            upload: self.read_multipart_upload()?,
            parts: self.read_repeated(Self::read_multipart_part)?,
            streaming_segments: self.read_repeated(Self::read_multipart_part_segment)?,
            stream_uploads: self.read_repeated(Self::read_terminal_stream_cleanup)?,
            stream_upload_segments: self.read_repeated(Self::read_stream_upload_segment)?,
        })
    }

    fn skip_object_etag(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            1 => {
                self.read_crc64_bytes("single-part etag CRC64")?;
                Ok(())
            }
            2 => {
                self.read_crc64_bytes("multipart etag CRC64")?;
                self.read_nonzero_u32("multipart etag parts count")
            }
            tag => Err(format!("invalid object etag tag {tag}")),
        }
    }

    fn read_object_etag(&mut self) -> Result<ObjectEtag, String> {
        match self.read_u8()? {
            1 => Ok(ObjectEtag::SinglePart(
                self.read_fixed_bytes("single-part etag CRC64")?,
            )),
            2 => Ok(ObjectEtag::MultipartComposite {
                crc64: self.read_fixed_bytes("multipart etag CRC64")?,
                parts: NonZeroU32::new(self.read_u32()?)
                    .ok_or_else(|| "multipart etag parts count must be non-zero".to_string())?,
            }),
            tag => Err(format!("invalid object etag tag {tag}")),
        }
    }

    fn read_crc64_bytes(&mut self, field: &'static str) -> Result<(), String> {
        let bytes = self.read_bytes()?;
        if bytes.len() == 8 {
            Ok(())
        } else {
            Err(format!("{field} must be exactly 8 bytes"))
        }
    }

    fn read_optional_checksum_bytes(&mut self) -> Result<Option<ChecksumBytes>, String> {
        self.read_optional(|decoder| {
            ChecksumBytes::new(decoder.read_bytes()?)
                .map_err(|reason| format!("invalid checksum bytes: {reason}"))
        })
    }

    fn skip_object_layout(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            1 => Ok(()),
            2 => self.read_nonzero_u32("multipart layout parts count"),
            tag => Err(format!("invalid object layout tag {tag}")),
        }
    }

    fn read_object_layout(&mut self) -> Result<ObjectLayout, String> {
        match self.read_u8()? {
            1 => Ok(ObjectLayout::Standard),
            2 => Ok(ObjectLayout::MultipartManifest {
                parts_count: NonZeroU32::new(self.read_u32()?)
                    .ok_or_else(|| "multipart layout parts count must be non-zero".to_string())?,
            }),
            tag => Err(format!("invalid object layout tag {tag}")),
        }
    }

    fn skip_object_lock_state(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            0 => {}
            1 => {
                self.read_u64()?;
                self.read_valid_u8("object lock retention mode", 0..=1)?;
            }
            tag => return Err(format!("invalid object retention tag {tag}")),
        }
        self.read_valid_u8("stored legal hold status", 0..=2)
    }

    fn read_object_lock_state(&mut self) -> Result<ObjectLockState, String> {
        let retention = match self.read_u8()? {
            0 => None,
            1 => {
                let retain_until_unix_seconds = self.read_u64()?;
                let mode = ObjectLockMode::from_u8(self.read_u8()?)
                    .ok_or_else(|| "invalid object lock retention mode".to_string())?;
                Some(ObjectRetention {
                    mode,
                    retain_until_unix_seconds,
                })
            }
            tag => return Err(format!("invalid object retention tag {tag}")),
        };
        let legal_hold = StoredLegalHoldStatus::from_u8(self.read_u8()?)
            .ok_or_else(|| "invalid stored legal hold status".to_string())?;
        Ok(ObjectLockState {
            retention,
            legal_hold,
        })
    }

    fn skip_object_encryption(&mut self) -> Result<(), String> {
        let encryption_type = self.read_u8()?;
        if !matches!(encryption_type, 0..=2) {
            return Err(format!("invalid object encryption type {encryption_type}"));
        }
        let has_state = match self.read_u8()? {
            0 => false,
            1 => {
                self.read_bytes()?;
                true
            }
            tag => {
                return Err(format!(
                    "invalid optional object encryption state tag {tag}"
                ))
            }
        };
        match (encryption_type, has_state) {
            (0, false) | (1 | 2, true) => Ok(()),
            (0, true) => Err("unencrypted object must not carry encryption state".to_string()),
            (1 | 2, false) => Err("encrypted object is missing encryption state".to_string()),
            _ => unreachable!("object encryption type was validated above"),
        }
    }

    fn read_object_encryption(&mut self) -> Result<ObjectEncryption, String> {
        let encryption_type = ObjectEncryptionType::from_u8(self.read_u8()?)
            .ok_or_else(|| "invalid object encryption type".to_string())?;
        let state = self.read_optional_bytes_value()?;
        ObjectEncryption::decode(encryption_type, state)
            .map_err(|reason| format!("invalid object encryption state: {reason}"))
    }

    fn skip_owner_identity(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()
    }

    fn read_owner_identity(&mut self) -> Result<OwnerIdentity, String> {
        Ok(OwnerIdentity::new(
            self.read_string("owner principal")?,
            self.read_canonical_user_id()?,
        ))
    }

    fn skip_optional_multipart_checksum_config(&mut self) -> Result<(), String> {
        self.skip_optional(|decoder| {
            decoder.read_valid_u8("multipart checksum algorithm", 0..=9)?;
            decoder.read_valid_u8("multipart checksum type", 0..=1)
        })
    }

    fn read_optional_multipart_checksum_config(
        &mut self,
    ) -> Result<Option<MultipartChecksumConfig>, String> {
        self.read_optional(|decoder| {
            let algorithm = ChecksumAlgorithm::from_u8(decoder.read_u8()?)
                .ok_or_else(|| "invalid multipart checksum algorithm".to_string())?;
            let checksum_type = ChecksumType::from_u8(decoder.read_u8()?)
                .ok_or_else(|| "invalid multipart checksum type".to_string())?;
            MultipartChecksumConfig::new(algorithm, Some(checksum_type))
                .map_err(|error| format!("invalid multipart checksum config: {error}"))
        })
    }

    fn read_bucket_write_reservation_proof(
        &mut self,
    ) -> Result<BucketWriteReservationProof, String> {
        Ok(BucketWriteReservationProof {
            bucket: self.read_bucket_name()?,
            reservation_id: self.read_string("bucket write reservation id")?,
            owner_token: self.read_string("bucket write reservation owner token")?,
            cluster_epoch: ClusterEpoch::new(self.read_u64()?)
                .ok_or_else(|| "invalid bucket write reservation cluster epoch".to_string())?,
            bucket_execution_generation: self.read_u64()?,
            bucket_incarnation_generation: self.read_u64()?,
            operation_kind: self.read_string("bucket write reservation operation kind")?,
            created_at: self.read_u64()?,
            lease_deadline: self.read_u64()?,
            target_context: self.read_optional_string("bucket write reservation target")?,
        })
    }

    fn read_optional_bucket_write_reservation_proof(
        &mut self,
    ) -> Result<Option<BucketWriteReservationProof>, String> {
        self.read_optional(Self::read_bucket_write_reservation_proof)
    }

    fn skip_object_payload_reclaim_claim_proof(&mut self) -> Result<(), String> {
        self.read_u64()?;
        self.read_u8()?;
        self.skip_str()?;
        self.skip_str()?;
        self.read_u64()?;
        Ok(())
    }

    fn read_object_payload_reclaim_claim_proof(
        &mut self,
    ) -> Result<ObjectPayloadReclaimClaimProof, String> {
        let bucket_incarnation_generation = self.read_u64()?;
        let reclaim_kind = ObjectPayloadReclaimKind::from_u8(self.read_u8()?)
            .ok_or_else(|| "invalid object payload reclaim claim kind".to_string())?;
        let claim_id = self.read_string("object payload reclaim claim id")?;
        let owner_token = self.read_string("object payload reclaim owner token")?;
        let cluster_epoch = ClusterEpoch::new(self.read_u64()?)
            .ok_or_else(|| "invalid object payload reclaim claim cluster epoch".to_string())?;
        Ok(ObjectPayloadReclaimClaimProof {
            bucket_incarnation_generation,
            reclaim_kind,
            claim_id,
            owner_token,
            cluster_epoch,
        })
    }

    fn skip_bucket_subresource_mutation(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            1 => {
                let kind = self.read_bucket_subresource_kind()?;
                self.skip_str()?;
                self.skip_bucket_subresource_aux(kind)
            }
            2 => {
                self.read_bucket_subresource_kind()?;
                Ok(())
            }
            tag => Err(format!("invalid bucket subresource mutation tag {tag}")),
        }
    }

    fn read_bucket_subresource_mutation(&mut self) -> Result<BucketSubresourceMutation, String> {
        match self.read_u8()? {
            1 => {
                let kind = self.read_bucket_subresource_kind()?;
                let body = self.read_string("bucket subresource body")?;
                let aux = self.read_bucket_subresource_aux(kind)?;
                Ok(BucketSubresourceMutation::Put { kind, body, aux })
            }
            2 => Ok(BucketSubresourceMutation::Delete {
                kind: self.read_bucket_subresource_kind()?,
            }),
            tag => Err(format!("invalid bucket subresource mutation tag {tag}")),
        }
    }

    fn read_bucket_subresource_kind(&mut self) -> Result<BucketSubresourceKind, String> {
        BucketSubresourceKind::from_u8(self.read_u8()?)
            .ok_or_else(|| "invalid bucket subresource kind".to_string())
    }

    fn skip_bucket_subresource_aux(&mut self, kind: BucketSubresourceKind) -> Result<(), String> {
        match self.read_u8()? {
            0 if matches!(
                kind,
                BucketSubresourceKind::Cors
                    | BucketSubresourceKind::Tagging
                    | BucketSubresourceKind::Lifecycle
            ) =>
            {
                Ok(())
            }
            1 if kind == BucketSubresourceKind::Policy => self.skip_bool(),
            tag => Err(format!(
                "bucket subresource kind {kind:?} does not support aux tag {tag}"
            )),
        }
    }

    fn read_bucket_subresource_aux(
        &mut self,
        kind: BucketSubresourceKind,
    ) -> Result<BucketSubresourceAux, String> {
        match self.read_u8()? {
            0 if matches!(
                kind,
                BucketSubresourceKind::Cors
                    | BucketSubresourceKind::Tagging
                    | BucketSubresourceKind::Lifecycle
            ) =>
            {
                Ok(BucketSubresourceAux::None)
            }
            1 if kind == BucketSubresourceKind::Policy => {
                Ok(BucketSubresourceAux::policy(self.read_bool()?))
            }
            tag => Err(format!(
                "bucket subresource kind {kind:?} does not support aux tag {tag}"
            )),
        }
    }

    fn skip_bucket_encryption(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            0 => {}
            1 => self.read_valid_u8("managed encryption algorithm", 1..=1)?,
            tag => return Err(format!("invalid bucket encryption tag {tag}")),
        }
        self.skip_bool()
    }

    fn read_bucket_encryption(&mut self) -> Result<BucketEncryptionConfig, String> {
        let default_encryption = match self.read_u8()? {
            0 => None,
            1 => Some(
                ManagedEncryptionAlgorithm::from_u8(self.read_u8()?)
                    .ok_or_else(|| "invalid managed encryption algorithm".to_string())?,
            ),
            tag => return Err(format!("invalid bucket encryption tag {tag}")),
        };
        Ok(BucketEncryptionConfig {
            default_encryption,
            sse_c_blocked: self.read_bool()?,
        })
    }

    fn skip_public_access_block(&mut self) -> Result<(), String> {
        self.skip_optional(|decoder| {
            decoder.skip_bool()?;
            decoder.skip_bool()?;
            decoder.skip_bool()?;
            decoder.skip_bool()
        })
    }

    fn read_public_access_block(&mut self) -> Result<Option<PublicAccessBlockConfig>, String> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(PublicAccessBlockConfig {
                block_public_acls: self.read_bool()?,
                ignore_public_acls: self.read_bool()?,
                block_public_policy: self.read_bool()?,
                restrict_public_buckets: self.read_bool()?,
            })),
            tag => Err(format!("invalid public access block optional tag {tag}")),
        }
    }

    fn skip_ownership_controls(&mut self) -> Result<(), String> {
        self.skip_optional(|decoder| decoder.read_valid_u8("bucket object ownership", 0..=2))
    }

    fn read_ownership_controls(&mut self) -> Result<Option<BucketOwnershipControls>, String> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::from_u8(self.read_u8()?)
                    .ok_or_else(|| "invalid bucket object ownership".to_string())?,
            })),
            tag => Err(format!("invalid ownership controls optional tag {tag}")),
        }
    }

    fn skip_bucket_object_lock(&mut self) -> Result<(), String> {
        self.skip_bool()?;
        match self.read_u8()? {
            0 => Ok(()),
            1 => {
                self.read_valid_u8("object lock default retention mode", 0..=1)?;
                match self.read_u8()? {
                    1 | 2 => self.read_nonzero_u32("object lock default retention period"),
                    tag => Err(format!(
                        "invalid object lock default retention period tag {tag}"
                    )),
                }
            }
            tag => Err(format!("invalid bucket object lock retention tag {tag}")),
        }
    }

    fn read_bucket_object_lock(&mut self) -> Result<BucketObjectLockConfig, String> {
        let enabled = self.read_bool()?;
        let default_retention = match self.read_u8()? {
            0 => None,
            1 => {
                let mode = ObjectLockMode::from_u8(self.read_u8()?)
                    .ok_or_else(|| "invalid object lock default retention mode".to_string())?;
                let period =
                    match self.read_u8()? {
                        1 => RetentionPeriod::Days(NonZeroU32::new(self.read_u32()?).ok_or_else(
                            || "object lock default days must be non-zero".to_string(),
                        )?),
                        2 => RetentionPeriod::Years(NonZeroU32::new(self.read_u32()?).ok_or_else(
                            || "object lock default years must be non-zero".to_string(),
                        )?),
                        tag => {
                            return Err(format!(
                                "invalid object lock default retention period tag {tag}"
                            ))
                        }
                    };
                Some(ObjectLockDefaultRetention { mode, period })
            }
            tag => return Err(format!("invalid bucket object lock retention tag {tag}")),
        };
        Ok(BucketObjectLockConfig {
            enabled,
            default_retention,
        })
    }

    fn finish(&self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("metadata command log entry has trailing bytes".to_string())
        }
    }
}

fn encode_create_bucket(out: &mut Vec<u8>, command: &CreateBucketCommand) {
    encode_bucket_record(out, &command.bucket);
}

fn encode_put_bucket_versioning(out: &mut Vec<u8>, command: &PutBucketVersioningCommand) {
    encode_bucket_record(out, &command.bucket);
}

fn encode_put_bucket_acl(out: &mut Vec<u8>, command: &PutBucketAclCommand) {
    encode_bucket_record(out, &command.bucket);
}

fn encode_put_bucket_property(out: &mut Vec<u8>, command: &PutBucketPropertyCommand) {
    encode_bucket_record(out, &command.bucket);
    put_u8(out, command.effect as u8);
}

fn encode_put_bucket_subresource(out: &mut Vec<u8>, command: &PutBucketSubresourceCommand) {
    put_str(out, command.name.as_str());
    encode_bucket_subresource_mutation(out, &command.mutation);
    put_u64(out, command.bucket_execution_generation);
}

fn encode_mark_bucket_deleting(out: &mut Vec<u8>, command: &MarkBucketDeletingCommand) {
    encode_bucket_record(out, &command.bucket);
}

fn encode_delete_finalized_bucket(out: &mut Vec<u8>, command: &DeleteFinalizedBucketCommand) {
    put_str(out, command.bucket.as_str());
    put_u64(out, command.bucket_execution_generation);
    put_u64(out, command.bucket_incarnation_generation);
}

fn encode_reserve_object_generation(out: &mut Vec<u8>, command: &ReserveObjectGenerationCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    put_str(out, command.reservation_id.as_str());
    put_u64(out, command.generation_id.get());
    put_u64(out, command.created_at_millis);
}

fn encode_release_object_generation(out: &mut Vec<u8>, command: &ReleaseObjectGenerationCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    put_str(out, command.reservation_id.as_str());
}

fn encode_reserve_object_version(out: &mut Vec<u8>, command: &ReserveObjectVersionCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    encode_version_id(out, command.version_id);
}

fn encode_commit_direct_put_object(out: &mut Vec<u8>, command: &CommitDirectPutObjectCommand) {
    encode_put_live_object(out, &command.object);
    put_u32(out, command.segments.len() as u32);
    for segment in &command.segments {
        encode_object_segment(out, segment);
    }
    put_str(out, command.generation_reservation_id.as_str());
    put_u64(out, command.write_sequence);
    put_u64(out, command.last_modified_millis);
    match &command.stale_payload {
        None => put_u8(out, 0),
        Some(ObjectPayloadReclaimCommand::Segments(reclaim)) => {
            put_u8(out, 1);
            encode_object_segments_reclaim(out, reclaim);
        }
        Some(ObjectPayloadReclaimCommand::Multipart(reclaim)) => {
            put_u8(out, 2);
            encode_multipart_reclaim(out, reclaim);
        }
    }
    encode_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn encode_commit_multipart_object(out: &mut Vec<u8>, command: &CommitMultipartObjectCommand) {
    put_str(out, command.upload_id.as_str());
    put_bytes(out, command.completion_fingerprint.as_bytes());
    encode_put_live_object(out, &command.object);
    put_u32(out, command.parts.len() as u32);
    for part in &command.parts {
        encode_object_part(out, part);
    }
    put_u32(out, command.selected_streaming_segments.len() as u32);
    for segment in &command.selected_streaming_segments {
        encode_multipart_part_segment(out, segment);
    }
    put_u32(out, command.omitted_parts.len() as u32);
    for part in &command.omitted_parts {
        encode_multipart_part(out, part);
    }
    put_u32(out, command.omitted_streaming_segments.len() as u32);
    for segment in &command.omitted_streaming_segments {
        encode_multipart_part_segment(out, segment);
    }
    put_u32(out, command.stream_uploads.len() as u32);
    for session in &command.stream_uploads {
        encode_terminal_stream_cleanup(out, session);
    }
    put_u32(out, command.stream_upload_segments.len() as u32);
    for segment in &command.stream_upload_segments {
        encode_stream_upload_segment(out, segment);
    }
    put_u64(out, command.write_sequence);
    put_u64(out, command.last_modified_millis);
    match &command.stale_payload {
        None => put_u8(out, 0),
        Some(ObjectPayloadReclaimCommand::Segments(reclaim)) => {
            put_u8(out, 1);
            encode_object_segments_reclaim(out, reclaim);
        }
        Some(ObjectPayloadReclaimCommand::Multipart(reclaim)) => {
            put_u8(out, 2);
            encode_multipart_reclaim(out, reclaim);
        }
    }
    encode_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn encode_delete_object_version(out: &mut Vec<u8>, command: &DeleteObjectVersionCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    encode_version_id(out, command.version_id);
    match &command.target {
        DeleteObjectVersionTarget::DeleteMarker { write_sequence } => {
            put_u8(out, 1);
            put_u64(out, *write_sequence);
        }
        DeleteObjectVersionTarget::Live {
            generation_id,
            layout,
            payload,
        } => {
            put_u8(out, 2);
            put_u64(out, generation_id.get());
            encode_object_layout(out, *layout);
            match payload {
                ObjectPayloadReclaimCommand::Segments(reclaim) => {
                    put_u8(out, 1);
                    encode_object_segments_reclaim(out, reclaim);
                }
                ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                    put_u8(out, 2);
                    encode_multipart_reclaim(out, reclaim);
                }
            }
        }
    }
    encode_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn encode_insert_delete_marker(out: &mut Vec<u8>, command: &InsertDeleteMarkerCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    encode_version_id(out, command.version_id);
    put_str(out, &command.owner.principal);
    put_str(out, command.owner.canonical_id.as_str());
    put_u64(out, command.write_sequence);
    put_u64(out, command.last_modified_millis);
    match &command.stale_payload {
        None => put_u8(out, 0),
        Some(ObjectPayloadReclaimCommand::Segments(reclaim)) => {
            put_u8(out, 1);
            encode_object_segments_reclaim(out, reclaim);
        }
        Some(ObjectPayloadReclaimCommand::Multipart(reclaim)) => {
            put_u8(out, 2);
            encode_multipart_reclaim(out, reclaim);
        }
    }
    encode_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn encode_put_object_metadata(out: &mut Vec<u8>, command: &PutObjectMetadataCommand) {
    encode_live_object_record(out, &command.object);
    encode_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn encode_create_stream_upload(out: &mut Vec<u8>, command: &CreateStreamUploadCommand) {
    encode_stream_upload_command_record(out, &command.session);
    put_u64(out, command.initial_next_segment_vid.get());
    encode_optional_u64(out, command.cleanup_after);
    encode_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn encode_append_stream_segment(out: &mut Vec<u8>, command: &AppendStreamSegmentCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    encode_stream_upload_segment(out, &command.segment);
}

fn encode_abort_stream_upload(out: &mut Vec<u8>, command: &AbortStreamUploadCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    put_str(out, command.session_id.as_str());
    put_u32(out, command.staged_segments.len() as u32);
    for segment in &command.staged_segments {
        encode_stream_upload_segment(out, segment);
    }
    encode_optional_bucket_write_reservation_proof(
        out,
        command.stream_create_bucket_write_reservation.as_ref(),
    );
}

fn encode_commit_stream_part(out: &mut Vec<u8>, command: &CommitStreamPartCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    put_str(out, command.session_id.as_str());
    encode_multipart_upload(out, &command.upload);
    encode_multipart_part(out, &command.part);
    put_u32(out, command.segments.len() as u32);
    for segment in &command.segments {
        encode_multipart_part_segment(out, segment);
    }
    match &command.existing_part {
        None => put_u8(out, 0),
        Some(part) => {
            put_u8(out, 1);
            encode_multipart_part(out, part);
        }
    }
    put_u32(out, command.displaced_segments.len() as u32);
    for segment in &command.displaced_segments {
        encode_multipart_part_segment(out, segment);
    }
    encode_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn encode_create_multipart_upload(out: &mut Vec<u8>, command: &CreateMultipartUploadCommand) {
    encode_multipart_upload(out, &command.upload);
    encode_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn encode_abort_multipart_upload(out: &mut Vec<u8>, command: &AbortMultipartUploadCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    put_str(out, command.upload_id.as_str());
    encode_abort_multipart_upload_cleanup(out, &command.cleanup);
    encode_bucket_write_reservation_proof(out, &command.bucket_write_reservation);
}

fn encode_delete_object_payload_reclaim(
    out: &mut Vec<u8>,
    command: &DeleteObjectPayloadReclaimCommand,
) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    put_u64(out, command.generation_id.get());
    encode_object_payload_reclaim(out, &command.payload);
    encode_object_payload_reclaim_claim_proof(out, &command.reclaim_claim);
}

fn encode_advance_multipart_completion_barrier(
    out: &mut Vec<u8>,
    command: &AdvanceMultipartCompletionBarrierCommand,
) {
    put_str(out, command.bucket.as_str());
    put_u64(out, command.barrier_sequence);
}

fn encode_object_payload_reclaim(out: &mut Vec<u8>, reclaim: &ObjectPayloadReclaimCommand) {
    match reclaim {
        ObjectPayloadReclaimCommand::Segments(reclaim) => {
            put_u8(out, 0);
            encode_object_segments_reclaim(out, reclaim);
        }
        ObjectPayloadReclaimCommand::Multipart(reclaim) => {
            put_u8(out, 1);
            encode_multipart_reclaim(out, reclaim);
        }
    }
}

fn encode_abort_multipart_upload_cleanup(out: &mut Vec<u8>, cleanup: &AbortMultipartUploadCleanup) {
    encode_multipart_upload(out, &cleanup.upload);
    put_u32(out, cleanup.parts.len() as u32);
    for part in &cleanup.parts {
        encode_multipart_part(out, part);
    }
    put_u32(out, cleanup.streaming_segments.len() as u32);
    for segment in &cleanup.streaming_segments {
        encode_multipart_part_segment(out, segment);
    }
    put_u32(out, cleanup.stream_uploads.len() as u32);
    for session in &cleanup.stream_uploads {
        encode_terminal_stream_cleanup(out, session);
    }
    put_u32(out, cleanup.stream_upload_segments.len() as u32);
    for segment in &cleanup.stream_upload_segments {
        encode_stream_upload_segment(out, segment);
    }
}

fn encode_stream_upload_command_record(out: &mut Vec<u8>, session: &StreamUploadCommandRecord) {
    put_str(out, session.session_id.as_str());
    put_str(out, session.bucket.as_str());
    put_str(out, session.key.as_str());
    encode_stream_upload_target(out, &session.target);
    put_u8(out, session.state as u8);
    put_u64(out, session.created_at);
    encode_object_encryption(out, &session.encryption);
}

fn encode_terminal_stream_cleanup(out: &mut Vec<u8>, session: &TerminalStreamCleanupRecord) {
    put_str(out, session.session_id.as_str());
    put_str(out, session.bucket.as_str());
    put_str(out, session.key.as_str());
    encode_stream_upload_target(out, &session.target);
    put_u8(out, session.state as u8);
    put_u64(out, session.created_at);
    encode_object_encryption(out, &session.encryption);
}

fn encode_multipart_upload(out: &mut Vec<u8>, upload: &MultipartUploadRecord) {
    put_str(out, upload.upload_id.as_str());
    put_str(out, upload.bucket.as_str());
    put_str(out, upload.key.as_str());
    put_u64(out, upload.initiated_at);
    put_u8(out, upload.state as u8);
    encode_optional_str(out, upload.tags.as_ref().map(|tags| tags.as_str()));
    put_bytes(out, upload.metadata_blob.as_slice());
    put_bytes(out, upload.system_metadata_blob.as_slice());
    encode_owner_identity(out, &upload.initiator);
    encode_owner_identity(out, &upload.owner);
    put_str(out, &upload.acl_grants.serialized());
    put_bool(out, upload.public_read);
    put_u64(out, upload.object_generation_id.get());
    encode_multipart_object_identity(out, upload.initiated_object_identity);
    encode_object_lock_state(out, upload.object_lock);
    encode_optional_multipart_checksum_config(out, upload.checksum);
    encode_object_encryption(out, &upload.encryption);
}

fn encode_multipart_object_identity(
    out: &mut Vec<u8>,
    identity: Option<crate::MultipartObjectIdentity>,
) {
    match identity {
        None => put_u8(out, 0),
        Some(crate::MultipartObjectIdentity::Live {
            version_id,
            generation_id,
        }) => {
            put_u8(out, 1);
            encode_version_id(out, version_id);
            put_u64(out, generation_id.get());
        }
        Some(crate::MultipartObjectIdentity::DeleteMarker {
            version_id,
            write_sequence,
        }) => {
            put_u8(out, 2);
            encode_version_id(out, version_id);
            put_u64(out, write_sequence);
        }
    }
}

fn encode_put_live_object(out: &mut Vec<u8>, object: &PutLiveObjectReq) {
    put_str(out, object.bucket.as_str());
    put_str(out, object.key.as_str());
    encode_version_id(out, object.version_id);
    put_str(out, &object.owner.principal);
    put_str(out, object.owner.canonical_id.as_str());
    put_str(out, &object.acl_grants.serialized());
    put_bool(out, object.public_read);
    put_u64(out, object.generation_id.get());
    put_u64(out, object.size);
    encode_object_etag(out, object.etag);
    put_u8(out, object.ec.k);
    put_u8(out, object.ec.m);
    encode_object_layout(out, object.layout);
    encode_optional_str(out, object.tags.as_deref());
    encode_optional_bytes(
        out,
        object.metadata_blob.as_ref().map(|blob| blob.as_slice()),
    );
    encode_optional_bytes(
        out,
        object
            .system_metadata_blob
            .as_ref()
            .map(|blob| blob.as_slice()),
    );
    encode_object_lock_state(out, object.object_lock);
    encode_object_encryption(out, &object.encryption);
}

fn encode_live_object_record(out: &mut Vec<u8>, object: &LiveObjectRecord) {
    put_str(out, object.bucket.as_str());
    put_str(out, object.key.as_str());
    encode_version_id(out, object.version_id);
    encode_owner_identity(out, &object.owner);
    put_str(out, &object.acl_grants.serialized());
    put_bool(out, object.public_read);
    put_u64(out, object.generation_id.get());
    put_u64(out, object.size);
    encode_object_etag(out, object.etag);
    put_u64(out, object.last_modified);
    encode_optional_u64(out, object.became_noncurrent_at);
    put_u8(out, object.storage_class as u8);
    put_u8(out, object.ec.k);
    put_u8(out, object.ec.m);
    encode_object_layout(out, object.layout);
    encode_optional_str(out, object.tags.as_deref());
    encode_optional_bytes(
        out,
        object.metadata_blob.as_ref().map(|blob| blob.as_slice()),
    );
    encode_optional_bytes(
        out,
        object
            .system_metadata_blob
            .as_ref()
            .map(|blob| blob.as_slice()),
    );
    encode_object_lock_state(out, object.object_lock);
    encode_object_encryption(out, &object.encryption);
}

fn encode_bucket_record(out: &mut Vec<u8>, bucket: &BucketRecord) {
    put_str(out, bucket.name.as_str());
    put_str(out, &bucket.owner_principal);
    put_str(out, bucket.owner_canonical_id.as_str());
    put_u64(out, bucket.created_at);
    put_u16(out, bucket.region);
    put_u8(out, bucket.state as u8);
    put_u8(out, bucket.versioning as u8);
    encode_object_lock(out, bucket.object_lock);
    put_str(out, &bucket.acl_grants.serialized());
    put_bool(out, bucket.public_read);
    put_bool(out, bucket.public_write);
    encode_public_access_block(out, bucket.public_access_block);
    encode_ownership_controls(out, bucket.ownership_controls);
    put_bool(out, bucket.bucket_policy_public);
    put_u64(out, bucket.bucket_policy_generation);
    put_u64(out, bucket.bucket_lifecycle_generation);
    put_u64(out, bucket.bucket_execution_generation);
    put_u64(out, bucket.bucket_incarnation_generation);
    put_bytes(out, bucket.multipart_upload_id_key.as_bytes());
    put_u64(out, bucket.multipart_completion_barrier_sequence);
    put_bool(out, bucket.bucket_abac_enabled);
    encode_bucket_encryption(out, bucket.encryption);
}

fn encode_object_segment(out: &mut Vec<u8>, segment: &ObjectSegmentRecord) {
    put_str(out, segment.bucket.as_str());
    put_str(out, segment.key.as_str());
    encode_version_id(out, segment.version_id);
    put_u32(out, segment.segment_index);
    put_u64(out, segment.size);
    put_u64(out, segment.segment_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
    put_u64(out, segment.placement_cluster_epoch.get());
    put_u8(out, segment.ec_k);
    put_u8(out, segment.ec_m);
}

fn encode_object_part(out: &mut Vec<u8>, part: &ObjectPartRecord) {
    put_str(out, part.bucket.as_str());
    put_str(out, part.key.as_str());
    encode_version_id(out, part.version_id);
    put_u32(out, part.part_number);
    put_u64(out, part.size);
    put_u64(out, part.payload_crc64);
    put_bytes(out, &part.etag);
    put_u8(out, part.etag_kind as u8);
    put_bytes(out, &part.part_okh);
    put_u64(out, part.part_vid.get());
    put_u64(out, part.placement_cluster_epoch.get());
    put_u8(out, part.ec_k);
    put_u8(out, part.ec_m);
    put_u32(out, part.data_pg_id);
    encode_optional_bytes(
        out,
        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
    );
}

fn encode_multipart_part(out: &mut Vec<u8>, part: &MultipartPartRecord) {
    put_str(out, part.upload_id.as_str());
    put_u32(out, part.part_number);
    put_u32(out, part.generation);
    put_u64(out, part.size);
    put_u64(out, part.payload_crc64);
    put_bytes(out, &part.etag);
    put_u8(out, part.etag_kind as u8);
    put_bytes(out, &part.part_okh);
    put_u64(out, part.part_vid.get());
    put_u64(out, part.placement_cluster_epoch.get());
    put_u8(out, part.ec_k);
    put_u8(out, part.ec_m);
    put_u64(out, part.last_modified);
    encode_optional_bytes(
        out,
        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
    );
}

fn encode_multipart_part_segment(out: &mut Vec<u8>, segment: &MultipartPartSegmentRecord) {
    put_str(out, segment.bucket.as_str());
    put_str(out, segment.key.as_str());
    put_str(out, segment.upload_id.as_str());
    put_u64(out, segment.version_id);
    put_u32(out, segment.part_number);
    put_u32(out, segment.segment_index);
    put_u64(out, segment.size);
    put_u64(out, segment.segment_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
    put_u64(out, segment.placement_cluster_epoch.get());
    put_u8(out, segment.ec_k);
    put_u8(out, segment.ec_m);
}

fn encode_stream_upload_target(out: &mut Vec<u8>, target: &StreamUploadTarget) {
    match target {
        StreamUploadTarget::PutObject => put_u8(out, 0),
        StreamUploadTarget::UploadPart {
            upload_id,
            part_number,
        } => {
            put_u8(out, 1);
            put_str(out, upload_id.as_str());
            put_u32(out, *part_number);
        }
    }
}

fn encode_stream_upload_segment(out: &mut Vec<u8>, segment: &StreamUploadSegmentRecord) {
    put_str(out, segment.session_id.as_str());
    put_u32(out, segment.segment_index);
    put_u64(out, segment.size);
    put_u64(out, segment.segment_crc64);
    put_u64(out, segment.payload_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
    put_u64(out, segment.placement_cluster_epoch.get());
    put_u8(out, segment.ec_k);
    put_u8(out, segment.ec_m);
}

fn encode_object_segments_reclaim(out: &mut Vec<u8>, reclaim: &ObjectSegmentsReclaimRecord) {
    put_str(out, reclaim.bucket.as_str());
    put_str(out, reclaim.key.as_str());
    put_u64(out, reclaim.generation_id.get());
    put_u64(out, reclaim.created_at);
    put_u32(out, reclaim.segments.len() as u32);
    for segment in &reclaim.segments {
        put_u32(out, segment.segment_index);
        put_bytes(out, &segment.segment_okh);
        put_u64(out, segment.segment_vid.get());
        put_u32(out, segment.data_pg_id);
        put_u8(out, segment.ec.k);
        put_u8(out, segment.ec.m);
    }
}

fn encode_multipart_reclaim(out: &mut Vec<u8>, reclaim: &MultipartReclaimRecord) {
    put_str(out, reclaim.bucket.as_str());
    put_str(out, reclaim.key.as_str());
    put_u64(out, reclaim.generation_id.get());
    put_u64(out, reclaim.created_at);
    put_u32(out, reclaim.parts.len() as u32);
    for part in &reclaim.parts {
        match part {
            MultipartReclaimPartRecord::ShardSet {
                part_number,
                part_okh,
                part_vid,
                data_pg_id,
                ec,
            } => {
                put_u8(out, 1);
                put_u32(out, *part_number);
                put_bytes(out, part_okh);
                put_u64(out, part_vid.get());
                put_u32(out, *data_pg_id);
                put_u8(out, ec.k);
                put_u8(out, ec.m);
            }
            MultipartReclaimPartRecord::Segments {
                part_number,
                segments,
            } => {
                put_u8(out, 2);
                put_u32(out, *part_number);
                put_u32(out, segments.len() as u32);
                for segment in segments {
                    put_u32(out, segment.part_number);
                    put_u32(out, segment.segment_index);
                    put_bytes(out, &segment.segment_okh);
                    put_u64(out, segment.segment_vid.get());
                    put_u32(out, segment.data_pg_id);
                    put_u8(out, segment.ec.k);
                    put_u8(out, segment.ec.m);
                }
            }
        }
    }
}

fn encode_version_id(out: &mut Vec<u8>, version_id: VersionId) {
    put_u64(out, version_id.to_u64());
}

fn encode_object_etag(out: &mut Vec<u8>, etag: ObjectEtag) {
    match etag {
        ObjectEtag::SinglePart(crc64) => {
            put_u8(out, 1);
            put_bytes(out, &crc64);
        }
        ObjectEtag::MultipartComposite { crc64, parts } => {
            put_u8(out, 2);
            put_bytes(out, &crc64);
            put_u32(out, parts.get());
        }
    }
}

fn encode_object_layout(out: &mut Vec<u8>, layout: ObjectLayout) {
    match layout {
        ObjectLayout::Standard => put_u8(out, 1),
        ObjectLayout::MultipartManifest { parts_count } => {
            put_u8(out, 2);
            put_u32(out, parts_count.get());
        }
    }
}

fn encode_object_lock_state(out: &mut Vec<u8>, object_lock: ObjectLockState) {
    match object_lock.retention {
        None => put_u8(out, 0),
        Some(retention) => {
            put_u8(out, 1);
            put_u64(out, retention.retain_until_unix_seconds);
            put_u8(
                out,
                match retention.mode {
                    ObjectLockMode::Governance => 0,
                    ObjectLockMode::Compliance => 1,
                },
            );
        }
    }
    put_u8(out, object_lock.legal_hold as u8);
}

fn encode_object_encryption(out: &mut Vec<u8>, encryption: &ObjectEncryption) {
    put_u8(out, encryption.encryption_type() as u8);
    encode_optional_bytes(out, encryption.encode_state().as_deref());
}

fn encode_owner_identity(out: &mut Vec<u8>, owner: &OwnerIdentity) {
    put_str(out, &owner.principal);
    put_str(out, owner.canonical_id.as_str());
}

fn encode_optional_multipart_checksum_config(
    out: &mut Vec<u8>,
    checksum: Option<crate::MultipartChecksumConfig>,
) {
    match checksum {
        None => put_u8(out, 0),
        Some(checksum) => {
            put_u8(out, 1);
            put_u8(out, checksum.algorithm() as u8);
            put_u8(out, checksum.checksum_type() as u8);
        }
    }
}

fn encode_optional_str(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_str(out, value);
        }
    }
}

fn encode_optional_bytes(out: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_bytes(out, value);
        }
    }
}

fn encode_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_u64(out, value);
        }
    }
}

fn encode_optional_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_str(out, value);
        }
    }
}

fn encode_bucket_write_reservation_proof(out: &mut Vec<u8>, proof: &BucketWriteReservationProof) {
    put_str(out, proof.bucket.as_str());
    put_str(out, &proof.reservation_id);
    put_str(out, &proof.owner_token);
    put_u64(out, proof.cluster_epoch.get());
    put_u64(out, proof.bucket_execution_generation);
    put_u64(out, proof.bucket_incarnation_generation);
    put_str(out, &proof.operation_kind);
    put_u64(out, proof.created_at);
    put_u64(out, proof.lease_deadline);
    encode_optional_string(out, proof.target_context.as_deref());
}

fn encode_optional_bucket_write_reservation_proof(
    out: &mut Vec<u8>,
    proof: Option<&BucketWriteReservationProof>,
) {
    match proof {
        Some(proof) => {
            put_u8(out, 1);
            encode_bucket_write_reservation_proof(out, proof);
        }
        None => put_u8(out, 0),
    }
}

fn encode_object_payload_reclaim_claim_proof(
    out: &mut Vec<u8>,
    proof: &ObjectPayloadReclaimClaimProof,
) {
    put_u64(out, proof.bucket_incarnation_generation);
    put_u8(out, proof.reclaim_kind as u8);
    put_str(out, &proof.claim_id);
    put_str(out, &proof.owner_token);
    put_u64(out, proof.cluster_epoch.get());
}

fn encode_bucket_subresource_mutation(out: &mut Vec<u8>, mutation: &BucketSubresourceMutation) {
    match mutation {
        BucketSubresourceMutation::Put { kind, body, aux } => {
            put_u8(out, 1);
            encode_bucket_subresource_kind(out, *kind);
            put_str(out, body);
            encode_bucket_subresource_aux(out, *aux);
        }
        BucketSubresourceMutation::Delete { kind } => {
            put_u8(out, 2);
            encode_bucket_subresource_kind(out, *kind);
        }
    }
}

fn encode_bucket_subresource_kind(out: &mut Vec<u8>, kind: BucketSubresourceKind) {
    put_u8(out, kind as u8);
}

fn encode_bucket_subresource_aux(out: &mut Vec<u8>, aux: BucketSubresourceAux) {
    match aux {
        BucketSubresourceAux::None => put_u8(out, 0),
        BucketSubresourceAux::Policy { is_public } => {
            put_u8(out, 1);
            put_bool(out, is_public);
        }
    }
}

fn encode_bucket_encryption(out: &mut Vec<u8>, config: BucketEncryptionConfig) {
    match config.default_encryption {
        None => put_u8(out, 0),
        Some(ManagedEncryptionAlgorithm::Aes256) => {
            put_u8(out, 1);
            put_u8(out, ManagedEncryptionAlgorithm::Aes256 as u8);
        }
    }
    put_bool(out, config.sse_c_blocked);
}

fn encode_public_access_block(out: &mut Vec<u8>, config: Option<PublicAccessBlockConfig>) {
    match config {
        None => put_u8(out, 0),
        Some(config) => {
            put_u8(out, 1);
            put_bool(out, config.block_public_acls);
            put_bool(out, config.ignore_public_acls);
            put_bool(out, config.block_public_policy);
            put_bool(out, config.restrict_public_buckets);
        }
    }
}

fn encode_ownership_controls(out: &mut Vec<u8>, config: Option<BucketOwnershipControls>) {
    match config {
        None => put_u8(out, 0),
        Some(config) => {
            put_u8(out, 1);
            put_u8(
                out,
                match config.object_ownership {
                    BucketObjectOwnership::BucketOwnerEnforced => 0,
                    BucketObjectOwnership::BucketOwnerPreferred => 1,
                    BucketObjectOwnership::ObjectWriter => 2,
                },
            );
        }
    }
}

fn encode_object_lock(out: &mut Vec<u8>, object_lock: BucketObjectLockConfig) {
    put_bool(out, object_lock.enabled);
    match object_lock.default_retention {
        None => put_u8(out, 0),
        Some(ObjectLockDefaultRetention { mode, period }) => {
            put_u8(out, 1);
            put_u8(
                out,
                match mode {
                    ObjectLockMode::Governance => 0,
                    ObjectLockMode::Compliance => 1,
                },
            );
            match period {
                RetentionPeriod::Days(days) => {
                    put_u8(out, 1);
                    put_u32(out, days.get());
                }
                RetentionPeriod::Years(years) => {
                    put_u8(out, 2);
                    put_u32(out, years.get());
                }
            }
        }
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn put_str(out: &mut Vec<u8>, value: &str) {
    put_bytes(out, value.as_bytes());
}

fn put_bool(out: &mut Vec<u8>, value: bool) {
    put_u8(out, u8::from(value));
}

fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::types::{
        ChecksumAlgorithm, ChecksumType, EcShape, MultipartChecksumConfig,
        MultipartReclaimPartSegmentRecord, ObjectSegmentsReclaimSegmentRecord, OwnerIdentity,
        SerializedMetadataBlob, SerializedSystemMetadataBlob, SseS3ObjectState, StorageClass,
        SSE_S3_CHECKSUM_NONCE_LEN, SSE_S3_SEGMENT_NONCE_PREFIX_LEN, SSE_S3_WRAPPED_DEK_LEN,
        SSE_S3_WRAP_NONCE_LEN,
    };

    #[test]
    fn metadata_command_publisher_registry_matches_guide() {
        let registered = MetadataCommandPublisherId::ALL
            .iter()
            .map(|id| {
                let descriptor = id.descriptor();
                (
                    descriptor.canonical_name.to_string(),
                    (
                        descriptor.command_kind.to_string(),
                        format!("{:?}", descriptor.class),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            registered.len(),
            MetadataCommandPublisherId::ALL.len(),
            "canonical publisher names must be unique"
        );

        let guide = include_str!("../../../guides/metadata-command-stream.md");
        let table = guide
            .split("Current production pending-command publishers:")
            .nth(1)
            .expect("publisher table heading")
            .split("Adding a production call site")
            .next()
            .expect("publisher table terminator");
        let documented = table
            .lines()
            .filter(|line| line.starts_with("| `"))
            .map(|line| {
                let columns = line.split('|').map(str::trim).collect::<Vec<_>>();
                assert!(columns.len() >= 5, "malformed publisher table row: {line}");
                (
                    columns[1].trim_matches('`').to_string(),
                    (
                        columns[2].replace('`', ""),
                        columns[3].trim_matches('`').to_string(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            documented.len(),
            table.lines().filter(|line| line.starts_with("| `")).count(),
            "guide publisher names must be unique"
        );
        assert_eq!(documented, registered);
    }

    fn test_bucket_record(name: &str, generation: u64) -> BucketRecord {
        let owner = CanonicalUserId::from_principal("owner");
        let acl_grants = AclGrants::default();
        let mut bucket = BucketRecord::from_create_config(
            &CreateBucketConfig {
                name,
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: true,
                versioning: BucketVersioningState::Enabled,
                object_lock: BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            },
            123,
            generation,
        )
        .unwrap();
        bucket.multipart_upload_id_key = MultipartUploadIdKey::from_bytes([0x42; 32]);
        bucket
    }

    fn assert_applied_log_decoder_accepts(envelope: &MetadataCommandEnvelope) {
        let header = decode_metadata_command_log_entry_header(&envelope.command_bytes())
            .unwrap_or_else(|error| {
                panic!(
                    "applied {} command bytes must decode: {error}",
                    envelope.payload().kind_name()
                )
            });
        assert_eq!(header.id(), envelope.id());
        assert_eq!(header.kind(), MetadataCommandLogEntryKind::Applied);

        let mut trailing_bytes = envelope.command_bytes();
        trailing_bytes.push(0);
        assert!(
            decode_metadata_command_log_entry_header(&trailing_bytes).is_err(),
            "applied command decoder must reject trailing bytes"
        );
    }

    fn assert_full_envelope_decoder_round_trips(envelope: &MetadataCommandEnvelope) {
        let decoded =
            decode_metadata_command_envelope(&envelope.command_bytes()).unwrap_or_else(|error| {
                panic!(
                    "full {} metadata command envelope must decode: {error}",
                    envelope.payload().kind_name()
                )
            });
        assert_eq!(decoded, *envelope);
    }

    #[test]
    fn commit_direct_put_object_rejects_missing_bucket_write_reservation_proof() {
        let bucket = BucketName::try_from("direct-proof-required".to_string()).unwrap();
        let key = ObjectKey::try_from("key".to_string()).unwrap();
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "direct-proof-required-reservation".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 7,
            bucket_incarnation_generation: 7,
            operation_kind: "direct-put-commit".to_string(),
            created_at: 10,
            lease_deadline: 20,
            target_context: Some(key.as_str().to_string()),
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
                object: PutLiveObjectReq {
                    bucket,
                    key,
                    version_id: VersionId::Null,
                    owner: OwnerIdentity::from_principal("owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 0,
                    etag: ObjectEtag::single_part(0),
                    ec: EcShape { k: 2, m: 1 },
                    layout: ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: Some(SerializedMetadataBlob::default()),
                    system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                },
                segments: Vec::new(),
                generation_reservation_id: SessionId::try_from("53".repeat(16)).unwrap(),
                write_sequence: 1,
                last_modified_millis: 2,
                stale_payload: None,
                bucket_write_reservation: proof.clone(),
            })),
        );

        let mut proof_bytes = Vec::new();
        encode_bucket_write_reservation_proof(&mut proof_bytes, &proof);
        let required_proof_suffix = proof_bytes;
        let mut proofless_bytes = command.command_bytes();
        assert!(proofless_bytes.ends_with(&required_proof_suffix));
        proofless_bytes.truncate(proofless_bytes.len() - required_proof_suffix.len());

        assert!(
            decode_metadata_command_envelope(&proofless_bytes).is_err(),
            "proofless direct PUT command bytes must fail full envelope decode"
        );
        assert!(
            decode_metadata_command_log_entry_header(&proofless_bytes).is_err(),
            "proofless direct PUT command bytes must fail applied-row validation"
        );
    }

    #[test]
    fn direct_put_stream_session_match_ignores_object_generation() {
        let bucket = BucketName::try_from("direct-stream-session-match").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        let session_id = SessionId::try_from("54".repeat(16)).unwrap();
        let generation_id = GenerationId::new(10).unwrap();
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "direct-stream-session-reservation".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 7,
            bucket_incarnation_generation: 7,
            operation_kind: "direct-put-commit".to_string(),
            created_at: 10,
            lease_deadline: 20,
            target_context: Some(key.as_str().to_string()),
        };
        let command = CommitDirectPutObjectCommand {
            object: PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: OwnerIdentity::from_principal("owner"),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id,
                size: 0,
                etag: ObjectEtag::single_part(0),
                ec: EcShape { k: 2, m: 1 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: Some(SerializedMetadataBlob::default()),
                system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            segments: Vec::new(),
            generation_reservation_id: session_id.clone(),
            write_sequence: 1,
            last_modified_millis: 2,
            stale_payload: None,
            bucket_write_reservation: proof,
        };
        let other_generation_id = GenerationId::new(generation_id.get() + 1).unwrap();

        assert!(command.matches_stream_session(&bucket, &key, &session_id));
        assert!(
            !command.matches_request(&bucket, &key, &session_id, other_generation_id),
            "request matching must still include object generation"
        );
    }

    #[test]
    fn create_stream_upload_rejects_missing_bucket_write_reservation_proof() {
        let bucket = BucketName::try_from("stream-proof-required".to_string()).unwrap();
        let key = ObjectKey::try_from("key".to_string()).unwrap();
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "stream-proof-required-reservation".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 7,
            bucket_incarnation_generation: 7,
            operation_kind: PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string(),
            created_at: 10,
            lease_deadline: 20,
            target_context: Some(key.as_str().to_string()),
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    CreateStreamUploadReq {
                        session_id: SessionId::try_from("54".repeat(16)).unwrap(),
                        bucket,
                        key,
                        target: StreamUploadTarget::PutObject,
                        encryption: ObjectEncryption::None,
                    },
                    11,
                    proof.clone(),
                ),
            )),
        );

        let mut proof_bytes = Vec::new();
        encode_bucket_write_reservation_proof(&mut proof_bytes, &proof);
        let mut proofless_bytes = command.command_bytes();
        assert!(proofless_bytes.ends_with(&proof_bytes));
        proofless_bytes.truncate(proofless_bytes.len() - proof_bytes.len());

        assert!(
            decode_metadata_command_envelope(&proofless_bytes).is_err(),
            "proofless stream-create command bytes must fail full envelope decode"
        );
        assert!(
            decode_metadata_command_log_entry_header(&proofless_bytes).is_err(),
            "proofless stream-create command bytes must fail applied-row validation"
        );
    }

    #[test]
    fn commit_multipart_object_rejects_missing_bucket_write_reservation_proof() {
        let bucket = BucketName::try_from("multipart-proof-required".to_string()).unwrap();
        let key = ObjectKey::try_from("key".to_string()).unwrap();
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "multipart-proof-required-reservation".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 7,
            bucket_incarnation_generation: 7,
            operation_kind: "complete-multipart-upload".to_string(),
            created_at: 10,
            lease_deadline: 20,
            target_context: Some(key.as_str().to_string()),
        };
        let parts_count = std::num::NonZeroU32::new(1).unwrap();
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CommitMultipartObject(Box::new(CommitMultipartObjectCommand {
                upload_id: UploadId::try_from("u".repeat(128)).unwrap(),
                completion_fingerprint: MultipartCompletionFingerprint::from_bytes([0x33; 32]),
                bucket_write_reservation: proof.clone(),
                object: PutLiveObjectReq {
                    bucket,
                    key,
                    version_id: VersionId::Null,
                    owner: OwnerIdentity::from_principal("owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 0,
                    etag: ObjectEtag::MultipartComposite {
                        crc64: [0; 8],
                        parts: parts_count,
                    },
                    ec: EcShape { k: 0, m: 0 },
                    layout: ObjectLayout::MultipartManifest { parts_count },
                    tags: None,
                    metadata_blob: Some(SerializedMetadataBlob::default()),
                    system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                },
                parts: Vec::new(),
                selected_streaming_segments: Vec::new(),
                omitted_parts: Vec::new(),
                omitted_streaming_segments: Vec::new(),
                stream_uploads: Vec::new(),
                stream_upload_segments: Vec::new(),
                write_sequence: 1,
                last_modified_millis: 2,
                stale_payload: None,
            })),
        );

        let mut proof_bytes = Vec::new();
        encode_bucket_write_reservation_proof(&mut proof_bytes, &proof);
        let mut proofless_bytes = command.command_bytes();
        assert!(proofless_bytes.ends_with(&proof_bytes));
        proofless_bytes.truncate(proofless_bytes.len() - proof_bytes.len());

        assert!(
            decode_metadata_command_envelope(&proofless_bytes).is_err(),
            "proofless complete-multipart command bytes must fail full envelope decode"
        );
        assert!(
            decode_metadata_command_log_entry_header(&proofless_bytes).is_err(),
            "proofless complete-multipart command bytes must fail applied-row validation"
        );
    }

    #[test]
    fn put_object_metadata_rejects_missing_bucket_write_reservation_proof() {
        let bucket = BucketName::try_from("metadata-proof-required".to_string()).unwrap();
        let key = ObjectKey::try_from("key".to_string()).unwrap();
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "metadata-proof-required-reservation".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 7,
            bucket_incarnation_generation: 7,
            operation_kind: "put-object-metadata".to_string(),
            created_at: 10,
            lease_deadline: 20,
            target_context: Some(key.as_str().to_string()),
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket_write_reservation: proof.clone(),
                object: LiveObjectRecord {
                    bucket,
                    key,
                    version_id: VersionId::Null,
                    owner: OwnerIdentity::from_principal("owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 0,
                    etag: ObjectEtag::single_part(0),
                    last_modified: 1,
                    became_noncurrent_at: None,
                    storage_class: StorageClass::Standard,
                    ec: EcShape { k: 0, m: 0 },
                    layout: ObjectLayout::Standard,
                    tags: Some(SerializedTagSet::new("<Tagging/>".to_string())),
                    metadata_blob: Some(SerializedMetadataBlob::default()),
                    system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                },
            })),
        );

        let mut proof_bytes = Vec::new();
        encode_bucket_write_reservation_proof(&mut proof_bytes, &proof);
        let mut proofless_bytes = command.command_bytes();
        assert!(proofless_bytes.ends_with(&proof_bytes));
        proofless_bytes.truncate(proofless_bytes.len() - proof_bytes.len());

        assert!(
            decode_metadata_command_envelope(&proofless_bytes).is_err(),
            "proofless put-object-metadata command bytes must fail full envelope decode"
        );
        assert!(
            decode_metadata_command_log_entry_header(&proofless_bytes).is_err(),
            "proofless put-object-metadata command bytes must fail applied-row validation"
        );
    }

    #[test]
    fn delete_object_version_rejects_missing_bucket_write_reservation_proof() {
        let bucket = BucketName::try_from("delete-proof-required".to_string()).unwrap();
        let key = ObjectKey::try_from("key".to_string()).unwrap();
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "delete-proof-required-reservation".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 7,
            bucket_incarnation_generation: 7,
            operation_kind: "delete-object-version".to_string(),
            created_at: 10,
            lease_deadline: 20,
            target_context: Some(key.as_str().to_string()),
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket_write_reservation: proof.clone(),
                bucket,
                key,
                version_id: VersionId::from_u64(7),
                target: DeleteObjectVersionTarget::DeleteMarker { write_sequence: 9 },
            })),
        );

        let mut proof_bytes = Vec::new();
        encode_bucket_write_reservation_proof(&mut proof_bytes, &proof);
        let mut proofless_bytes = command.command_bytes();
        assert!(proofless_bytes.ends_with(&proof_bytes));
        proofless_bytes.truncate(proofless_bytes.len() - proof_bytes.len());

        assert!(
            decode_metadata_command_envelope(&proofless_bytes).is_err(),
            "proofless delete-object-version command bytes must fail full envelope decode"
        );
        assert!(
            decode_metadata_command_log_entry_header(&proofless_bytes).is_err(),
            "proofless delete-object-version command bytes must fail applied-row validation"
        );
    }

    #[test]
    fn insert_delete_marker_rejects_missing_bucket_write_reservation_proof() {
        let bucket = BucketName::try_from("marker-proof-required".to_string()).unwrap();
        let key = ObjectKey::try_from("key".to_string()).unwrap();
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "marker-proof-required-reservation".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 7,
            bucket_incarnation_generation: 7,
            operation_kind: "insert-delete-marker".to_string(),
            created_at: 10,
            lease_deadline: 20,
            target_context: Some(key.as_str().to_string()),
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket_write_reservation: proof.clone(),
                bucket,
                key,
                version_id: VersionId::from_u64(7),
                owner: OwnerIdentity::from_principal("owner"),
                write_sequence: 1,
                last_modified_millis: 2,
                stale_payload: None,
            }),
        );

        let mut proof_bytes = Vec::new();
        encode_bucket_write_reservation_proof(&mut proof_bytes, &proof);
        let mut proofless_bytes = command.command_bytes();
        assert!(proofless_bytes.ends_with(&proof_bytes));
        proofless_bytes.truncate(proofless_bytes.len() - proof_bytes.len());

        assert!(
            decode_metadata_command_envelope(&proofless_bytes).is_err(),
            "proofless insert-delete-marker command bytes must fail full envelope decode"
        );
        assert!(
            decode_metadata_command_log_entry_header(&proofless_bytes).is_err(),
            "proofless insert-delete-marker command bytes must fail applied-row validation"
        );
    }

    #[test]
    fn metadata_command_canonical_encoding_is_stable() {
        let owner = CanonicalUserId::from_principal("owner");
        let acl_grants = AclGrants::default();
        let mut command = CreateBucketCommand::from_config(
            &CreateBucketConfig {
                name: "bucket",
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: true,
                versioning: BucketVersioningState::Enabled,
                object_lock: BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            },
            123,
            7,
        )
        .unwrap();
        command.bucket.multipart_upload_id_key = MultipartUploadIdKey::from_bytes([0x42; 32]);
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(9).unwrap(),
        );
        let envelope =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::CreateBucket(command.clone()));
        let duplicate =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::CreateBucket(command));

        assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
        assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
        assert!(envelope.verify_checksum());
        assert_applied_log_decoder_accepts(&envelope);
        assert_full_envelope_decoder_round_trips(&envelope);
        assert_eq!(envelope.checksum_crc64(), 0xd1a22a0be38facfd);
    }

    #[test]
    fn metadata_command_log_entry_header_decodes_applied_and_abandoned_rows() {
        let owner = CanonicalUserId::from_principal("owner");
        let acl_grants = AclGrants::default();
        let command = CreateBucketCommand::from_config(
            &CreateBucketConfig {
                name: "bucket",
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: true,
                versioning: BucketVersioningState::Enabled,
                object_lock: BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            },
            123,
            7,
        )
        .unwrap();
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(9).unwrap(),
        );
        let envelope =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::CreateBucket(command));

        let applied_header = decode_metadata_command_log_entry_header(&envelope.command_bytes())
            .expect("applied command bytes decode");
        assert_eq!(applied_header.id(), id);
        assert_eq!(applied_header.kind(), MetadataCommandLogEntryKind::Applied);
        assert_eq!(applied_header.command_kind_name(), Some("CreateBucket"));

        let mut old_version = envelope.command_bytes();
        let version_offset = 4 + METADATA_COMMAND_MAGIC.len();
        old_version[version_offset..version_offset + 2].copy_from_slice(&3_u16.to_le_bytes());
        assert_eq!(
            decode_metadata_command_envelope(&old_version),
            Err("unsupported metadata command encoding version 3".to_string())
        );
        assert_eq!(
            decode_metadata_command_log_entry_header(&old_version),
            Err("unsupported metadata command encoding version 3".to_string())
        );

        let mut applied_with_trailing_bytes = envelope.command_bytes();
        applied_with_trailing_bytes.push(0);
        assert!(
            decode_metadata_command_log_entry_header(&applied_with_trailing_bytes).is_err(),
            "applied command decoder must reject trailing bytes"
        );

        let mut applied_with_unknown_kind = envelope.command_bytes();
        let kind_offset = 4 + METADATA_COMMAND_MAGIC.len() + 2 + 8 + 4 + 8;
        applied_with_unknown_kind[kind_offset..kind_offset + 2]
            .copy_from_slice(&u16::MAX.to_le_bytes());
        assert!(
            decode_metadata_command_log_entry_header(&applied_with_unknown_kind).is_err(),
            "applied command decoder must reject unknown payload kinds"
        );

        let abandoned_header =
            decode_metadata_command_log_entry_header(&envelope.abandoned_log_bytes())
                .expect("abandoned command bytes decode");
        assert_eq!(abandoned_header.id(), id);
        assert_eq!(
            abandoned_header.kind(),
            MetadataCommandLogEntryKind::Abandoned {
                original_command_checksum: envelope.checksum_crc64()
            }
        );
        assert_eq!(abandoned_header.command_kind_name(), None);
    }

    #[test]
    fn metadata_command_versioning_encoding_is_stable() {
        let command = PutBucketVersioningCommand::from_bucket(
            test_bucket_record("bucket", 11),
            BucketVersioningState::Enabled,
        );
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(10).unwrap(),
        );
        let envelope = MetadataCommandEnvelope::new(
            id,
            MetadataCommandPayload::PutBucketVersioning(command.clone()),
        );
        let duplicate =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::PutBucketVersioning(command));

        assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
        assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
        assert_eq!(envelope.checksum_crc64(), 0x7909c3cbd7e48aee);
        assert!(envelope.verify_checksum());
        assert_applied_log_decoder_accepts(&envelope);
        assert_full_envelope_decoder_round_trips(&envelope);
    }

    #[test]
    fn metadata_command_bucket_acl_encoding_is_stable() {
        let command = PutBucketAclCommand::from_bucket(
            test_bucket_record("bucket", 12),
            AclGrants::default(),
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        );
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(11).unwrap(),
        );
        let envelope =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::PutBucketAcl(command.clone()));
        let duplicate =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::PutBucketAcl(command));

        assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
        assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
        assert_eq!(envelope.checksum_crc64(), 0xa11f229e60cc45de);
        assert!(envelope.verify_checksum());
        assert_applied_log_decoder_accepts(&envelope);
        assert_full_envelope_decoder_round_trips(&envelope);
    }

    #[test]
    fn bucket_command_encoding_preserves_multipart_completion_barrier_sequence() {
        let mut current = test_bucket_record("bucket", 13);
        current.multipart_completion_barrier_sequence = 11;
        let duplicate_current = current.clone();

        let command = PutBucketAclCommand::from_bucket(
            current,
            AclGrants::default(),
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        );
        assert_eq!(command.bucket.multipart_completion_barrier_sequence, 11);

        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(13).unwrap(),
        );
        let envelope =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::PutBucketAcl(command));
        let duplicate = MetadataCommandEnvelope::new(
            id,
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                duplicate_current,
                AclGrants::default(),
                BucketAclSummary {
                    public_read: true,
                    public_write: false,
                },
            )),
        );

        assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
        assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
        assert!(envelope.verify_checksum());
        assert_applied_log_decoder_accepts(&envelope);
        assert_full_envelope_decoder_round_trips(&envelope);
    }

    #[test]
    fn metadata_command_mark_bucket_deleting_encoding_is_stable() {
        let command = MarkBucketDeletingCommand::from_bucket(test_bucket_record("bucket", 14));
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(14).unwrap(),
        );
        let envelope = MetadataCommandEnvelope::new(
            id,
            MetadataCommandPayload::MarkBucketDeleting(command.clone()),
        );
        let duplicate =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::MarkBucketDeleting(command));

        assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
        assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
        assert_eq!(envelope.checksum_crc64(), 0x8766b489ba43b242);
        assert!(envelope.verify_checksum());
        assert_applied_log_decoder_accepts(&envelope);
        assert_full_envelope_decoder_round_trips(&envelope);
    }

    #[test]
    fn metadata_command_bucket_property_encoding_is_stable() {
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(12).unwrap(),
        );
        let mutations = [
            (
                BucketPropertyMutation::ObjectLock(BucketObjectLockConfig {
                    enabled: true,
                    default_retention: Some(ObjectLockDefaultRetention {
                        mode: ObjectLockMode::Governance,
                        period: RetentionPeriod::days(7).unwrap(),
                    }),
                }),
                0,
            ),
            (
                BucketPropertyMutation::Encryption(BucketEncryptionConfig {
                    default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
                    sse_c_blocked: false,
                }),
                0,
            ),
            (
                BucketPropertyMutation::PublicAccessBlock(Some(PublicAccessBlockConfig {
                    block_public_acls: true,
                    ignore_public_acls: false,
                    block_public_policy: true,
                    restrict_public_buckets: false,
                })),
                0,
            ),
            (BucketPropertyMutation::PublicAccessBlock(None), 0),
            (
                BucketPropertyMutation::OwnershipControls(Some(BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
                })),
                0,
            ),
            (BucketPropertyMutation::OwnershipControls(None), 0),
            (BucketPropertyMutation::AbacEnabled(true), 0),
        ];

        let mut checksums = Vec::new();
        for (offset, (mutation, _expected_checksum)) in mutations.into_iter().enumerate() {
            let command = PutBucketPropertyCommand::from_bucket_and_mutation(
                test_bucket_record("bucket", 20 + offset as u64),
                mutation.clone(),
            );
            let id = MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(id.log_index().get() + offset as u64).unwrap(),
            );
            let envelope = MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::PutBucketProperty(command.clone()),
            );
            let duplicate = MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::PutBucketProperty(command),
            );

            assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
            assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
            assert!(envelope.verify_checksum());
            assert_applied_log_decoder_accepts(&envelope);
            assert_full_envelope_decoder_round_trips(&envelope);
            checksums.push(envelope.checksum_crc64());
        }
        assert_eq!(
            checksums,
            [
                0xdcbba49b1f6edab2,
                0x669237fa05b26184,
                0xc26a15efa8011034,
                0x19fdfda93e9026c1,
                0x3ea2c7f8cb1f8eb2,
                0x3d668f26077ad62b,
                0x55f6c77c55699749,
            ]
        );
    }

    #[test]
    fn metadata_command_bucket_subresource_encoding_is_stable() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(19).unwrap(),
        );
        let mutations = [
            BucketSubresourceMutation::Put {
                kind: BucketSubresourceKind::Policy,
                body: r#"{"Statement":[]}"#.to_owned(),
                aux: BucketSubresourceAux::policy(true),
            },
            BucketSubresourceMutation::Delete {
                kind: BucketSubresourceKind::Policy,
            },
            BucketSubresourceMutation::Put {
                kind: BucketSubresourceKind::Tagging,
                body: "<Tagging/>".to_owned(),
                aux: BucketSubresourceAux::None,
            },
            BucketSubresourceMutation::Delete {
                kind: BucketSubresourceKind::Tagging,
            },
            BucketSubresourceMutation::Put {
                kind: BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>".to_owned(),
                aux: BucketSubresourceAux::None,
            },
            BucketSubresourceMutation::Delete {
                kind: BucketSubresourceKind::Lifecycle,
            },
            BucketSubresourceMutation::Put {
                kind: BucketSubresourceKind::Cors,
                body: "<CORSConfiguration/>".to_owned(),
                aux: BucketSubresourceAux::None,
            },
            BucketSubresourceMutation::Delete {
                kind: BucketSubresourceKind::Cors,
            },
        ];

        let mut checksums = Vec::new();
        for (offset, mutation) in mutations.into_iter().enumerate() {
            let command =
                PutBucketSubresourceCommand::new(bucket.clone(), mutation, 30 + offset as u64);
            let id = MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(id.log_index().get() + offset as u64).unwrap(),
            );
            let envelope = MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::PutBucketSubresource(command.clone()),
            );
            let duplicate = MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::PutBucketSubresource(command),
            );

            assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
            assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
            assert!(envelope.verify_checksum());
            assert_applied_log_decoder_accepts(&envelope);
            assert_full_envelope_decoder_round_trips(&envelope);
            checksums.push(envelope.checksum_crc64());
        }
        assert_eq!(
            checksums,
            [
                0xf99a23e7c7dbf1c1,
                0x8d385918b38eb90d,
                0x5f714340121e9bea,
                0xd1a325f80a439cc2,
                0x1ecce4060d9f9c01,
                0x426484b6ce8f555c,
                0xbebf29527ff5742e,
                0xbc0219392333aaa2,
            ]
        );
    }

    #[test]
    fn metadata_command_object_encoding_is_stable() {
        let bucket = BucketName::try_from("bucket").unwrap();
        let key = ObjectKey::try_from("key").unwrap();
        let reservation_id = SessionId::try_from("21".repeat(16)).unwrap();
        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(4),
            MetadataCommandLogIndex::new(40).unwrap(),
        );
        let generation_id = GenerationId::MIN;
        let segment = ObjectSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            segment_index: 0,
            size: 11,
            segment_crc64: 9,
            segment_okh: [7; 16],
            segment_vid: generation_id,
            data_pg_id: 2,
            placement_cluster_epoch: ClusterEpoch::new(7).unwrap(),
            ec_k: 2,
            ec_m: 1,
        };
        let object = PutLiveObjectReq {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: true,
            generation_id,
            size: 11,
            etag: ObjectEtag::single_part(8),
            ec: EcShape { k: 2, m: 1 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: Some(SerializedMetadataBlob::default()),
            system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        };
        let multipart_object = PutLiveObjectReq {
            layout: ObjectLayout::MultipartManifest {
                parts_count: std::num::NonZeroU32::new(1).unwrap(),
            },
            etag: ObjectEtag::multipart([3; 8], 1),
            ..object.clone()
        };
        let metadata_object = LiveObjectRecord {
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            version_id: object.version_id,
            owner: object.owner.clone(),
            acl_grants: object.acl_grants.clone(),
            public_read: object.public_read,
            generation_id: object.generation_id,
            size: object.size,
            etag: object.etag,
            last_modified: 555,
            became_noncurrent_at: None,
            storage_class: StorageClass::Standard,
            ec: object.ec,
            layout: object.layout,
            tags: object.tags.clone(),
            metadata_blob: object.metadata_blob.clone(),
            system_metadata_blob: object.system_metadata_blob.clone(),
            object_lock: object.object_lock,
            encryption: object.encryption.clone(),
        };
        let part = ObjectPartRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            part_number: 2,
            size: 13,
            payload_crc64: 99,
            etag: vec![4; 8],
            etag_kind: crate::types::EtagKind::Crc64,
            part_okh: [0; 16],
            part_vid: generation_id,
            placement_cluster_epoch: ClusterEpoch::new(10).unwrap(),
            ec_k: 2,
            ec_m: 1,
            data_pg_id: 2,
            checksum: None,
        };
        let upload_id = UploadId::try_from(format!("{}{}", "upload", ".".repeat(122))).unwrap();
        let multipart_checksum =
            MultipartChecksumConfig::new(ChecksumAlgorithm::Sha256, Some(ChecksumType::Composite))
                .unwrap();
        let sse_s3_encryption = ObjectEncryption::SseS3(SseS3ObjectState {
            wrapping_key_id: 9,
            wrap_nonce: [10; SSE_S3_WRAP_NONCE_LEN],
            wrapped_dek: [11; SSE_S3_WRAPPED_DEK_LEN],
            segment_nonce_prefix: [12; SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
            checksum_nonce: [13; SSE_S3_CHECKSUM_NONCE_LEN],
            encrypted_checksum_metadata: vec![14, 15, 16],
        });
        let multipart_upload = MultipartUploadRecord {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: 560,
            state: crate::UploadState::Aborting,
            tags: Some(crate::SerializedTagSet::new("<Tagging/>".to_string())),
            metadata_blob: SerializedMetadataBlob::new(vec![1, 2, 3]),
            system_metadata_blob: SerializedSystemMetadataBlob::new(vec![4, 5, 6]),
            initiator: OwnerIdentity::from_principal("initiator"),
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: true,
            object_generation_id: generation_id,
            initiated_object_identity: Some(crate::MultipartObjectIdentity::Live {
                version_id: VersionId::from_u64(7),
                generation_id,
            }),
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        };
        let multipart_upload_with_checksum = MultipartUploadRecord {
            checksum: Some(multipart_checksum),
            ..multipart_upload.clone()
        };
        let uploaded_part = MultipartPartRecord {
            upload_id: upload_id.clone(),
            part_number: 2,
            generation: 1,
            size: 13,
            payload_crc64: 99,
            etag: vec![4; 8],
            etag_kind: crate::types::EtagKind::Crc64,
            part_okh: [0; 16],
            part_vid: generation_id,
            placement_cluster_epoch: ClusterEpoch::new(11).unwrap(),
            ec_k: 2,
            ec_m: 1,
            last_modified: 444,
            checksum: None,
        };
        let stream_session_id = SessionId::try_from("31".repeat(16)).unwrap();
        let stream_segment = StreamUploadSegmentRecord {
            session_id: stream_session_id.clone(),
            segment_index: 3,
            size: 17,
            segment_crc64: 12,
            payload_crc64: 12,
            segment_okh: [12; 16],
            segment_vid: generation_id,
            data_pg_id: 2,
            placement_cluster_epoch: ClusterEpoch::new(8).unwrap(),
            ec_k: 2,
            ec_m: 1,
        };
        let selected_streaming_segment = MultipartPartSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            version_id: VersionId::Null.to_u64(),
            part_number: 2,
            segment_index: 0,
            size: 13,
            segment_crc64: 10,
            segment_okh: [10; 16],
            segment_vid: generation_id,
            data_pg_id: 2,
            placement_cluster_epoch: ClusterEpoch::new(9).unwrap(),
            ec_k: 2,
            ec_m: 1,
        };
        let omitted_streaming_segment = MultipartPartSegmentRecord {
            version_id: u64::MAX,
            part_number: 3,
            segment_index: 1,
            segment_okh: [11; 16],
            ..selected_streaming_segment.clone()
        };
        let segment_reclaim = ObjectPayloadReclaimCommand::Segments(ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: 222,
            segments: vec![ObjectSegmentsReclaimSegmentRecord {
                segment_index: 0,
                segment_okh: [6; 16],
                segment_vid: generation_id,
                data_pg_id: 2,
                ec: EcShape { k: 2, m: 1 },
            }],
        });
        let multipart_reclaim = ObjectPayloadReclaimCommand::Multipart(MultipartReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: 333,
            parts: vec![MultipartReclaimPartRecord::Segments {
                part_number: 1,
                segments: vec![MultipartReclaimPartSegmentRecord {
                    part_number: 1,
                    segment_index: 0,
                    segment_okh: [5; 16],
                    segment_vid: generation_id,
                    data_pg_id: 2,
                    ec: EcShape { k: 2, m: 1 },
                }],
            }],
        });
        let bucket_write_reservation = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "direct-put-proof".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 9,
            bucket_incarnation_generation: 9,
            operation_kind: "direct-put-commit".to_string(),
            created_at: 444,
            lease_deadline: 555,
            target_context: Some(key.as_str().to_string()),
        };
        let segment_reclaim_claim = ObjectPayloadReclaimClaimProof {
            bucket_incarnation_generation: 9,
            reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
            claim_id: "segment-reclaim-claim".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
        };
        let multipart_reclaim_claim = ObjectPayloadReclaimClaimProof {
            bucket_incarnation_generation: 9,
            reclaim_kind: ObjectPayloadReclaimKind::Multipart,
            claim_id: "multipart-reclaim-claim".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
        };
        let payloads = [
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                bucket.clone(),
                key.clone(),
                reservation_id.clone(),
                generation_id,
                111,
            )),
            MetadataCommandPayload::ReleaseObjectGeneration(ReleaseObjectGenerationCommand::new(
                bucket.clone(),
                key.clone(),
                reservation_id.clone(),
            )),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                bucket.clone(),
                key.clone(),
                VersionId::from_u64(7),
            )),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
                object: object.clone(),
                segments: vec![segment.clone()],
                generation_reservation_id: reservation_id.clone(),
                write_sequence: 44,
                last_modified_millis: 555,
                stale_payload: Some(segment_reclaim.clone()),
                bucket_write_reservation: bucket_write_reservation.clone(),
            })),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
                object: object.clone(),
                segments: vec![segment.clone()],
                generation_reservation_id: reservation_id.clone(),
                write_sequence: 45,
                last_modified_millis: 556,
                stale_payload: Some(multipart_reclaim.clone()),
                bucket_write_reservation: bucket_write_reservation.clone(),
            })),
            MetadataCommandPayload::CommitMultipartObject(Box::new(CommitMultipartObjectCommand {
                upload_id: upload_id.clone(),
                completion_fingerprint: MultipartCompletionFingerprint::from_bytes([0x44; 32]),
                bucket_write_reservation: bucket_write_reservation.clone(),
                object: multipart_object,
                parts: vec![part],
                selected_streaming_segments: vec![selected_streaming_segment.clone()],
                omitted_parts: vec![uploaded_part.clone()],
                omitted_streaming_segments: vec![omitted_streaming_segment.clone()],
                stream_uploads: vec![TerminalStreamCleanupRecord {
                    session_id: stream_session_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    target: StreamUploadTarget::UploadPart {
                        upload_id: upload_id.clone(),
                        part_number: 3,
                    },
                    state: StreamUploadState::InProgress,
                    created_at: 558,
                    encryption: ObjectEncryption::None,
                }],
                stream_upload_segments: vec![stream_segment.clone()],
                write_sequence: 43,
                last_modified_millis: 557,
                stale_payload: Some(multipart_reclaim.clone()),
            })),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket_write_reservation: bucket_write_reservation.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(7),
                target: DeleteObjectVersionTarget::DeleteMarker { write_sequence: 49 },
            })),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket_write_reservation: bucket_write_reservation.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                target: DeleteObjectVersionTarget::Live {
                    generation_id,
                    layout: ObjectLayout::Standard,
                    payload: segment_reclaim.clone(),
                },
            })),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket_write_reservation: bucket_write_reservation.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(11),
                target: DeleteObjectVersionTarget::Live {
                    generation_id,
                    layout: ObjectLayout::MultipartManifest {
                        parts_count: std::num::NonZeroU32::new(1).unwrap(),
                    },
                    payload: multipart_reclaim.clone(),
                },
            })),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket_write_reservation: bucket_write_reservation.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(8),
                owner: OwnerIdentity::from_principal("owner"),
                write_sequence: 46,
                last_modified_millis: 558,
                stale_payload: None,
            }),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket_write_reservation: bucket_write_reservation.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(9),
                owner: OwnerIdentity::from_principal("owner"),
                write_sequence: 47,
                last_modified_millis: 559,
                stale_payload: Some(segment_reclaim.clone()),
            }),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket_write_reservation: bucket_write_reservation.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(10),
                owner: OwnerIdentity::from_principal("owner"),
                write_sequence: 48,
                last_modified_millis: 560,
                stale_payload: Some(multipart_reclaim.clone()),
            }),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket_write_reservation: bucket_write_reservation.clone(),
                object: LiveObjectRecord {
                    version_id: VersionId::from_u64(8),
                    tags: Some(SerializedTagSet::new("<Tagging/>".to_string())),
                    ..metadata_object.clone()
                },
            })),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket_write_reservation: bucket_write_reservation.clone(),
                object: LiveObjectRecord {
                    version_id: VersionId::from_u64(8),
                    tags: None,
                    ..metadata_object.clone()
                },
            })),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket_write_reservation: bucket_write_reservation.clone(),
                object: LiveObjectRecord {
                    version_id: VersionId::from_u64(8),
                    object_lock: ObjectLockState {
                        retention: Some(ObjectRetention {
                            mode: ObjectLockMode::Governance,
                            retain_until_unix_seconds: 999,
                        }),
                        ..ObjectLockState::default()
                    },
                    ..metadata_object.clone()
                },
            })),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket_write_reservation: bucket_write_reservation.clone(),
                object: LiveObjectRecord {
                    version_id: VersionId::from_u64(8),
                    object_lock: ObjectLockState {
                        legal_hold: StoredLegalHoldStatus::On,
                        ..ObjectLockState::default()
                    },
                    ..metadata_object.clone()
                },
            })),
            MetadataCommandPayload::CreateMultipartUpload(Box::new(
                CreateMultipartUploadCommand::from_request_with_bucket_write_reservation(
                    CreateMultipartUploadReq {
                        upload_id: upload_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        tags: Some(crate::SerializedTagSet::new("<Tagging/>".to_string())),
                        metadata_blob: SerializedMetadataBlob::new(vec![1, 2, 3]),
                        system_metadata_blob: SerializedSystemMetadataBlob::new(vec![4, 5, 6]),
                        initiator: OwnerIdentity::from_principal("initiator"),
                        owner: OwnerIdentity::from_principal("owner"),
                        acl_grants: AclGrants::default(),
                        public_read: true,
                        object_lock: ObjectLockState::default(),
                        checksum: None,
                        encryption: ObjectEncryption::None,
                    },
                    generation_id,
                    None,
                    560,
                    bucket_write_reservation.clone(),
                ),
            )),
            MetadataCommandPayload::CreateMultipartUpload(Box::new(CreateMultipartUploadCommand {
                upload: multipart_upload_with_checksum,
                bucket_write_reservation: bucket_write_reservation.clone(),
            })),
            MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
                cleanup: AbortMultipartUploadCleanup {
                    upload: multipart_upload.clone(),
                    parts: vec![uploaded_part.clone()],
                    streaming_segments: vec![omitted_streaming_segment.clone()],
                    stream_uploads: vec![TerminalStreamCleanupRecord {
                        session_id: stream_session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: StreamUploadTarget::UploadPart {
                            upload_id: upload_id.clone(),
                            part_number: 2,
                        },
                        state: StreamUploadState::InProgress,
                        created_at: 562,
                        encryption: ObjectEncryption::None,
                    }],
                    stream_upload_segments: vec![stream_segment.clone()],
                },
                bucket_write_reservation: bucket_write_reservation.clone(),
            })),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request_with_bucket_write_reservation_and_cleanup_deadline(
                    CreateStreamUploadReq {
                        session_id: stream_session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: StreamUploadTarget::PutObject,
                        encryption: ObjectEncryption::None,
                    },
                    561,
                    Some(9_999),
                    bucket_write_reservation.clone(),
                ),
            )),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    CreateStreamUploadReq {
                        session_id: SessionId::try_from("32".repeat(16)).unwrap(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: StreamUploadTarget::UploadPart {
                            upload_id: upload_id.clone(),
                            part_number: 2,
                        },
                        encryption: ObjectEncryption::None,
                    },
                    562,
                    bucket_write_reservation.clone(),
                ),
            )),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    CreateStreamUploadReq {
                        session_id: SessionId::try_from("33".repeat(16)).unwrap(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: StreamUploadTarget::PutObject,
                        encryption: sse_s3_encryption,
                    },
                    563,
                    bucket_write_reservation.clone(),
                ),
            )),
            MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                segment: stream_segment.clone(),
            })),
            MetadataCommandPayload::AbortStreamUpload(Box::new(AbortStreamUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: stream_session_id.clone(),
                staged_segments: vec![stream_segment.clone()],
                stream_create_bucket_write_reservation: None,
            })),
            MetadataCommandPayload::CommitStreamPart(Box::new(CommitStreamPartCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: stream_session_id,
                upload: multipart_upload,
                part: uploaded_part.clone(),
                segments: vec![omitted_streaming_segment.clone()],
                existing_part: Some(uploaded_part),
                displaced_segments: vec![omitted_streaming_segment],
                bucket_write_reservation: bucket_write_reservation.clone(),
            })),
            MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(
                DeleteObjectPayloadReclaimCommand::new(
                    bucket.clone(),
                    key.clone(),
                    generation_id,
                    segment_reclaim,
                    segment_reclaim_claim,
                ),
            )),
            MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(
                DeleteObjectPayloadReclaimCommand::new(
                    bucket.clone(),
                    key.clone(),
                    generation_id,
                    multipart_reclaim,
                    multipart_reclaim_claim,
                ),
            )),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket_write_reservation: bucket_write_reservation.clone(),
                object: LiveObjectRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::from_u64(8),
                    acl_grants: AclGrants::default(),
                    public_read: true,
                    ..metadata_object
                },
            })),
            MetadataCommandPayload::DeleteFinalizedBucket(DeleteFinalizedBucketCommand::new(
                bucket.clone(),
                14,
                7,
            )),
            MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
                AdvanceMultipartCompletionBarrierCommand {
                    bucket: BucketName::try_from("bucket".to_string()).unwrap(),
                    barrier_sequence: 13,
                },
            ),
        ];

        let mut checksums = Vec::new();
        for (offset, payload) in payloads.into_iter().enumerate() {
            let id = MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(id.log_index().get() + offset as u64).unwrap(),
            );
            let envelope = MetadataCommandEnvelope::new(id, payload.clone());
            let duplicate = MetadataCommandEnvelope::new(id, payload);

            assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
            assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
            assert!(envelope.verify_checksum());
            assert_applied_log_decoder_accepts(&envelope);
            assert_full_envelope_decoder_round_trips(&envelope);
            checksums.push(envelope.checksum_crc64());
        }
        assert_eq!(
            checksums,
            [
                0xe6e9702e798439d7,
                0x2b473262c236c93c,
                0xb1a4c18f99321674,
                0xd0ac1dee93867277,
                0x9cc3f0ab4b6567b0,
                0x9e548d67965d9412,
                0x8bf3eb212f25c644,
                0xa20752aaa6934862,
                0x01db3c4fc9f3f0e0,
                0xde6097cf102c5bc7,
                0x355c40debbaeab86,
                0x5b00fefb00b15186,
                0x5dd34bf2a7b009ee,
                0x13be8ab4ceb534f9,
                0x701320a932b2ee85,
                0x0bbc901316940e04,
                0xb0aae20d8a032766,
                0x928ca850986716fc,
                0x10ed5a7db79ade46,
                0x6701cd8445cfe8e3,
                0x94fdcd9d6617eb8c,
                0x6957ed2f4f7ef220,
                0xf0eca11e9ad7f739,
                0x57bea7e100ee87af,
                0x2b7ea60c11738900,
                0xb1ec8810700ac401,
                0xceb57ea44698d642,
                0x2f8dc0173b130399,
                0x6b42c8e45ba2c4fc,
                0x11f512cc986933c0,
            ]
        );
    }
}
