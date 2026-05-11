use std::num::{NonZeroU32, NonZeroU64};

use s3_types::{
    AclGrants, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId,
    ObjectLockDefaultRetention, ObjectLockMode, ObjectLockState, ObjectRetention, RetentionPeriod,
    StoredLegalHoldStatus,
};

use crate::types::{
    AbortMultipartUploadCleanup, BucketEncryptionConfig, BucketName, BucketObjectOwnership,
    BucketOwnershipControls, BucketState, BucketSubresourceAux, BucketSubresourceKind,
    ClusterEpoch, CompletedMultipartUploadRecord, CreateBucketConfig, CreateMultipartUploadReq,
    CreateStreamUploadReq, GenerationId, LiveObjectRecord, ManagedEncryptionAlgorithm,
    MultipartPartRecord, MultipartPartSegmentRecord, MultipartReclaimPartRecord,
    MultipartReclaimRecord, MultipartUploadRecord, ObjectEncryption, ObjectEtag, ObjectKey,
    ObjectLayout, ObjectPartRecord, ObjectSegmentRecord, ObjectSegmentsReclaimRecord,
    OwnerIdentity, PgId, PublicAccessBlockConfig, PutLiveObjectReq, SerializedTagSet, SessionId,
    StreamUploadRecord, StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget, UploadId,
    VersionId,
};

const METADATA_COMMAND_MAGIC: &[u8] = b"argmin-metadata-command";
const METADATA_COMMAND_ENCODING_VERSION: u16 = 1;
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
const METADATA_COMMAND_DELETE_COMPLETED_MULTIPART_UPLOAD: u16 = 20;
const METADATA_COMMAND_ADVANCE_COMPLETED_MULTIPART_UPLOAD_SEQUENCE: u16 = 21;
const METADATA_COMMAND_RESERVE_OBJECT_VERSION: u16 = 22;
const METADATA_COMMAND_MARK_BUCKET_DELETING: u16 = 23;

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
}

impl MetadataCommandLogEntryHeader {
    pub(crate) fn id(self) -> MetadataCommandId {
        self.id
    }

    pub(crate) fn kind(self) -> MetadataCommandLogEntryKind {
        self.kind
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetadataCommandReplicaState {
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) applied_log_index: u64,
    pub(crate) applied_log_hash: u64,
    pub(crate) state_digest: u64,
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
    pub(crate) write_reservations_blocked: bool,
    pub(crate) active_write_reservations: u32,
    pub(crate) public_access_block: Option<PublicAccessBlockConfig>,
    pub(crate) ownership_controls: Option<BucketOwnershipControls>,
    pub(crate) bucket_policy_public: bool,
    pub(crate) bucket_policy_generation: u64,
    pub(crate) bucket_lifecycle_generation: u64,
    pub(crate) bucket_execution_generation: u64,
    pub(crate) completed_multipart_upload_sequence: u64,
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
            write_reservations_blocked: false,
            active_write_reservations: 0,
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            bucket_lifecycle_generation: 0,
            bucket_execution_generation,
            completed_multipart_upload_sequence: 0,
            bucket_abac_enabled: false,
            encryption: BucketEncryptionConfig {
                default_encryption: None,
                sse_c_blocked: true,
            },
        })
    }

    pub(crate) fn matches_create_config(&self, config: &CreateBucketConfig<'_>) -> bool {
        Self::from_create_config(config, self.created_at, self.bucket_execution_generation)
            .is_ok_and(|expected| expected.command_metadata_eq(self))
    }

    pub(crate) fn with_execution_generation(mut self, generation: u64) -> Self {
        self.bucket_execution_generation = generation;
        self
    }

    pub(crate) fn command_metadata_projection(mut self) -> Self {
        self.write_reservations_blocked = false;
        self.active_write_reservations = 0;
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
    DeleteCompletedMultipartUpload(Box<DeleteCompletedMultipartUploadCommand>),
    AdvanceCompletedMultipartUploadSequence(AdvanceCompletedMultipartUploadSequenceCommand),
}

impl MetadataCommandPayload {
    fn kind_id(&self) -> u16 {
        match self {
            Self::CreateBucket(_) => METADATA_COMMAND_CREATE_BUCKET,
            Self::PutBucketVersioning(_) => METADATA_COMMAND_PUT_BUCKET_VERSIONING,
            Self::PutBucketAcl(_) => METADATA_COMMAND_PUT_BUCKET_ACL,
            Self::PutBucketProperty(_) => METADATA_COMMAND_PUT_BUCKET_PROPERTY,
            Self::PutBucketSubresource(_) => METADATA_COMMAND_PUT_BUCKET_SUBRESOURCE,
            Self::MarkBucketDeleting(_) => METADATA_COMMAND_MARK_BUCKET_DELETING,
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
            Self::DeleteCompletedMultipartUpload(_) => {
                METADATA_COMMAND_DELETE_COMPLETED_MULTIPART_UPLOAD
            }
            Self::AdvanceCompletedMultipartUploadSequence(_) => {
                METADATA_COMMAND_ADVANCE_COMPLETED_MULTIPART_UPLOAD_SEQUENCE
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
            Self::DeleteCompletedMultipartUpload(delete) => &delete.record.bucket,
            Self::AdvanceCompletedMultipartUploadSequence(advance) => &advance.bucket,
        }
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
        public_read: bool,
        public_write: bool,
    ) -> Self {
        bucket.acl_grants = acl_grants;
        bucket.public_read = public_read;
        bucket.public_write = public_write;
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

    pub(crate) fn matches_request(&self, bucket: &BucketName, key: &ObjectKey) -> bool {
        self.bucket == *bucket && self.key == *key
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
}

impl CommitDirectPutObjectCommand {
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
    pub(crate) object: PutLiveObjectReq,
    pub(crate) parts: Vec<ObjectPartRecord>,
    pub(crate) selected_streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub(crate) omitted_parts: Vec<MultipartPartRecord>,
    pub(crate) omitted_streaming_segments: Vec<MultipartPartSegmentRecord>,
    pub(crate) stream_uploads: Vec<StreamUploadRecord>,
    pub(crate) stream_upload_segments: Vec<StreamUploadSegmentRecord>,
    pub(crate) write_sequence: u64,
    pub(crate) completion_order: u64,
    pub(crate) completed_at_millis: u64,
    pub(crate) initiator: Option<OwnerIdentity>,
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
        parts: &[MultipartPartRecord],
    ) -> bool {
        self.object.bucket == *bucket
            && self.object.key == *key
            && self.upload_id == *upload_id
            && self.object.generation_id == generation_id
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeleteObjectPayloadReclaimCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) generation_id: GenerationId,
    pub(crate) payload: ObjectPayloadReclaimCommand,
}

impl DeleteObjectPayloadReclaimCommand {
    pub(crate) fn new(
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
        payload: ObjectPayloadReclaimCommand,
    ) -> Self {
        Self {
            bucket,
            key,
            generation_id,
            payload,
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
    DeleteMarker,
    Live {
        generation_id: GenerationId,
        layout: ObjectLayout,
        payload: ObjectPayloadReclaimCommand,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeleteObjectVersionCommand {
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
    pub(crate) object: LiveObjectRecord,
}

impl PutObjectMetadataCommand {
    pub(crate) fn from_live_object_and_mutation(
        mut object: LiveObjectRecord,
        mutation: PutObjectMetadataMutation,
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
        Self { object }
    }

    pub(crate) fn matches_object(&self, object: &LiveObjectRecord) -> bool {
        self.object == *object
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateStreamUploadCommand {
    pub(crate) session: StreamUploadRecord,
}

impl CreateStreamUploadCommand {
    pub(crate) fn from_request(request: CreateStreamUploadReq, created_at_millis: u64) -> Self {
        Self {
            session: StreamUploadRecord {
                session_id: request.session_id,
                bucket: request.bucket,
                key: request.key,
                target: request.target,
                state: StreamUploadState::InProgress,
                created_at: created_at_millis,
                encryption: request.encryption,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendStreamSegmentCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) segment: StreamUploadSegmentRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbortStreamUploadCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) session_id: SessionId,
    pub(crate) staged_segments: Vec<StreamUploadSegmentRecord>,
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
}

impl CreateMultipartUploadCommand {
    pub(crate) fn from_request(
        request: CreateMultipartUploadReq,
        object_generation_id: GenerationId,
        initiated_at_millis: u64,
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
                object_lock: request.object_lock,
                checksum: request.checksum,
                encryption: request.encryption,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbortMultipartUploadCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) upload_id: UploadId,
    pub(crate) cleanup: AbortMultipartUploadCleanup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeleteCompletedMultipartUploadCommand {
    pub(crate) record: CompletedMultipartUploadRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdvanceCompletedMultipartUploadSequenceCommand {
    pub(crate) bucket: BucketName,
    pub(crate) completion_order: u64,
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
        MetadataCommandPayload::DeleteCompletedMultipartUpload(command) => {
            encode_delete_completed_multipart_upload(&mut out, command);
        }
        MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(command) => {
            encode_advance_completed_multipart_upload_sequence(&mut out, command);
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
                self.skip_optional_stale_payload()
            }
            METADATA_COMMAND_COMMIT_MULTIPART_OBJECT => {
                self.skip_str()?;
                self.skip_put_live_object()?;
                self.skip_repeated(Self::skip_object_part)?;
                self.skip_repeated(Self::skip_multipart_part_segment)?;
                self.skip_repeated(Self::skip_multipart_part)?;
                self.skip_repeated(Self::skip_multipart_part_segment)?;
                self.skip_repeated(Self::skip_stream_upload)?;
                self.skip_repeated(Self::skip_stream_upload_segment)?;
                self.read_u64()?;
                self.read_u64()?;
                self.read_u64()?;
                self.skip_optional_owner_identity()?;
                self.read_u64()?;
                self.skip_optional_stale_payload()
            }
            METADATA_COMMAND_DELETE_OBJECT_VERSION => {
                self.skip_str()?;
                self.skip_str()?;
                self.read_u64()?;
                match self.read_u8()? {
                    1 => Ok(()),
                    2 => {
                        self.read_nonzero_u64("deleted object generation")?;
                        self.skip_object_layout()?;
                        self.skip_live_payload_reclaim()
                    }
                    tag => Err(format!("invalid delete object target tag {tag}")),
                }
            }
            METADATA_COMMAND_INSERT_DELETE_MARKER => {
                self.skip_str()?;
                self.skip_str()?;
                self.read_u64()?;
                self.skip_owner_identity()?;
                self.read_u64()?;
                self.read_u64()?;
                self.skip_optional_stale_payload()
            }
            METADATA_COMMAND_PUT_OBJECT_METADATA => self.skip_live_object_record(),
            METADATA_COMMAND_CREATE_STREAM_UPLOAD => self.skip_stream_upload(),
            METADATA_COMMAND_APPEND_STREAM_SEGMENT => {
                self.skip_str()?;
                self.skip_str()?;
                self.skip_stream_upload_segment()
            }
            METADATA_COMMAND_ABORT_STREAM_UPLOAD => {
                self.skip_str()?;
                self.skip_str()?;
                self.skip_str()?;
                self.skip_repeated(Self::skip_stream_upload_segment)
            }
            METADATA_COMMAND_COMMIT_STREAM_PART => {
                self.skip_str()?;
                self.skip_str()?;
                self.skip_str()?;
                self.skip_multipart_upload()?;
                self.skip_multipart_part()?;
                self.skip_repeated(Self::skip_multipart_part_segment)?;
                self.skip_optional_multipart_part()?;
                self.skip_repeated(Self::skip_multipart_part_segment)
            }
            METADATA_COMMAND_CREATE_MULTIPART_UPLOAD => self.skip_multipart_upload(),
            METADATA_COMMAND_ABORT_MULTIPART_UPLOAD => {
                self.skip_str()?;
                self.skip_str()?;
                self.skip_str()?;
                self.skip_abort_multipart_upload_cleanup()
            }
            METADATA_COMMAND_DELETE_OBJECT_PAYLOAD_RECLAIM => {
                self.skip_str()?;
                self.skip_str()?;
                self.read_nonzero_u64("payload reclaim generation")?;
                self.skip_object_payload_reclaim()
            }
            METADATA_COMMAND_DELETE_COMPLETED_MULTIPART_UPLOAD => {
                self.skip_completed_multipart_upload()
            }
            METADATA_COMMAND_ADVANCE_COMPLETED_MULTIPART_UPLOAD_SEQUENCE => {
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
            METADATA_COMMAND_DELETE_COMPLETED_MULTIPART_UPLOAD => {
                Ok(MetadataCommandPayload::DeleteCompletedMultipartUpload(
                    Box::new(DeleteCompletedMultipartUploadCommand {
                        record: self.read_completed_multipart_upload()?,
                    }),
                ))
            }
            METADATA_COMMAND_ADVANCE_COMPLETED_MULTIPART_UPLOAD_SEQUENCE => Ok(
                MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
                    AdvanceCompletedMultipartUploadSequenceCommand {
                        bucket: self.read_bucket_name()?,
                        completion_order: self.read_u64()?,
                    },
                ),
            ),
            _ => Err(format!(
                "pending metadata command payload kind {kind_id} does not have a typed decoder yet"
            )),
        }
    }

    fn skip_repeated(
        &mut self,
        mut skip_item: impl FnMut(&mut Self) -> Result<(), String>,
    ) -> Result<(), String> {
        let count = self.read_u32()?;
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
            write_reservations_blocked: false,
            active_write_reservations: 0,
            public_access_block: self.read_public_access_block()?,
            ownership_controls: self.read_ownership_controls()?,
            bucket_policy_public: self.read_bool()?,
            bucket_policy_generation: self.read_u64()?,
            bucket_lifecycle_generation: self.read_u64()?,
            bucket_execution_generation: self.read_u64()?,
            completed_multipart_upload_sequence: self.read_u64()?,
            bucket_abac_enabled: self.read_bool()?,
            encryption: self.read_bucket_encryption()?,
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
        self.skip_optional_u64()?;
        self.read_bytes()?;
        self.read_nonzero_u64("object segment VID")?;
        self.read_u32()?;
        self.read_u8()?;
        self.read_u8()?;
        Ok(())
    }

    fn skip_object_part(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.read_u64()?;
        self.read_u32()?;
        self.read_u64()?;
        self.read_bytes()?;
        self.read_valid_u8("etag kind", 0..=1)?;
        self.read_bytes()?;
        self.read_nonzero_u64("object part VID")?;
        self.read_u8()?;
        self.read_u8()?;
        self.read_u32()?;
        self.skip_optional_bytes()
    }

    fn skip_multipart_part(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.read_u32()?;
        self.read_u32()?;
        self.read_u64()?;
        self.read_bytes()?;
        self.read_valid_u8("etag kind", 0..=1)?;
        self.read_bytes()?;
        self.read_nonzero_u64("multipart part VID")?;
        self.read_u8()?;
        self.read_u8()?;
        self.read_u64()?;
        self.skip_optional_bytes()
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
        self.skip_optional_u64()?;
        self.read_bytes()?;
        self.read_nonzero_u64("multipart part segment VID")?;
        self.read_u32()?;
        self.read_u8()?;
        self.read_u8()?;
        Ok(())
    }

    fn skip_stream_upload(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.skip_str()?;
        self.skip_stream_upload_target()?;
        self.read_valid_u8("stream upload state", 0..=3)?;
        self.read_u64()?;
        self.skip_object_encryption()
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

    fn skip_stream_upload_segment(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.read_u32()?;
        self.read_u64()?;
        self.skip_optional_u64()?;
        self.read_bytes()?;
        self.read_nonzero_u64("stream upload segment VID")?;
        self.read_u32()?;
        self.read_u8()?;
        self.read_u8()?;
        Ok(())
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
        self.skip_optional_owner_identity()?;
        self.skip_owner_identity()?;
        self.skip_str()?;
        self.skip_bool()?;
        self.read_nonzero_u64("multipart upload object generation")?;
        self.skip_object_lock_state()?;
        self.skip_optional_multipart_checksum_config()?;
        self.skip_object_encryption()
    }

    fn skip_completed_multipart_upload(&mut self) -> Result<(), String> {
        self.skip_str()?;
        self.skip_str()?;
        self.skip_str()?;
        self.read_u64()?;
        self.read_u64()?;
        self.skip_optional_owner_identity()?;
        self.skip_owner_identity()
    }

    fn read_completed_multipart_upload(
        &mut self,
    ) -> Result<CompletedMultipartUploadRecord, String> {
        Ok(CompletedMultipartUploadRecord {
            upload_id: self.read_upload_id()?,
            bucket: self.read_bucket_name()?,
            key: self.read_object_key()?,
            completion_order: self.read_u64()?,
            completed_at: self.read_u64()?,
            initiator: self.read_optional_owner_identity()?,
            owner: self.read_owner_identity()?,
        })
    }

    fn skip_object_payload_reclaim(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            0 => self.skip_object_segments_reclaim(),
            1 => self.skip_multipart_reclaim(),
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

    fn skip_optional_stale_payload(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            0 => Ok(()),
            1 => self.skip_object_segments_reclaim(),
            2 => self.skip_multipart_reclaim(),
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

    fn skip_abort_multipart_upload_cleanup(&mut self) -> Result<(), String> {
        self.skip_multipart_upload()?;
        self.skip_repeated(Self::skip_multipart_part)?;
        self.skip_repeated(Self::skip_multipart_part_segment)?;
        self.skip_repeated(Self::skip_stream_upload)?;
        self.skip_repeated(Self::skip_stream_upload_segment)
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

    fn read_crc64_bytes(&mut self, field: &'static str) -> Result<(), String> {
        let bytes = self.read_bytes()?;
        if bytes.len() == 8 {
            Ok(())
        } else {
            Err(format!("{field} must be exactly 8 bytes"))
        }
    }

    fn skip_object_layout(&mut self) -> Result<(), String> {
        match self.read_u8()? {
            1 => Ok(()),
            2 => self.read_nonzero_u32("multipart layout parts count"),
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

    fn skip_optional_owner_identity(&mut self) -> Result<(), String> {
        self.skip_optional(Self::skip_owner_identity)
    }

    fn read_optional_owner_identity(&mut self) -> Result<Option<OwnerIdentity>, String> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_owner_identity().map(Some),
            tag => Err(format!("invalid optional owner identity tag {tag}")),
        }
    }

    fn skip_optional_multipart_checksum_config(&mut self) -> Result<(), String> {
        self.skip_optional(|decoder| {
            decoder.read_valid_u8("multipart checksum algorithm", 0..=4)?;
            decoder.read_valid_u8("multipart checksum type", 0..=1)
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
}

fn encode_commit_multipart_object(out: &mut Vec<u8>, command: &CommitMultipartObjectCommand) {
    put_str(out, command.upload_id.as_str());
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
        encode_stream_upload(out, session);
    }
    put_u32(out, command.stream_upload_segments.len() as u32);
    for segment in &command.stream_upload_segments {
        encode_stream_upload_segment(out, segment);
    }
    put_u64(out, command.write_sequence);
    put_u64(out, command.completion_order);
    put_u64(out, command.completed_at_millis);
    match &command.initiator {
        None => put_u8(out, 0),
        Some(initiator) => {
            put_u8(out, 1);
            put_str(out, &initiator.principal);
            put_str(out, initiator.canonical_id.as_str());
        }
    }
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
}

fn encode_delete_object_version(out: &mut Vec<u8>, command: &DeleteObjectVersionCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    encode_version_id(out, command.version_id);
    match &command.target {
        DeleteObjectVersionTarget::DeleteMarker => put_u8(out, 1),
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
}

fn encode_put_object_metadata(out: &mut Vec<u8>, command: &PutObjectMetadataCommand) {
    encode_live_object_record(out, &command.object);
}

fn encode_create_stream_upload(out: &mut Vec<u8>, command: &CreateStreamUploadCommand) {
    encode_stream_upload(out, &command.session);
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
}

fn encode_create_multipart_upload(out: &mut Vec<u8>, command: &CreateMultipartUploadCommand) {
    encode_multipart_upload(out, &command.upload);
}

fn encode_abort_multipart_upload(out: &mut Vec<u8>, command: &AbortMultipartUploadCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    put_str(out, command.upload_id.as_str());
    encode_abort_multipart_upload_cleanup(out, &command.cleanup);
}

fn encode_delete_object_payload_reclaim(
    out: &mut Vec<u8>,
    command: &DeleteObjectPayloadReclaimCommand,
) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    put_u64(out, command.generation_id.get());
    encode_object_payload_reclaim(out, &command.payload);
}

fn encode_delete_completed_multipart_upload(
    out: &mut Vec<u8>,
    command: &DeleteCompletedMultipartUploadCommand,
) {
    encode_completed_multipart_upload(out, &command.record);
}

fn encode_advance_completed_multipart_upload_sequence(
    out: &mut Vec<u8>,
    command: &AdvanceCompletedMultipartUploadSequenceCommand,
) {
    put_str(out, command.bucket.as_str());
    put_u64(out, command.completion_order);
}

fn encode_completed_multipart_upload(out: &mut Vec<u8>, record: &CompletedMultipartUploadRecord) {
    put_str(out, record.upload_id.as_str());
    put_str(out, record.bucket.as_str());
    put_str(out, record.key.as_str());
    put_u64(out, record.completion_order);
    put_u64(out, record.completed_at);
    encode_optional_owner_identity(out, record.initiator.as_ref());
    encode_owner_identity(out, &record.owner);
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
        encode_stream_upload(out, session);
    }
    put_u32(out, cleanup.stream_upload_segments.len() as u32);
    for segment in &cleanup.stream_upload_segments {
        encode_stream_upload_segment(out, segment);
    }
}

fn encode_stream_upload(out: &mut Vec<u8>, session: &StreamUploadRecord) {
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
    encode_optional_owner_identity(out, upload.initiator.as_ref());
    encode_owner_identity(out, &upload.owner);
    put_str(out, &upload.acl_grants.serialized());
    put_bool(out, upload.public_read);
    put_u64(out, upload.object_generation_id.get());
    encode_object_lock_state(out, upload.object_lock);
    encode_optional_multipart_checksum_config(out, upload.checksum);
    encode_object_encryption(out, &upload.encryption);
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
    put_u64(out, bucket.completed_multipart_upload_sequence);
    put_bool(out, bucket.bucket_abac_enabled);
    encode_bucket_encryption(out, bucket.encryption);
}

fn encode_object_segment(out: &mut Vec<u8>, segment: &ObjectSegmentRecord) {
    put_str(out, segment.bucket.as_str());
    put_str(out, segment.key.as_str());
    encode_version_id(out, segment.version_id);
    put_u32(out, segment.segment_index);
    put_u64(out, segment.size);
    encode_optional_u64(out, segment.segment_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
    put_u8(out, segment.ec_k);
    put_u8(out, segment.ec_m);
}

fn encode_object_part(out: &mut Vec<u8>, part: &ObjectPartRecord) {
    put_str(out, part.bucket.as_str());
    put_str(out, part.key.as_str());
    encode_version_id(out, part.version_id);
    put_u32(out, part.part_number);
    put_u64(out, part.size);
    put_bytes(out, &part.etag);
    put_u8(out, part.etag_kind as u8);
    put_bytes(out, &part.part_okh);
    put_u64(out, part.part_vid.get());
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
    put_bytes(out, &part.etag);
    put_u8(out, part.etag_kind as u8);
    put_bytes(out, &part.part_okh);
    put_u64(out, part.part_vid.get());
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
    encode_optional_u64(out, segment.segment_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
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
    encode_optional_u64(out, segment.segment_crc64);
    put_bytes(out, &segment.segment_okh);
    put_u64(out, segment.segment_vid.get());
    put_u32(out, segment.data_pg_id);
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

fn encode_optional_owner_identity(out: &mut Vec<u8>, owner: Option<&OwnerIdentity>) {
    match owner {
        None => put_u8(out, 0),
        Some(owner) => {
            put_u8(out, 1);
            encode_owner_identity(out, owner);
        }
    }
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
    use super::*;
    use crate::types::{
        ChecksumAlgorithm, ChecksumType, EcShape, MultipartChecksumConfig,
        MultipartReclaimPartSegmentRecord, ObjectSegmentsReclaimSegmentRecord, OwnerIdentity,
        SerializedMetadataBlob, SerializedSystemMetadataBlob, SseS3ObjectState, StorageClass,
        SSE_S3_CHECKSUM_NONCE_LEN, SSE_S3_SEGMENT_NONCE_PREFIX_LEN, SSE_S3_WRAPPED_DEK_LEN,
        SSE_S3_WRAP_NONCE_LEN,
    };

    fn test_bucket_record(name: &str, generation: u64) -> BucketRecord {
        let owner = CanonicalUserId::from_principal("owner");
        let acl_grants = AclGrants::default();
        BucketRecord::from_create_config(
            &CreateBucketConfig {
                name,
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: true,
                versioning: BucketVersioningState::Enabled,
                object_lock: BucketObjectLockConfig::default(),
            },
            123,
            generation,
        )
        .unwrap()
    }

    fn assert_applied_log_decoder_accepts(envelope: &MetadataCommandEnvelope) {
        let header = decode_metadata_command_log_entry_header(&envelope.command_bytes())
            .expect("applied command bytes must decode");
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
        let decoded = decode_metadata_command_envelope(&envelope.command_bytes())
            .expect("full metadata command envelope must decode");
        assert_eq!(decoded, *envelope);
    }

    #[test]
    fn metadata_command_canonical_encoding_is_stable() {
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
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::CreateBucket(command.clone()));
        let duplicate =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::CreateBucket(command));

        assert_eq!(envelope.canonical_bytes(), duplicate.canonical_bytes());
        assert_eq!(envelope.checksum_crc64(), duplicate.checksum_crc64());
        assert!(envelope.verify_checksum());
        assert_applied_log_decoder_accepts(&envelope);
        assert_full_envelope_decoder_round_trips(&envelope);
        assert_eq!(envelope.checksum_crc64(), 0xb946d8ee1f29e72d);
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
        assert_eq!(envelope.checksum_crc64(), 0x0c1951d65edbd251);
        assert!(envelope.verify_checksum());
        assert_applied_log_decoder_accepts(&envelope);
        assert_full_envelope_decoder_round_trips(&envelope);
    }

    #[test]
    fn metadata_command_bucket_acl_encoding_is_stable() {
        let command = PutBucketAclCommand::from_bucket(
            test_bucket_record("bucket", 12),
            AclGrants::default(),
            true,
            false,
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
        assert_eq!(envelope.checksum_crc64(), 0x06ba0a6e63da3c40);
        assert!(envelope.verify_checksum());
        assert_applied_log_decoder_accepts(&envelope);
        assert_full_envelope_decoder_round_trips(&envelope);
    }

    #[test]
    fn bucket_command_encoding_ignores_runtime_write_reservation_state() {
        let mut current = test_bucket_record("bucket", 13);
        current.write_reservations_blocked = true;
        current.active_write_reservations = 7;
        current.completed_multipart_upload_sequence = 11;

        let command = PutBucketAclCommand::from_bucket(current, AclGrants::default(), true, false);
        assert!(!command.bucket.write_reservations_blocked);
        assert_eq!(command.bucket.active_write_reservations, 0);
        assert_eq!(command.bucket.completed_multipart_upload_sequence, 11);

        let id = MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(3),
            MetadataCommandLogIndex::new(13).unwrap(),
        );
        let envelope =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::PutBucketAcl(command));
        let mut duplicate_current = test_bucket_record("bucket", 13);
        duplicate_current.completed_multipart_upload_sequence = 11;
        let duplicate = MetadataCommandEnvelope::new(
            id,
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                duplicate_current,
                AclGrants::default(),
                true,
                false,
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
        assert_eq!(envelope.checksum_crc64(), 0x8d2d435216256076);
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
                0x095cf1d0417e611c,
                0x4711437e605b526e,
                0xe610caa6d4b779ba,
                0x711d9104009568b9,
                0x23c58b23008c3a11,
                0xd6c41b2d75142020,
                0xe1c94f024274c273,
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
                0x2c71312aba70ec12,
                0x7a3aad2f1477e115,
                0xa9d13d13566d4ae2,
                0x26a1d1cfadbac4da,
                0x48043526c6d0ce88,
                0xb566708169760d44,
                0xfcc8ed210f34db99,
                0x4b00ed0e84caf2ba,
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
            segment_crc64: Some(9),
            segment_okh: [7; 16],
            segment_vid: generation_id,
            data_pg_id: 2,
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
            etag: vec![4; 8],
            etag_kind: crate::types::EtagKind::Crc64,
            part_okh: [0; 16],
            part_vid: generation_id,
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
            initiator: Some(OwnerIdentity::from_principal("initiator")),
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: true,
            object_generation_id: generation_id,
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
            etag: vec![4; 8],
            etag_kind: crate::types::EtagKind::Crc64,
            part_okh: [0; 16],
            part_vid: generation_id,
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
            segment_crc64: Some(12),
            segment_okh: [12; 16],
            segment_vid: generation_id,
            data_pg_id: 2,
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
            segment_crc64: Some(10),
            segment_okh: [10; 16],
            segment_vid: generation_id,
            data_pg_id: 2,
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
            })),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
                object: object.clone(),
                segments: vec![segment.clone()],
                generation_reservation_id: reservation_id.clone(),
                write_sequence: 45,
                last_modified_millis: 556,
                stale_payload: Some(multipart_reclaim.clone()),
            })),
            MetadataCommandPayload::CommitMultipartObject(Box::new(CommitMultipartObjectCommand {
                upload_id: upload_id.clone(),
                object: multipart_object,
                parts: vec![part],
                selected_streaming_segments: vec![selected_streaming_segment.clone()],
                omitted_parts: vec![uploaded_part.clone()],
                omitted_streaming_segments: vec![omitted_streaming_segment.clone()],
                stream_uploads: vec![StreamUploadRecord {
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
                completion_order: 12,
                completed_at_millis: 556,
                initiator: Some(OwnerIdentity::from_principal("initiator")),
                last_modified_millis: 557,
                stale_payload: Some(multipart_reclaim.clone()),
            })),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(7),
                target: DeleteObjectVersionTarget::DeleteMarker,
            })),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
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
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(8),
                owner: OwnerIdentity::from_principal("owner"),
                write_sequence: 46,
                last_modified_millis: 558,
                stale_payload: None,
            }),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(9),
                owner: OwnerIdentity::from_principal("owner"),
                write_sequence: 47,
                last_modified_millis: 559,
                stale_payload: Some(segment_reclaim.clone()),
            }),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(10),
                owner: OwnerIdentity::from_principal("owner"),
                write_sequence: 48,
                last_modified_millis: 560,
                stale_payload: Some(multipart_reclaim.clone()),
            }),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                object: LiveObjectRecord {
                    version_id: VersionId::from_u64(8),
                    tags: Some(SerializedTagSet::new("<Tagging/>".to_string())),
                    ..metadata_object.clone()
                },
            })),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                object: LiveObjectRecord {
                    version_id: VersionId::from_u64(8),
                    tags: None,
                    ..metadata_object.clone()
                },
            })),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
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
                CreateMultipartUploadCommand::from_request(
                    CreateMultipartUploadReq {
                        upload_id: upload_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        tags: Some(crate::SerializedTagSet::new("<Tagging/>".to_string())),
                        metadata_blob: SerializedMetadataBlob::new(vec![1, 2, 3]),
                        system_metadata_blob: SerializedSystemMetadataBlob::new(vec![4, 5, 6]),
                        initiator: Some(OwnerIdentity::from_principal("initiator")),
                        owner: OwnerIdentity::from_principal("owner"),
                        acl_grants: AclGrants::default(),
                        public_read: true,
                        object_lock: ObjectLockState::default(),
                        checksum: None,
                        encryption: ObjectEncryption::None,
                    },
                    generation_id,
                    560,
                ),
            )),
            MetadataCommandPayload::CreateMultipartUpload(Box::new(CreateMultipartUploadCommand {
                upload: multipart_upload_with_checksum,
            })),
            MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
                cleanup: AbortMultipartUploadCleanup {
                    upload: multipart_upload.clone(),
                    parts: vec![uploaded_part.clone()],
                    streaming_segments: vec![omitted_streaming_segment.clone()],
                    stream_uploads: vec![StreamUploadRecord {
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
            })),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request(
                    CreateStreamUploadReq {
                        session_id: stream_session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: StreamUploadTarget::PutObject,
                        encryption: ObjectEncryption::None,
                    },
                    561,
                ),
            )),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request(
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
                ),
            )),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request(
                    CreateStreamUploadReq {
                        session_id: SessionId::try_from("33".repeat(16)).unwrap(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: StreamUploadTarget::PutObject,
                        encryption: sse_s3_encryption,
                    },
                    563,
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
            })),
            MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(
                DeleteObjectPayloadReclaimCommand::new(
                    bucket.clone(),
                    key.clone(),
                    generation_id,
                    segment_reclaim,
                ),
            )),
            MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(
                DeleteObjectPayloadReclaimCommand::new(
                    bucket.clone(),
                    key.clone(),
                    generation_id,
                    multipart_reclaim,
                ),
            )),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                object: LiveObjectRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::from_u64(8),
                    acl_grants: AclGrants::default(),
                    public_read: true,
                    ..metadata_object
                },
            })),
            MetadataCommandPayload::DeleteCompletedMultipartUpload(Box::new(
                DeleteCompletedMultipartUploadCommand {
                    record: CompletedMultipartUploadRecord {
                        upload_id,
                        bucket,
                        key,
                        completion_order: 12,
                        completed_at: 556,
                        initiator: Some(OwnerIdentity::from_principal("initiator")),
                        owner: OwnerIdentity::from_principal("owner"),
                    },
                },
            )),
            MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
                AdvanceCompletedMultipartUploadSequenceCommand {
                    bucket: BucketName::try_from("bucket".to_string()).unwrap(),
                    completion_order: 13,
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
            if matches!(
                envelope.payload(),
                MetadataCommandPayload::DeleteCompletedMultipartUpload(_)
                    | MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_)
            ) {
                assert_full_envelope_decoder_round_trips(&envelope);
            }
            checksums.push(envelope.checksum_crc64());
        }
        assert_eq!(
            checksums,
            [
                0x5fc3fd9935e6b23a,
                0x56db6be41cc9a89c,
                0x3acf49df359790d4,
                0xb5a8e642f639b9a8,
                0x892df6c0f857bf33,
                0x7920c33a006e1d68,
                0x53fdbf4c6f062d53,
                0x6df04a5fc73e478a,
                0x2a3c1d82cb08bbdd,
                0x6a5e23df842faebe,
                0x8e2a154ef6fa870e,
                0xa3de4500905f67bf,
                0xc0d44346162de207,
                0x74df7243409a2637,
                0x5fd68ba34c3c927a,
                0xaaee1aa183a67da1,
                0x423d5ce8ecc3f471,
                0xa10946bfbddcbb08,
                0x8f0590d0286f0dc4,
                0x4c8d2ebe4cd0d8d4,
                0x5dd9ac5f99954560,
                0xf89fe8126b9974bf,
                0x8d3e5d6cb995e021,
                0x873424a13234f823,
                0x7cf30e2471f346ae,
                0xa9cde2110916a8a6,
                0x0e53aa8cb595ea77,
                0x7198824f0ecf3d31,
                0x13ddd49bdbc91001,
                0x1946524188e07bbb,
            ]
        );
    }
}
