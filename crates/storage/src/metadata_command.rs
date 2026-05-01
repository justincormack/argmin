use std::num::NonZeroU64;

use s3_types::{
    AclGrants, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId,
    ObjectLockDefaultRetention, ObjectLockMode, ObjectLockState, ObjectRetention, RetentionPeriod,
    StoredLegalHoldStatus,
};

use crate::types::{
    AbortMultipartUploadCleanup, BucketEncryptionConfig, BucketName, BucketObjectOwnership,
    BucketOwnershipControls, BucketSubresourceAux, BucketSubresourceKind, ClusterEpoch,
    CreateBucketConfig, CreateMultipartUploadReq, CreateStreamUploadReq, GenerationId,
    ManagedEncryptionAlgorithm, MultipartPartRecord, MultipartPartSegmentRecord,
    MultipartReclaimPartRecord, MultipartReclaimRecord, MultipartUploadRecord, ObjectEncryption,
    ObjectEtag, ObjectKey, ObjectLayout, ObjectPartRecord, ObjectSegmentRecord,
    ObjectSegmentsReclaimRecord, OwnerIdentity, PgId, PublicAccessBlockConfig, PutLiveObjectReq,
    SessionId, StreamUploadSegmentRecord, StreamUploadTarget, UploadId, VersionId,
};

const METADATA_COMMAND_MAGIC: &[u8] = b"argmin-metadata-command";
const METADATA_COMMAND_ENCODING_VERSION: u16 = 1;
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
pub(crate) struct CreateBucketCommand {
    pub(crate) name: BucketName,
    pub(crate) owner_principal: String,
    pub(crate) owner_canonical_id: CanonicalUserId,
    pub(crate) acl_grants: AclGrants,
    pub(crate) public_read: bool,
    pub(crate) public_write: bool,
    pub(crate) versioning: BucketVersioningState,
    pub(crate) object_lock: BucketObjectLockConfig,
    pub(crate) created_at_millis: u64,
    pub(crate) bucket_execution_generation: u64,
}

impl CreateBucketCommand {
    pub(crate) fn from_config(
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
            acl_grants: config.acl_grants.clone(),
            public_read: config.public_read,
            public_write: config.public_write,
            versioning: config.versioning,
            object_lock: config.object_lock,
            created_at_millis,
            bucket_execution_generation,
        })
    }

    pub(crate) fn config(&self) -> CreateBucketConfig<'_> {
        CreateBucketConfig {
            name: self.name.as_str(),
            owner_principal: &self.owner_principal,
            owner_canonical_id: &self.owner_canonical_id,
            acl_grants: &self.acl_grants,
            public_read: self.public_read,
            public_write: self.public_write,
            versioning: self.versioning,
            object_lock: self.object_lock,
        }
    }

    pub(crate) fn matches_config(&self, config: &CreateBucketConfig<'_>) -> bool {
        let Ok(name) = BucketName::try_from(config.name.to_string()) else {
            return false;
        };
        self.name == name
            && self.owner_principal == config.owner_principal
            && &self.owner_canonical_id == config.owner_canonical_id
            && &self.acl_grants == config.acl_grants
            && self.public_read == config.public_read
            && self.public_write == config.public_write
            && self.versioning == config.versioning
            && self.object_lock == config.object_lock
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MetadataCommandPayload {
    CreateBucket(CreateBucketCommand),
    PutBucketVersioning(PutBucketVersioningCommand),
    PutBucketAcl(PutBucketAclCommand),
    PutBucketProperty(PutBucketPropertyCommand),
    PutBucketSubresource(PutBucketSubresourceCommand),
    ReserveObjectGeneration(ReserveObjectGenerationCommand),
    ReleaseObjectGeneration(ReleaseObjectGenerationCommand),
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
}

impl MetadataCommandPayload {
    fn kind_id(&self) -> u16 {
        match self {
            Self::CreateBucket(_) => METADATA_COMMAND_CREATE_BUCKET,
            Self::PutBucketVersioning(_) => METADATA_COMMAND_PUT_BUCKET_VERSIONING,
            Self::PutBucketAcl(_) => METADATA_COMMAND_PUT_BUCKET_ACL,
            Self::PutBucketProperty(_) => METADATA_COMMAND_PUT_BUCKET_PROPERTY,
            Self::PutBucketSubresource(_) => METADATA_COMMAND_PUT_BUCKET_SUBRESOURCE,
            Self::ReserveObjectGeneration(_) => METADATA_COMMAND_RESERVE_OBJECT_GENERATION,
            Self::ReleaseObjectGeneration(_) => METADATA_COMMAND_RELEASE_OBJECT_GENERATION,
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutBucketVersioningCommand {
    pub(crate) name: BucketName,
    pub(crate) state: BucketVersioningState,
    pub(crate) bucket_execution_generation: u64,
}

impl PutBucketVersioningCommand {
    pub(crate) fn new(
        name: BucketName,
        state: BucketVersioningState,
        bucket_execution_generation: u64,
    ) -> Self {
        Self {
            name,
            state,
            bucket_execution_generation,
        }
    }

    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        state: BucketVersioningState,
    ) -> bool {
        self.name == *bucket && self.state == state
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutBucketAclCommand {
    pub(crate) name: BucketName,
    pub(crate) acl_grants: AclGrants,
    pub(crate) public_read: bool,
    pub(crate) public_write: bool,
    pub(crate) bucket_execution_generation: u64,
}

impl PutBucketAclCommand {
    pub(crate) fn new(
        name: BucketName,
        acl_grants: AclGrants,
        public_read: bool,
        public_write: bool,
        bucket_execution_generation: u64,
    ) -> Self {
        Self {
            name,
            acl_grants,
            public_read,
            public_write,
            bucket_execution_generation,
        }
    }

    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> bool {
        self.name == *bucket
            && self.acl_grants == *acl_grants
            && self.public_read == public_read
            && self.public_write == public_write
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutBucketPropertyCommand {
    pub(crate) name: BucketName,
    pub(crate) mutation: BucketPropertyMutation,
    pub(crate) bucket_execution_generation: u64,
}

impl PutBucketPropertyCommand {
    pub(crate) fn new(
        name: BucketName,
        mutation: BucketPropertyMutation,
        bucket_execution_generation: u64,
    ) -> Self {
        Self {
            name,
            mutation,
            bucket_execution_generation,
        }
    }

    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        mutation: &BucketPropertyMutation,
    ) -> bool {
        self.name == *bucket && self.mutation == *mutation
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

    pub(crate) fn matches_request(
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
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) version_id: VersionId,
    pub(crate) mutation: PutObjectMetadataMutation,
}

impl PutObjectMetadataCommand {
    pub(crate) fn matches_request(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        mutation: &PutObjectMetadataMutation,
    ) -> bool {
        self.bucket == *bucket
            && self.key == *key
            && self.version_id == version_id
            && self.mutation == *mutation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateStreamUploadCommand {
    pub(crate) request: CreateStreamUploadReq,
    pub(crate) created_at_millis: u64,
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
    pub(crate) request: CreateMultipartUploadReq,
    pub(crate) object_generation_id: GenerationId,
    pub(crate) initiated_at_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbortMultipartUploadCommand {
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) upload_id: UploadId,
    pub(crate) cleanup: AbortMultipartUploadCleanup,
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

    #[cfg(test)]
    pub(crate) fn checksum_crc64(&self) -> u64 {
        self.checksum_crc64
    }

    #[cfg(test)]
    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = canonical_command_bytes(self.id, &self.payload);
        put_u64(&mut out, self.checksum_crc64);
        out
    }

    pub(crate) fn verify_checksum(&self) -> bool {
        checksum::crc64::checksum(&canonical_command_bytes(self.id, &self.payload))
            == self.checksum_crc64
    }
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
        MetadataCommandPayload::ReserveObjectGeneration(command) => {
            encode_reserve_object_generation(&mut out, command);
        }
        MetadataCommandPayload::ReleaseObjectGeneration(command) => {
            encode_release_object_generation(&mut out, command);
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
    }
    out
}

fn encode_create_bucket(out: &mut Vec<u8>, command: &CreateBucketCommand) {
    put_str(out, command.name.as_str());
    put_str(out, &command.owner_principal);
    put_str(out, command.owner_canonical_id.as_str());
    put_str(out, &command.acl_grants.serialized());
    put_bool(out, command.public_read);
    put_bool(out, command.public_write);
    put_u8(out, command.versioning as u8);
    encode_object_lock(out, command.object_lock);
    put_u64(out, command.created_at_millis);
    put_u64(out, command.bucket_execution_generation);
}

fn encode_put_bucket_versioning(out: &mut Vec<u8>, command: &PutBucketVersioningCommand) {
    put_str(out, command.name.as_str());
    put_u8(out, command.state as u8);
    put_u64(out, command.bucket_execution_generation);
}

fn encode_put_bucket_acl(out: &mut Vec<u8>, command: &PutBucketAclCommand) {
    put_str(out, command.name.as_str());
    put_str(out, &command.acl_grants.serialized());
    put_bool(out, command.public_read);
    put_bool(out, command.public_write);
    put_u64(out, command.bucket_execution_generation);
}

fn encode_put_bucket_property(out: &mut Vec<u8>, command: &PutBucketPropertyCommand) {
    put_str(out, command.name.as_str());
    encode_bucket_property_mutation(out, &command.mutation);
    put_u64(out, command.bucket_execution_generation);
}

fn encode_put_bucket_subresource(out: &mut Vec<u8>, command: &PutBucketSubresourceCommand) {
    put_str(out, command.name.as_str());
    encode_bucket_subresource_mutation(out, &command.mutation);
    put_u64(out, command.bucket_execution_generation);
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
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    encode_version_id(out, command.version_id);
    match &command.mutation {
        PutObjectMetadataMutation::PutTags(tags) => {
            put_u8(out, 1);
            put_str(out, tags);
        }
        PutObjectMetadataMutation::DeleteTags => {
            put_u8(out, 2);
        }
        PutObjectMetadataMutation::PutRetention(retention) => {
            put_u8(out, 3);
            put_u8(out, retention.mode as u8);
            put_u64(out, retention.retain_until_unix_seconds);
        }
        PutObjectMetadataMutation::PutLegalHold(legal_hold) => {
            put_u8(out, 4);
            put_u8(out, *legal_hold as u8);
        }
        PutObjectMetadataMutation::PutAcl {
            acl_grants,
            public_read,
        } => {
            put_u8(out, 5);
            put_str(out, &acl_grants.serialized());
            put_bool(out, *public_read);
        }
    }
}

fn encode_create_stream_upload(out: &mut Vec<u8>, command: &CreateStreamUploadCommand) {
    put_str(out, command.request.session_id.as_str());
    put_str(out, command.request.bucket.as_str());
    put_str(out, command.request.key.as_str());
    encode_stream_upload_target(out, &command.request.target);
    encode_object_encryption(out, &command.request.encryption);
    put_u64(out, command.created_at_millis);
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
    let request = &command.request;
    put_str(out, request.upload_id.as_str());
    put_str(out, request.bucket.as_str());
    put_str(out, request.key.as_str());
    encode_optional_str(out, request.tags.as_ref().map(|tags| tags.as_str()));
    put_bytes(out, request.metadata_blob.as_slice());
    put_bytes(out, request.system_metadata_blob.as_slice());
    encode_optional_owner_identity(out, request.initiator.as_ref());
    encode_owner_identity(out, &request.owner);
    put_str(out, &request.acl_grants.serialized());
    put_bool(out, request.public_read);
    encode_object_lock_state(out, request.object_lock);
    encode_optional_multipart_checksum_config(out, request.checksum);
    encode_object_encryption(out, &request.encryption);
    put_u64(out, command.object_generation_id.get());
    put_u64(out, command.initiated_at_millis);
}

fn encode_abort_multipart_upload(out: &mut Vec<u8>, command: &AbortMultipartUploadCommand) {
    put_str(out, command.bucket.as_str());
    put_str(out, command.key.as_str());
    put_str(out, command.upload_id.as_str());
    encode_abort_multipart_upload_cleanup(out, &command.cleanup);
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

fn encode_bucket_property_mutation(out: &mut Vec<u8>, mutation: &BucketPropertyMutation) {
    match mutation {
        BucketPropertyMutation::ObjectLock(config) => {
            put_u8(out, 1);
            encode_object_lock(out, *config);
        }
        BucketPropertyMutation::Encryption(config) => {
            put_u8(out, 2);
            encode_bucket_encryption(out, *config);
        }
        BucketPropertyMutation::PublicAccessBlock(config) => {
            put_u8(out, 3);
            encode_public_access_block(out, *config);
        }
        BucketPropertyMutation::OwnershipControls(config) => {
            put_u8(out, 4);
            encode_ownership_controls(out, *config);
        }
        BucketPropertyMutation::AbacEnabled(enabled) => {
            put_u8(out, 5);
            put_bool(out, *enabled);
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
        EcShape, MultipartReclaimPartSegmentRecord, ObjectSegmentsReclaimSegmentRecord,
        OwnerIdentity, SerializedMetadataBlob, SerializedSystemMetadataBlob,
    };

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
        assert_eq!(envelope.checksum_crc64(), 0xa1bf3d54b1685454);
    }

    #[test]
    fn metadata_command_versioning_encoding_is_stable() {
        let command = PutBucketVersioningCommand::new(
            BucketName::try_from("bucket").unwrap(),
            BucketVersioningState::Enabled,
            11,
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
        assert_eq!(envelope.checksum_crc64(), 0xa7a53964fbc32e58);
        assert!(envelope.verify_checksum());
    }

    #[test]
    fn metadata_command_bucket_acl_encoding_is_stable() {
        let command = PutBucketAclCommand::new(
            BucketName::try_from("bucket").unwrap(),
            AclGrants::default(),
            true,
            false,
            12,
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
        assert_eq!(envelope.checksum_crc64(), 0x3947184ebe5f3b3b);
        assert!(envelope.verify_checksum());
    }

    #[test]
    fn metadata_command_bucket_property_encoding_is_stable() {
        let bucket = BucketName::try_from("bucket").unwrap();
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
            let command =
                PutBucketPropertyCommand::new(bucket.clone(), mutation.clone(), 20 + offset as u64);
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
            checksums.push(envelope.checksum_crc64());
        }
        assert_eq!(
            checksums,
            [
                0x239f63ca5e298fa1,
                0xb5c042bf2bf79c2d,
                0x01ff273102f24631,
                0x703c1fc321a3a801,
                0xbc2d9efb84c3ed11,
                0xd8eb45a268b77749,
                0xcc7e17ba9b256211,
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
                stale_payload: Some(segment_reclaim),
            }),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(10),
                owner: OwnerIdentity::from_principal("owner"),
                write_sequence: 48,
                last_modified_millis: 560,
                stale_payload: Some(multipart_reclaim),
            }),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(8),
                mutation: PutObjectMetadataMutation::PutTags("<Tagging/>".to_string()),
            })),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(8),
                mutation: PutObjectMetadataMutation::DeleteTags,
            })),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(8),
                mutation: PutObjectMetadataMutation::PutRetention(ObjectRetention {
                    mode: ObjectLockMode::Governance,
                    retain_until_unix_seconds: 999,
                }),
            })),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::from_u64(8),
                mutation: PutObjectMetadataMutation::PutLegalHold(StoredLegalHoldStatus::On),
            })),
            MetadataCommandPayload::CreateMultipartUpload(Box::new(CreateMultipartUploadCommand {
                request: CreateMultipartUploadReq {
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
                object_generation_id: generation_id,
                initiated_at_millis: 560,
            })),
            MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
                cleanup: AbortMultipartUploadCleanup {
                    upload: multipart_upload.clone(),
                    parts: vec![uploaded_part.clone()],
                    streaming_segments: vec![omitted_streaming_segment.clone()],
                },
            })),
            MetadataCommandPayload::CreateStreamUpload(Box::new(CreateStreamUploadCommand {
                request: CreateStreamUploadReq {
                    session_id: stream_session_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    target: StreamUploadTarget::PutObject,
                    encryption: ObjectEncryption::None,
                },
                created_at_millis: 561,
            })),
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
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                bucket,
                key,
                version_id: VersionId::from_u64(8),
                mutation: PutObjectMetadataMutation::PutAcl {
                    acl_grants: AclGrants::default(),
                    public_read: true,
                },
            })),
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
            checksums.push(envelope.checksum_crc64());
        }
        assert_eq!(
            checksums,
            [
                0x5fc3fd9935e6b23a,
                0x56db6be41cc9a89c,
                0xee750f881921402d,
                0xfe04371c6a46ec80,
                0x8e971303e9b59031,
                0x66ff987ed0727bc1,
                0x37077018dfee09b9,
                0xfce4f84779e4e364,
                0xa1b20e6eb91852f7,
                0x4a877a8134817636,
                0x1f4a02b6464a538c,
                0x01cd87bdfd723201,
                0x12751ef35639efd3,
                0x22fd0e282e38997a,
                0x1c7aebe9c981fa68,
                0x2c69c1feb283495d,
                0xb67d60bbbb74f9d1,
                0xf1b058002ad6040e,
                0x7555192579a20442,
                0x505fc17b646186f2,
                0xb9a1ca30cfa59630,
            ]
        );
    }
}
