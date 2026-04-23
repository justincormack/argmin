use std::sync::Arc;

use auth::BucketPolicy;
use checksum::MultipartChecksumConfig;

use super::authz_types::{ActiveWriteEncryption, AuthorizedPutObjectWrite, ValidatedBucket};
use super::pg_guards::{LockedReadObject, ObjectPgGuards};
use super::request_types::{CreateBucketAcl, Requester};
use super::response_types::{BucketSummary, GetBucketAclResult, GetObjectAclResult};
use crate::sse::SseCustomerWriteContext;
use s3_types::{
    AclGrants, BucketLifecycleConfiguration, BucketVersioningState, CanonicalUserId,
    LegalHoldStatus, ObjectRetention, StoredLegalHoldStatus, VersionId,
};
use storage::BucketObjectOwnership;
use storage::{
    BucketEncryptionConfig, BucketName, BucketObjectLockConfig, BucketOwnershipControls,
    EffectiveBucketEncryptionConfig, MultipartUploadRecord, ObjectKey, ObjectLockState,
    ObjectReadSnapshot, OwnerIdentity, PublicAccessBlockConfig, StoredObject, UploadId,
};

#[derive(Debug)]
pub(super) struct AuthorizedBucketSubresourcePut {
    pub(super) bucket: BucketName,
    pub(super) kind: storage::BucketSubresourceKind,
    pub(super) body: String,
}

#[derive(Debug)]
pub(super) struct AuthorizedBucketSubresourceGet {
    pub(super) bucket: BucketName,
    pub(super) kind: storage::BucketSubresourceKind,
}

#[derive(Debug)]
pub(super) struct AuthorizedBucketSubresourceBodyGet {
    pub(super) body: Option<String>,
}

#[derive(Debug)]
pub(super) struct AuthorizedBucketSubresourceDelete {
    pub(super) bucket: BucketName,
    pub(super) kind: storage::BucketSubresourceKind,
}

#[derive(Debug)]
pub(super) struct AuthorizedBucketConfigAccess {
    pub(super) bucket: BucketName,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketAbac {
    pub(super) enabled: bool,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketPublicAccessBlock {
    pub(super) config: Option<PublicAccessBlockConfig>,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketOwnershipControls {
    pub(super) config: Option<BucketOwnershipControls>,
}

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketPublicAccessBlock {
    pub(super) bucket: BucketName,
    pub(super) config: PublicAccessBlockConfig,
}

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketOwnershipControls {
    pub(super) bucket: BucketName,
    pub(super) config: BucketOwnershipControls,
}

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketAbac {
    pub(super) bucket: BucketName,
    pub(super) enabled: bool,
}

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketPolicy {
    pub(super) bucket: BucketName,
    pub(super) body: String,
    pub(super) parsed_policy: Arc<BucketPolicy>,
    pub(super) policy_is_public: bool,
}

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketLifecycle {
    pub(super) bucket: BucketName,
    pub(super) body: String,
    pub(super) parsed_config: BucketLifecycleConfiguration,
}

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketEncryption {
    pub(super) bucket: BucketName,
    pub(super) config: BucketEncryptionConfig,
}

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketVersioning {
    pub(super) bucket: BucketName,
    pub(super) state: BucketVersioningState,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketVersioning {
    pub(super) state: BucketVersioningState,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketLocation;

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketObjectLockConfiguration {
    pub(super) bucket: BucketName,
    pub(super) config: BucketObjectLockConfig,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketEncryption {
    pub(super) config: EffectiveBucketEncryptionConfig,
}

#[derive(Debug)]
pub(super) struct AuthorizedDeleteBucketEncryption {
    pub(super) bucket: BucketName,
}

#[derive(Debug)]
pub(super) struct AuthorizedCreateBucket {
    pub(super) name: BucketName,
    pub(super) requester: Requester,
    pub(super) owner: OwnerIdentity,
    pub(super) locked_to_account_region: bool,
    pub(super) acl: CreateBucketAcl,
    pub(super) ownership: BucketObjectOwnership,
    pub(super) object_lock_enabled: bool,
    pub(super) acl_grants: AclGrants,
}

#[derive(Debug)]
pub(super) struct AuthorizedHeadBucket {
    pub(super) bucket_info: BucketSummary,
}

#[derive(Debug)]
pub(super) struct AuthorizedDeleteBucket {
    pub(super) name: BucketName,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketObjectLockConfiguration {
    pub(super) config: BucketObjectLockConfig,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketPolicyStatus {
    pub(super) is_public: bool,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketAcl {
    pub(super) result: GetBucketAclResult,
}

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketAcl {
    pub(super) bucket: BucketName,
    pub(super) acl_grants: AclGrants,
    pub(super) public_read: bool,
    pub(super) public_write: bool,
}

pub(super) struct AuthorizedObjectTagsAccess<'a> {
    pub(super) bucket: BucketName,
    pub(super) key: ObjectKey,
    pub(super) version_id: VersionId,
    pub(super) pgs: ObjectPgGuards<'a>,
}

pub(super) struct AuthorizedPutObjectRetention<'a> {
    pub(super) bucket: BucketName,
    pub(super) key: ObjectKey,
    pub(super) version_id: VersionId,
    pub(super) retention: ObjectRetention,
    pub(super) pgs: ObjectPgGuards<'a>,
}

pub(super) struct AuthorizedGetObjectRetention {
    pub(super) retention: Option<ObjectRetention>,
}

pub(super) struct AuthorizedPutObjectLegalHold<'a> {
    pub(super) bucket: BucketName,
    pub(super) key: ObjectKey,
    pub(super) version_id: VersionId,
    pub(super) legal_hold: StoredLegalHoldStatus,
    pub(super) pgs: ObjectPgGuards<'a>,
}

pub(super) struct AuthorizedGetObjectLegalHold {
    pub(super) legal_hold: Option<LegalHoldStatus>,
}

pub(super) struct AuthorizedPutObjectAclUpdate<'a> {
    pub(super) bucket: BucketName,
    pub(super) key: ObjectKey,
    pub(super) version_id: VersionId,
    pub(super) acl_grants: AclGrants,
    pub(super) public_read: bool,
    pub(super) pgs: ObjectPgGuards<'a>,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetObjectAcl {
    pub(super) result: GetObjectAclResult,
}

pub(super) struct AuthorizedObjectRead<'a> {
    pub(super) locked: LockedReadObject<'a>,
}

pub(super) struct LoadedObjectState<'a> {
    pub(super) bucket_info: ValidatedBucket,
    pub(super) bucket_policy: Option<Arc<BucketPolicy>>,
    pub(super) bucket_tags: Option<Vec<(String, String)>>,
    pub(super) locked: LockedReadObject<'a>,
}

pub(super) struct AuthorizedCopyObject {
    pub(super) source: ObjectReadSnapshot,
    pub(super) destination: AuthorizedPutObjectWrite,
}

#[derive(Debug)]
pub(super) struct AuthorizedCreateMultipartUpload {
    pub(super) bucket_info: BucketSummary,
    pub(super) bucket: BucketName,
    pub(super) key: ObjectKey,
    pub(super) tags: Option<String>,
    pub(super) checksum: Option<MultipartChecksumConfig>,
    pub(super) initiator: Option<OwnerIdentity>,
    pub(super) owner: OwnerIdentity,
    pub(super) acl_grants: AclGrants,
    pub(super) public_read: bool,
    pub(super) object_lock: ObjectLockState,
    pub(super) write_encryption: ActiveWriteEncryption,
}

pub(super) struct AuthorizedBeginStreamPart {
    pub(super) bucket: BucketName,
    pub(super) key: ObjectKey,
    pub(super) upload_id: UploadId,
    pub(super) part_number: u32,
    pub(super) upload: MultipartUploadRecord,
    pub(super) sse_customer: Option<SseCustomerWriteContext>,
}

#[derive(Debug)]
pub(super) struct AuthorizedMultipartPartWrite {
    pub(super) bucket: BucketName,
    pub(super) key: ObjectKey,
    pub(super) upload_id: UploadId,
    pub(super) part_number: u32,
    pub(super) upload: MultipartUploadRecord,
    pub(super) sse_customer: Option<SseCustomerWriteContext>,
}

pub(super) struct AuthorizedUploadPartCopy {
    pub(super) source: ObjectReadSnapshot,
    pub(super) destination: AuthorizedMultipartPartWrite,
}

#[derive(Debug)]
pub(super) struct AuthorizedCompleteMultipartUpload {
    pub(super) bucket_info: BucketSummary,
    pub(super) bucket: BucketName,
    pub(super) key: ObjectKey,
    pub(super) upload_id: UploadId,
    pub(super) upload: MultipartUploadRecord,
    pub(super) multipart_write_encryption: ActiveWriteEncryption,
}

#[derive(Debug)]
pub(super) enum AuthorizedAbortMultipartUpload {
    InProgress {
        bucket: BucketName,
        key: ObjectKey,
        upload_id: UploadId,
    },
    Completed,
}

#[derive(Debug)]
pub(super) struct AuthorizedListObjectsV2 {
    pub(super) bucket_info: BucketSummary,
}

#[derive(Debug)]
pub(super) struct AuthorizedListObjectVersions {
    pub(super) bucket_info: BucketSummary,
}

#[derive(Debug)]
pub(super) struct AuthorizedListMultipartUploads {
    pub(super) bucket: BucketName,
}

#[derive(Debug)]
pub(super) struct AuthorizedListBuckets {
    pub(super) owner_canonical_id: CanonicalUserId,
}

pub(super) enum AuthorizedDeleteObject<'a> {
    UnversionedMissing,
    UnversionedStored {
        bucket: BucketName,
        key: ObjectKey,
        stored: StoredObject,
        pgs: ObjectPgGuards<'a>,
    },
    SpecificVersionMissing {
        version_id: VersionId,
    },
    SpecificVersion {
        bucket: BucketName,
        key: ObjectKey,
        version_id: VersionId,
        requester: Requester,
        bucket_info: ValidatedBucket,
        bucket_policy: Option<Arc<BucketPolicy>>,
        bucket_tags: Option<Vec<(String, String)>>,
        bypass_governance: bool,
    },
    CurrentDeleteMarkerInsert {
        bucket: BucketName,
        key: ObjectKey,
        owner: OwnerIdentity,
        current: Option<LockedReadObject<'a>>,
    },
}

impl std::fmt::Debug for AuthorizedObjectTagsAccess<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizedObjectTagsAccess")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("version_id", &self.version_id)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AuthorizedPutObjectRetention<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizedPutObjectRetention")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("version_id", &self.version_id)
            .field("retention", &self.retention)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AuthorizedPutObjectLegalHold<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizedPutObjectLegalHold")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("version_id", &self.version_id)
            .field("legal_hold", &self.legal_hold)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AuthorizedPutObjectAclUpdate<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizedPutObjectAclUpdate")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("version_id", &self.version_id)
            .field("acl_grants", &self.acl_grants)
            .field("public_read", &self.public_read)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AuthorizedObjectRead<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizedObjectRead")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AuthorizedCopyObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizedCopyObject")
            .field("destination", &self.destination)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AuthorizedBeginStreamPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizedBeginStreamPart")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("upload_id", &self.upload_id)
            .field("part_number", &self.part_number)
            .field("upload", &self.upload)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AuthorizedUploadPartCopy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizedUploadPartCopy")
            .field("destination", &self.destination)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AuthorizedDeleteObject<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnversionedMissing => f.write_str("AuthorizedDeleteObject::UnversionedMissing"),
            Self::UnversionedStored { bucket, key, .. } => f
                .debug_struct("AuthorizedDeleteObject::UnversionedStored")
                .field("bucket", bucket)
                .field("key", key)
                .finish_non_exhaustive(),
            Self::SpecificVersionMissing { version_id } => f
                .debug_struct("AuthorizedDeleteObject::SpecificVersionMissing")
                .field("version_id", version_id)
                .finish(),
            Self::SpecificVersion {
                bucket,
                key,
                version_id,
                ..
            } => f
                .debug_struct("AuthorizedDeleteObject::SpecificVersion")
                .field("bucket", bucket)
                .field("key", key)
                .field("version_id", version_id)
                .finish_non_exhaustive(),
            Self::CurrentDeleteMarkerInsert { bucket, key, .. } => f
                .debug_struct("AuthorizedDeleteObject::CurrentDeleteMarkerInsert")
                .field("bucket", bucket)
                .field("key", key)
                .finish_non_exhaustive(),
        }
    }
}
