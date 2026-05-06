#![cfg_attr(test, allow(dead_code))]

use std::sync::Arc;

use auth::BucketPolicy;
use checksum::MultipartChecksumConfig;

use super::authz_types::{ActiveWriteEncryption, AuthorizedPutObjectWrite, ValidatedBucket};
use super::request_types::{CreateBucketAcl, Requester};
use super::response_types::{BucketSummary, GetBucketAclResult};
use crate::sse::SseCustomerWriteContext;
use s3_types::{AclGrants, BucketVersioningState, CanonicalUserId, VersionId};
use storage::BucketObjectOwnership;
use storage::{
    AuthorizedMultipartUploadRecord, BucketEncryptionConfig, BucketName, BucketObjectLockConfig,
    BucketOwnershipControls, EffectiveBucketEncryptionConfig, ObjectKey, ObjectLockState,
    ObjectReadSnapshot, OwnerIdentity, PublicAccessBlockConfig, UploadId,
};

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketCors {
    pub(super) bucket: BucketName,
    pub(super) body: String,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketCors {
    pub(super) body: Option<String>,
}

#[derive(Debug)]
pub(super) struct AuthorizedLoadBucketCorsConfig {
    pub(super) bucket: BucketName,
}

#[derive(Debug)]
pub(super) struct AuthorizedDeleteBucketCors {
    pub(super) bucket: BucketName,
}

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketTagging {
    pub(super) bucket: BucketName,
    pub(super) body: String,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketTagging {
    pub(super) body: Option<String>,
}

#[derive(Debug)]
pub(super) struct AuthorizedDeleteBucketTagging {
    pub(super) bucket: BucketName,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketPolicy {
    pub(super) body: Option<String>,
}

#[derive(Debug)]
pub(super) struct AuthorizedDeleteBucketPolicy {
    pub(super) bucket: BucketName,
}

#[derive(Debug)]
pub(super) struct AuthorizedGetBucketLifecycle {
    pub(super) body: Option<String>,
}

#[derive(Debug)]
pub(super) struct AuthorizedLoadBucketLifecycleConfig {
    pub(super) bucket: BucketName,
}

#[derive(Debug)]
pub(super) struct AuthorizedDeleteBucketLifecycle {
    pub(super) bucket: BucketName,
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
    pub(super) policy_is_public: bool,
}

#[derive(Debug)]
pub(super) struct AuthorizedPutBucketLifecycle {
    pub(super) bucket: BucketName,
    pub(super) body: String,
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

pub(super) struct AuthorizedObjectRead {
    pub(super) bucket: BucketSummary,
    pub(super) snapshot: ObjectReadSnapshot,
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
    pub(super) upload: AuthorizedMultipartUploadRecord,
    pub(super) sse_customer: Option<SseCustomerWriteContext>,
}

#[derive(Debug)]
pub(super) struct AuthorizedMultipartPartWrite {
    pub(super) bucket: BucketName,
    pub(super) key: ObjectKey,
    pub(super) upload_id: UploadId,
    pub(super) part_number: u32,
    pub(super) upload: AuthorizedMultipartUploadRecord,
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
    pub(super) upload: AuthorizedMultipartUploadRecord,
    pub(super) multipart_write_encryption: ActiveWriteEncryption,
}

#[derive(Debug)]
pub(super) enum AuthorizedAbortMultipartUpload {
    InProgress {
        upload: Box<AuthorizedMultipartUploadRecord>,
    },
    Completed,
}

#[derive(Debug)]
pub(super) struct AuthorizedListParts {
    pub(super) bucket_info: BucketSummary,
    pub(super) upload: AuthorizedMultipartUploadRecord,
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

pub(super) enum AuthorizedDeleteObject {
    UnversionedDelete {
        bucket: BucketName,
        key: ObjectKey,
        requester: Requester,
        bucket_info: ValidatedBucket,
        bucket_policy: Option<Arc<BucketPolicy>>,
        bucket_tags: Option<Vec<(String, String)>>,
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
        requester: Requester,
        bucket_info: ValidatedBucket,
        bucket_policy: Option<Arc<BucketPolicy>>,
        bucket_tags: Option<Vec<(String, String)>>,
    },
}

impl std::fmt::Debug for AuthorizedObjectRead {
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

impl std::fmt::Debug for AuthorizedDeleteObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnversionedDelete { bucket, key, .. } => f
                .debug_struct("AuthorizedDeleteObject::UnversionedDelete")
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
