use checksum::{ChecksumAlgorithm, ChecksumType, RawChecksum};

use super::authz_types::AuthorizedPutObjectWrite;
use super::read_core::ReadHandle;
use crate::metadata_blob::MetadataBlob;
use crate::sse::{SseCustomerResponseHeaders, SseCustomerWriteContext};
use crate::system_metadata::SystemMetadata;
use s3_types::{AclGrants, BucketVersioningState, CanonicalUserId, VersionId};
use storage::{
    BucketName, BucketObjectLockConfig, BucketOwnershipControls, EffectiveBucketEncryptionConfig,
    ManagedEncryptionAlgorithm, ObjectLockState, OwnerIdentity, PublicAccessBlockConfig, SessionId,
    UploadId,
};

/// Result of a PutObject operation.
#[derive(Debug)]
pub struct PutObjectResult {
    pub etag: String,
    pub version_id: VersionId,
    pub system_metadata: SystemMetadata,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub lifecycle_expiration: Option<LifecycleExpirationHeader>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleExpirationHeader {
    pub expiry_time_millis: u64,
    pub rule_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleAbortHeaders {
    pub abort_time_millis: u64,
    pub rule_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NoncurrentLifecycleExpiration {
    pub(super) version_id: VersionId,
    pub(super) expiry_time_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DeleteMarkerLifecycleExpiration {
    pub(super) version_id: VersionId,
    pub(super) expiry_time_millis: u64,
}

/// Core-owned bucket summary exposed above the storage layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketSummary {
    pub name: BucketName,
    pub owner_principal: String,
    pub owner_canonical_id: CanonicalUserId,
    pub created_at: u64,
    pub acl_grants: AclGrants,
    pub public_read: bool,
    pub public_write: bool,
    pub versioning: BucketVersioningState,
    pub object_lock: BucketObjectLockConfig,
    pub public_access_block: Option<PublicAccessBlockConfig>,
    pub ownership_controls: Option<BucketOwnershipControls>,
    pub bucket_policy_present: bool,
    pub bucket_policy_public: bool,
    pub bucket_policy_generation: u64,
    pub bucket_lifecycle_present: bool,
    pub bucket_lifecycle_generation: u64,
    pub bucket_abac_enabled: bool,
    pub encryption: EffectiveBucketEncryptionConfig,
}

/// Result of a GetBucketAcl operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetBucketAclResult {
    pub owner_principal: String,
    pub owner_canonical_id: CanonicalUserId,
    pub acl_grants: AclGrants,
}

/// Result of a GetObjectAcl operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetObjectAclResult {
    pub owner_principal: String,
    pub owner_canonical_id: CanonicalUserId,
    pub acl_grants: AclGrants,
    pub version_id: VersionId,
}

/// Result of beginning a streaming UploadPart session.
#[derive(Debug)]
pub struct BeginStreamPartResult {
    pub session_id: SessionId,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub sse_customer: Option<SseCustomerWriteContext>,
}

#[derive(Debug)]
pub struct PreparedStreamPut {
    pub authorized_write: AuthorizedPutObjectWrite,
    pub session_id: SessionId,
}

/// Result of a GetObject operation.
#[derive(Debug)]
pub struct GetObjectResult {
    pub body: ReadHandle,
    pub metadata: MetadataBlob,
    pub system_metadata: SystemMetadata,
    pub object_lock: ObjectLockState,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub version_id: VersionId,
    pub tags: Option<String>,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer: Option<SseCustomerResponseHeaders>,
    pub lifecycle_expiration: Option<LifecycleExpirationHeader>,
}

/// Result of a HeadObject operation.
#[derive(Debug)]
pub struct HeadObjectResult {
    pub metadata: MetadataBlob,
    pub system_metadata: SystemMetadata,
    pub object_lock: ObjectLockState,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub version_id: VersionId,
    pub tags: Option<String>,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer: Option<SseCustomerResponseHeaders>,
    pub lifecycle_expiration: Option<LifecycleExpirationHeader>,
}

/// Result of a HeadObject with partNumber.
#[derive(Debug)]
pub struct HeadObjectPartResult {
    pub metadata: MetadataBlob,
    pub system_metadata: SystemMetadata,
    pub object_lock: ObjectLockState,
    pub etag: String,
    pub part_size: u64,
    pub part_start: u64,
    pub part_end: u64,
    pub total_size: u64,
    pub last_modified: u64,
    pub parts_count: u32,
    pub version_id: VersionId,
    pub tags: Option<String>,
    /// Per-part checksum (algorithm + raw bytes).
    pub checksum: Option<RawChecksum>,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer: Option<SseCustomerResponseHeaders>,
    pub lifecycle_expiration: Option<LifecycleExpirationHeader>,
}

/// A single part entry for GetObjectAttributes ObjectParts response.
#[derive(Debug)]
pub struct ObjectPartEntry {
    pub part_number: u32,
    pub size: u64,
    /// Base64-encoded checksum for this part (None if no checksum).
    pub checksum: Option<String>,
}

/// Pagination info for ObjectParts in GetObjectAttributes.
#[derive(Debug)]
pub struct ObjectPartsInfo {
    pub total_parts_count: u32,
    /// True for checksummed multipart uploads (full detail: parts, pagination).
    /// False for non-checksummed multipart (only PartsCount in XML).
    pub has_detail: bool,
    pub parts: Vec<ObjectPartEntry>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<u32>,
    pub max_parts: u32,
    pub part_number_marker: u32,
}

/// Result of a GetObjectAttributes operation.
#[derive(Debug)]
pub struct GetObjectAttributesResult {
    pub metadata: MetadataBlob,
    pub system_metadata: SystemMetadata,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub version_id: VersionId,
    pub object_parts: Option<ObjectPartsInfo>,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer: Option<SseCustomerResponseHeaders>,
}

/// Result of a range GetObject operation (206 Partial Content).
#[derive(Debug)]
pub struct GetObjectRangeResult {
    pub body: ReadHandle,
    pub metadata: MetadataBlob,
    pub system_metadata: SystemMetadata,
    pub object_lock: ObjectLockState,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub range_start: u64,
    pub range_end: u64,
    pub version_id: VersionId,
    pub tags: Option<String>,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer: Option<SseCustomerResponseHeaders>,
    pub lifecycle_expiration: Option<LifecycleExpirationHeader>,
}

/// Result of a part-level GetObject operation (206 Partial Content).
#[derive(Debug)]
pub struct GetObjectPartResult {
    pub body: ReadHandle,
    pub metadata: MetadataBlob,
    pub system_metadata: SystemMetadata,
    pub object_lock: ObjectLockState,
    pub etag: String,
    pub size: u64,
    pub part_size: u64,
    pub last_modified: u64,
    pub part_start: u64,
    pub part_end: u64,
    pub parts_count: u32,
    pub version_id: VersionId,
    pub tags: Option<String>,
    /// Per-part checksum (algorithm + raw bytes).
    pub checksum: Option<RawChecksum>,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer: Option<SseCustomerResponseHeaders>,
    pub lifecycle_expiration: Option<LifecycleExpirationHeader>,
}

/// Result of a `CopyObject` operation.
#[derive(Debug)]
pub struct CopyObjectResult {
    pub etag: String,
    pub last_modified: u64,
    pub system_metadata: SystemMetadata,
    pub version_id: VersionId,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer: Option<SseCustomerResponseHeaders>,
    pub lifecycle_expiration: Option<LifecycleExpirationHeader>,
}

/// Object entry for listing.
#[derive(Debug, Clone)]
pub struct ListEntry {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: u64,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<ChecksumType>,
}

/// Result of a ListObjectsV2 operation.
#[derive(Debug)]
pub struct ListObjectsResult {
    pub objects: Vec<ListEntry>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
    pub owner_principal: String,
    pub owner_canonical_id: CanonicalUserId,
}

/// Entry in a ListObjectVersions result.
#[derive(Debug, Clone)]
pub struct VersionEntry {
    pub key: String,
    pub version_id: VersionId,
    pub is_latest: bool,
    pub size: u64,
    pub etag: String,
    pub last_modified: u64,
    pub is_delete_marker: bool,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<ChecksumType>,
}

/// Result of a ListObjectVersions operation.
#[derive(Debug)]
pub struct ListObjectVersionsResult {
    pub versions: Vec<VersionEntry>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<VersionId>,
    pub owner_principal: String,
    pub owner_canonical_id: CanonicalUserId,
}

/// Result of a DeleteObject operation.
#[derive(Debug)]
pub struct DeleteObjectResult {
    pub version_id: VersionId,
    pub delete_marker: bool,
}

/// Result entry for a successfully deleted object in a batch delete.
#[derive(Debug)]
pub struct DeletedObject {
    pub key: String,
    pub version_id: VersionId,
    pub delete_marker: bool,
}

/// Result entry for a failed deletion in a batch delete.
#[derive(Debug)]
pub struct DeleteError {
    pub key: String,
    pub version_id: Option<VersionId>,
    pub code: String,
    pub message: String,
}

/// Result of a DeleteObjects (batch delete) operation.
#[derive(Debug)]
pub struct DeleteObjectsResult {
    pub deleted: Vec<DeletedObject>,
    pub errors: Vec<DeleteError>,
}

/// Result of an UploadPart operation.
#[derive(Debug)]
pub struct UploadPartResult {
    pub etag: String,
    /// Verified checksum for this part (if any).
    pub checksum: Option<RawChecksum>,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
}

/// Result of an UploadPartCopy operation.
#[derive(Debug)]
pub struct UploadPartCopyResult {
    pub etag: String,
    pub last_modified: u64,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer: Option<SseCustomerResponseHeaders>,
}

/// Result of a CreateMultipartUpload operation.
#[derive(Debug)]
pub struct CreateMultipartUploadResult {
    pub upload_id: UploadId,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub lifecycle_abort: Option<LifecycleAbortHeaders>,
}

/// Result of a CompleteMultipartUpload operation.
#[derive(Debug)]
pub struct CompleteMultipartUploadResult {
    pub etag: String,
    pub version_id: VersionId,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    /// Object-level checksum algorithm (if configured).
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// Object-level checksum type.
    pub checksum_type: Option<ChecksumType>,
    /// Object-level checksum (base64-encoded).
    pub checksum_value: Option<String>,
    pub lifecycle_expiration: Option<LifecycleExpirationHeader>,
}

/// Entry in a ListParts result.
#[derive(Debug, Clone)]
pub struct PartEntry {
    pub part_number: u32,
    pub size: u64,
    pub etag: String,
    pub last_modified: u64,
    /// Base64-encoded checksum for this part (None if no checksum).
    pub checksum: Option<String>,
}

/// Result of a ListParts operation.
#[derive(Debug)]
pub struct ListPartsResult {
    pub parts: Vec<PartEntry>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<u32>,
    /// Upload-level checksum algorithm.
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// Upload-level checksum type.
    pub checksum_type: Option<ChecksumType>,
    pub lifecycle_abort: Option<LifecycleAbortHeaders>,
}

/// Entry in a ListMultipartUploads result.
#[derive(Debug, Clone)]
pub struct MultipartUploadEntry {
    pub key: String,
    pub upload_id: UploadId,
    pub initiated: u64,
    pub owner: OwnerIdentity,
    pub initiator: Option<OwnerIdentity>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<ChecksumType>,
}

/// Result of a ListMultipartUploads operation.
#[derive(Debug)]
pub struct ListMultipartUploadsResult {
    pub uploads: Vec<MultipartUploadEntry>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<UploadId>,
}
