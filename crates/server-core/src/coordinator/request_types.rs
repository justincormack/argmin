use checksum::{ChecksumAlgorithm, RawChecksum};

use super::authz_types::ActiveWriteEncryptionRef;
use crate::checksum_claim::{ChecksumClaim, EncodedChecksumClaim};
use crate::conditional::{DeleteCondition, ReadCondition, WriteCondition};
use crate::error::ServerError;
use crate::metadata_blob::MetadataBlob;
use crate::range::ByteRange;
use crate::sse::SseCustomerRequest;
use crate::system_metadata::SystemMetadata;
use s3_types::{
    AccountIdentity, AclGrant, AclGrantee, AclGrants, AclPermission, BucketNamespace,
    BucketVersioningState, CanonicalUserId, LegalHoldStatus, ObjectLockDefaultRetention,
    ObjectRetention, VersionId, WebsiteRedirectLocation,
};
use storage::{
    BucketEncryptionConfig, BucketName, BucketObjectOwnership, BucketOwnershipControls,
    ManagedEncryptionAlgorithm, MultipartChecksumConfig, ObjectKey, ObjectLockState,
    PublicAccessBlockConfig, SessionId, UploadId,
};

/// Metadata handling directive for `CopyObject`.
#[derive(Debug)]
pub enum MetadataDirective<'a> {
    /// Preserve source object's metadata.
    Copy,
    /// Preserve source metadata with an explicit `x-amz-metadata-directive: COPY` header.
    CopyExplicit,
    /// Replace metadata with an already-parsed blob and optional checksum algorithm.
    ///
    /// The `MetadataBlob` should already have checksum value headers stripped
    /// (they can't be verified on CopyObject since there's no body).
    /// If `checksum_algorithm` is provided, a fresh checksum will be computed
    /// from the copied data.
    Replace {
        metadata: &'a MetadataBlob,
        system_metadata: &'a SystemMetadata,
        checksum_algorithm: Option<ChecksumAlgorithm>,
    },
}

impl MetadataDirective<'_> {
    #[must_use]
    pub const fn policy_condition_value(&self) -> Option<&'static str> {
        match self {
            Self::Copy => None,
            Self::CopyExplicit => Some("COPY"),
            Self::Replace { .. } => Some("REPLACE"),
        }
    }
}

/// Tagging handling directive for `CopyObject`.
#[derive(Debug)]
pub enum TaggingDirective<'a> {
    Copy,
    Replace(Option<&'a str>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PutObjectPolicyContext<'a> {
    pub copy_source: Option<&'a str>,
    pub metadata_directive: Option<&'a str>,
    pub canned_acl: Option<&'a str>,
    pub website_redirect_location: Option<&'a str>,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer_algorithm: Option<&'a str>,
    pub grant_read: Option<&'a str>,
    pub grant_write: Option<&'a str>,
    pub grant_read_acp: Option<&'a str>,
    pub grant_write_acp: Option<&'a str>,
    pub grant_full_control: Option<&'a str>,
    pub request_object_tags_xml: Option<&'a str>,
    pub request_tags: Option<&'a [(String, String)]>,
    pub if_match: Option<&'a str>,
    pub if_none_match: Option<&'a str>,
    pub object_creation_operation: Option<bool>,
    pub prefix: Option<&'a str>,
    pub delimiter: Option<&'a str>,
    pub requested_max_keys: Option<u32>,
    pub object_ownership: Option<&'a str>,
    pub version_id: Option<VersionId>,
}

impl<'a> PutObjectPolicyContext<'a> {
    #[must_use]
    pub const fn new(
        copy_source: Option<&'a str>,
        metadata_directive: Option<&'a str>,
        canned_acl: Option<&'a str>,
    ) -> Self {
        Self {
            copy_source,
            metadata_directive,
            canned_acl,
            website_redirect_location: None,
            managed_encryption: None,
            sse_customer_algorithm: None,
            grant_read: None,
            grant_write: None,
            grant_read_acp: None,
            grant_write_acp: None,
            grant_full_control: None,
            request_object_tags_xml: None,
            request_tags: None,
            if_match: None,
            if_none_match: None,
            object_creation_operation: None,
            prefix: None,
            delimiter: None,
            requested_max_keys: None,
            object_ownership: None,
            version_id: None,
        }
    }

    #[must_use]
    pub const fn with_website_redirect_location(
        mut self,
        website_redirect_location: Option<&'a str>,
    ) -> Self {
        self.website_redirect_location = website_redirect_location;
        self
    }

    #[must_use]
    pub const fn with_managed_encryption(
        mut self,
        managed_encryption: Option<ManagedEncryptionAlgorithm>,
    ) -> Self {
        self.managed_encryption = managed_encryption;
        self
    }

    #[must_use]
    pub const fn with_sse_customer_algorithm(
        mut self,
        sse_customer_algorithm: Option<&'a str>,
    ) -> Self {
        self.sse_customer_algorithm = sse_customer_algorithm;
        self
    }

    #[must_use]
    pub const fn with_request_object_tags_xml(
        mut self,
        request_object_tags_xml: Option<&'a str>,
    ) -> Self {
        self.request_object_tags_xml = request_object_tags_xml;
        self
    }

    #[must_use]
    pub const fn with_request_tags(mut self, request_tags: Option<&'a [(String, String)]>) -> Self {
        self.request_tags = request_tags;
        self
    }

    #[must_use]
    pub const fn with_acl_grant_headers(
        mut self,
        grant_read: Option<&'a str>,
        grant_write: Option<&'a str>,
        grant_read_acp: Option<&'a str>,
        grant_write_acp: Option<&'a str>,
        grant_full_control: Option<&'a str>,
    ) -> Self {
        self.grant_read = grant_read;
        self.grant_write = grant_write;
        self.grant_read_acp = grant_read_acp;
        self.grant_write_acp = grant_write_acp;
        self.grant_full_control = grant_full_control;
        self
    }

    #[must_use]
    pub const fn with_default_canned_acl(mut self, canned_acl: Option<&'a str>) -> Self {
        if self.canned_acl.is_none() {
            self.canned_acl = canned_acl;
        }
        self
    }

    #[must_use]
    pub const fn with_if_match(mut self, if_match: Option<&'a str>) -> Self {
        self.if_match = if_match;
        self
    }

    #[must_use]
    pub const fn with_if_none_match(mut self, if_none_match: Option<&'a str>) -> Self {
        self.if_none_match = if_none_match;
        self
    }

    #[must_use]
    pub const fn with_object_creation_operation(mut self, object_creation_operation: bool) -> Self {
        self.object_creation_operation = Some(object_creation_operation);
        self
    }

    #[must_use]
    pub const fn with_optional_object_creation_operation(
        mut self,
        object_creation_operation: Option<bool>,
    ) -> Self {
        self.object_creation_operation = object_creation_operation;
        self
    }

    #[must_use]
    pub const fn with_prefix(mut self, prefix: Option<&'a str>) -> Self {
        self.prefix = prefix;
        self
    }

    #[must_use]
    pub const fn with_delimiter(mut self, delimiter: Option<&'a str>) -> Self {
        self.delimiter = delimiter;
        self
    }

    #[must_use]
    pub const fn with_requested_max_keys(mut self, requested_max_keys: Option<u32>) -> Self {
        self.requested_max_keys = requested_max_keys;
        self
    }

    #[must_use]
    pub const fn with_object_ownership(mut self, object_ownership: Option<&'a str>) -> Self {
        self.object_ownership = object_ownership;
        self
    }

    #[must_use]
    pub const fn with_version_id(mut self, version_id: Option<VersionId>) -> Self {
        self.version_id = version_id;
        self
    }
}

/// Parsed copy-source reference, shared by CopyObject and UploadPartCopy.
#[derive(Debug)]
pub struct CopySource<'a> {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub version_id: Option<VersionId>,
    pub condition: &'a ReadCondition,
    pub(super) expected_bucket_owner: Option<&'a str>,
}

/// Parsed CopyObject request from the HTTP layer.
#[derive(Debug)]
pub struct CopyObjectRequest<'a> {
    pub source: CopySource<'a>,
    pub destination: ObjectRequest<'a>,
    pub dst_condition: &'a WriteCondition,
    pub directive: MetadataDirective<'a>,
    pub website_redirect_location: Option<WebsiteRedirectLocation>,
    pub tagging: TaggingDirective<'a>,
    pub acl: PutObjectWriteAcl<'a>,
    pub policy_context: PutObjectPolicyContext<'a>,
    pub source_sse_customer: Option<&'a SseCustomerRequest>,
    pub destination_encryption: WriteEncryptionRequest<'a>,
    pub object_lock: ObjectLockState,
}

/// Parsed UploadPartCopy request from the HTTP layer.
#[derive(Debug)]
pub struct UploadPartCopyRequest<'a> {
    pub source: CopySource<'a>,
    pub upload: MultipartObjectRequest<'a>,
    pub part_number: u32,
    pub copy_source_range: Option<(u64, u64)>,
    pub policy_context: PutObjectPolicyContext<'a>,
    pub source_sse_customer: Option<&'a SseCustomerRequest>,
    pub sse_customer: Option<&'a SseCustomerRequest>,
}

impl<'a> CopySource<'a> {
    pub fn new(
        bucket: BucketName,
        key: ObjectKey,
        version_id: Option<VersionId>,
        condition: &'a ReadCondition,
        expected_bucket_owner: Option<&'a str>,
    ) -> Self {
        Self {
            bucket,
            key,
            version_id,
            condition,
            expected_bucket_owner,
        }
    }

    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.expected_bucket_owner
    }
}

/// Explicit caller-selected encryption for write APIs before bucket-default
/// encryption is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteEncryptionRequest<'a> {
    None,
    SseCustomer(&'a SseCustomerRequest),
    Managed(ManagedEncryptionAlgorithm),
}

impl<'a> WriteEncryptionRequest<'a> {
    #[must_use]
    pub const fn none() -> Self {
        Self::None
    }

    #[must_use]
    pub const fn sse_customer(request: &'a SseCustomerRequest) -> Self {
        Self::SseCustomer(request)
    }

    #[must_use]
    pub const fn managed(algorithm: ManagedEncryptionAlgorithm) -> Self {
        Self::Managed(algorithm)
    }

    pub fn from_request_parts(
        sse_customer: Option<&'a SseCustomerRequest>,
        managed_encryption: Option<ManagedEncryptionAlgorithm>,
    ) -> Result<Self, ServerError> {
        match (sse_customer, managed_encryption) {
            (Some(request), None) => Ok(Self::sse_customer(request)),
            (None, Some(algorithm)) => Ok(Self::managed(algorithm)),
            (None, None) => Ok(Self::none()),
            (Some(_), Some(_)) => Err(ServerError::InvalidArgument {
                reason: "x-amz-server-side-encryption may not be used with SSE-C headers"
                    .to_string(),
            }),
        }
    }

    #[must_use]
    pub(super) fn sse_customer_request(self) -> Option<&'a SseCustomerRequest> {
        match self {
            Self::SseCustomer(request) => Some(request),
            Self::None | Self::Managed(_) => None,
        }
    }

    #[must_use]
    pub(super) fn explicit_managed_encryption(self) -> Option<ManagedEncryptionAlgorithm> {
        match self {
            Self::Managed(algorithm) => Some(algorithm),
            Self::None | Self::SseCustomer(_) => None,
        }
    }

    #[must_use]
    pub(super) fn with_policy_context(
        self,
        policy_context: PutObjectPolicyContext<'a>,
    ) -> PutObjectPolicyContext<'a> {
        match self {
            Self::None => policy_context
                .with_managed_encryption(None)
                .with_sse_customer_algorithm(None),
            Self::SseCustomer(request) => policy_context
                .with_managed_encryption(None)
                .with_sse_customer_algorithm(Some(request.algorithm())),
            Self::Managed(algorithm) => policy_context
                .with_managed_encryption(Some(algorithm))
                .with_sse_customer_algorithm(None),
        }
    }
}

/// Request for a PutObject operation.
#[derive(Debug)]
pub struct PutObjectRequest<'a> {
    pub object: ObjectRequest<'a>,
    pub data: &'a [u8],
    pub metadata: &'a MetadataBlob,
    pub system_metadata: &'a SystemMetadata,
    pub tags: Option<&'a str>,
    pub cond: &'a WriteCondition,
    pub acl: PutObjectWriteAcl<'a>,
    pub policy_context: PutObjectPolicyContext<'a>,
    pub object_lock: ObjectLockState,
    pub encryption: WriteEncryptionRequest<'a>,
}

#[derive(Debug)]
pub struct AuthorizePutObjectRequest<'a> {
    pub object: ObjectRequest<'a>,
    pub acl: PutObjectWriteAcl<'a>,
    pub policy_context: PutObjectPolicyContext<'a>,
    pub object_lock: ObjectLockState,
    pub tags: Option<&'a str>,
    pub encryption: WriteEncryptionRequest<'a>,
}

pub struct AuthorizedPutObjectCommitRequest<'a> {
    pub data: &'a [u8],
    pub metadata: &'a MetadataBlob,
    pub system_metadata: &'a SystemMetadata,
    pub cond: &'a WriteCondition,
}

pub struct AuthorizedFinalizeStreamPutRequest<'a> {
    pub session_id: &'a SessionId,
    pub crc64: u64,
    pub total_size: u64,
    pub metadata_blob: &'a MetadataBlob,
    pub system_metadata: &'a SystemMetadata,
    pub write_encryption: ActiveWriteEncryptionRef<'a>,
    pub cond: &'a WriteCondition,
}

pub(super) enum AuthorizedWriteTags<'a> {
    Bound,
    TrustedDerived(Option<&'a str>),
}

/// Authenticated requester context needed by core-side authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requester {
    pub(super) identity: Option<auth::AuthenticatedIdentity>,
    pub(super) authorization_profile: auth::AuthorizationProfile,
    pub(super) source_ip: Option<std::net::IpAddr>,
    pub(super) request_epoch_seconds: Option<u64>,
    pub(super) secure_transport: Option<bool>,
    pub(super) requested_region: Option<String>,
    pub(super) referer: Option<Option<String>>,
    pub(super) auth_type: Option<Option<&'static str>>,
    pub(super) signature_version: Option<Option<&'static str>>,
    pub(super) signature_age_millis: Option<Option<u64>>,
    pub(super) tls_version: Option<Option<String>>,
    pub(super) content_sha256: Option<Option<String>>,
}

/// Parsed x-amz-acl value relevant to PutObject authorization rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PutObjectAcl<'a> {
    #[default]
    None,
    Private,
    PublicRead,
    PublicReadWrite,
    AuthenticatedRead,
    AwsExecRead,
    BucketOwnerRead,
    BucketOwnerFullControl,
    Invalid(&'a str),
}

/// Parsed ACL input relevant to direct PutObject authorization rules.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PutObjectWriteAcl<'a> {
    #[default]
    None,
    Canned(PutObjectAcl<'a>),
    Grants(AclGrants),
}

/// Parsed bucket ACL value relevant to bucket ACL and ownership-control rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketAcl {
    Private,
    PublicRead,
    PublicReadWrite,
    AuthenticatedRead,
}

/// Parsed CreateBucket ACL input.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CreateBucketAcl {
    #[default]
    DefaultPrivate,
    Canned(BucketAcl),
    Grants(AclGrants),
}

/// Request for a CreateBucket operation.
#[derive(Debug)]
pub struct CreateBucketRequest {
    pub name: BucketName,
    pub requester: Requester,
    pub namespace: BucketNamespace,
    pub acl: CreateBucketAcl,
    pub ownership: BucketObjectOwnership,
    pub object_lock_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BucketCreateOutcome {
    Created,
    AlreadyOwned,
}

/// Request for a ListBuckets operation.
#[derive(Debug)]
pub struct ListBucketsRequest {
    pub requester: Requester,
}

/// Request for a bucket-scoped operation.
#[derive(Debug)]
pub struct BucketRequest<'a> {
    pub name: BucketName,
    pub requester: Requester,
    pub(super) expected_bucket_owner: Option<&'a str>,
}

/// Request for an object-scoped operation.
#[derive(Debug)]
pub struct ObjectRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub key: ObjectKey,
}

/// Request for a bucket-scoped string configuration operation.
#[derive(Debug)]
pub struct PutBucketConfigRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub config: &'a str,
}

/// Request for a minimal `s3-control` bucket-tag operation.
#[derive(Debug)]
pub struct BucketTagControlRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub account_id: &'a str,
}

/// Request for a control-plane bucket-tag update that stores a full tag set.
#[derive(Debug)]
pub struct PutBucketTagControlRequest<'a> {
    pub control: BucketTagControlRequest<'a>,
    pub config: &'a str,
    pub request_tags: &'a [(String, String)],
}

/// Request for an `s3-control` `UntagResource` operation.
#[derive(Debug)]
pub struct UntagBucketTagControlRequest<'a> {
    pub control: BucketTagControlRequest<'a>,
    pub request_tags: &'a [(String, String)],
}

/// Request for a partial `s3-control` `UntagResource` operation that stores
/// the tags left after removing the requested keys.
#[derive(Debug)]
pub struct PutBucketTagsForUntagResourceRequest<'a> {
    pub control: BucketTagControlRequest<'a>,
    pub config: &'a str,
    pub request_tags: &'a [(String, String)],
}

/// Request for a PutBucketPolicy operation.
#[derive(Debug)]
pub struct PutBucketPolicyRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub config: &'a str,
    pub confirm_remove_self_bucket_access: bool,
}

/// Request for a PutBucketPublicAccessBlock operation.
#[derive(Debug)]
pub struct PutBucketPublicAccessBlockRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub config: PublicAccessBlockConfig,
}

/// Request for a PutBucketOwnershipControls operation.
#[derive(Debug)]
pub struct PutBucketOwnershipControlsRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub config: BucketOwnershipControls,
}

/// Request for a PutBucketAbac operation.
#[derive(Debug)]
pub struct PutBucketAbacRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub enabled: bool,
}

/// Request for a PutBucketVersioning operation.
#[derive(Debug)]
pub struct PutBucketVersioningRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub state: BucketVersioningState,
}

/// Parsed `PutObjectLockConfiguration` bucket update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketObjectLockConfigurationUpdate {
    pub object_lock_enabled: Option<bool>,
    pub default_retention: Option<ObjectLockDefaultRetention>,
}

/// Request for a `PutObjectLockConfiguration` operation.
#[derive(Debug)]
pub struct PutBucketObjectLockConfigurationRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub config: BucketObjectLockConfigurationUpdate,
}

/// Request for a PutBucketEncryption operation.
#[derive(Debug)]
pub struct PutBucketEncryptionRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub config: BucketEncryptionConfig,
}

/// Request for a PutBucketAcl operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutBucketAclInput {
    Canned(BucketAcl),
    Grants(AclGrants),
}

/// Request for a PutBucketAcl operation.
#[derive(Debug)]
pub struct PutBucketAclRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub acl: PutBucketAclInput,
    pub policy_context: PutObjectPolicyContext<'a>,
}

/// Request for an object or object-version-scoped operation.
#[derive(Debug)]
pub struct ObjectVersionRequest<'a> {
    pub object: ObjectRequest<'a>,
    pub version_id: Option<VersionId>,
}

/// Request for a multipart upload-scoped operation.
#[derive(Debug)]
pub struct MultipartObjectRequest<'a> {
    pub object: ObjectRequest<'a>,
    upload_id: UploadId,
}

/// Request for a PutObjectTagging operation.
#[derive(Debug)]
pub struct PutObjectTagsRequest<'a> {
    pub object: ObjectVersionRequest<'a>,
    pub tags: &'a str,
}

/// Request for a PutObjectRetention operation.
#[derive(Debug)]
pub struct PutObjectRetentionRequest<'a> {
    pub object: ObjectVersionRequest<'a>,
    pub retention: ObjectRetention,
    pub bypass_governance: bool,
}

/// Request for a PutObjectLegalHold operation.
#[derive(Debug)]
pub struct PutObjectLegalHoldRequest<'a> {
    pub object: ObjectVersionRequest<'a>,
    pub legal_hold: LegalHoldStatus,
}

/// Request for a PutObjectAcl operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutObjectAclInput<'a> {
    Canned(PutObjectAcl<'a>),
    Grants(AclGrants),
}

/// Request for a PutObjectAcl operation.
#[derive(Debug)]
pub struct PutObjectAclRequest<'a> {
    pub object: ObjectVersionRequest<'a>,
    pub acl: PutObjectAclInput<'a>,
    pub policy_context: PutObjectPolicyContext<'a>,
}

/// Request for a GetObject or HeadObject operation.
#[derive(Debug)]
pub struct GetObjectRequest<'a> {
    pub object: ObjectVersionRequest<'a>,
    pub cond: &'a ReadCondition,
    pub sse_customer: Option<&'a SseCustomerRequest>,
}

/// Request for a GetObjectPart or HeadObjectPart operation.
#[derive(Debug)]
pub struct GetObjectPartRequest<'a> {
    pub object: ObjectVersionRequest<'a>,
    pub part_number: u32,
    pub cond: &'a ReadCondition,
    pub sse_customer: Option<&'a SseCustomerRequest>,
}

/// Request for a GetObjectRange operation.
#[derive(Debug)]
pub struct GetObjectRangeRequest<'a> {
    pub object: ObjectVersionRequest<'a>,
    pub range: ByteRange,
    pub cond: &'a ReadCondition,
    pub sse_customer: Option<&'a SseCustomerRequest>,
}

/// Request for a DeleteObject operation.
#[derive(Debug)]
pub struct DeleteObjectRequest<'a> {
    pub object: ObjectVersionRequest<'a>,
    pub bypass_governance: bool,
    pub cond: &'a DeleteCondition,
}

/// Request for a ListObjectsV2 operation.
#[derive(Debug)]
pub struct ListObjectsV2Request<'a> {
    pub bucket: BucketRequest<'a>,
    pub prefix: Option<&'a str>,
    pub delimiter: Option<&'a str>,
    pub continuation_token: Option<&'a str>,
    pub max_keys: u32,
    pub requested_max_keys: Option<u32>,
}

/// Request for a ListObjectVersions operation.
#[derive(Debug)]
pub struct ListObjectVersionsRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub prefix: Option<&'a str>,
    pub delimiter: Option<&'a str>,
    pub key_marker: Option<&'a str>,
    pub version_id_marker: Option<VersionId>,
    pub max_keys: u32,
    pub requested_max_keys: Option<u32>,
}

/// Request for a ListParts operation.
#[derive(Debug)]
pub struct ListPartsRequest<'a> {
    pub upload: MultipartObjectRequest<'a>,
    pub part_number_marker: Option<u32>,
    pub max_parts: u32,
}

/// Request for a ListMultipartUploads operation.
#[derive(Debug)]
pub struct ListMultipartUploadsRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub prefix: Option<&'a str>,
    pub delimiter: Option<&'a str>,
    pub key_marker: Option<&'a str>,
    pub upload_id_marker: Option<UploadId>,
    pub max_uploads: u32,
}

/// A single entry in a batch-delete request, with an already-parsed version ID
/// and any per-object conditional delete settings from the XML body.
#[derive(Debug)]
pub struct DeleteEntry {
    pub key: ObjectKey,
    pub version_id: Option<VersionId>,
    pub cond: DeleteCondition,
}

/// Request for a DeleteObjects (multi-delete) operation.
#[derive(Debug)]
pub struct DeleteObjectsRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub entries: &'a [DeleteEntry],
    pub bypass_governance: bool,
}

/// Request for a CreateMultipartUpload operation.
#[derive(Debug)]
pub struct CreateMultipartUploadRequest<'a> {
    pub object: ObjectRequest<'a>,
    pub metadata: &'a MetadataBlob,
    pub system_metadata: &'a SystemMetadata,
    pub tags: Option<&'a str>,
    pub checksum: Option<MultipartChecksumConfig>,
    pub acl: PutObjectWriteAcl<'a>,
    pub policy_context: PutObjectPolicyContext<'a>,
    pub object_lock: ObjectLockState,
    pub encryption: WriteEncryptionRequest<'a>,
}

/// Request for a GetObjectAttributes operation.
#[derive(Debug)]
pub struct GetObjectAttributesRequest<'a> {
    pub object: ObjectVersionRequest<'a>,
    pub cond: &'a ReadCondition,
    pub want_parts: bool,
    pub part_number_marker: Option<u32>,
    pub max_parts: u32,
    pub sse_customer: Option<&'a SseCustomerRequest>,
}

/// Request for a CompleteMultipartUpload operation.
#[derive(Debug, Clone)]
pub struct CompletePart {
    pub part_number: u32,
    pub etag: String,
    /// Per-part checksum from the request XML.
    pub checksum: Option<ChecksumClaim>,
}

/// Request for a CompleteMultipartUpload operation.
#[derive(Debug)]
pub struct CompleteMultipartUploadRequest<'a> {
    pub upload: MultipartObjectRequest<'a>,
    pub parts: &'a [CompletePart],
    pub claimed_checksum: Option<&'a EncodedChecksumClaim>,
    pub expected_object_size: Option<u64>,
    pub cond: &'a WriteCondition,
    pub sse_customer: Option<&'a SseCustomerRequest>,
}

/// Request for beginning a streaming UploadPart session.
#[derive(Debug)]
pub struct BeginStreamPartRequest<'a> {
    pub upload: MultipartObjectRequest<'a>,
    pub part_number: u32,
    pub policy_context: PutObjectPolicyContext<'a>,
    pub sse_customer: Option<&'a SseCustomerRequest>,
}

/// Parsed request for appending plaintext to a streaming UploadPart session.
#[derive(Debug)]
pub struct AppendStreamPartRequest<'a> {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub upload_id: &'a storage::UploadId,
    pub session_id: &'a SessionId,
    pub part_number: u32,
    pub segment_index: u32,
    pub data: &'a [u8],
    pub sse_customer: Option<&'a SseCustomerRequest>,
}

/// Parsed request for appending plaintext to a streaming PutObject session.
#[derive(Debug)]
pub struct AppendStreamPutRequest<'a> {
    pub bucket: &'a BucketName,
    pub key: &'a ObjectKey,
    pub session_id: &'a SessionId,
    pub segment_index: u32,
    pub data: &'a [u8],
    pub sse_customer: Option<&'a SseCustomerRequest>,
}

pub(super) trait ExpectedBucketOwnerRequest {
    fn expected_bucket_owner(&self) -> Option<&str>;
}

pub(super) trait BucketScopedRequest: ExpectedBucketOwnerRequest {
    fn bucket_name_typed(&self) -> &BucketName;
}

pub(super) trait BucketScopedAuthorizationRequest: BucketScopedRequest {
    fn requester(&self) -> &Requester;
}

impl<'a> BucketRequest<'a> {
    pub fn new(
        name: BucketName,
        requester: Requester,
        expected_bucket_owner: Option<&'a str>,
    ) -> Self {
        Self {
            name,
            requester,
            expected_bucket_owner,
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn name_typed(&self) -> &BucketName {
        &self.name
    }

    pub fn requester(&self) -> &Requester {
        &self.requester
    }

    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.expected_bucket_owner
    }
}

impl BucketScopedRequest for BucketRequest<'_> {
    fn bucket_name_typed(&self) -> &BucketName {
        BucketRequest::name_typed(self)
    }
}

impl ExpectedBucketOwnerRequest for BucketRequest<'_> {
    fn expected_bucket_owner(&self) -> Option<&str> {
        BucketRequest::expected_bucket_owner(self)
    }
}

impl BucketScopedAuthorizationRequest for BucketRequest<'_> {
    fn requester(&self) -> &Requester {
        BucketRequest::requester(self)
    }
}

impl<'a> ObjectRequest<'a> {
    pub fn new(
        bucket: BucketName,
        key: ObjectKey,
        requester: Requester,
        expected_bucket_owner: Option<&'a str>,
    ) -> Self {
        Self {
            bucket: BucketRequest::new(bucket, requester, expected_bucket_owner),
            key,
        }
    }

    pub fn from_bucket(bucket: BucketRequest<'a>, key: ObjectKey) -> Self {
        Self { bucket, key }
    }

    pub fn bucket(&self) -> &BucketRequest<'a> {
        &self.bucket
    }

    pub fn bucket_name(&self) -> &str {
        self.bucket.name()
    }

    pub fn bucket_name_typed(&self) -> &BucketName {
        self.bucket.name_typed()
    }

    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    pub fn key_typed(&self) -> &ObjectKey {
        &self.key
    }

    pub fn requester(&self) -> &Requester {
        self.bucket.requester()
    }

    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.bucket.expected_bucket_owner()
    }
}

impl BucketScopedRequest for ObjectRequest<'_> {
    fn bucket_name_typed(&self) -> &BucketName {
        ObjectRequest::bucket_name_typed(self)
    }
}

impl ExpectedBucketOwnerRequest for ObjectRequest<'_> {
    fn expected_bucket_owner(&self) -> Option<&str> {
        ObjectRequest::expected_bucket_owner(self)
    }
}

impl BucketScopedAuthorizationRequest for ObjectRequest<'_> {
    fn requester(&self) -> &Requester {
        ObjectRequest::requester(self)
    }
}

impl<'a> ObjectVersionRequest<'a> {
    pub fn new(
        bucket: BucketName,
        key: ObjectKey,
        version_id: Option<VersionId>,
        requester: Requester,
        expected_bucket_owner: Option<&'a str>,
    ) -> Self {
        Self {
            object: ObjectRequest::new(bucket, key, requester, expected_bucket_owner),
            version_id,
        }
    }

    pub fn from_object(object: ObjectRequest<'a>, version_id: Option<VersionId>) -> Self {
        Self { object, version_id }
    }

    pub fn object(&self) -> &ObjectRequest<'a> {
        &self.object
    }

    pub fn bucket(&self) -> &BucketRequest<'a> {
        self.object.bucket()
    }

    pub fn bucket_name(&self) -> &str {
        self.object.bucket_name()
    }

    pub fn bucket_name_typed(&self) -> &BucketName {
        self.object.bucket.name_typed()
    }

    pub fn key(&self) -> &str {
        self.object.key()
    }

    pub fn key_typed(&self) -> &ObjectKey {
        self.object.key_typed()
    }

    pub fn version_id(&self) -> Option<VersionId> {
        self.version_id
    }

    pub fn requester(&self) -> &Requester {
        self.object.requester()
    }

    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.object.expected_bucket_owner()
    }
}

impl BucketScopedRequest for ObjectVersionRequest<'_> {
    fn bucket_name_typed(&self) -> &BucketName {
        ObjectVersionRequest::bucket_name_typed(self)
    }
}

impl ExpectedBucketOwnerRequest for ObjectVersionRequest<'_> {
    fn expected_bucket_owner(&self) -> Option<&str> {
        ObjectVersionRequest::expected_bucket_owner(self)
    }
}

impl BucketScopedAuthorizationRequest for ObjectVersionRequest<'_> {
    fn requester(&self) -> &Requester {
        ObjectVersionRequest::requester(self)
    }
}

impl<'a> MultipartObjectRequest<'a> {
    pub fn new(
        bucket: BucketName,
        key: ObjectKey,
        upload_id: UploadId,
        requester: Requester,
        expected_bucket_owner: Option<&'a str>,
    ) -> Self {
        Self {
            object: ObjectRequest::new(bucket, key, requester, expected_bucket_owner),
            upload_id,
        }
    }

    pub fn from_object(object: ObjectRequest<'a>, upload_id: UploadId) -> Self {
        Self { object, upload_id }
    }

    pub fn object(&self) -> &ObjectRequest<'a> {
        &self.object
    }

    pub fn bucket(&self) -> &BucketRequest<'a> {
        self.object.bucket()
    }

    pub fn bucket_name(&self) -> &str {
        self.object.bucket_name()
    }

    pub fn bucket_name_typed(&self) -> &BucketName {
        self.object.bucket.name_typed()
    }

    pub fn key(&self) -> &str {
        self.object.key()
    }

    pub fn key_typed(&self) -> &ObjectKey {
        self.object.key_typed()
    }

    pub fn upload_id(&self) -> &UploadId {
        &self.upload_id
    }

    pub fn requester(&self) -> &Requester {
        self.object.requester()
    }

    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.object.expected_bucket_owner()
    }
}

impl BucketScopedRequest for MultipartObjectRequest<'_> {
    fn bucket_name_typed(&self) -> &BucketName {
        MultipartObjectRequest::bucket_name_typed(self)
    }
}

impl ExpectedBucketOwnerRequest for MultipartObjectRequest<'_> {
    fn expected_bucket_owner(&self) -> Option<&str> {
        MultipartObjectRequest::expected_bucket_owner(self)
    }
}

impl BucketScopedAuthorizationRequest for MultipartObjectRequest<'_> {
    fn requester(&self) -> &Requester {
        MultipartObjectRequest::requester(self)
    }
}

impl<'a> CopyObjectRequest<'a> {
    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.destination.expected_bucket_owner()
    }
}

impl<'a> UploadPartCopyRequest<'a> {
    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.upload.expected_bucket_owner()
    }
}

impl<'a> PutObjectRequest<'a> {
    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.object.expected_bucket_owner()
    }
}

impl<'a> GetObjectRequest<'a> {
    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.object.expected_bucket_owner()
    }
}

impl<'a> GetObjectPartRequest<'a> {
    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.object.expected_bucket_owner()
    }
}

impl<'a> GetObjectRangeRequest<'a> {
    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.object.expected_bucket_owner()
    }
}

impl<'a> ListPartsRequest<'a> {
    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.upload.expected_bucket_owner()
    }
}

impl<'a> DeleteObjectsRequest<'a> {
    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.bucket.expected_bucket_owner()
    }
}

impl<'a> GetObjectAttributesRequest<'a> {
    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.object.expected_bucket_owner()
    }
}

impl<'a> BeginStreamPartRequest<'a> {
    pub(super) fn effective_policy_context(&self) -> PutObjectPolicyContext<'a> {
        self.policy_context.with_object_creation_operation(false)
    }
}

/// Parsed request for finalizing a streaming PutObject.
#[derive(Debug)]
pub struct FinalizeStreamPutRequest<'a> {
    pub object: ObjectRequest<'a>,
    pub session_id: &'a SessionId,
    pub crc64: u64,
    pub total_size: u64,
    pub metadata_blob: &'a MetadataBlob,
    pub system_metadata: &'a SystemMetadata,
    pub write_encryption: ActiveWriteEncryptionRef<'a>,
    pub tags: Option<&'a str>,
    pub cond: &'a WriteCondition,
    pub acl: PutObjectWriteAcl<'a>,
    pub policy_context: PutObjectPolicyContext<'a>,
    pub requested_object_lock: ObjectLockState,
}

/// Parsed request for finalizing a streaming UploadPart.
#[derive(Debug)]
pub struct FinalizeStreamPartRequest<'a> {
    pub upload: MultipartObjectRequest<'a>,
    pub session_id: &'a SessionId,
    pub part_number: u32,
    pub crc64: u64,
    pub total_size: u64,
    pub claimed_checksum: Option<&'a ChecksumClaim>,
    pub computed_checksum: Option<RawChecksum>,
}

impl Requester {
    #[must_use]
    pub fn from_auth(auth: &auth::AuthContext) -> Self {
        Self {
            identity: auth.identity.clone(),
            authorization_profile: auth.authorization_profile,
            source_ip: None,
            request_epoch_seconds: None,
            secure_transport: None,
            requested_region: None,
            referer: None,
            auth_type: None,
            signature_version: None,
            signature_age_millis: None,
            tls_version: None,
            content_sha256: None,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub const fn anonymous() -> Self {
        Self {
            identity: None,
            authorization_profile: auth::AuthorizationProfile::Standard,
            source_ip: None,
            request_epoch_seconds: None,
            secure_transport: None,
            requested_region: None,
            referer: None,
            auth_type: None,
            signature_version: None,
            signature_age_millis: None,
            tls_version: None,
            content_sha256: None,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn authenticated(account: AccountIdentity) -> Self {
        Self {
            identity: Some(Self::configured_identity_from_account(account)),
            authorization_profile: auth::AuthorizationProfile::Standard,
            source_ip: None,
            request_epoch_seconds: None,
            secure_transport: None,
            requested_region: None,
            referer: None,
            auth_type: None,
            signature_version: None,
            signature_age_millis: None,
            tls_version: None,
            content_sha256: None,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_account(account: Option<&AccountIdentity>) -> Self {
        Self {
            identity: account.cloned().map(Self::configured_identity_from_account),
            authorization_profile: auth::AuthorizationProfile::Standard,
            source_ip: None,
            request_epoch_seconds: None,
            secure_transport: None,
            requested_region: None,
            referer: None,
            auth_type: None,
            signature_version: None,
            signature_age_millis: None,
            tls_version: None,
            content_sha256: None,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn authenticated_owner_account_admin(account: AccountIdentity) -> Self {
        Self {
            identity: Some(Self::configured_identity_from_account(account)),
            authorization_profile: auth::AuthorizationProfile::OwnerAccountAdmin,
            source_ip: None,
            request_epoch_seconds: None,
            secure_transport: None,
            requested_region: None,
            referer: None,
            auth_type: None,
            signature_version: None,
            signature_age_millis: None,
            tls_version: None,
            content_sha256: None,
        }
    }

    #[must_use]
    pub const fn with_source_ip(mut self, source_ip: Option<std::net::IpAddr>) -> Self {
        self.source_ip = source_ip;
        self
    }

    #[must_use]
    pub const fn source_ip(&self) -> Option<std::net::IpAddr> {
        self.source_ip
    }

    #[must_use]
    pub const fn with_request_epoch_seconds(mut self, request_epoch_seconds: Option<u64>) -> Self {
        self.request_epoch_seconds = request_epoch_seconds;
        self
    }

    #[must_use]
    pub const fn request_epoch_seconds(&self) -> Option<u64> {
        self.request_epoch_seconds
    }

    #[must_use]
    pub const fn with_secure_transport(mut self, secure_transport: Option<bool>) -> Self {
        self.secure_transport = secure_transport;
        self
    }

    #[must_use]
    pub const fn secure_transport(&self) -> Option<bool> {
        self.secure_transport
    }

    #[must_use]
    pub fn with_requested_region(mut self, requested_region: Option<String>) -> Self {
        self.requested_region = requested_region;
        self
    }

    #[must_use]
    pub fn requested_region(&self) -> Option<&str> {
        self.requested_region.as_deref()
    }

    #[must_use]
    pub fn with_referer(mut self, referer: Option<String>) -> Self {
        self.referer = Some(referer);
        self
    }

    #[must_use]
    pub fn referer(&self) -> Option<Option<&str>> {
        self.referer.as_ref().map(|referer| referer.as_deref())
    }

    #[must_use]
    pub const fn with_auth_type(mut self, auth_type: Option<&'static str>) -> Self {
        self.auth_type = Some(auth_type);
        self
    }

    #[must_use]
    pub const fn auth_type(&self) -> Option<Option<&'static str>> {
        self.auth_type
    }

    #[must_use]
    pub const fn with_signature_version(mut self, signature_version: Option<&'static str>) -> Self {
        self.signature_version = Some(signature_version);
        self
    }

    #[must_use]
    pub const fn signature_version(&self) -> Option<Option<&'static str>> {
        self.signature_version
    }

    #[must_use]
    pub const fn with_signature_age_millis(mut self, signature_age_millis: Option<u64>) -> Self {
        self.signature_age_millis = Some(signature_age_millis);
        self
    }

    #[must_use]
    pub const fn signature_age_millis(&self) -> Option<Option<u64>> {
        self.signature_age_millis
    }

    #[must_use]
    pub fn with_tls_version(mut self, tls_version: Option<String>) -> Self {
        self.tls_version = Some(tls_version);
        self
    }

    #[must_use]
    pub fn tls_version(&self) -> Option<Option<&str>> {
        self.tls_version
            .as_ref()
            .map(|tls_version| tls_version.as_deref())
    }

    #[must_use]
    pub fn with_content_sha256(mut self, content_sha256: Option<String>) -> Self {
        self.content_sha256 = Some(content_sha256);
        self
    }

    #[must_use]
    pub fn content_sha256(&self) -> Option<Option<&str>> {
        self.content_sha256
            .as_ref()
            .map(|content_sha256| content_sha256.as_deref())
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_account_owner_account_admin(account: Option<&AccountIdentity>) -> Self {
        Self {
            identity: account.cloned().map(Self::configured_identity_from_account),
            authorization_profile: auth::AuthorizationProfile::OwnerAccountAdmin,
            source_ip: None,
            request_epoch_seconds: None,
            secure_transport: None,
            requested_region: None,
            referer: None,
            auth_type: None,
            signature_version: None,
            signature_age_millis: None,
            tls_version: None,
            content_sha256: None,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn authenticated_with_profile(
        account: AccountIdentity,
        authorization_profile: auth::AuthorizationProfile,
    ) -> Self {
        Self {
            identity: Some(Self::configured_identity_from_account(account)),
            authorization_profile,
            source_ip: None,
            request_epoch_seconds: None,
            secure_transport: None,
            requested_region: None,
            referer: None,
            auth_type: None,
            signature_version: None,
            signature_age_millis: None,
            tls_version: None,
            content_sha256: None,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_account_with_profile(
        account: Option<&AccountIdentity>,
        authorization_profile: auth::AuthorizationProfile,
    ) -> Self {
        Self {
            identity: account.cloned().map(Self::configured_identity_from_account),
            authorization_profile,
            source_ip: None,
            request_epoch_seconds: None,
            secure_transport: None,
            requested_region: None,
            referer: None,
            auth_type: None,
            signature_version: None,
            signature_age_millis: None,
            tls_version: None,
            content_sha256: None,
        }
    }

    #[must_use]
    pub fn account(&self) -> Option<&AccountIdentity> {
        self.identity
            .as_ref()
            .map(auth::AuthenticatedIdentity::account)
    }

    #[must_use]
    pub fn configured_principal(&self) -> Option<&str> {
        self.identity
            .as_ref()?
            .configured_principal()
            .map(auth::ConfiguredPrincipalIdentity::principal)
    }

    #[must_use]
    pub fn session_principal_arn(&self) -> Option<&auth::AssumedRoleSessionArn> {
        self.identity.as_ref()?.session_principal_arn()
    }

    #[must_use]
    pub fn role_principal_arn(&self) -> Option<&auth::IamRoleArn> {
        self.identity.as_ref()?.role_principal_arn()
    }

    #[must_use]
    pub fn aws_userid(&self) -> Option<&auth::AssumedRoleId> {
        self.identity.as_ref()?.aws_userid()
    }

    #[must_use]
    pub fn canonical_user_id(&self) -> Option<&CanonicalUserId> {
        self.configured_principal()?;
        self.account().map(AccountIdentity::canonical_user_id)
    }

    #[must_use]
    pub fn is_anonymous(&self) -> bool {
        self.identity.is_none()
    }

    #[must_use]
    pub const fn authorization_profile(&self) -> auth::AuthorizationProfile {
        self.authorization_profile
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn configured_identity_from_account(account: AccountIdentity) -> auth::AuthenticatedIdentity {
        let principal = auth::ConfiguredPrincipalIdentity::new(account.principal());
        auth::AuthenticatedIdentity::configured(account, principal)
    }
}

impl PutObjectAcl<'_> {
    pub(super) const fn is_public(self) -> bool {
        matches!(
            self,
            Self::PublicRead | Self::PublicReadWrite | Self::AuthenticatedRead
        )
    }

    pub(super) const fn is_supported_with_bucket_owner_enforced(self) -> bool {
        matches!(
            self,
            Self::None | Self::Private | Self::BucketOwnerRead | Self::BucketOwnerFullControl
        )
    }

    pub const fn policy_condition_value(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Private => Some("private"),
            Self::PublicRead => Some("public-read"),
            Self::PublicReadWrite => Some("public-read-write"),
            Self::AuthenticatedRead => Some("authenticated-read"),
            Self::AwsExecRead => Some("aws-exec-read"),
            Self::BucketOwnerRead => Some("bucket-owner-read"),
            Self::BucketOwnerFullControl => Some("bucket-owner-full-control"),
            Self::Invalid(_) => None,
        }
    }
}

impl<'a> From<PutObjectAcl<'a>> for PutObjectWriteAcl<'a> {
    fn from(value: PutObjectAcl<'a>) -> Self {
        match value {
            PutObjectAcl::None => Self::None,
            other => Self::Canned(other),
        }
    }
}

impl PutObjectWriteAcl<'_> {
    pub const fn policy_condition_value(&self) -> Option<&'static str> {
        match self {
            Self::None | Self::Grants(_) => None,
            Self::Canned(acl) => acl.policy_condition_value(),
        }
    }
}

pub(super) fn authorization_policy_context_for_put_object_write_acl<'a>(
    operation: &str,
    acl: &PutObjectWriteAcl<'_>,
    policy_context: PutObjectPolicyContext<'a>,
) -> Result<PutObjectPolicyContext<'a>, ServerError> {
    let base_policy_context = || {
        PutObjectPolicyContext::default()
            .with_if_match(policy_context.if_match)
            .with_if_none_match(policy_context.if_none_match)
            .with_optional_object_creation_operation(policy_context.object_creation_operation)
    };
    match acl {
        PutObjectWriteAcl::None => {
            if policy_context.canned_acl.is_some() {
                return Err(ServerError::InvalidArgument {
                    reason: format!("{operation} policy context cannot include canned ACL"),
                });
            }
            if parse_acl_grants_from_policy_context(policy_context)?.is_some() {
                return Err(ServerError::InvalidArgument {
                    reason: format!("{operation} policy context cannot include grant headers"),
                });
            }
            Ok(base_policy_context())
        }
        PutObjectWriteAcl::Canned(acl) => {
            if policy_context.grant_read.is_some()
                || policy_context.grant_write.is_some()
                || policy_context.grant_read_acp.is_some()
                || policy_context.grant_write_acp.is_some()
                || policy_context.grant_full_control.is_some()
            {
                return Err(ServerError::InvalidArgument {
                    reason: format!(
                        "{operation} canned ACL policy context cannot include grant headers"
                    ),
                });
            }
            let expected_canned_acl = acl.policy_condition_value();
            if policy_context.canned_acl.is_some()
                && policy_context.canned_acl != expected_canned_acl
            {
                return Err(ServerError::InvalidArgument {
                    reason: format!("{operation} canned ACL policy context mismatch"),
                });
            }
            Ok(base_policy_context().with_default_canned_acl(policy_context.canned_acl))
        }
        PutObjectWriteAcl::Grants(acl_grants) => {
            if policy_context.canned_acl.is_some() {
                return Err(ServerError::InvalidArgument {
                    reason: format!("{operation} grant policy context cannot include canned ACL"),
                });
            }
            let header_grants = parse_acl_grants_from_policy_context(policy_context)?;
            if let Some(header_grants) = header_grants {
                if &header_grants != acl_grants {
                    return Err(ServerError::InvalidArgument {
                        reason: format!("{operation} grant policy context mismatch"),
                    });
                }
                Ok(base_policy_context().with_acl_grant_headers(
                    policy_context.grant_read,
                    policy_context.grant_write,
                    policy_context.grant_read_acp,
                    policy_context.grant_write_acp,
                    policy_context.grant_full_control,
                ))
            } else {
                Ok(base_policy_context())
            }
        }
    }
}

impl BucketAcl {
    pub(super) const fn is_public(self) -> bool {
        matches!(
            self,
            Self::PublicRead | Self::PublicReadWrite | Self::AuthenticatedRead
        )
    }

    pub const fn policy_condition_value(&self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::PublicRead => "public-read",
            Self::PublicReadWrite => "public-read-write",
            Self::AuthenticatedRead => "authenticated-read",
        }
    }
}

impl CreateBucketAcl {
    pub(super) const fn is_explicit(&self) -> bool {
        !matches!(self, Self::DefaultPrivate)
    }
}

impl<'a> PutBucketAclRequest<'a> {
    pub(super) fn authorization_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        let policy_context = self.policy_context;
        if policy_context.copy_source.is_some()
            || policy_context.metadata_directive.is_some()
            || policy_context.managed_encryption.is_some()
            || policy_context.sse_customer_algorithm.is_some()
            || policy_context.request_object_tags_xml.is_some()
        {
            return Err(ServerError::InvalidArgument {
                reason: "PutBucketAcl policy context contains unsupported fields".to_string(),
            });
        }

        match &self.acl {
            PutBucketAclInput::Canned(acl) => {
                if policy_context.grant_read.is_some()
                    || policy_context.grant_write.is_some()
                    || policy_context.grant_read_acp.is_some()
                    || policy_context.grant_write_acp.is_some()
                    || policy_context.grant_full_control.is_some()
                {
                    return Err(ServerError::InvalidArgument {
                        reason:
                            "PutBucketAcl canned ACL policy context cannot include grant headers"
                                .to_string(),
                    });
                }
                let expected_canned_acl = Some(acl.policy_condition_value());
                if policy_context.canned_acl.is_some()
                    && policy_context.canned_acl != expected_canned_acl
                {
                    return Err(ServerError::InvalidArgument {
                        reason: "PutBucketAcl canned ACL policy context mismatch".to_string(),
                    });
                }
                Ok(PutObjectPolicyContext::default()
                    .with_default_canned_acl(policy_context.canned_acl))
            }
            PutBucketAclInput::Grants(acl_grants) => {
                if policy_context.canned_acl.is_some() {
                    return Err(ServerError::InvalidArgument {
                        reason: "PutBucketAcl grant policy context cannot include canned ACL"
                            .to_string(),
                    });
                }
                let header_grants = parse_acl_grants_from_policy_context(policy_context)?;
                if let Some(header_grants) = header_grants {
                    if &header_grants != acl_grants {
                        return Err(ServerError::InvalidArgument {
                            reason: "PutBucketAcl grant policy context mismatch".to_string(),
                        });
                    }
                    Ok(PutObjectPolicyContext::default().with_acl_grant_headers(
                        policy_context.grant_read,
                        policy_context.grant_write,
                        policy_context.grant_read_acp,
                        policy_context.grant_write_acp,
                        policy_context.grant_full_control,
                    ))
                } else {
                    Ok(PutObjectPolicyContext::default())
                }
            }
        }
    }
}

impl PutObjectAclInput<'_> {
    pub const fn policy_condition_value(&self) -> Option<&'static str> {
        match self {
            Self::Canned(acl) => acl.policy_condition_value(),
            Self::Grants(_) => None,
        }
    }
}

impl<'a> PutObjectAclRequest<'a> {
    pub(super) fn authorization_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        let policy_context = self.policy_context;
        if policy_context.copy_source.is_some()
            || policy_context.metadata_directive.is_some()
            || policy_context.managed_encryption.is_some()
            || policy_context.sse_customer_algorithm.is_some()
            || policy_context.request_object_tags_xml.is_some()
        {
            return Err(ServerError::InvalidArgument {
                reason: "PutObjectAcl policy context contains unsupported fields".to_string(),
            });
        }

        match &self.acl {
            PutObjectAclInput::Canned(acl) => {
                if policy_context.grant_read.is_some()
                    || policy_context.grant_write.is_some()
                    || policy_context.grant_read_acp.is_some()
                    || policy_context.grant_write_acp.is_some()
                    || policy_context.grant_full_control.is_some()
                {
                    return Err(ServerError::InvalidArgument {
                        reason:
                            "PutObjectAcl canned ACL policy context cannot include grant headers"
                                .to_string(),
                    });
                }
                let expected_canned_acl = acl.policy_condition_value();
                if policy_context.canned_acl.is_some()
                    && policy_context.canned_acl != expected_canned_acl
                {
                    return Err(ServerError::InvalidArgument {
                        reason: "PutObjectAcl canned ACL policy context mismatch".to_string(),
                    });
                }
                Ok(PutObjectPolicyContext::default()
                    .with_default_canned_acl(policy_context.canned_acl))
            }
            PutObjectAclInput::Grants(acl_grants) => {
                if policy_context.canned_acl.is_some() {
                    return Err(ServerError::InvalidArgument {
                        reason: "PutObjectAcl grant policy context cannot include canned ACL"
                            .to_string(),
                    });
                }
                let header_grants = parse_acl_grants_from_policy_context(policy_context)?;
                if let Some(header_grants) = header_grants {
                    if &header_grants != acl_grants {
                        return Err(ServerError::InvalidArgument {
                            reason: "PutObjectAcl grant policy context mismatch".to_string(),
                        });
                    }
                    Ok(PutObjectPolicyContext::default().with_acl_grant_headers(
                        policy_context.grant_read,
                        policy_context.grant_write,
                        policy_context.grant_read_acp,
                        policy_context.grant_write_acp,
                        policy_context.grant_full_control,
                    ))
                } else {
                    Ok(PutObjectPolicyContext::default())
                }
            }
        }
    }
}

fn parse_put_object_acl_grant_header_value(
    value: &str,
    permission: AclPermission,
) -> Result<Vec<AclGrant>, ServerError> {
    let mut grants = Vec::new();
    let mut remaining = value.trim();
    if remaining.is_empty() {
        return Err(ServerError::InvalidArgument {
            reason: "empty ACL grant header value".to_string(),
        });
    }

    while !remaining.is_empty() {
        let (grantee_kind, rest) =
            remaining
                .split_once('=')
                .ok_or_else(|| ServerError::InvalidArgument {
                    reason: format!("invalid ACL grant header entry: {remaining}"),
                })?;
        let grantee_kind = grantee_kind.trim();
        let rest = rest.trim_start();
        let quoted = rest
            .strip_prefix('"')
            .ok_or_else(|| ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {remaining}"),
            })?;
        let quote_end = quoted
            .find('"')
            .ok_or_else(|| ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {remaining}"),
            })?;
        let grantee_value = &quoted[..quote_end];
        let next = &quoted[quote_end + 1..];
        if grantee_value.is_empty() {
            return Err(ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {remaining}"),
            });
        }
        let grantee =
            match grantee_kind {
                "id" => AclGrantee::CanonicalUser(CanonicalUserId::new(grantee_value).ok_or_else(
                    || ServerError::InvalidArgument {
                        reason: "invalid canonical user ID in ACL grant header".to_string(),
                    },
                )?),
                "uri" => AclGrantee::parse_group_uri(grantee_value).ok_or_else(|| {
                    ServerError::InvalidArgument {
                        reason: format!("unsupported ACL group URI: {grantee_value}"),
                    }
                })?,
                other => {
                    return Err(ServerError::InvalidArgument {
                        reason: format!("unsupported ACL grant header grantee: {other}"),
                    });
                }
            };
        grants.push(AclGrant::new(grantee, permission));

        remaining = next.trim_start();
        if remaining.is_empty() {
            break;
        }
        remaining = remaining
            .strip_prefix(',')
            .ok_or_else(|| ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {remaining}"),
            })?;
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            return Err(ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {value}"),
            });
        }
    }

    Ok(grants)
}

fn extend_put_object_acl_grants_from_header(
    grants: &mut Vec<AclGrant>,
    value: Option<&str>,
    permission: AclPermission,
) -> Result<(), ServerError> {
    if let Some(value) = value {
        grants.extend(parse_put_object_acl_grant_header_value(value, permission)?);
    }
    Ok(())
}

fn parse_acl_grants_from_policy_context(
    policy_context: PutObjectPolicyContext<'_>,
) -> Result<Option<AclGrants>, ServerError> {
    let mut grants = Vec::new();
    extend_put_object_acl_grants_from_header(
        &mut grants,
        policy_context.grant_read,
        AclPermission::Read,
    )?;
    extend_put_object_acl_grants_from_header(
        &mut grants,
        policy_context.grant_write,
        AclPermission::Write,
    )?;
    extend_put_object_acl_grants_from_header(
        &mut grants,
        policy_context.grant_read_acp,
        AclPermission::ReadAcp,
    )?;
    extend_put_object_acl_grants_from_header(
        &mut grants,
        policy_context.grant_write_acp,
        AclPermission::WriteAcp,
    )?;
    extend_put_object_acl_grants_from_header(
        &mut grants,
        policy_context.grant_full_control,
        AclPermission::FullControl,
    )?;
    if grants.is_empty() {
        Ok(None)
    } else {
        Ok(Some(AclGrants::new(grants)))
    }
}

impl<'a> PutObjectRequest<'a> {
    pub(super) fn authorization_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        authorization_policy_context_for_put_object_write_acl(
            "PutObject",
            &self.acl,
            self.policy_context,
        )
    }

    pub(super) fn effective_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        let policy_context = self
            .encryption
            .with_policy_context(self.authorization_policy_context()?);
        let policy_context = policy_context
            .with_if_match(self.cond.if_match_policy_value())
            .with_if_none_match(self.cond.if_none_match_policy_value())
            .with_object_creation_operation(true);
        if policy_context.request_object_tags_xml.is_some() {
            Ok(policy_context)
        } else {
            Ok(policy_context.with_request_object_tags_xml(self.tags))
        }
    }
}

impl<'a> CreateMultipartUploadRequest<'a> {
    pub(super) fn authorization_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        authorization_policy_context_for_put_object_write_acl(
            "CreateMultipartUpload",
            &self.acl,
            self.policy_context,
        )
    }

    pub(super) fn effective_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        let policy_context = self
            .encryption
            .with_policy_context(self.authorization_policy_context()?);
        let policy_context = policy_context.with_object_creation_operation(false);
        if policy_context.request_object_tags_xml.is_some() {
            Ok(policy_context)
        } else {
            Ok(policy_context.with_request_object_tags_xml(self.tags))
        }
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn assumed_role_requester_does_not_acquire_configured_principal_authorization() {
        let account_id = "123456789012";
        let account = AccountIdentity::new(
            account_id,
            CanonicalUserId::from_principal(account_id),
            "Test account",
        );
        let role = auth::IamRoleIdentity::new(
            auth::AwsAccountId::new(account_id).unwrap(),
            auth::StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap(),
            auth::RoleName::new("test-role").unwrap(),
            auth::IamPath::new("/team/").unwrap(),
        );
        let session = auth::AssumedRoleSessionIdentity::new(
            role,
            auth::RoleSessionName::new("test-session").unwrap(),
            auth::SessionLifetime::new(1_700_000_000, 1_700_003_600).unwrap(),
            None,
        );
        let identity = auth::AuthenticatedIdentity::assumed_role_session(account, session).unwrap();
        let context = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("ARGS0123456789ABCDEFGHIJ".to_string()),
            identity: Some(identity),
            authorization_profile: auth::AuthorizationProfile::OwnerAccountAdmin,
            request_epoch_secs: Some(1_700_000_000),
            signing_region: Some("us-east-1".to_string()),
            streaming: None,
        };

        let requester = Requester::from_auth(&context);
        assert!(!requester.is_anonymous());
        assert!(requester.account().is_some());
        assert!(requester.configured_principal().is_none());
        assert!(requester.canonical_user_id().is_none());
        assert_eq!(
            requester.session_principal_arn().unwrap().as_str(),
            "arn:aws:sts::123456789012:assumed-role/test-role/test-session"
        );
        assert_eq!(
            requester.role_principal_arn().unwrap().as_str(),
            "arn:aws:iam::123456789012:role/team/test-role"
        );
        assert_eq!(
            requester.aws_userid().unwrap().as_str(),
            "ARGR0123456789ABCDEFGHIJ:test-session"
        );

        let same_account_owner = storage::OwnerIdentity::new(
            requester.session_principal_arn().unwrap().as_str(),
            CanonicalUserId::from_principal(account_id),
        );
        assert!(
            !crate::coordinator::Coordinator::requester_matches_owner_identity(
                &requester,
                &same_account_owner,
            )
        );
        assert!(
            crate::coordinator::Coordinator::requester_owner_identity(&requester).is_none(),
            "an assumed-role session must not become a legacy S3 object owner before role authorization exists"
        );
    }
}
