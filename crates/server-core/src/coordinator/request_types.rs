use super::*;

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
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer_algorithm: Option<&'a str>,
    pub grant_read: Option<&'a str>,
    pub grant_write: Option<&'a str>,
    pub grant_read_acp: Option<&'a str>,
    pub grant_write_acp: Option<&'a str>,
    pub grant_full_control: Option<&'a str>,
    pub request_object_tags_xml: Option<&'a str>,
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
            managed_encryption: None,
            sse_customer_algorithm: None,
            grant_read: None,
            grant_write: None,
            grant_read_acp: None,
            grant_write_acp: None,
            grant_full_control: None,
            request_object_tags_xml: None,
        }
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

    #[must_use]
    pub fn from_request_parts(
        sse_customer: Option<&'a SseCustomerRequest>,
        managed_encryption: Option<ManagedEncryptionAlgorithm>,
    ) -> Self {
        match (sse_customer, managed_encryption) {
            (Some(request), None) => Self::sse_customer(request),
            (None, Some(algorithm)) => Self::managed(algorithm),
            (None, None) => Self::none(),
            (Some(_), Some(_)) => {
                unreachable!("request parsing should reject conflicting SSE-C and SSE-S3")
            }
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

pub(super) struct PreparedPutCommit {
    pub(super) version_id: VersionId,
    pub(super) generation_id: GenerationId,
    pub(super) tags: Option<SerializedTagSet>,
    pub(super) metadata_blob: SerializedMetadataBlob,
    pub(super) system_metadata_blob: SerializedSystemMetadataBlob,
    pub(super) encryption: ObjectEncryption,
    pub(super) stale_payload: Option<StaleObjectPayload>,
}

pub(super) struct PutCommitRequest<'a> {
    pub(super) bucket: &'a BucketName,
    pub(super) key: &'a ObjectKey,
    pub(super) metadata_blob: &'a MetadataBlob,
    pub(super) system_metadata: &'a SystemMetadata,
    pub(super) write_encryption: &'a ActiveWriteEncryption,
    pub(super) tags: Option<&'a str>,
    pub(super) cond: &'a WriteCondition,
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
    pub(super) account: Option<AccountIdentity>,
    pub(super) authorization_profile: auth::AuthorizationProfile,
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
}

/// Request for a ListObjectVersions operation.
#[derive(Debug)]
pub struct ListObjectVersionsRequest<'a> {
    pub bucket: BucketRequest<'a>,
    pub prefix: Option<&'a str>,
    pub key_marker: Option<&'a str>,
    pub version_id_marker: Option<VersionId>,
    pub max_keys: u32,
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
    pub session_id: &'a SessionId,
    pub part_number: u32,
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

    pub fn from_object_typed(object: ObjectRequest<'a>, upload_id: UploadId) -> Self {
        Self { object, upload_id }
    }

    pub fn new_typed(
        bucket: BucketName,
        key: ObjectKey,
        upload_id: UploadId,
        requester: Requester,
        expected_bucket_owner: Option<&'a str>,
    ) -> Self {
        Self::from_object_typed(
            ObjectRequest::new(bucket, key, requester, expected_bucket_owner),
            upload_id,
        )
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

    pub fn upload_id_typed(&self) -> &UploadId {
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
    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.upload.expected_bucket_owner()
    }

    pub(super) fn effective_policy_context(&self) -> PutObjectPolicyContext<'a> {
        self.policy_context
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
