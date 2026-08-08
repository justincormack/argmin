/// Unified error type for the server crate.
use s3_types::VersionId;
use storage::{
    BucketListingFailure, BucketSnapshotLoadFailure, BucketWriteDrainFailure, DirectPutFailure,
    LifecycleMaintenanceFailure, LifecycleMutationFailure, MultipartCompletionFailure,
    MultipartManagementFailure, ObjectMetadataListingFailure, ObjectMetadataMutationFailure,
    ObjectReadFailure, StoreFailure, StreamUploadFailure,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedEncryptionReadHeaderContext {
    StandardObjectRead,
    ObjectAttributes,
    Multipart,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManagedEncryptionReadHeader {
    ServerSideEncryption { value: String },
    KmsKeyId,
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("bucket not found: {name}")]
    BucketNotFound { name: String },

    #[error("bucket already exists")]
    BucketAlreadyExists,

    #[error("bucket already owned by you")]
    BucketAlreadyOwnedByYou,

    #[error("bucket ACL cannot be public when block public access is enabled")]
    InvalidBucketAclWithBlockPublicAccessError,

    #[error("bucket not empty")]
    BucketNotEmpty,

    #[error("object not found: {bucket}/{key}")]
    ObjectNotFound { bucket: String, key: String },

    #[error("version not found: {bucket}/{key} version {version_id}")]
    VersionNotFound {
        bucket: String,
        key: String,
        version_id: String,
    },

    #[error("delete marker hit: {bucket}/{key}")]
    DeleteMarkerHit { bucket: String, key: String },

    #[error("storage error: {0}")]
    Store(StoreFailure),

    #[error("bucket write drain error: {0}")]
    BucketWriteDrain(BucketWriteDrainFailure),

    #[error("bucket snapshot load error: {0}")]
    BucketSnapshotLoad(BucketSnapshotLoadFailure),

    #[error("bucket listing error: {0}")]
    BucketListing(BucketListingFailure),

    #[error("lifecycle maintenance error: {0}")]
    LifecycleMaintenance(LifecycleMaintenanceFailure),

    #[error("lifecycle mutation error: {0}")]
    LifecycleMutation(LifecycleMutationFailure),

    #[error("object read error: {0}")]
    ObjectRead(ObjectReadFailure),

    #[error("object metadata listing error: {0}")]
    ObjectMetadataListing(ObjectMetadataListingFailure),

    #[error("object metadata mutation error: {0}")]
    ObjectMetadataMutation(ObjectMetadataMutationFailure),

    #[error("stream upload error: {0}")]
    StreamUpload(StreamUploadFailure),

    #[error("direct PutObject error: {0}")]
    DirectPut(DirectPutFailure),

    #[error("multipart management error: {0}")]
    MultipartManagement(MultipartManagementFailure),

    #[error("multipart completion error: {0}")]
    MultipartCompletion(MultipartCompletionFailure),

    #[error("EC error: {0}")]
    Ec(#[from] ec::EcError),

    #[error("auth error: {0}")]
    Auth(#[from] auth::AuthError),

    #[error("identity provider error: {0}")]
    IdentityProvider(#[from] auth::IdentityProviderError),

    #[error("authorization header region {provided_region} is wrong; expecting {expected_region}")]
    WrongRegion {
        provided_region: String,
        expected_region: String,
        bucket_region_header: bool,
    },

    #[error("invalid request: {reason}")]
    InvalidRequest { reason: String },

    #[error("invalid request: {reason}")]
    InvalidRequestHostId { reason: String },

    #[error("bad request: {reason}")]
    BadRequest { reason: String },

    #[error("invalid argument: {reason}")]
    InvalidArgument { reason: String },

    #[error("invalid argument: {argument_name}={argument_value}: {reason}")]
    InvalidArgumentValue {
        reason: String,
        argument_name: String,
        argument_value: String,
    },

    #[error("invalid managed encryption read header: {header:?}")]
    InvalidManagedEncryptionReadHeader {
        context: ManagedEncryptionReadHeaderContext,
        header: ManagedEncryptionReadHeader,
    },

    #[error("invalid version id specified: {argument_name}={argument_value}")]
    InvalidVersionId {
        argument_name: String,
        argument_value: String,
    },

    #[error("duplicate checksum header: {header}")]
    DuplicateChecksumHeader { header: String, value: String },

    #[error("invalid redirect location: {reason}")]
    InvalidRedirectLocation { reason: String },

    #[error("unexpected content")]
    UnexpectedContent,

    #[error("invalid URI: {reason}")]
    InvalidURI { reason: String },

    #[error("invalid bucket name: {reason}")]
    InvalidBucketName { reason: String },

    #[error("key too long: {size} bytes (max {max_size_allowed})")]
    KeyTooLongError {
        size: usize,
        max_size_allowed: usize,
    },

    #[error("invalid bucket namespace: {reason}")]
    InvalidBucketNamespace {
        reason: String,
        bucket_namespace: String,
    },

    #[error("missing x-amz-bucket-namespace header")]
    MissingNamespaceHeader,

    #[error("account-regional namespace header requires an account-regional bucket-name suffix")]
    AccountRegionalNamespaceHeaderRequiresSuffix { bucket: String },

    #[error("global namespace header cannot be used with an account-regional bucket name")]
    GlobalNamespaceHeaderRejectedForAccountRegionalBucket { bucket: String },
    #[error("metadata blob error: {reason}")]
    MetadataBlobError { reason: String },

    #[error("object too large: {size} bytes (max {max})")]
    ObjectTooLarge { size: u64, max: u64 },

    #[error("metadata too large")]
    MetadataTooLarge,

    #[error("metadata too large: {size} bytes (max {max_size_allowed})")]
    MetadataTooLargeDetailed {
        size: usize,
        max_size_allowed: usize,
    },

    #[error("request header section too large")]
    RequestHeaderSectionTooLarge,

    #[error("request too large (max {max_message_length_bytes} bytes)")]
    MaxMessageLengthExceeded { max_message_length_bytes: usize },

    #[error("method not allowed")]
    MethodNotAllowed,

    #[error("PUT is not allowed on a multipart upload resource")]
    PutMultipartUploadMethodNotAllowed,

    #[error("head on delete marker version not allowed")]
    HeadDeleteMarkerMethodNotAllowed {
        version_id: VersionId,
        last_modified: u64,
    },

    #[error("invalid range")]
    InvalidRange {
        range_requested: String,
        total_size: u64,
    },

    #[error("invalid part number")]
    InvalidPartNumber { part_number: u32, parts_count: u32 },

    #[error("UploadPart requires an upload ID")]
    UploadPartMissingUploadId,

    #[error("UploadPartCopy requires an upload ID")]
    UploadPartCopyMissingUploadId,

    #[error("invalid upload part number: {value}")]
    InvalidUploadPartNumber { value: String },

    #[error("invalid upload part copy number: {value}")]
    InvalidUploadPartCopyNumber { value: String },

    /// A read/write/delete conditional header did not hold; `condition`
    /// names the failing header for the AWS-shaped `<Condition>` element.
    #[error("precondition failed: {condition}")]
    PreconditionFailed { condition: &'static str },

    #[error("conditional request conflict for {key}: {condition}")]
    ConditionalRequestConflict {
        key: String,
        condition: &'static str,
    },

    #[error("not modified")]
    NotModified { etag: String, last_modified: u64 },

    #[error("Please reduce your request rate.")]
    SlowDown,

    #[error("bad digest")]
    BadDigest,

    #[error("Content-MD5 did not match the request body")]
    ContentMd5Mismatch,

    #[error("checksum digest mismatch for {algorithm}")]
    ChecksumDigestMismatch { algorithm: String },

    #[error("invalid digest")]
    InvalidDigest,

    #[error("The calculated MD5 hash of the key did not match the hash that was provided.")]
    InvalidSseCustomerKeyMd5,

    #[error("Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm.")]
    MissingSseCustomerAlgorithm,

    #[error("Requests specifying Server Side Encryption with Customer provided keys must provide an appropriate secret key.")]
    MissingSseCustomerKey,

    #[error("Requests specifying Server Side Encryption with Customer provided keys must provide the client calculated MD5 of the secret key.")]
    MissingSseCustomerKeyMd5,

    #[error("invalid encryption algorithm: {value}")]
    InvalidEncryptionAlgorithmError { value: String },

    #[error("invalid chunk size: only the last chunk may be smaller than {min_size} bytes (chunk {chunk} was {chunk_size} bytes)")]
    InvalidChunkSize {
        chunk: usize,
        chunk_size: usize,
        min_size: usize,
    },

    #[error("no CORS configuration")]
    NoSuchCorsConfiguration { bucket: String },

    #[error("no such tag set")]
    NoSuchTagSet { resource: String },

    #[error("invalid tag: {reason}")]
    InvalidTag {
        reason: String,
        /// The offending tag, echoed as `<TagKey>`/`<TagValue>` when known.
        tag_key: Option<String>,
        tag_value: Option<String>,
    },

    #[error("request body is empty")]
    MissingRequestBodyError,

    /// An `x-amz-tagging` header that is not valid URL-encoded UTF-8.
    #[error("invalid x-amz-tagging header")]
    InvalidTaggingHeader { value: String },

    /// MalformedXML raised while parsing a DeleteObjects or
    /// OwnershipControls body; AWS renders those operations' errors
    /// without the XML declaration (like the complete-multipart family).
    #[error("malformed XML (no declaration): {reason}")]
    MalformedXMLNoDecl { reason: String },

    /// KeyTooLongError raised while parsing a DeleteObjects body; AWS
    /// renders these without the XML declaration.
    #[error("delete objects key too long")]
    DeleteObjectsKeyTooLong {
        size: usize,
        max_size_allowed: usize,
    },

    #[error("no public access block configuration: {bucket}")]
    NoSuchPublicAccessBlockConfiguration { bucket: String },

    #[error("no bucket policy: {bucket}")]
    NoSuchBucketPolicy { bucket: String },

    #[error("no lifecycle configuration: {bucket}")]
    NoSuchLifecycleConfiguration { bucket: String },

    #[error("malformed policy: {reason}")]
    MalformedPolicy {
        reason: String,
        /// Offending value echoed in the error body's `<Detail>` element.
        detail: Option<String>,
    },

    #[error("invalid policy document: {reason}")]
    InvalidPolicyDocument { reason: String },

    #[error("ownership controls not found: {bucket}")]
    OwnershipControlsNotFound { bucket: String },

    #[error("object lock configuration not found: {bucket}")]
    ObjectLockConfigurationNotFound { bucket: String },

    #[error("server-side encryption configuration not found: {bucket}")]
    ServerSideEncryptionConfigurationNotFound { bucket: String },

    #[error("bucket is in an invalid state for this operation")]
    InvalidBucketState,

    /// An AWS-defined resource conflict, selected by an operation-specific
    /// boundary rather than by generic storage retry classification.
    #[error("a conflicting conditional operation is currently in progress against this resource")]
    OperationAborted,

    #[error("ACL not supported with BucketOwnerEnforced")]
    AccessControlListNotSupported,

    #[error("invalid bucket ACL with object ownership")]
    InvalidBucketAclWithObjectOwnership,

    #[error("access denied")]
    AccessDenied,

    /// Denial because governance/compliance retention or a legal hold
    /// protects the object (retention shortening, mode change, or delete
    /// without an allowed bypass).
    #[error("access denied because object protected by object lock")]
    ObjectLockProtectedAccessDenied,

    #[error("post policy access denied: {reason}")]
    PostPolicyAccessDenied { reason: String },
    #[error("POST policy condition access denied")]
    PostPolicyConditionAccessDenied {
        expression: auth::PostPolicyConditionExpression,
    },
    #[error("POST object request presented a session-token header without header authentication")]
    PostObjectNoAccessKeyPresented,

    #[error("block public policy access denied for requester {requester_principal} on {bucket}")]
    BlockPublicPolicyAccessDenied {
        requester_principal: String,
        bucket: String,
    },

    #[error("SSE-C blocked access denied for requester {requester_principal} action {action} resource {resource}")]
    SseCBlockedAccessDenied {
        requester_principal: String,
        action: String,
        resource: String,
    },

    #[error("anonymous users cannot invoke this API")]
    AnonymousApiAccessDenied,

    #[error("no such upload: {upload_id}")]
    NoSuchUpload { upload_id: String },

    #[error("invalid part: part {part_number}")]
    InvalidPart { part_number: u32 },

    #[error("invalid part order")]
    InvalidPartOrder,

    #[error(
        "complete multipart missing per-part checksum for part {part_number} using {algorithm}"
    )]
    CompleteMultipartMissingPartChecksum { algorithm: String, part_number: u32 },

    #[error("complete multipart checksum header {header_name} is invalid")]
    CompleteMultipartChecksumHeaderInvalid { header_name: String },

    #[error("CompleteMultipartUpload contains more than 10000 parts")]
    CompleteMultipartTooManyParts,

    #[error("CompleteMultipartUpload If-Match value is empty")]
    CompleteMultipartEmptyIfMatch,

    #[error("DeleteObject If-Match value is empty")]
    DeleteObjectEmptyIfMatch,

    #[error("CompleteMultipartUpload does not support this If-None-Match value")]
    CompleteMultipartIfNoneMatchNotImplemented,

    #[error("invalid CompleteMultipartUpload expected object size: {value}")]
    CompleteMultipartExpectedSizeHeaderInvalid { value: String },

    #[error("upload part copy source range invalid: {range_header} for source size {source_size}")]
    UploadPartCopyInvalidRange {
        range_header: String,
        source_size: u64,
    },

    #[error("upload part copy precondition failed for {condition}")]
    UploadPartCopyPreconditionFailed { condition: String },

    #[error("entity too small: part {part_number} is {size} bytes (min {min})")]
    EntityTooSmall {
        part_number: u32,
        size: u64,
        min: u64,
    },

    #[error("not implemented: {feature}")]
    NotImplemented { feature: String },

    #[error("header not implemented: {header}")]
    HeaderNotImplemented { header: String },

    #[error("query parameter not implemented: {query_parameter}")]
    QueryParameterNotImplemented { query_parameter: String },

    #[error("x-amz-content-sha256 mismatch: client={client_hash}, server={server_hash}")]
    XAmzContentSHA256Mismatch {
        client_hash: String,
        server_hash: String,
    },

    /// Presigned requests carrying a streaming payload marker do not activate
    /// AWS's aws-chunked decoder. This variant preserves that response's
    /// distinct XML shape, which omits Resource.
    #[error("presigned streaming x-amz-content-sha256 mismatch: client={client_hash}, server={server_hash}")]
    PresignedStreamingContentSHA256Mismatch {
        client_hash: String,
        server_hash: String,
    },

    #[error("malformed XML: {reason}")]
    MalformedXML { reason: String },

    #[error("illegal versioning configuration: {reason}")]
    IllegalVersioningConfiguration { reason: String },

    #[error("malformed POST request: {reason}")]
    MalformedPOSTRequest { reason: String },

    #[error("malformed chunked body: {reason}")]
    MalformedChunkedBody { reason: String },

    #[error("incomplete body")]
    IncompleteBody,

    /// Presigned streaming markers compare the raw body length with
    /// x-amz-decoded-content-length and report both values in the response.
    #[error("presigned streaming incomplete body: expected={expected}, provided={provided}")]
    PresignedStreamingIncompleteBody { expected: u64, provided: u64 },

    #[error("missing content length")]
    MissingContentLength,

    #[error("unsupported streaming token: {token}")]
    UnsupportedStreamingToken { token: String },

    #[error("POST Object does not accept SigV4 Authorization header authentication")]
    PostObjectHeaderAuthUnsupported,

    #[error("malformed trailer: {reason}")]
    MalformedTrailerError { reason: String },

    #[error("internal error: {reason}")]
    InternalError { reason: String },

    #[error("data integrity error for {bucket}/{key}")]
    IntegrityError {
        bucket: String,
        key: String,
        expected: u64,
        actual: u64,
    },
}

impl ServerError {
    pub const UNSUPPORTED_CHECKSUM_ALGORITHM_MESSAGE: &'static str = "Checksum algorithm provided is unsupported. Please try again with any of the valid types: [CRC32, CRC32C, CRC64NVME, MD5, SHA1, SHA256, SHA512, XXHASH128, XXHASH3, XXHASH64]";

    pub fn unsupported_checksum_algorithm() -> Self {
        Self::InvalidRequestHostId {
            reason: Self::UNSUPPORTED_CHECKSUM_ALGORITHM_MESSAGE.to_string(),
        }
    }

    /// Stable, redacted label for server-side diagnostics.
    ///
    /// This is intentionally coarser than `Debug`/`Display`: labels must be
    /// safe to emit in production traces and metrics without leaking request,
    /// policy, object, auth, or payload details.
    pub fn diagnostic_cause_label(&self) -> &'static str {
        match self {
            Self::Store(error) => error.diagnostic_cause_label(),
            Self::BucketWriteDrain(error) => error.diagnostic_cause_label(),
            Self::BucketSnapshotLoad(error) => error.diagnostic_cause_label(),
            Self::BucketListing(error) => error.diagnostic_cause_label(),
            Self::LifecycleMaintenance(error) => error.diagnostic_cause_label(),
            Self::LifecycleMutation(error) => error.diagnostic_cause_label(),
            Self::ObjectRead(error) => error.diagnostic_cause_label(),
            Self::ObjectMetadataListing(error) => error.diagnostic_cause_label(),
            Self::ObjectMetadataMutation(error) => error.diagnostic_cause_label(),
            Self::StreamUpload(error) => error.diagnostic_cause_label(),
            Self::DirectPut(error) => error.diagnostic_cause_label(),
            Self::MultipartManagement(error) => error.diagnostic_cause_label(),
            Self::MultipartCompletion(error) => error.diagnostic_cause_label(),
            Self::Ec(_) => "ec_error",
            Self::MetadataBlobError { .. } => "metadata_blob_error",
            Self::InternalError { .. } => "internal_error",
            Self::IntegrityError { .. } => "integrity_error",
            Self::SlowDown => "slow_down",
            Self::OperationAborted => "operation_aborted",
            Self::IdentityProvider(error)
            | Self::Auth(auth::AuthError::IdentityProviderFailure(error)) => {
                error.diagnostic_cause_label()
            }
            Self::Auth(auth::AuthError::SessionTokenKeyRingUnavailable) => {
                "session_token_key_ring_unavailable"
            }
            Self::Auth(_) => "auth_error",
            _ => self.s3_error_code(),
        }
    }

    /// Stable, redacted cause chain for server-side diagnostics.
    ///
    /// This intentionally uses only the same bounded labels as
    /// `diagnostic_cause_label`, preserving nesting that is useful when a 500
    /// crosses multiple subsystems.
    pub fn diagnostic_cause_chain(&self) -> String {
        match self {
            Self::Store(error) => format!(
                "server_error>store_error>{}",
                error.diagnostic_cause_label()
            ),
            Self::BucketWriteDrain(error) => format!(
                "server_error>bucket_write_drain>{}",
                error.diagnostic_cause_label()
            ),
            Self::BucketSnapshotLoad(error) => format!(
                "server_error>bucket_snapshot_load>{}",
                error.diagnostic_cause_label()
            ),
            Self::BucketListing(error) => format!(
                "server_error>bucket_listing>{}",
                error.diagnostic_cause_label()
            ),
            Self::LifecycleMaintenance(error) => format!(
                "server_error>lifecycle_maintenance>{}",
                error.diagnostic_cause_label()
            ),
            Self::LifecycleMutation(error) => format!(
                "server_error>lifecycle_mutation>{}",
                error.diagnostic_cause_label()
            ),
            Self::ObjectRead(error) => format!(
                "server_error>object_read>{}",
                error.diagnostic_cause_label()
            ),
            Self::ObjectMetadataListing(error) => format!(
                "server_error>object_metadata_listing>{}",
                error.diagnostic_cause_label()
            ),
            Self::ObjectMetadataMutation(error) => format!(
                "server_error>object_metadata_mutation>{}",
                error.diagnostic_cause_label()
            ),
            Self::StreamUpload(error) => format!(
                "server_error>stream_upload>{}",
                error.diagnostic_cause_label()
            ),
            Self::DirectPut(error) => {
                format!("server_error>direct_put>{}", error.diagnostic_cause_label())
            }
            Self::MultipartManagement(error) => format!(
                "server_error>multipart_management>{}",
                error.diagnostic_cause_label()
            ),
            Self::MultipartCompletion(error) => format!(
                "server_error>multipart_completion>{}",
                error.diagnostic_cause_label()
            ),
            _ => format!("server_error>{}", self.diagnostic_cause_label()),
        }
    }

    /// Map to S3 error code string.
    pub fn s3_error_code(&self) -> &'static str {
        match self {
            Self::BucketNotFound { .. } => "NoSuchBucket",
            Self::BucketAlreadyExists => "BucketAlreadyExists",
            Self::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            Self::InvalidBucketAclWithBlockPublicAccessError => {
                "InvalidBucketAclWithBlockPublicAccessError"
            }
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::ObjectNotFound { .. } => "NoSuchKey",
            Self::VersionNotFound { .. } => "NoSuchVersion",
            Self::DeleteMarkerHit { .. } => "NoSuchKey",
            Self::Auth(auth::AuthError::MissingAuth) => "AccessDenied",
            Self::Auth(
                auth::AuthError::MalformedAuth
                | auth::AuthError::MalformedAuthComponents
                | auth::AuthError::MalformedSignedHeaders
                | auth::AuthError::InvalidHeaderCredentialRegion { .. }
                | auth::AuthError::InvalidHeaderCredentialService { .. },
            )
            | Self::WrongRegion { .. } => "AuthorizationHeaderMalformed",
            Self::Auth(
                auth::AuthError::UnsupportedAuthType
                | auth::AuthError::InvalidCredentialScope { .. }
                | auth::AuthError::InvalidCredentialScopeRegion { .. }
                | auth::AuthError::InvalidCredentialScopeService { .. },
            ) => "InvalidArgument",
            Self::Auth(auth::AuthError::UnknownAccessKey { .. }) => "InvalidAccessKeyId",
            Self::IdentityProvider(_) => "InternalError",
            Self::Auth(auth::AuthError::IdentityProviderFailure(_)) => "InternalError",
            Self::Auth(auth::AuthError::SessionTokenKeyRingUnavailable) => "InternalError",
            Self::Auth(auth::AuthError::DuplicateAuthorizationHeader) => "NotImplemented",
            Self::Auth(auth::AuthError::MultipleAuthMechanisms { .. }) => "InvalidArgument",
            Self::Auth(auth::AuthError::SignatureMismatch { .. }) => "SignatureDoesNotMatch",
            Self::Auth(auth::AuthError::RequestExpired) => "RequestTimeTooSkewed",
            Self::Auth(auth::AuthError::PresignedRequestExpired { .. }) => "AccessDenied",
            Self::Auth(
                auth::AuthError::ExpiredToken | auth::AuthError::ExpiredSessionToken { .. },
            ) => "ExpiredToken",
            Self::Auth(auth::AuthError::UnexpectedSecurityToken { .. }) => "InvalidToken",
            Self::Auth(auth::AuthError::InvalidQueryParam { .. }) => {
                "AuthorizationQueryParametersError"
            }
            Self::Auth(
                auth::AuthError::InvalidQueryCredentialRegion { .. }
                | auth::AuthError::InvalidQueryCredentialService { .. },
            ) => "AuthorizationQueryParametersError",
            Self::Auth(auth::AuthError::MissingQueryParam { .. }) => {
                "AuthorizationQueryParametersError"
            }
            Self::Auth(_) => "AccessDenied",
            Self::PreconditionFailed { .. } => "PreconditionFailed",
            Self::ConditionalRequestConflict { .. } => "ConditionalRequestConflict",
            Self::NotModified { .. } => "NotModified",
            Self::InvalidRequest { .. } | Self::InvalidRequestHostId { .. } => "InvalidRequest",
            Self::BadRequest { .. } => "BadRequest",
            Self::InvalidArgument { .. }
            | Self::InvalidArgumentValue { .. }
            | Self::UploadPartMissingUploadId
            | Self::UploadPartCopyMissingUploadId
            | Self::InvalidUploadPartNumber { .. }
            | Self::InvalidUploadPartCopyNumber { .. }
            | Self::InvalidManagedEncryptionReadHeader { .. }
            | Self::InvalidVersionId { .. }
            | Self::DuplicateChecksumHeader { .. } => "InvalidArgument",
            Self::InvalidRedirectLocation { .. } => "InvalidRedirectLocation",
            Self::UnexpectedContent => "UnexpectedContent",
            Self::InvalidURI { .. } => "InvalidURI",
            Self::InvalidBucketName { .. } => "InvalidBucketName",
            Self::KeyTooLongError { .. } => "KeyTooLongError",
            Self::InvalidBucketNamespace { .. } => "InvalidBucketNamespace",
            Self::MissingNamespaceHeader => "MissingNamespaceHeader",
            Self::AccountRegionalNamespaceHeaderRequiresSuffix { .. }
            | Self::GlobalNamespaceHeaderRejectedForAccountRegionalBucket { .. } => {
                "InvalidNamespaceHeader"
            }
            Self::MetadataBlobError { .. } => "InternalError",
            Self::ObjectTooLarge { .. } => "EntityTooLarge",
            Self::MetadataTooLarge | Self::MetadataTooLargeDetailed { .. } => "MetadataTooLarge",
            Self::RequestHeaderSectionTooLarge => "RequestHeaderSectionTooLarge",
            Self::MaxMessageLengthExceeded { .. } => "MaxMessageLengthExceeded",
            Self::MethodNotAllowed
            | Self::PutMultipartUploadMethodNotAllowed
            | Self::HeadDeleteMarkerMethodNotAllowed { .. } => "MethodNotAllowed",
            Self::InvalidRange { .. } => "InvalidRange",
            Self::InvalidPartNumber { .. } => "InvalidPartNumber",
            Self::SlowDown => "SlowDown",
            Self::BadDigest | Self::ContentMd5Mismatch | Self::ChecksumDigestMismatch { .. } => {
                "BadDigest"
            }
            Self::InvalidDigest => "InvalidDigest",
            Self::InvalidSseCustomerKeyMd5
            | Self::MissingSseCustomerAlgorithm
            | Self::MissingSseCustomerKey
            | Self::MissingSseCustomerKeyMd5 => "InvalidArgument",
            Self::InvalidEncryptionAlgorithmError { .. } => "InvalidEncryptionAlgorithmError",
            Self::InvalidChunkSize { .. } => "InvalidChunkSizeError",
            Self::NoSuchCorsConfiguration { .. } => "NoSuchCORSConfiguration",
            Self::NoSuchTagSet { .. } => "NoSuchTagSet",
            Self::InvalidTag { .. } => "InvalidTag",
            Self::MissingRequestBodyError => "MissingRequestBodyError",
            Self::InvalidTaggingHeader { .. } => "InvalidArgument",
            Self::MalformedXMLNoDecl { .. } => "MalformedXML",
            Self::DeleteObjectsKeyTooLong { .. } => "KeyTooLongError",
            Self::NoSuchPublicAccessBlockConfiguration { .. } => {
                "NoSuchPublicAccessBlockConfiguration"
            }
            Self::NoSuchBucketPolicy { .. } => "NoSuchBucketPolicy",
            Self::NoSuchLifecycleConfiguration { .. } => "NoSuchLifecycleConfiguration",
            Self::MalformedPolicy { .. } => "MalformedPolicy",
            Self::InvalidPolicyDocument { .. } => "InvalidPolicyDocument",
            Self::OwnershipControlsNotFound { .. } => "OwnershipControlsNotFoundError",
            Self::ObjectLockConfigurationNotFound { .. } => "ObjectLockConfigurationNotFoundError",
            Self::ServerSideEncryptionConfigurationNotFound { .. } => {
                "ServerSideEncryptionConfigurationNotFoundError"
            }
            Self::InvalidBucketState => "InvalidBucketState",
            Self::OperationAborted => "OperationAborted",
            Self::AccessControlListNotSupported => "AccessControlListNotSupported",
            Self::InvalidBucketAclWithObjectOwnership => "InvalidBucketAclWithObjectOwnership",
            Self::AccessDenied
            | Self::ObjectLockProtectedAccessDenied
            | Self::PostPolicyAccessDenied { .. }
            | Self::PostPolicyConditionAccessDenied { .. }
            | Self::PostObjectNoAccessKeyPresented
            | Self::BlockPublicPolicyAccessDenied { .. }
            | Self::SseCBlockedAccessDenied { .. }
            | Self::AnonymousApiAccessDenied => "AccessDenied",
            Self::NoSuchUpload { .. } => "NoSuchUpload",
            Self::InvalidPart { .. } => "InvalidPart",
            Self::InvalidPartOrder => "InvalidPartOrder",
            Self::CompleteMultipartMissingPartChecksum { .. }
            | Self::CompleteMultipartChecksumHeaderInvalid { .. }
            | Self::CompleteMultipartExpectedSizeHeaderInvalid { .. } => "InvalidRequest",
            Self::CompleteMultipartEmptyIfMatch
            | Self::DeleteObjectEmptyIfMatch
            | Self::CompleteMultipartTooManyParts => "InvalidArgument",
            Self::UploadPartCopyInvalidRange { .. } => "InvalidArgument",
            Self::UploadPartCopyPreconditionFailed { .. } => "PreconditionFailed",
            Self::EntityTooSmall { .. } => "EntityTooSmall",
            Self::MalformedXML { .. } => "MalformedXML",
            Self::IllegalVersioningConfiguration { .. } => {
                "IllegalVersioningConfigurationException"
            }
            Self::MalformedPOSTRequest { .. } => "MalformedPOSTRequest",
            Self::MalformedChunkedBody { .. } => "InvalidRequest",
            Self::IncompleteBody | Self::PresignedStreamingIncompleteBody { .. } => {
                "IncompleteBody"
            }
            Self::MissingContentLength => "MissingContentLength",
            Self::UnsupportedStreamingToken { .. } => "InvalidArgument",
            Self::PostObjectHeaderAuthUnsupported => "InvalidArgument",
            Self::MalformedTrailerError { .. } => "MalformedTrailerError",
            Self::XAmzContentSHA256Mismatch { .. }
            | Self::PresignedStreamingContentSHA256Mismatch { .. } => "XAmzContentSHA256Mismatch",
            Self::NotImplemented { .. }
            | Self::HeaderNotImplemented { .. }
            | Self::QueryParameterNotImplemented { .. }
            | Self::CompleteMultipartIfNoneMatchNotImplemented => "NotImplemented",
            Self::InternalError { .. } => "InternalError",
            Self::IntegrityError { .. } => "InternalError",
            Self::Store(_) => "InternalError",
            Self::BucketWriteDrain(_) => "InternalError",
            Self::BucketSnapshotLoad(_) => "InternalError",
            Self::BucketListing(_) => "InternalError",
            Self::LifecycleMaintenance(_) => "InternalError",
            Self::LifecycleMutation(_) => "InternalError",
            Self::ObjectRead(_) => "InternalError",
            Self::ObjectMetadataListing(_) => "InternalError",
            Self::ObjectMetadataMutation(_) => "InternalError",
            Self::StreamUpload(_) => "InternalError",
            Self::DirectPut(_) => "InternalError",
            Self::MultipartManagement(_) => "InternalError",
            Self::MultipartCompletion(_) => "InternalError",
            Self::Ec(_) => "InternalError",
        }
    }

    /// Map to HTTP status code.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::BucketNotFound { .. } => 404,
            Self::BucketAlreadyExists => 409,
            Self::BucketAlreadyOwnedByYou => 409,
            Self::InvalidBucketAclWithBlockPublicAccessError => 400,
            Self::BucketNotEmpty => 409,
            Self::ObjectNotFound { .. } => 404,
            Self::VersionNotFound { .. } => 404,
            Self::DeleteMarkerHit { .. } => 404,
            Self::WrongRegion { .. }
            | Self::Auth(
                auth::AuthError::MalformedAuth
                | auth::AuthError::MalformedAuthComponents
                | auth::AuthError::MalformedSignedHeaders
                | auth::AuthError::InvalidHeaderCredentialRegion { .. }
                | auth::AuthError::InvalidHeaderCredentialService { .. }
                | auth::AuthError::UnsupportedAuthType
                | auth::AuthError::InvalidCredentialScope { .. }
                | auth::AuthError::InvalidCredentialScopeRegion { .. }
                | auth::AuthError::InvalidCredentialScopeService { .. }
                | auth::AuthError::InvalidQueryParam { .. }
                | auth::AuthError::InvalidQueryCredentialRegion { .. }
                | auth::AuthError::InvalidQueryCredentialService { .. }
                | auth::AuthError::MissingQueryParam { .. },
            ) => 400,
            Self::Auth(auth::AuthError::MultipleAuthMechanisms { .. }) => 400,
            Self::Auth(
                auth::AuthError::UnexpectedSecurityToken { .. }
                | auth::AuthError::ExpiredSessionToken { .. },
            ) => 400,
            Self::Auth(auth::AuthError::DuplicateAuthorizationHeader) => 501,
            Self::IdentityProvider(_) => 500,
            Self::Auth(auth::AuthError::IdentityProviderFailure(_)) => 500,
            Self::Auth(auth::AuthError::SessionTokenKeyRingUnavailable) => 500,
            Self::Auth(_) => 403,
            Self::InvalidRequest { .. }
            | Self::InvalidRequestHostId { .. }
            | Self::BadRequest { .. }
            | Self::InvalidArgument { .. }
            | Self::InvalidArgumentValue { .. }
            | Self::UploadPartMissingUploadId
            | Self::UploadPartCopyMissingUploadId
            | Self::InvalidUploadPartNumber { .. }
            | Self::InvalidUploadPartCopyNumber { .. }
            | Self::InvalidManagedEncryptionReadHeader { .. }
            | Self::InvalidVersionId { .. }
            | Self::DuplicateChecksumHeader { .. }
            | Self::InvalidRedirectLocation { .. }
            | Self::UnexpectedContent
            | Self::InvalidURI { .. }
            | Self::InvalidBucketName { .. }
            | Self::KeyTooLongError { .. }
            | Self::InvalidBucketNamespace { .. }
            | Self::MissingNamespaceHeader
            | Self::AccountRegionalNamespaceHeaderRequiresSuffix { .. }
            | Self::GlobalNamespaceHeaderRejectedForAccountRegionalBucket { .. }
            | Self::MaxMessageLengthExceeded { .. }
            | Self::BadDigest
            | Self::ContentMd5Mismatch
            | Self::ChecksumDigestMismatch { .. }
            | Self::InvalidDigest
            | Self::InvalidSseCustomerKeyMd5
            | Self::MissingSseCustomerAlgorithm
            | Self::MissingSseCustomerKey
            | Self::MissingSseCustomerKeyMd5
            | Self::CompleteMultipartMissingPartChecksum { .. }
            | Self::CompleteMultipartChecksumHeaderInvalid { .. }
            | Self::CompleteMultipartEmptyIfMatch
            | Self::DeleteObjectEmptyIfMatch
            | Self::CompleteMultipartTooManyParts
            | Self::CompleteMultipartExpectedSizeHeaderInvalid { .. }
            | Self::UploadPartCopyInvalidRange { .. }
            | Self::InvalidEncryptionAlgorithmError { .. } => 400,
            Self::InvalidChunkSize { .. } => 403,
            Self::NoSuchCorsConfiguration { .. } => 404,
            Self::NoSuchTagSet { .. } => 404,
            Self::NoSuchPublicAccessBlockConfiguration { .. }
            | Self::NoSuchBucketPolicy { .. }
            | Self::NoSuchLifecycleConfiguration { .. } => 404,
            Self::MalformedPolicy { .. } | Self::InvalidPolicyDocument { .. } => 400,
            Self::OwnershipControlsNotFound { .. } => 404,
            Self::ObjectLockConfigurationNotFound { .. } => 404,
            Self::ServerSideEncryptionConfigurationNotFound { .. } => 404,
            Self::InvalidBucketState | Self::OperationAborted => 409,
            Self::InvalidTag { .. }
            | Self::MissingRequestBodyError
            | Self::InvalidTaggingHeader { .. }
            | Self::MalformedXMLNoDecl { .. }
            | Self::DeleteObjectsKeyTooLong { .. } => 400,
            Self::AccessControlListNotSupported
            | Self::InvalidBucketAclWithObjectOwnership
            | Self::XAmzContentSHA256Mismatch { .. }
            | Self::PresignedStreamingContentSHA256Mismatch { .. }
            | Self::MalformedXML { .. }
            | Self::IllegalVersioningConfiguration { .. }
            | Self::MalformedPOSTRequest { .. }
            | Self::MalformedChunkedBody { .. }
            | Self::IncompleteBody
            | Self::PresignedStreamingIncompleteBody { .. }
            | Self::MalformedTrailerError { .. } => 400,
            Self::MissingContentLength => 411,
            Self::UnsupportedStreamingToken { .. } => 400,
            Self::PostObjectHeaderAuthUnsupported => 400,
            Self::AccessDenied
            | Self::ObjectLockProtectedAccessDenied
            | Self::PostPolicyAccessDenied { .. }
            | Self::PostPolicyConditionAccessDenied { .. }
            | Self::PostObjectNoAccessKeyPresented
            | Self::BlockPublicPolicyAccessDenied { .. }
            | Self::SseCBlockedAccessDenied { .. }
            | Self::AnonymousApiAccessDenied => 403,
            Self::NoSuchUpload { .. } => 404,
            Self::InvalidPart { .. } | Self::InvalidPartOrder | Self::EntityTooSmall { .. } => 400,
            Self::NotImplemented { .. }
            | Self::HeaderNotImplemented { .. }
            | Self::QueryParameterNotImplemented { .. }
            | Self::CompleteMultipartIfNoneMatchNotImplemented => 501,
            Self::InternalError { .. } => 500,
            Self::ObjectTooLarge { .. }
            | Self::MetadataTooLarge
            | Self::MetadataTooLargeDetailed { .. }
            | Self::RequestHeaderSectionTooLarge => 400,
            Self::MethodNotAllowed
            | Self::PutMultipartUploadMethodNotAllowed
            | Self::HeadDeleteMarkerMethodNotAllowed { .. } => 405,
            Self::InvalidRange { .. } | Self::InvalidPartNumber { .. } => 416,
            Self::PreconditionFailed { .. } | Self::UploadPartCopyPreconditionFailed { .. } => 412,
            Self::ConditionalRequestConflict { .. } => 409,
            Self::NotModified { .. } => 304,
            Self::SlowDown => 503,
            _ => 500,
        }
    }
}

impl From<checksum::InvalidChecksumConfig> for ServerError {
    fn from(e: checksum::InvalidChecksumConfig) -> Self {
        ServerError::InvalidArgument { reason: e.reason }
    }
}

/// Owner-provided semantic fixtures for cross-crate response tests.
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
pub mod test_support {
    use super::ServerError;

    /// Return an internal implementation failure and the private diagnostic
    /// fragments which must not appear in its protocol response.
    pub fn internal_implementation_error_redaction_fixture(
    ) -> (ServerError, &'static [&'static str]) {
        (
            ServerError::Ec(ec::EcError::SmokeTestFailed {
                reason: "matrix inversion failed in /tmp/secret-ec".to_string(),
            }),
            &["matrix inversion failed", "/tmp/secret-ec"],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_error_code_bucket_not_found() {
        let err = ServerError::BucketNotFound { name: "b".into() };
        assert_eq!(err.s3_error_code(), "NoSuchBucket");
    }

    #[test]
    fn s3_error_code_bucket_already_exists() {
        assert_eq!(
            ServerError::BucketAlreadyExists.s3_error_code(),
            "BucketAlreadyExists"
        );
    }

    #[test]
    fn s3_error_code_bucket_not_empty() {
        assert_eq!(
            ServerError::BucketNotEmpty.s3_error_code(),
            "BucketNotEmpty"
        );
    }

    #[test]
    fn s3_error_code_malformed_policy() {
        assert_eq!(
            ServerError::MalformedPolicy {
                reason: "bad".to_string(),
                detail: None,
            }
            .s3_error_code(),
            "MalformedPolicy"
        );
    }

    #[test]
    fn s3_error_code_object_lock_configuration_not_found() {
        assert_eq!(
            ServerError::ObjectLockConfigurationNotFound { bucket: "b".into() }.s3_error_code(),
            "ObjectLockConfigurationNotFoundError"
        );
    }

    #[test]
    fn s3_error_code_invalid_bucket_state() {
        assert_eq!(
            ServerError::InvalidBucketState.s3_error_code(),
            "InvalidBucketState"
        );
    }

    #[test]
    fn s3_error_code_operation_aborted() {
        assert_eq!(
            ServerError::OperationAborted.s3_error_code(),
            "OperationAborted"
        );
    }

    #[test]
    fn s3_error_code_object_not_found() {
        let err = ServerError::ObjectNotFound {
            bucket: "b".into(),
            key: "k".into(),
        };
        assert_eq!(err.s3_error_code(), "NoSuchKey");
    }

    #[test]
    fn storage_diagnostics_use_bounded_categories_and_redact_implementation_errors() {
        let (store_failure, secret_fragments) =
            storage::test_support::store_failure_diagnostic_fixture();
        let store_failure = ServerError::Store(store_failure);
        assert_eq!(store_failure.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(
            store_failure.diagnostic_cause_chain(),
            "server_error>store_error>store_io_failure"
        );
        let rendered = format!("{store_failure:?} {store_failure}");
        for secret in secret_fragments {
            assert!(!rendered.contains(secret));
        }

        let snapshot = ServerError::BucketSnapshotLoad(
            storage::test_support::bucket_snapshot_load_failure_for_kind(
                storage::BucketSnapshotLoadFailureKind::InternalError,
            ),
        );
        assert_eq!(snapshot.diagnostic_cause_label(), "metadata_failure");
        assert_eq!(
            snapshot.diagnostic_cause_chain(),
            "server_error>bucket_snapshot_load>metadata_failure"
        );

        let (bucket_listing_failure, secret_fragments) =
            storage::test_support::bucket_listing_failure_diagnostic_fixture();
        let bucket_listing = ServerError::BucketListing(bucket_listing_failure);
        assert_eq!(bucket_listing.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(
            bucket_listing.diagnostic_cause_chain(),
            "server_error>bucket_listing>store_io_failure"
        );
        let rendered = format!("{bucket_listing:?} {bucket_listing}");
        for secret in secret_fragments {
            assert!(!rendered.contains(secret));
        }

        let (lifecycle_failure, secret_fragments) =
            storage::test_support::lifecycle_maintenance_failure_diagnostic_fixture();
        let lifecycle = ServerError::LifecycleMaintenance(lifecycle_failure);
        assert_eq!(lifecycle.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(
            lifecycle.diagnostic_cause_chain(),
            "server_error>lifecycle_maintenance>store_io_failure"
        );
        let rendered = format!("{lifecycle:?} {lifecycle}");
        for secret in secret_fragments {
            assert!(!rendered.contains(secret));
        }

        let (lifecycle_mutation_failure, secret_fragments) =
            storage::test_support::lifecycle_mutation_failure_diagnostic_fixture();
        let lifecycle_mutation = ServerError::LifecycleMutation(lifecycle_mutation_failure);
        assert_eq!(
            lifecycle_mutation.diagnostic_cause_label(),
            "store_io_failure"
        );
        assert_eq!(
            lifecycle_mutation.diagnostic_cause_chain(),
            "server_error>lifecycle_mutation>store_io_failure"
        );
        let rendered = format!("{lifecycle_mutation:?} {lifecycle_mutation}");
        for secret in secret_fragments {
            assert!(!rendered.contains(secret));
        }

        let object_read = ServerError::ObjectRead(
            storage::test_support::object_read_failure_diagnostic_fixture().0,
        );
        assert_eq!(object_read.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(
            object_read.diagnostic_cause_chain(),
            "server_error>object_read>store_io_failure"
        );

        let (listing_failure, secret_fragments) =
            storage::test_support::object_metadata_listing_failure_diagnostic_fixture();
        let listing = ServerError::ObjectMetadataListing(listing_failure);
        assert_eq!(listing.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(
            listing.diagnostic_cause_chain(),
            "server_error>object_metadata_listing>store_io_failure"
        );
        let rendered = format!("{listing:?} {listing}");
        for secret in secret_fragments {
            assert!(!rendered.contains(secret));
        }

        let (object_mutation_failure, secret_fragments) =
            storage::test_support::object_metadata_mutation_failure_diagnostic_fixture();
        let object_mutation = ServerError::ObjectMetadataMutation(object_mutation_failure);
        assert_eq!(object_mutation.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(
            object_mutation.diagnostic_cause_chain(),
            "server_error>object_metadata_mutation>store_io_failure"
        );
        let rendered = format!("{object_mutation:?} {object_mutation}");
        for secret in secret_fragments {
            assert!(!rendered.contains(secret));
        }

        let (stream_failure, secret_fragments) =
            storage::test_support::stream_upload_failure_diagnostic_fixture();
        let stream_upload = ServerError::StreamUpload(stream_failure);
        assert_eq!(stream_upload.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(
            stream_upload.diagnostic_cause_chain(),
            "server_error>stream_upload>store_io_failure"
        );
        let rendered = format!("{stream_upload:?} {stream_upload}");
        for secret in secret_fragments {
            assert!(!rendered.contains(secret));
        }

        let (direct_put_failure, secret_fragments) =
            storage::test_support::direct_put_failure_diagnostic_fixture();
        let direct_put = ServerError::DirectPut(direct_put_failure);
        assert_eq!(direct_put.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(
            direct_put.diagnostic_cause_chain(),
            "server_error>direct_put>store_io_failure"
        );
        let rendered = format!("{direct_put:?} {direct_put}");
        for secret in secret_fragments {
            assert!(!rendered.contains(secret));
        }

        let (management_failure, secret_fragments) =
            storage::test_support::multipart_management_failure_diagnostic_fixture();
        let management = ServerError::MultipartManagement(management_failure);
        assert_eq!(management.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(
            management.diagnostic_cause_chain(),
            "server_error>multipart_management>store_io_failure"
        );
        let rendered = format!("{management:?} {management}");
        for secret in secret_fragments {
            assert!(!rendered.contains(secret));
        }

        let (completion_failure, secret_fragments) =
            storage::test_support::multipart_completion_failure_diagnostic_fixture();
        let completion = ServerError::MultipartCompletion(completion_failure);
        assert_eq!(completion.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(
            completion.diagnostic_cause_chain(),
            "server_error>multipart_completion>store_io_failure"
        );
        let rendered = format!("{completion:?} {completion}");
        for secret in secret_fragments {
            assert!(!rendered.contains(secret));
        }
    }

    #[test]
    fn s3_error_code_auth_missing() {
        let err = ServerError::Auth(auth::AuthError::MissingAuth);
        assert_eq!(err.s3_error_code(), "AccessDenied");
    }

    #[test]
    fn s3_error_code_auth_unknown_key() {
        let err = ServerError::Auth(auth::AuthError::UnknownAccessKey {
            access_key_id: "unknown".to_string(),
        });
        assert_eq!(err.s3_error_code(), "InvalidAccessKeyId");
    }

    #[test]
    fn identity_provider_failure_is_internal_error() {
        for (provider_error, expected_label) in [
            (
                auth::IdentityProviderError::Unavailable,
                "identity_provider_unavailable",
            ),
            (
                auth::IdentityProviderError::InvalidRecord,
                "identity_provider_invalid_record",
            ),
        ] {
            for err in [
                ServerError::Auth(auth::AuthError::IdentityProviderFailure(provider_error)),
                ServerError::IdentityProvider(provider_error),
            ] {
                assert_eq!(err.s3_error_code(), "InternalError");
                assert_eq!(err.http_status(), 500);
                assert_eq!(err.diagnostic_cause_label(), expected_label);
                assert_eq!(
                    err.diagnostic_cause_chain(),
                    format!("server_error>{expected_label}")
                );
            }
        }
    }

    #[test]
    fn s3_error_code_auth_duplicate_authorization() {
        let err = ServerError::Auth(auth::AuthError::DuplicateAuthorizationHeader);
        assert_eq!(err.s3_error_code(), "NotImplemented");
    }

    #[test]
    fn auth_multiple_mechanisms_is_invalid_argument_400() {
        let err = ServerError::Auth(auth::AuthError::MultipleAuthMechanisms {
            authorization: "redacted".to_string(),
        });
        assert_eq!(err.s3_error_code(), "InvalidArgument");
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn auth_header_region_mismatch_is_authorization_header_malformed_400() {
        let err = ServerError::Auth(auth::AuthError::InvalidHeaderCredentialRegion {
            provided_region: "us-west-2".to_string(),
            expected_region: "us-east-1".to_string(),
        });
        assert_eq!(err.s3_error_code(), "AuthorizationHeaderMalformed");
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn auth_header_service_mismatch_is_authorization_header_malformed_400() {
        let err = ServerError::Auth(auth::AuthError::InvalidHeaderCredentialService {
            provided_service: "sts".to_string(),
            expected_service: "s3".to_string(),
        });
        assert_eq!(err.s3_error_code(), "AuthorizationHeaderMalformed");
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn session_token_key_ring_failure_is_distinct_internal_error() {
        let err = ServerError::Auth(auth::AuthError::SessionTokenKeyRingUnavailable);
        assert_eq!(err.s3_error_code(), "InternalError");
        assert_eq!(err.http_status(), 500);
        assert_eq!(
            err.diagnostic_cause_label(),
            "session_token_key_ring_unavailable"
        );
        assert_eq!(
            err.diagnostic_cause_chain(),
            "server_error>session_token_key_ring_unavailable"
        );
    }

    #[test]
    fn s3_error_code_auth_signature_mismatch() {
        let err = ServerError::Auth(auth::AuthError::SignatureMismatch { diagnostics: None });
        assert_eq!(err.s3_error_code(), "SignatureDoesNotMatch");
    }

    #[test]
    fn s3_error_code_auth_expired() {
        let err = ServerError::Auth(auth::AuthError::RequestExpired);
        assert_eq!(err.s3_error_code(), "RequestTimeTooSkewed");
    }

    #[test]
    fn s3_error_code_auth_presigned_expired() {
        let err = ServerError::Auth(auth::AuthError::PresignedRequestExpired {
            x_amz_expires: 60,
            expires_epoch: 1_705_321_845,
            server_time_epoch: 1_705_321_900,
        });
        assert_eq!(err.s3_error_code(), "AccessDenied");
    }

    #[test]
    fn s3_error_code_auth_wildcard() {
        let err = ServerError::Auth(auth::AuthError::MissingSignedHeader {
            header: "host".to_string(),
        });
        assert_eq!(err.s3_error_code(), "AccessDenied");
    }

    #[test]
    fn s3_error_code_auth_malformed() {
        let err = ServerError::Auth(auth::AuthError::MalformedAuth);
        assert_eq!(err.s3_error_code(), "AuthorizationHeaderMalformed");
    }

    #[test]
    fn s3_error_code_wrong_region() {
        let err = ServerError::WrongRegion {
            provided_region: "us-east-1".to_string(),
            expected_region: "us-west-2".to_string(),
            bucket_region_header: true,
        };
        assert_eq!(err.s3_error_code(), "AuthorizationHeaderMalformed");
    }

    #[test]
    fn s3_error_code_auth_unexpected_security_token() {
        let err = ServerError::Auth(auth::AuthError::UnexpectedSecurityToken {
            token: "unexpected".into(),
        });
        assert_eq!(err.s3_error_code(), "InvalidToken");
    }

    #[test]
    fn display_redacts_unexpected_security_token() {
        let err = ServerError::Auth(auth::AuthError::UnexpectedSecurityToken {
            token: "tok\nen".to_string(),
        });
        let display = err.to_string();
        assert_eq!(display, "auth error: unexpected security token");
        assert!(!display.contains("tok\nen"));
    }

    #[test]
    fn s3_error_code_auth_expired_token() {
        let err = ServerError::Auth(auth::AuthError::ExpiredToken);
        assert_eq!(err.s3_error_code(), "ExpiredToken");
    }

    #[test]
    fn expired_session_token_is_expired_token_400() {
        let err = ServerError::Auth(auth::AuthError::ExpiredSessionToken {
            tokens: vec!["expired".to_string()],
        });
        assert_eq!(err.s3_error_code(), "ExpiredToken");
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn http_status_auth_malformed_400() {
        assert_eq!(
            ServerError::Auth(auth::AuthError::MalformedAuth).http_status(),
            400
        );
    }

    #[test]
    fn http_status_wrong_region_400() {
        assert_eq!(
            ServerError::WrongRegion {
                provided_region: "us-east-1".to_string(),
                expected_region: "us-west-2".to_string(),
                bucket_region_header: true,
            }
            .http_status(),
            400
        );
    }

    #[test]
    fn http_status_auth_duplicate_authorization_501() {
        assert_eq!(
            ServerError::Auth(auth::AuthError::DuplicateAuthorizationHeader).http_status(),
            501
        );
    }

    #[test]
    fn s3_error_code_unsupported_auth_type() {
        let err = ServerError::Auth(auth::AuthError::UnsupportedAuthType);
        assert_eq!(err.s3_error_code(), "InvalidArgument");
    }

    #[test]
    fn http_status_unsupported_auth_type_400() {
        assert_eq!(
            ServerError::Auth(auth::AuthError::UnsupportedAuthType).http_status(),
            400
        );
    }

    #[test]
    fn http_status_unexpected_security_token_400() {
        assert_eq!(
            ServerError::Auth(auth::AuthError::UnexpectedSecurityToken {
                token: "unexpected".into(),
            })
            .http_status(),
            400
        );
    }

    #[test]
    fn s3_error_code_invalid_request() {
        let err = ServerError::InvalidRequest {
            reason: "bad".into(),
        };
        assert_eq!(err.s3_error_code(), "InvalidRequest");
    }

    #[test]
    fn s3_error_code_bad_request() {
        let err = ServerError::BadRequest {
            reason: "bad".into(),
        };
        assert_eq!(err.s3_error_code(), "BadRequest");
    }

    #[test]
    fn s3_error_code_invalid_redirect_location() {
        let err = ServerError::InvalidRedirectLocation {
            reason: "bad redirect".into(),
        };
        assert_eq!(err.s3_error_code(), "InvalidRedirectLocation");
    }

    #[test]
    fn s3_error_code_unexpected_content() {
        assert_eq!(
            ServerError::UnexpectedContent.s3_error_code(),
            "UnexpectedContent"
        );
    }

    #[test]
    fn s3_error_code_metadata_blob_error() {
        let err = ServerError::MetadataBlobError {
            reason: "corrupt".into(),
        };
        assert_eq!(err.s3_error_code(), "InternalError");
    }

    #[test]
    fn s3_error_code_object_too_large() {
        let err = ServerError::ObjectTooLarge {
            size: 1000,
            max: 500,
        };
        assert_eq!(err.s3_error_code(), "EntityTooLarge");
    }

    #[test]
    fn s3_error_code_metadata_too_large() {
        assert_eq!(
            ServerError::MetadataTooLarge.s3_error_code(),
            "MetadataTooLarge"
        );
        assert_eq!(
            ServerError::MetadataTooLargeDetailed {
                size: 2049,
                max_size_allowed: 2048,
            }
            .s3_error_code(),
            "MetadataTooLarge"
        );
    }

    #[test]
    fn s3_error_code_request_header_section_too_large() {
        assert_eq!(
            ServerError::RequestHeaderSectionTooLarge.s3_error_code(),
            "RequestHeaderSectionTooLarge"
        );
    }

    #[test]
    fn s3_error_code_method_not_allowed() {
        assert_eq!(
            ServerError::MethodNotAllowed.s3_error_code(),
            "MethodNotAllowed"
        );
    }

    #[test]
    fn s3_error_code_invalid_range() {
        let err = ServerError::InvalidRange {
            range_requested: "bytes=200-300".to_string(),
            total_size: 100,
        };
        assert_eq!(err.s3_error_code(), "InvalidRange");
    }

    #[test]
    fn s3_error_code_invalid_part_number() {
        let err = ServerError::InvalidPartNumber {
            part_number: 5,
            parts_count: 4,
        };
        assert_eq!(err.s3_error_code(), "InvalidPartNumber");
    }

    #[test]
    fn http_status_416() {
        assert_eq!(
            ServerError::InvalidRange {
                range_requested: "bytes=200-300".to_string(),
                total_size: 100,
            }
            .http_status(),
            416
        );
        assert_eq!(
            ServerError::InvalidPartNumber {
                part_number: 5,
                parts_count: 4,
            }
            .http_status(),
            416
        );
    }

    #[test]
    fn s3_error_code_precondition_failed() {
        assert_eq!(
            ServerError::PreconditionFailed {
                condition: "If-Match"
            }
            .s3_error_code(),
            "PreconditionFailed"
        );
    }

    #[test]
    fn http_status_412() {
        assert_eq!(
            ServerError::PreconditionFailed {
                condition: "If-Match"
            }
            .http_status(),
            412
        );
    }

    #[test]
    fn s3_error_code_not_modified() {
        let err = ServerError::NotModified {
            etag: "\"abc\"".into(),
            last_modified: 0,
        };
        assert_eq!(err.s3_error_code(), "NotModified");
    }

    #[test]
    fn http_status_304() {
        let err = ServerError::NotModified {
            etag: "\"abc\"".into(),
            last_modified: 0,
        };
        assert_eq!(err.http_status(), 304);
    }

    #[test]
    fn s3_error_code_store() {
        let err = ServerError::Store(
            storage::test_support::store_failure_for_operation_failure_class(
                storage::StoreOperationFailureClass::Other,
            ),
        );
        assert_eq!(err.s3_error_code(), "InternalError");
    }

    #[test]
    fn s3_error_code_object_read() {
        let err = ServerError::ObjectRead(storage::test_support::object_read_failure_for_kind(
            storage::ObjectReadFailureKind::InternalError,
        ));
        assert_eq!(err.s3_error_code(), "InternalError");
        assert_eq!(err.http_status(), 500);
    }

    #[test]
    fn s3_error_code_bucket_listing() {
        let err =
            ServerError::BucketListing(storage::test_support::bucket_listing_failure_for_kind(
                storage::BucketListingFailureKind::InternalError,
            ));
        assert_eq!(err.s3_error_code(), "InternalError");
        assert_eq!(err.http_status(), 500);
    }

    #[test]
    fn s3_error_code_lifecycle_maintenance() {
        let err = ServerError::LifecycleMaintenance(
            storage::test_support::lifecycle_maintenance_failure_for_kind(
                storage::LifecycleMaintenanceFailureKind::InternalError,
            ),
        );
        assert_eq!(err.s3_error_code(), "InternalError");
        assert_eq!(err.http_status(), 500);
    }

    #[test]
    fn s3_error_code_lifecycle_mutation() {
        let err = ServerError::LifecycleMutation(
            storage::test_support::lifecycle_mutation_failure_for_kind(
                storage::LifecycleMutationFailureKind::InternalError,
            ),
        );
        assert_eq!(err.s3_error_code(), "InternalError");
        assert_eq!(err.http_status(), 500);
    }

    #[test]
    fn s3_error_code_object_metadata_listing() {
        let err = ServerError::ObjectMetadataListing(
            storage::test_support::object_metadata_listing_failure_for_kind(
                storage::ObjectMetadataListingFailureKind::InternalError,
            ),
        );
        assert_eq!(err.s3_error_code(), "InternalError");
        assert_eq!(err.http_status(), 500);
    }

    #[test]
    fn s3_error_code_object_metadata_mutation() {
        let err = ServerError::ObjectMetadataMutation(
            storage::test_support::object_metadata_mutation_failure_for_kind(
                storage::ObjectMetadataMutationFailureKind::InternalError,
            ),
        );
        assert_eq!(err.s3_error_code(), "InternalError");
        assert_eq!(err.http_status(), 500);
    }

    #[test]
    fn s3_error_code_stream_upload() {
        let err = ServerError::StreamUpload(storage::test_support::stream_upload_failure_for_kind(
            storage::StreamUploadFailureKind::InternalError,
        ));
        assert_eq!(err.s3_error_code(), "InternalError");
        assert_eq!(err.http_status(), 500);
    }

    #[test]
    fn s3_error_code_direct_put() {
        let err = ServerError::DirectPut(storage::test_support::direct_put_failure_for_kind(
            storage::DirectPutFailureKind::InternalError,
        ));
        assert_eq!(err.s3_error_code(), "InternalError");
        assert_eq!(err.http_status(), 500);
    }

    #[test]
    fn s3_error_code_multipart_management() {
        let err = ServerError::MultipartManagement(
            storage::test_support::multipart_management_failure_for_kind(
                storage::MultipartManagementFailureKind::InternalError,
            ),
        );
        assert_eq!(err.s3_error_code(), "InternalError");
        assert_eq!(err.http_status(), 500);
    }

    #[test]
    fn s3_error_code_multipart_completion() {
        let err = ServerError::MultipartCompletion(
            storage::test_support::multipart_completion_failure_for_kind(
                storage::MultipartCompletionFailureKind::InternalError,
            ),
        );
        assert_eq!(err.s3_error_code(), "InternalError");
        assert_eq!(err.http_status(), 500);
    }

    #[test]
    fn s3_error_code_ec() {
        let err = ServerError::Ec(ec::EcError::InvalidConfig { reason: "bad" });
        assert_eq!(err.s3_error_code(), "InternalError");
    }

    #[test]
    fn http_status_404() {
        assert_eq!(
            ServerError::BucketNotFound { name: "b".into() }.http_status(),
            404
        );
        assert_eq!(
            ServerError::ObjectNotFound {
                bucket: "b".into(),
                key: "k".into()
            }
            .http_status(),
            404
        );
        assert_eq!(
            ServerError::NoSuchPublicAccessBlockConfiguration { bucket: "b".into() }.http_status(),
            404
        );
        assert_eq!(
            ServerError::NoSuchBucketPolicy { bucket: "b".into() }.http_status(),
            404
        );
        assert_eq!(
            ServerError::ObjectLockConfigurationNotFound { bucket: "b".into() }.http_status(),
            404
        );
    }

    #[test]
    fn http_status_409() {
        assert_eq!(ServerError::BucketAlreadyExists.http_status(), 409);
        assert_eq!(ServerError::BucketAlreadyOwnedByYou.http_status(), 409);
        assert_eq!(ServerError::BucketNotEmpty.http_status(), 409);
        assert_eq!(ServerError::InvalidBucketState.http_status(), 409);
        assert_eq!(ServerError::OperationAborted.http_status(), 409);
    }

    #[test]
    fn http_status_invalid_bucket_acl_with_block_public_access() {
        assert_eq!(
            ServerError::InvalidBucketAclWithBlockPublicAccessError.http_status(),
            400
        );
    }

    #[test]
    fn http_status_403() {
        assert_eq!(
            ServerError::Auth(auth::AuthError::MissingAuth).http_status(),
            403
        );
    }

    #[test]
    fn http_status_400() {
        assert_eq!(
            ServerError::InvalidRequest { reason: "x".into() }.http_status(),
            400
        );
        assert_eq!(
            ServerError::BadRequest { reason: "x".into() }.http_status(),
            400
        );
        assert_eq!(
            ServerError::InvalidRedirectLocation { reason: "x".into() }.http_status(),
            400
        );
        assert_eq!(ServerError::UnexpectedContent.http_status(), 400);
        assert_eq!(
            ServerError::ObjectTooLarge { size: 1, max: 0 }.http_status(),
            400
        );
        assert_eq!(ServerError::MetadataTooLarge.http_status(), 400);
        assert_eq!(
            ServerError::MetadataTooLargeDetailed {
                size: 2049,
                max_size_allowed: 2048,
            }
            .http_status(),
            400
        );
        assert_eq!(ServerError::RequestHeaderSectionTooLarge.http_status(), 400);
        assert_eq!(
            ServerError::MalformedPolicy {
                reason: "bad".into(),
                detail: None,
            }
            .http_status(),
            400
        );
    }

    #[test]
    fn http_status_post_policy_access_denied_403() {
        assert_eq!(
            ServerError::PostPolicyAccessDenied {
                reason: "field denied".into(),
            }
            .http_status(),
            403
        );
    }

    #[test]
    fn http_status_405() {
        assert_eq!(ServerError::MethodNotAllowed.http_status(), 405);
    }

    #[test]
    fn http_status_500_wildcard() {
        assert_eq!(
            ServerError::Store(
                storage::test_support::store_failure_for_operation_failure_class(
                    storage::StoreOperationFailureClass::Other,
                ),
            )
            .http_status(),
            500
        );
        assert_eq!(
            ServerError::BucketSnapshotLoad(
                storage::test_support::bucket_snapshot_load_failure_for_kind(
                    storage::BucketSnapshotLoadFailureKind::InternalError,
                ),
            )
            .http_status(),
            500
        );
        assert_eq!(
            ServerError::ObjectRead(storage::test_support::object_read_failure_for_kind(
                storage::ObjectReadFailureKind::InternalError,
            ))
            .http_status(),
            500
        );
        assert_eq!(
            ServerError::ObjectMetadataMutation(
                storage::test_support::object_metadata_mutation_failure_for_kind(
                    storage::ObjectMetadataMutationFailureKind::InternalError,
                ),
            )
            .http_status(),
            500
        );
        assert_eq!(
            ServerError::DirectPut(storage::test_support::direct_put_failure_for_kind(
                storage::DirectPutFailureKind::InternalError,
            ))
            .http_status(),
            500
        );
        assert_eq!(
            ServerError::MetadataBlobError { reason: "x".into() }.http_status(),
            500
        );
        assert_eq!(
            ServerError::Ec(ec::EcError::InvalidConfig { reason: "x" }).http_status(),
            500
        );
    }

    #[test]
    fn from_auth_error() {
        let err: ServerError = auth::AuthError::MissingAuth.into();
        assert!(matches!(err, ServerError::Auth(_)));
    }

    #[test]
    fn from_ec_error() {
        let err: ServerError = ec::EcError::InvalidConfig { reason: "x" }.into();
        assert!(matches!(err, ServerError::Ec(_)));
    }

    #[test]
    fn s3_error_code_no_such_upload() {
        let err = ServerError::NoSuchUpload {
            upload_id: "abc".into(),
        };
        assert_eq!(err.s3_error_code(), "NoSuchUpload");
        assert_eq!(err.http_status(), 404);
    }

    #[test]
    fn s3_error_code_slow_down() {
        assert_eq!(ServerError::SlowDown.s3_error_code(), "SlowDown");
    }

    #[test]
    fn http_status_503() {
        assert_eq!(ServerError::SlowDown.http_status(), 503);
    }

    #[test]
    fn s3_error_code_invalid_part() {
        let err = ServerError::InvalidPart { part_number: 3 };
        assert_eq!(err.s3_error_code(), "InvalidPart");
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn s3_error_code_invalid_part_order() {
        assert_eq!(
            ServerError::InvalidPartOrder.s3_error_code(),
            "InvalidPartOrder"
        );
        assert_eq!(ServerError::InvalidPartOrder.http_status(), 400);
    }

    #[test]
    fn s3_error_code_entity_too_small() {
        let err = ServerError::EntityTooSmall {
            part_number: 1,
            size: 100,
            min: 5242880,
        };
        assert_eq!(err.s3_error_code(), "EntityTooSmall");
        assert_eq!(err.http_status(), 400);
    }

    #[test]
    fn display_messages() {
        let err = ServerError::BucketNotFound {
            name: "test".into(),
        };
        assert!(err.to_string().contains("test"));

        let err = ServerError::ObjectNotFound {
            bucket: "b".into(),
            key: "k".into(),
        };
        assert!(err.to_string().contains("b/k"));

        let err = ServerError::ObjectTooLarge { size: 100, max: 50 };
        assert!(err.to_string().contains("100"));
        assert!(err.to_string().contains("50"));
    }
}
