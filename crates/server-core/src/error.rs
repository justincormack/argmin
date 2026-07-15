/// Unified error type for the server crate.
use s3_types::VersionId;
use storage::error::{MetadataError, StoreError};

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
    Store(StoreError),

    #[error("metadata error: {0}")]
    Metadata(MetadataError),

    #[error("EC error: {0}")]
    Ec(#[from] ec::EcError),

    #[error("auth error: {0}")]
    Auth(#[from] auth::AuthError),

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

    #[error("not modified")]
    NotModified { etag: String, last_modified: u64 },

    #[error("Please reduce your request rate.")]
    SlowDown,

    #[error("bad digest")]
    BadDigest,

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

impl From<StoreError> for ServerError {
    fn from(error: StoreError) -> Self {
        if store_error_is_resource_exhausted(&error) {
            Self::SlowDown
        } else {
            Self::Store(error)
        }
    }
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
            Self::Store(error) => store_error_diagnostic_cause_label(error),
            Self::Metadata(error) => metadata_error_diagnostic_cause_label(error),
            Self::Ec(_) => "ec_error",
            Self::MetadataBlobError { .. } => "metadata_blob_error",
            Self::InternalError { .. } => "internal_error",
            Self::IntegrityError { .. } => "integrity_error",
            Self::SlowDown => "slow_down",
            Self::OperationAborted => "operation_aborted",
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
                store_error_diagnostic_cause_chain(error)
            ),
            Self::Metadata(error) => format!(
                "server_error>metadata_error>{}",
                metadata_error_diagnostic_cause_chain(error)
            ),
            _ => format!("server_error>{}", self.diagnostic_cause_label()),
        }
    }

    /// Server-side diagnostic detail for storage RPC failures.
    ///
    /// Unlike the stable cause labels, this may include the storage RPC
    /// operation and message. It is intended for local/server logs only, not
    /// response bodies or customer-visible errors.
    pub fn server_storage_rpc_detail(&self) -> Option<String> {
        match self {
            Self::Store(error) => store_error_storage_rpc_detail(error),
            _ => None,
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
                | auth::AuthError::InvalidHeaderCredentialRegion { .. },
            )
            | Self::WrongRegion { .. } => "AuthorizationHeaderMalformed",
            Self::Auth(
                auth::AuthError::UnsupportedAuthType
                | auth::AuthError::InvalidCredentialScope { .. }
                | auth::AuthError::InvalidCredentialScopeRegion { .. }
                | auth::AuthError::InvalidCredentialScopeService { .. },
            ) => "InvalidArgument",
            Self::Auth(auth::AuthError::UnknownAccessKey) => "InvalidAccessKeyId",
            Self::Auth(auth::AuthError::DuplicateAuthorizationHeader) => "NotImplemented",
            Self::Auth(auth::AuthError::MultipleAuthMechanisms { .. }) => "InvalidArgument",
            Self::Auth(auth::AuthError::SignatureMismatch { .. }) => "SignatureDoesNotMatch",
            Self::Auth(auth::AuthError::RequestExpired) => "RequestTimeTooSkewed",
            Self::Auth(auth::AuthError::PresignedRequestExpired { .. }) => "AccessDenied",
            Self::Auth(auth::AuthError::ExpiredToken) => "ExpiredToken",
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
            Self::NotModified { .. } => "NotModified",
            Self::InvalidRequest { .. } | Self::InvalidRequestHostId { .. } => "InvalidRequest",
            Self::BadRequest { .. } => "BadRequest",
            Self::InvalidArgument { .. }
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
            Self::BadDigest | Self::ChecksumDigestMismatch { .. } => "BadDigest",
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
            | Self::BlockPublicPolicyAccessDenied { .. }
            | Self::SseCBlockedAccessDenied { .. }
            | Self::AnonymousApiAccessDenied => "AccessDenied",
            Self::NoSuchUpload { .. } => "NoSuchUpload",
            Self::InvalidPart { .. } => "InvalidPart",
            Self::InvalidPartOrder => "InvalidPartOrder",
            Self::CompleteMultipartMissingPartChecksum { .. }
            | Self::CompleteMultipartChecksumHeaderInvalid { .. }
            | Self::CompleteMultipartExpectedSizeHeaderInvalid { .. } => "InvalidRequest",
            Self::CompleteMultipartEmptyIfMatch | Self::CompleteMultipartTooManyParts => {
                "InvalidArgument"
            }
            Self::UploadPartCopyInvalidRange { .. } => "InvalidArgument",
            Self::UploadPartCopyPreconditionFailed { .. } => "PreconditionFailed",
            Self::EntityTooSmall { .. } => "EntityTooSmall",
            Self::MalformedXML { .. } => "MalformedXML",
            Self::IllegalVersioningConfiguration { .. } => {
                "IllegalVersioningConfigurationException"
            }
            Self::MalformedPOSTRequest { .. } => "MalformedPOSTRequest",
            Self::MalformedChunkedBody { .. } => "InvalidRequest",
            Self::IncompleteBody => "IncompleteBody",
            Self::MissingContentLength => "MissingContentLength",
            Self::UnsupportedStreamingToken { .. } => "InvalidArgument",
            Self::PostObjectHeaderAuthUnsupported => "InvalidArgument",
            Self::MalformedTrailerError { .. } => "MalformedTrailerError",
            Self::XAmzContentSHA256Mismatch { .. } => "XAmzContentSHA256Mismatch",
            Self::NotImplemented { .. }
            | Self::HeaderNotImplemented { .. }
            | Self::QueryParameterNotImplemented { .. }
            | Self::CompleteMultipartIfNoneMatchNotImplemented => "NotImplemented",
            Self::InternalError { .. } => "InternalError",
            Self::IntegrityError { .. } => "InternalError",
            Self::Store(_) => "InternalError",
            Self::Metadata(_) => "InternalError",
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
            Self::Auth(auth::AuthError::UnexpectedSecurityToken { .. }) => 400,
            Self::Auth(auth::AuthError::DuplicateAuthorizationHeader) => 501,
            Self::Auth(_) => 403,
            Self::InvalidRequest { .. }
            | Self::InvalidRequestHostId { .. }
            | Self::BadRequest { .. }
            | Self::InvalidArgument { .. }
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
            | Self::ChecksumDigestMismatch { .. }
            | Self::InvalidDigest
            | Self::InvalidSseCustomerKeyMd5
            | Self::MissingSseCustomerAlgorithm
            | Self::MissingSseCustomerKey
            | Self::MissingSseCustomerKeyMd5
            | Self::CompleteMultipartMissingPartChecksum { .. }
            | Self::CompleteMultipartChecksumHeaderInvalid { .. }
            | Self::CompleteMultipartEmptyIfMatch
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
            | Self::InvalidTaggingHeader { .. }
            | Self::MalformedXMLNoDecl { .. }
            | Self::DeleteObjectsKeyTooLong { .. } => 400,
            Self::AccessControlListNotSupported
            | Self::InvalidBucketAclWithObjectOwnership
            | Self::XAmzContentSHA256Mismatch { .. }
            | Self::MalformedXML { .. }
            | Self::IllegalVersioningConfiguration { .. }
            | Self::MalformedPOSTRequest { .. }
            | Self::MalformedChunkedBody { .. }
            | Self::IncompleteBody
            | Self::MalformedTrailerError { .. } => 400,
            Self::MissingContentLength => 411,
            Self::UnsupportedStreamingToken { .. } => 400,
            Self::PostObjectHeaderAuthUnsupported => 400,
            Self::AccessDenied
            | Self::ObjectLockProtectedAccessDenied
            | Self::PostPolicyAccessDenied { .. }
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
            Self::NotModified { .. } => 304,
            Self::SlowDown => 503,
            _ => 500,
        }
    }
}

fn store_error_is_resource_exhausted(error: &StoreError) -> bool {
    match error {
        StoreError::StorageRpcResourceExhausted { .. }
        | StoreError::ClusterMapHistoryReferenceLimitExceeded { .. } => true,
        StoreError::ShardStore { source, .. } => store_error_is_resource_exhausted(source),
        _ => false,
    }
}

fn store_error_diagnostic_cause_label(error: &StoreError) -> &'static str {
    match error {
        StoreError::NotFound => "store_not_found",
        StoreError::IntegrityError { .. } => "store_integrity_error",
        StoreError::ShardAckMismatch { .. } => "shard_ack_mismatch",
        StoreError::PayloadShardSetMismatch { .. } => "payload_shard_set_mismatch",
        StoreError::ClusterMapHistoryReferenceLimitExceeded { .. } => {
            "cluster_map_history_reference_limit_exceeded"
        }
        StoreError::PgNotFound { .. } => "pg_not_found",
        StoreError::InvalidPgTopology { .. } => "invalid_pg_topology",
        StoreError::ClusterPgNotFound { .. } => "cluster_pg_not_found",
        StoreError::ShardPgNotFound { .. } => "shard_pg_not_found",
        StoreError::PgNotActive { .. } => "pg_not_active",
        StoreError::ShardPgNotActive { .. } => "shard_pg_not_active",
        StoreError::MetadataCommandLogConflict { .. } => "metadata_command_log_conflict",
        StoreError::MetadataCommandLogGap { .. } => "metadata_command_log_gap",
        StoreError::MetadataCommandPendingConflict { .. } => "metadata_command_pending_conflict",
        StoreError::MetadataCommandContention { .. } => "metadata_command_contention",
        StoreError::MetadataTransferEmpty { .. } => "metadata_transfer_empty",
        StoreError::MetadataTransferUnsupportedProof { .. } => {
            "metadata_transfer_unsupported_proof"
        }
        StoreError::MetadataCheckpointInvalid { .. } => "metadata_checkpoint_invalid",
        StoreError::MetadataCommandPendingOnNonPrimary { .. } => {
            "metadata_command_pending_on_non_primary"
        }
        StoreError::MetadataCommandLogChecksumMismatch { .. }
        | StoreError::MetadataCommandLogHashMismatch { .. }
        | StoreError::MetadataCommandReplicaStateDiverged { .. }
        | StoreError::MetadataStateDigestMismatch { .. } => "metadata_command_replica_diverged",
        StoreError::StorageRpcResourceExhausted { .. } => "storage_rpc_resource_exhausted",
        StoreError::StorageRpcShardDeleteInProgress { .. } => {
            "storage_rpc_shard_delete_in_progress"
        }
        StoreError::StorageRpc { .. } => "storage_rpc_error",
        StoreError::ShardStore { source, .. } => match store_error_diagnostic_cause_label(source) {
            "storage_rpc_resource_exhausted" => "shard_store_storage_rpc_resource_exhausted",
            "storage_rpc_shard_delete_in_progress" => {
                "shard_store_storage_rpc_shard_delete_in_progress"
            }
            "storage_rpc_error" => "shard_store_storage_rpc_error",
            "stale_payload_operation" => "shard_store_stale_payload_operation",
            _ => "shard_store_error",
        },
        StoreError::RouteMapExpired { .. } => "route_map_expired",
        StoreError::StalePayloadOperation { .. } => "stale_payload_operation",
        StoreError::StaleMetadataPrimaryBridge { .. } => "stale_metadata_primary_bridge",
        StoreError::StaleMetadataOperation { .. } => "stale_metadata_operation",
        StoreError::StaleMetadataRoute { .. } => "stale_metadata_route",
        StoreError::StaleMetadataCommand { .. } => "stale_metadata_command",
        StoreError::MetadataCommandWrongPg { .. } => "metadata_command_wrong_pg",
        StoreError::MetadataCommandFromNonPrimary { .. } => "metadata_command_from_non_primary",
        StoreError::MetadataCommandReplicaStateMissing { .. } => {
            "metadata_command_replica_state_missing"
        }
        StoreError::StaleShardOperation { .. } => "stale_shard_operation",
        StoreError::StaleShardLocation { .. } => "stale_shard_location",
        StoreError::NodeNotFound { .. } => "storage_node_not_found",
        StoreError::NodeNotInActingSet { .. } => "storage_node_not_in_acting_set",
        StoreError::ShardIndexMismatch { .. } => "shard_index_mismatch",
        StoreError::ShardScavengerObservationWrongPg { .. } => {
            "shard_scavenger_observation_wrong_pg"
        }
        StoreError::ShardScavengerObservationShardIndexMismatch { .. } => {
            "shard_scavenger_observation_shard_index_mismatch"
        }
        StoreError::ShardScavengerObservationInconsistentReason { .. } => {
            "shard_scavenger_observation_inconsistent_reason"
        }
        StoreError::InvalidKeyLength { .. } => "invalid_shard_key_length",
        StoreError::InvalidShardKeyHex => "invalid_shard_key_hex",
        StoreError::ShardScavengerScanIncomplete { .. } => "shard_scavenger_scan_incomplete",
        StoreError::Io { .. } => "store_io_error",
        StoreError::Db { .. } => "store_db_error",
        StoreError::ErasureCoding { .. } => "store_erasure_coding_error",
    }
}

fn store_error_diagnostic_cause_chain(error: &StoreError) -> String {
    match error {
        StoreError::ShardStore { source, .. } => {
            format!("shard_store>{}", store_error_diagnostic_cause_chain(source))
        }
        _ => store_error_diagnostic_cause_label(error).to_string(),
    }
}

fn store_error_storage_rpc_detail(error: &StoreError) -> Option<String> {
    match error {
        StoreError::ShardStore {
            node_id,
            pg_id,
            cluster_epoch,
            source,
        } => store_error_storage_rpc_detail(source).map(|detail| {
            format!(
                "shard_store node_id={node_id} pg_id={pg_id} cluster_epoch={} source=({detail})",
                cluster_epoch.get()
            )
        }),
        StoreError::StorageRpc {
            node_id,
            operation,
            code,
            message,
        } => Some(format!(
            "storage_rpc node_id={node_id} operation={operation:?} code={code:?} message={message:?}"
        )),
        StoreError::StorageRpcResourceExhausted {
            node_id,
            operation,
            message,
        } => Some(format!(
            "storage_rpc_resource_exhausted node_id={node_id} operation={operation:?} message={message:?}"
        )),
        StoreError::StorageRpcShardDeleteInProgress {
            node_id,
            operation,
            message,
        } => Some(format!(
            "storage_rpc_shard_delete_in_progress node_id={node_id} operation={operation:?} message={message:?}"
        )),
        _ => None,
    }
}

fn metadata_error_diagnostic_cause_label(error: &MetadataError) -> &'static str {
    match error {
        MetadataError::BucketWriteDraining => "bucket_write_draining",
        MetadataError::BucketWriteReservationConflict { .. } => "bucket_write_reservation_conflict",
        MetadataError::BucketWriteDrainConflict { .. } => "bucket_write_drain_conflict",
        MetadataError::ReclaimClaimConflict { .. } => "reclaim_claim_conflict",
        MetadataError::ObjectGenerationReservationConflict { .. } => {
            "object_generation_reservation_conflict"
        }
        MetadataError::ObjectVersionReservationConflict { .. } => {
            "object_version_reservation_conflict"
        }
        MetadataError::StaleBucketMetadataCommand { .. } => "stale_bucket_metadata_command",
        MetadataError::StaleObjectWriteCommand { .. } => "stale_object_write_command",
        MetadataError::Db { .. } => "metadata_db_error",
        _ => "metadata_error",
    }
}

fn metadata_error_diagnostic_cause_chain(error: &MetadataError) -> String {
    metadata_error_diagnostic_cause_label(error).to_string()
}

impl From<MetadataError> for ServerError {
    fn from(e: MetadataError) -> Self {
        match e {
            MetadataError::NoSuchUpload { upload_id } => ServerError::NoSuchUpload { upload_id },
            MetadataError::InvalidBucketName { reason } => {
                ServerError::InvalidBucketName { reason }
            }
            MetadataError::InvalidObjectKey { reason } => ServerError::InvalidArgument { reason },
            MetadataError::StreamSegmentConflict { .. } => ServerError::InvalidRequest {
                reason: "stream segment index already exists".to_string(),
            },
            other => ServerError::Metadata(other),
        }
    }
}

impl From<checksum::InvalidChecksumConfig> for ServerError {
    fn from(e: checksum::InvalidChecksumConfig) -> Self {
        ServerError::InvalidArgument { reason: e.reason }
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
    fn diagnostic_cause_label_classifies_contention_and_rpc_overload() {
        let log_conflict = ServerError::Store(StoreError::MetadataCommandLogConflict {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            log_index: 3,
        });
        assert_eq!(
            log_conflict.diagnostic_cause_label(),
            "metadata_command_log_conflict"
        );

        let log_gap = ServerError::Store(StoreError::MetadataCommandLogGap {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            log_index: 5,
            expected_log_index: 4,
        });
        assert_eq!(log_gap.diagnostic_cause_label(), "metadata_command_log_gap");

        let pending_conflict = ServerError::Store(StoreError::MetadataCommandPendingConflict {
            pg_id: 2,
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            existing_log_index: 3,
            candidate_log_index: 4,
        });
        assert_eq!(
            pending_conflict.diagnostic_cause_label(),
            "metadata_command_pending_conflict"
        );

        let contention = ServerError::Store(StoreError::MetadataCommandContention {
            context: "pending command displaced during cleanup",
        });
        assert_eq!(
            contention.diagnostic_cause_label(),
            "metadata_command_contention"
        );

        let shard_overload = ServerError::Store(StoreError::ShardStore {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            source: Box::new(StoreError::StorageRpcResourceExhausted {
                node_id: 1,
                operation: "ReadHandlesAcquire",
                message: "limit exceeded".to_string(),
            }),
        });
        assert_eq!(
            shard_overload.diagnostic_cause_label(),
            "shard_store_storage_rpc_resource_exhausted"
        );
        assert_eq!(
            shard_overload.diagnostic_cause_chain(),
            "server_error>store_error>shard_store>storage_rpc_resource_exhausted"
        );
        let server_detail = shard_overload
            .server_storage_rpc_detail()
            .expect("storage RPC detail should be available server-side");
        assert!(server_detail.contains("shard_store node_id=1 pg_id=2"));
        assert!(server_detail.contains("storage_rpc_resource_exhausted"));
        assert!(server_detail.contains("operation=\"ReadHandlesAcquire\""));
        assert!(server_detail.contains("message=\"limit exceeded\""));
        let converted_overload = ServerError::from(StoreError::ShardStore {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            source: Box::new(StoreError::StorageRpcResourceExhausted {
                node_id: 1,
                operation: "ReadHandlesAcquire",
                message: "limit exceeded".to_string(),
            }),
        });
        assert!(matches!(converted_overload, ServerError::SlowDown));

        let stale_bucket = ServerError::Metadata(MetadataError::StaleBucketMetadataCommand {
            name: storage::BucketName::try_from("bucket".to_string()).unwrap(),
            bucket_execution_generation: 7,
        });
        assert_eq!(
            stale_bucket.diagnostic_cause_label(),
            "stale_bucket_metadata_command"
        );

        let stale_object = ServerError::Metadata(MetadataError::StaleObjectWriteCommand {
            bucket: storage::BucketName::try_from("bucket".to_string()).unwrap(),
            key: storage::ObjectKey::try_from("key".to_string()).unwrap(),
            write_sequence: 3,
            generation_id: Some(9),
        });
        assert_eq!(
            stale_object.diagnostic_cause_label(),
            "stale_object_write_command"
        );
        assert_eq!(
            stale_object.diagnostic_cause_chain(),
            "server_error>metadata_error>stale_object_write_command"
        );
    }

    #[test]
    fn s3_error_code_auth_missing() {
        let err = ServerError::Auth(auth::AuthError::MissingAuth);
        assert_eq!(err.s3_error_code(), "AccessDenied");
    }

    #[test]
    fn s3_error_code_auth_unknown_key() {
        let err = ServerError::Auth(auth::AuthError::UnknownAccessKey);
        assert_eq!(err.s3_error_code(), "InvalidAccessKeyId");
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
        let err = ServerError::Store(StoreError::NotFound);
        assert_eq!(err.s3_error_code(), "InternalError");
    }

    #[test]
    fn s3_error_code_metadata() {
        let err = ServerError::Metadata(MetadataError::ObjectNotFound);
        assert_eq!(err.s3_error_code(), "InternalError");
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
        assert_eq!(ServerError::Store(StoreError::NotFound).http_status(), 500);
        assert_eq!(
            ServerError::Metadata(MetadataError::ObjectNotFound).http_status(),
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
    fn from_store_error() {
        let err: ServerError = StoreError::NotFound.into();
        assert!(matches!(err, ServerError::Store(_)));
    }

    #[test]
    fn from_metadata_error() {
        let err: ServerError = MetadataError::ObjectNotFound.into();
        assert!(matches!(err, ServerError::Metadata(_)));
    }

    #[test]
    fn from_invalid_object_key_metadata_error() {
        let err: ServerError = MetadataError::InvalidObjectKey { reason: "x".into() }.into();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));
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
    fn from_metadata_no_such_upload() {
        let err: ServerError = MetadataError::NoSuchUpload {
            upload_id: "abc".to_string(),
        }
        .into();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));
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
