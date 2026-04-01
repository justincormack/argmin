/// Unified error type for the server crate.
use storage::error::{MetadataError, StoreError};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("bucket not found: {name}")]
    BucketNotFound { name: String },

    #[error("bucket already exists")]
    BucketAlreadyExists,

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
    Store(#[from] StoreError),

    #[error("metadata error: {0}")]
    Metadata(MetadataError),

    #[error("EC error: {0}")]
    Ec(#[from] ec::EcError),

    #[error("auth error: {0}")]
    Auth(#[from] auth::AuthError),

    #[error("invalid request: {reason}")]
    InvalidRequest { reason: String },

    #[error("invalid argument: {reason}")]
    InvalidArgument { reason: String },

    #[error("invalid URI: {reason}")]
    InvalidURI { reason: String },

    #[error("invalid bucket name: {reason}")]
    InvalidBucketName { reason: String },
    #[error("metadata blob error: {reason}")]
    MetadataBlobError { reason: String },

    #[error("object too large: {size} bytes (max {max})")]
    ObjectTooLarge { size: u64, max: u64 },

    #[error("method not allowed")]
    MethodNotAllowed,

    #[error("invalid range")]
    InvalidRange { total_size: u64 },

    #[error("precondition failed")]
    PreconditionFailed,

    #[error("not modified")]
    NotModified { etag: String, last_modified: u64 },

    #[error("please reduce your request rate")]
    SlowDown,

    #[error("bad digest")]
    BadDigest,

    #[error("invalid digest")]
    InvalidDigest,

    #[error("The calculated MD5 hash of the key did not match the hash that was provided.")]
    InvalidSseCustomerKeyMd5,

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
    InvalidTag { reason: String },

    #[error("no public access block configuration: {bucket}")]
    NoSuchPublicAccessBlockConfiguration { bucket: String },

    #[error("no bucket policy: {bucket}")]
    NoSuchBucketPolicy { bucket: String },

    #[error("no lifecycle configuration: {bucket}")]
    NoSuchLifecycleConfiguration { bucket: String },

    #[error("malformed policy: {reason}")]
    MalformedPolicy { reason: String },

    #[error("ownership controls not found: {bucket}")]
    OwnershipControlsNotFound { bucket: String },

    #[error("object lock configuration not found: {bucket}")]
    ObjectLockConfigurationNotFound { bucket: String },

    #[error("bucket is in an invalid state for this operation")]
    InvalidBucketState,

    #[error("ACL not supported with BucketOwnerEnforced")]
    AccessControlListNotSupported,

    #[error("invalid bucket ACL with object ownership")]
    InvalidBucketAclWithObjectOwnership,

    #[error("access denied")]
    AccessDenied,

    #[error("no such upload: {upload_id}")]
    NoSuchUpload { upload_id: String },

    #[error("invalid part: part {part_number}")]
    InvalidPart { part_number: u32 },

    #[error("invalid part order")]
    InvalidPartOrder,

    #[error("entity too small: part {part_number} is {size} bytes (min {min})")]
    EntityTooSmall {
        part_number: u32,
        size: u64,
        min: u64,
    },

    #[error("not implemented: {feature}")]
    NotImplemented { feature: String },

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
    /// Map to S3 error code string.
    pub fn s3_error_code(&self) -> &'static str {
        match self {
            Self::BucketNotFound { .. } => "NoSuchBucket",
            Self::BucketAlreadyExists => "BucketAlreadyExists",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::ObjectNotFound { .. } => "NoSuchKey",
            Self::VersionNotFound { .. } => "NoSuchVersion",
            Self::DeleteMarkerHit { .. } => "NoSuchKey",
            Self::Auth(auth::AuthError::MissingAuth) => "AccessDenied",
            Self::Auth(auth::AuthError::MalformedAuth) => "AuthorizationHeaderMalformed",
            Self::Auth(auth::AuthError::UnsupportedAuthType) => "InvalidArgument",
            Self::Auth(auth::AuthError::UnknownAccessKey) => "InvalidAccessKeyId",
            Self::Auth(auth::AuthError::DuplicateAuthorizationHeader) => "NotImplemented",
            Self::Auth(auth::AuthError::SignatureMismatch) => "SignatureDoesNotMatch",
            Self::Auth(auth::AuthError::RequestExpired) => "RequestTimeTooSkewed",
            Self::Auth(auth::AuthError::ExpiredToken) => "ExpiredToken",
            Self::Auth(auth::AuthError::InvalidToken) => "InvalidToken",
            Self::Auth(auth::AuthError::InvalidQueryParam { .. }) => {
                "AuthorizationQueryParametersError"
            }
            Self::Auth(auth::AuthError::MissingQueryParam { .. }) => {
                "AuthorizationQueryParametersError"
            }
            Self::Auth(_) => "AccessDenied",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::NotModified { .. } => "NotModified",
            Self::InvalidRequest { .. } => "InvalidRequest",
            Self::InvalidArgument { .. } => "InvalidArgument",
            Self::InvalidURI { .. } => "InvalidURI",
            Self::InvalidBucketName { .. } => "InvalidBucketName",
            Self::MetadataBlobError { .. } => "InternalError",
            Self::ObjectTooLarge { .. } => "EntityTooLarge",
            Self::MethodNotAllowed => "MethodNotAllowed",
            Self::InvalidRange { .. } => "InvalidRange",
            Self::SlowDown => "SlowDown",
            Self::BadDigest => "BadDigest",
            Self::InvalidDigest => "InvalidDigest",
            Self::InvalidSseCustomerKeyMd5 => "InvalidArgument",
            Self::InvalidEncryptionAlgorithmError { .. } => "InvalidEncryptionAlgorithmError",
            Self::InvalidChunkSize { .. } => "InvalidChunkSizeError",
            Self::NoSuchCorsConfiguration { .. } => "NoSuchCORSConfiguration",
            Self::NoSuchTagSet { .. } => "NoSuchTagSet",
            Self::InvalidTag { .. } => "InvalidTag",
            Self::NoSuchPublicAccessBlockConfiguration { .. } => {
                "NoSuchPublicAccessBlockConfiguration"
            }
            Self::NoSuchBucketPolicy { .. } => "NoSuchBucketPolicy",
            Self::NoSuchLifecycleConfiguration { .. } => "NoSuchLifecycleConfiguration",
            Self::MalformedPolicy { .. } => "MalformedPolicy",
            Self::OwnershipControlsNotFound { .. } => "OwnershipControlsNotFoundError",
            Self::ObjectLockConfigurationNotFound { .. } => "ObjectLockConfigurationNotFoundError",
            Self::InvalidBucketState => "InvalidBucketState",
            Self::AccessControlListNotSupported => "AccessControlListNotSupported",
            Self::InvalidBucketAclWithObjectOwnership => "InvalidBucketAclWithObjectOwnership",
            Self::AccessDenied => "AccessDenied",
            Self::NoSuchUpload { .. } => "NoSuchUpload",
            Self::InvalidPart { .. } => "InvalidPart",
            Self::InvalidPartOrder => "InvalidPartOrder",
            Self::EntityTooSmall { .. } => "EntityTooSmall",
            Self::MalformedXML { .. } => "MalformedXML",
            Self::IllegalVersioningConfiguration { .. } => {
                "IllegalVersioningConfigurationException"
            }
            Self::MalformedPOSTRequest { .. } => "MalformedPOSTRequest",
            Self::MalformedChunkedBody { .. } => "InvalidRequest",
            Self::IncompleteBody => "IncompleteBody",
            Self::MissingContentLength => "MissingContentLength",
            Self::MalformedTrailerError { .. } => "MalformedTrailerError",
            Self::XAmzContentSHA256Mismatch { .. } => "XAmzContentSHA256Mismatch",
            Self::NotImplemented { .. } => "NotImplemented",
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
            Self::BucketNotEmpty => 409,
            Self::ObjectNotFound { .. } => 404,
            Self::VersionNotFound { .. } => 404,
            Self::DeleteMarkerHit { .. } => 404,
            Self::Auth(
                auth::AuthError::MalformedAuth
                | auth::AuthError::UnsupportedAuthType
                | auth::AuthError::InvalidQueryParam { .. }
                | auth::AuthError::MissingQueryParam { .. },
            ) => 400,
            Self::Auth(auth::AuthError::DuplicateAuthorizationHeader) => 501,
            Self::Auth(_) => 403,
            Self::InvalidRequest { .. }
            | Self::InvalidArgument { .. }
            | Self::InvalidURI { .. }
            | Self::InvalidBucketName { .. }
            | Self::BadDigest
            | Self::InvalidDigest
            | Self::InvalidSseCustomerKeyMd5
            | Self::InvalidEncryptionAlgorithmError { .. } => 400,
            Self::InvalidChunkSize { .. } => 403,
            Self::NoSuchCorsConfiguration { .. } => 404,
            Self::NoSuchTagSet { .. } => 404,
            Self::NoSuchPublicAccessBlockConfiguration { .. }
            | Self::NoSuchBucketPolicy { .. }
            | Self::NoSuchLifecycleConfiguration { .. } => 404,
            Self::MalformedPolicy { .. } => 400,
            Self::OwnershipControlsNotFound { .. } => 404,
            Self::ObjectLockConfigurationNotFound { .. } => 404,
            Self::InvalidBucketState => 409,
            Self::InvalidTag { .. } => 400,
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
            Self::AccessDenied => 403,
            Self::NoSuchUpload { .. } => 404,
            Self::InvalidPart { .. } | Self::InvalidPartOrder | Self::EntityTooSmall { .. } => 400,
            Self::NotImplemented { .. } => 501,
            Self::InternalError { .. } => 500,
            Self::ObjectTooLarge { .. } => 400,
            Self::MethodNotAllowed => 405,
            Self::InvalidRange { .. } => 416,
            Self::PreconditionFailed => 412,
            Self::NotModified { .. } => 304,
            Self::SlowDown => 503,
            _ => 500,
        }
    }
}

impl From<MetadataError> for ServerError {
    fn from(e: MetadataError) -> Self {
        match e {
            MetadataError::NoSuchUpload { upload_id } => ServerError::NoSuchUpload {
                upload_id: upload_id.into_string(),
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
                reason: "bad".to_string()
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
    fn s3_error_code_object_not_found() {
        let err = ServerError::ObjectNotFound {
            bucket: "b".into(),
            key: "k".into(),
        };
        assert_eq!(err.s3_error_code(), "NoSuchKey");
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
    fn s3_error_code_auth_signature_mismatch() {
        let err = ServerError::Auth(auth::AuthError::SignatureMismatch);
        assert_eq!(err.s3_error_code(), "SignatureDoesNotMatch");
    }

    #[test]
    fn s3_error_code_auth_expired() {
        let err = ServerError::Auth(auth::AuthError::RequestExpired);
        assert_eq!(err.s3_error_code(), "RequestTimeTooSkewed");
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
    fn s3_error_code_auth_invalid_token() {
        let err = ServerError::Auth(auth::AuthError::InvalidToken);
        assert_eq!(err.s3_error_code(), "InvalidToken");
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
    fn s3_error_code_invalid_request() {
        let err = ServerError::InvalidRequest {
            reason: "bad".into(),
        };
        assert_eq!(err.s3_error_code(), "InvalidRequest");
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
    fn s3_error_code_method_not_allowed() {
        assert_eq!(
            ServerError::MethodNotAllowed.s3_error_code(),
            "MethodNotAllowed"
        );
    }

    #[test]
    fn s3_error_code_invalid_range() {
        let err = ServerError::InvalidRange { total_size: 100 };
        assert_eq!(err.s3_error_code(), "InvalidRange");
    }

    #[test]
    fn http_status_416() {
        assert_eq!(
            ServerError::InvalidRange { total_size: 100 }.http_status(),
            416
        );
    }

    #[test]
    fn s3_error_code_precondition_failed() {
        assert_eq!(
            ServerError::PreconditionFailed.s3_error_code(),
            "PreconditionFailed"
        );
    }

    #[test]
    fn http_status_412() {
        assert_eq!(ServerError::PreconditionFailed.http_status(), 412);
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
        assert_eq!(ServerError::BucketNotEmpty.http_status(), 409);
        assert_eq!(ServerError::InvalidBucketState.http_status(), 409);
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
            ServerError::ObjectTooLarge { size: 1, max: 0 }.http_status(),
            400
        );
        assert_eq!(
            ServerError::MalformedPolicy {
                reason: "bad".into()
            }
            .http_status(),
            400
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
            upload_id: storage::types::UploadId::from("abc"),
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
