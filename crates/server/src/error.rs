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

    #[error("delete marker hit: {bucket}/{key}")]
    DeleteMarkerHit { bucket: String, key: String },

    #[error("storage error: {0}")]
    Store(#[from] StoreError),

    #[error("metadata error: {0}")]
    Metadata(#[from] MetadataError),

    #[error("EC error: {0}")]
    Ec(#[from] ec::EcError),

    #[error("auth error: {0}")]
    Auth(#[from] auth::AuthError),

    #[error("invalid request: {reason}")]
    InvalidRequest { reason: String },

    #[error("invalid argument: {reason}")]
    InvalidArgument { reason: String },

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
}

impl ServerError {
    /// Map to S3 error code string.
    pub fn s3_error_code(&self) -> &'static str {
        match self {
            Self::BucketNotFound { .. } => "NoSuchBucket",
            Self::BucketAlreadyExists => "BucketAlreadyExists",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::ObjectNotFound { .. } => "NoSuchKey",
            Self::DeleteMarkerHit { .. } => "NoSuchKey",
            Self::Auth(auth::AuthError::MissingAuth) => "AccessDenied",
            Self::Auth(auth::AuthError::UnknownAccessKey) => "InvalidAccessKeyId",
            Self::Auth(auth::AuthError::SignatureMismatch) => "SignatureDoesNotMatch",
            Self::Auth(auth::AuthError::RequestExpired) => "RequestTimeTooSkewed",
            Self::Auth(_) => "AccessDenied",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::NotModified { .. } => "NotModified",
            Self::InvalidRequest { .. } => "InvalidRequest",
            Self::InvalidArgument { .. } => "InvalidArgument",
            Self::InvalidBucketName { .. } => "InvalidBucketName",
            Self::MetadataBlobError { .. } => "InternalError",
            Self::ObjectTooLarge { .. } => "EntityTooLarge",
            Self::MethodNotAllowed => "MethodNotAllowed",
            Self::InvalidRange { .. } => "InvalidRange",
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
            Self::DeleteMarkerHit { .. } => 404,
            Self::Auth(_) => 403,
            Self::InvalidRequest { .. }
            | Self::InvalidArgument { .. }
            | Self::InvalidBucketName { .. } => 400,
            Self::ObjectTooLarge { .. } => 400,
            Self::MethodNotAllowed => 405,
            Self::InvalidRange { .. } => 416,
            Self::PreconditionFailed => 412,
            Self::NotModified { .. } => 304,
            _ => 500,
        }
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
        let err = ServerError::Auth(auth::AuthError::MissingSignedHeader { header: "host" });
        assert_eq!(err.s3_error_code(), "AccessDenied");
    }

    #[test]
    fn s3_error_code_auth_malformed() {
        let err = ServerError::Auth(auth::AuthError::MalformedAuth);
        assert_eq!(err.s3_error_code(), "AccessDenied");
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
    }

    #[test]
    fn http_status_409() {
        assert_eq!(ServerError::BucketAlreadyExists.http_status(), 409);
        assert_eq!(ServerError::BucketNotEmpty.http_status(), 409);
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
