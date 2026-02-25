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

    #[error("metadata blob error: {reason}")]
    MetadataBlobError { reason: String },

    #[error("object too large: {size} bytes (max {max})")]
    ObjectTooLarge { size: u64, max: u64 },

    #[error("method not allowed")]
    MethodNotAllowed,
}

impl ServerError {
    /// Map to S3 error code string.
    pub fn s3_error_code(&self) -> &'static str {
        match self {
            Self::BucketNotFound { .. } => "NoSuchBucket",
            Self::BucketAlreadyExists => "BucketAlreadyOwnedByYou",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::ObjectNotFound { .. } => "NoSuchKey",
            Self::Auth(auth::AuthError::MissingAuth) => "AccessDenied",
            Self::Auth(auth::AuthError::UnknownAccessKey) => "InvalidAccessKeyId",
            Self::Auth(auth::AuthError::SignatureMismatch) => "SignatureDoesNotMatch",
            Self::Auth(_) => "AccessDenied",
            Self::InvalidRequest { .. } => "InvalidRequest",
            Self::MetadataBlobError { .. } => "InternalError",
            Self::ObjectTooLarge { .. } => "EntityTooLarge",
            Self::MethodNotAllowed => "MethodNotAllowed",
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
            Self::Auth(_) => 403,
            Self::InvalidRequest { .. } => 400,
            Self::ObjectTooLarge { .. } => 400,
            Self::MethodNotAllowed => 405,
            _ => 500,
        }
    }
}
