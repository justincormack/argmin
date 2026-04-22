/// Storage layer error types.
/// Shard-level storage errors.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("shard not found")]
    NotFound,

    #[error("integrity error: expected CRC {expected:#018X}, got {actual:#018X}")]
    IntegrityError { expected: u64, actual: u64 },

    #[error("PG {pg_id} not found on this node")]
    PgNotFound { pg_id: u32 },

    #[error("invalid shard key length: {len} (expected {expected})")]
    InvalidKeyLength { len: usize, expected: usize },

    #[error("IO error: {context}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error("database error: {context}: {source}")]
    Db {
        context: &'static str,
        #[source]
        source: rusqlite::Error,
    },
}

/// Metadata-level errors (object records, bucket operations).
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("bucket not found: {name}")]
    BucketNotFound { name: crate::types::BucketName },

    #[error("invalid bucket name: {reason}")]
    InvalidBucketName { reason: String },

    #[error("invalid object key: {reason}")]
    InvalidObjectKey { reason: String },

    #[error("bucket already exists")]
    BucketAlreadyExists,

    #[error("bucket not empty")]
    BucketNotEmpty,

    #[error("bucket write reservations are draining")]
    BucketWriteDraining,

    #[error("object not found")]
    ObjectNotFound,

    #[error("method not allowed on delete marker")]
    MethodNotAllowedOnDeleteMarker,

    #[error("invalid versioning transition from {from:?} to {to:?}")]
    InvalidVersioningTransition {
        from: crate::types::BucketVersioningState,
        to: crate::types::BucketVersioningState,
    },

    #[error("multipart upload not found: {upload_id}")]
    NoSuchUpload { upload_id: String },

    #[error("upload not in InProgress state (current: {state})")]
    UploadNotInProgress { state: u8 },

    #[error("multipart part not found: upload={upload_id} part={part_number}")]
    PartNotFound { upload_id: String, part_number: u32 },

    #[error("stream session not found: {session_id}")]
    StreamSessionNotFound { session_id: String },

    #[error("stream session not in InProgress state (current: {state})")]
    StreamSessionNotInProgress { state: u8 },

    #[error("not implemented: {context}")]
    NotImplemented { context: &'static str },

    #[error("database error: {context}: {source}")]
    Db {
        context: &'static str,
        #[source]
        source: rusqlite::Error,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum BucketSnapshotLoadError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
}

#[derive(Debug, thiserror::Error)]
pub enum BucketWriteDrainError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectPgActionError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("invalid request: {reason}")]
    InvalidRequest { reason: String },
}
