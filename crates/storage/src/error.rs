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
    BucketNotFound { name: String },

    #[error("bucket already exists")]
    BucketAlreadyExists,

    #[error("bucket not empty")]
    BucketNotEmpty,

    #[error("object not found")]
    ObjectNotFound,

    #[error("invalid versioning transition from {from} to {to}")]
    InvalidVersioningTransition { from: u8, to: u8 },

    #[error("multipart upload not found")]
    NoSuchUpload,

    #[error("upload not in InProgress state (current: {state})")]
    UploadNotInProgress { state: u8 },

    #[error("multipart part not found: upload={upload_id} part={part_number}")]
    PartNotFound { upload_id: String, part_number: u32 },

    #[error("not implemented: {context}")]
    NotImplemented { context: &'static str },

    #[error("database error: {context}: {source}")]
    Db {
        context: &'static str,
        #[source]
        source: rusqlite::Error,
    },
}
