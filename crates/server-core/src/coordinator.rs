/// Coordinator: orchestrates S3 operations across EC, storage, and metadata layers.
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{
    Arc, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
};
use std::thread::JoinHandle;

use checksum::{
    ChecksumAlgorithm, ChecksumBytes, ChecksumType, MultipartChecksumConfig, RawChecksum,
};
use ec::{EcConfig, ErasureCodec};
use s3_types::{
    aws_account_id_from_principal, parse_account_regional_bucket_name, AccountIdentity, AclGrant,
    AclGrantee, AclGrants, AclPermission, BucketNamespace, BucketVersioningState, CanonicalUserId,
    LegalHoldStatus, ObjectLockDefaultRetention, ObjectLockMode, ObjectRetention, RetentionPeriod,
    StoredLegalHoldStatus, VersionId,
};
use storage::traits::{PgMetadataStore, ShardStore};
#[cfg(test)]
use storage::SimplePayloadReclaimRecord;
use storage::{
    BucketEncryptionConfig, BucketFastPathInfo, BucketInfo, BucketLifecycleConfiguration,
    BucketName, BucketObjectLockConfig, BucketOwnershipControls, BucketState, CommitMultipartReq,
    CommitStreamPutReq, CreateMultipartUploadReq, CreateStreamUploadReq, EcShape,
    EffectiveBucketEncryptionConfig, GenerationId, LifecycleDate, LifecycleExpiration,
    LifecycleRule, LifecycleRuleStatus, ListMultipartUploadsReq, ListObjectVersionsReq,
    ListObjectsReq, ListPartsReq, LiveObjectRecord, ManagedEncryptionAlgorithm,
    MultipartPartRecord, MultipartPartSegmentRecord, MultipartReclaimPartRecord,
    MultipartReclaimPartSegmentRecord, MultipartReclaimRecord, MultipartUploadRecord,
    ObjectEncryption, ObjectKey, ObjectLayout, ObjectLockState, ObjectPartRecord,
    ObjectSegmentRecord, ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord,
    OwnerIdentity, PublicAccessBlockConfig, PutDeleteMarkerReq, PutLiveObjectReq, PutObjectReq,
    ReclaimWorkItem, SerializedMetadataBlob, SerializedSystemMetadataBlob, SerializedTagSet,
    SessionId, ShardKey, SharedStorageNode, StoredObject, StreamUploadRecord,
    StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget, UploadId, UploadState,
    UPLOAD_ID_ALPHABET, UPLOAD_ID_LEN,
};

use self::authz::CachedBucketPolicy;
use self::authz_results::*;
pub use self::authz_types::{
    ActiveWriteEncryption, ActiveWriteEncryptionRef, AuthorizedPutObjectWrite,
};
use self::authz_types::{AuthorizedPutObjectWriteAcl, ValidatedBucket};
use self::lifecycle::CachedBucketLifecycle;
use self::object_state::{SnapshottedMultipartPart, StaleObjectPayload};
#[cfg(test)]
use self::payload::encode_parity_scratch_len;
use self::pg_guards::{BucketObjectPgGuards, LockedReadObject, ObjectPgGuards, TwoPgGuards};
use self::read_core::{
    segment_payloads_from_object_segments, MultipartReader, PayloadLease, ReadObjectContext,
    ReadRuntime, SegmentListReader, SegmentPayloadRecord,
};
pub use self::read_core::{ReadChunk, ReadHandle};
use self::request_types::authorization_policy_context_for_put_object_write_acl;
pub use self::request_types::*;
use self::request_types::{
    AuthorizedWriteTags, BucketCreateOutcome, BucketScopedAuthorizationRequest,
    BucketScopedRequest, ExpectedBucketOwnerRequest, PreparedPutCommit, PutCommitRequest,
};
pub use self::response_types::*;
use self::response_types::{DeleteMarkerLifecycleExpiration, NoncurrentLifecycleExpiration};
use self::runtime::{LifecycleSweeper, ReclaimSweeper};
#[cfg(test)]
use self::test_hooks::*;
pub use crate::checksum_claim::{ChecksumClaim, EncodedChecksumClaim};
use crate::conditional::{
    check_copy_source_conditions, check_delete_conditions, check_read_conditions,
    check_write_conditions, DeleteCondition, ReadCondition, WriteCondition,
};
use crate::error::ServerError;
pub use storage::BucketObjectOwnership;

fn lock_mutex_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

#[cfg(test)]
fn trusted_bucket_name(name: impl Into<String>) -> BucketName {
    BucketName::try_from(name.into())
        .expect("coordinator must only construct BucketName from validated values")
}

#[cfg(test)]
fn trusted_object_key(key: impl Into<String>) -> ObjectKey {
    ObjectKey::try_from(key.into())
        .expect("coordinator must only construct ObjectKey from validated values")
}

#[cfg(test)]
fn trusted_upload_id(seed: &str) -> UploadId {
    let mut bytes = [b'.'; UPLOAD_ID_LEN];
    let mut encoded = String::with_capacity(seed.len() * 2);
    for byte in seed.bytes() {
        use std::fmt::Write;
        write!(encoded, "{byte:02x}").unwrap();
    }
    let take = encoded.len().min(UPLOAD_ID_LEN);
    bytes[..take].copy_from_slice(&encoded.as_bytes()[..take]);
    UploadId::try_from(String::from_utf8(bytes.to_vec()).unwrap())
        .expect("coordinator tests must use valid upload IDs")
}

#[cfg(test)]
fn trusted_session_id(seed: &str) -> SessionId {
    let mut bytes = [b'0'; storage::SESSION_ID_LEN];
    let mut encoded = String::with_capacity(seed.len() * 2);
    for byte in seed.bytes() {
        use std::fmt::Write;
        write!(encoded, "{byte:02x}").unwrap();
    }
    let take = encoded.len().min(storage::SESSION_ID_LEN);
    bytes[..take].copy_from_slice(&encoded.as_bytes()[..take]);
    SessionId::try_from(String::from_utf8(bytes.to_vec()).unwrap())
        .expect("coordinator tests must use valid session IDs")
}

fn parse_list_object_key(value: &str) -> Result<ObjectKey, ServerError> {
    ObjectKey::try_from(value).map_err(|error| ServerError::InvalidArgument {
        reason: error.to_string(),
    })
}

fn optional_list_object_key(value: Option<&str>) -> Result<Option<ObjectKey>, ServerError> {
    value
        .filter(|value| !value.is_empty())
        .map(parse_list_object_key)
        .transpose()
}

fn read_rwlock_unpoisoned<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|err| err.into_inner())
}

fn write_rwlock_unpoisoned<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|err| err.into_inner())
}
use crate::etag::{compute_multipart_etag, crc64_to_etag_bytes, etag_bytes_to_crc64, format_etag};
use crate::metadata_blob::MetadataBlob;
use crate::pg::{object_key_hash, part_key_hash, stream_segment_key_hash, PgTopology};
use crate::range::ByteRange;
use crate::sse::{
    decrypt_managed_encryption_checksum, decrypt_managed_encryption_segment,
    decrypt_sse_customer_checksum, decrypt_sse_customer_segment, prepare_managed_encryption_write,
    prepare_sse_customer_write, resume_managed_encryption_write, resume_sse_customer_write,
    validate_sse_customer_read, ManagedEncryptionWriteContext, SseCustomerRequest,
    SseCustomerResponseHeaders, SseCustomerSegmentScope, SseCustomerValidatorConfig,
    SseCustomerWriteContext, StaticManagedKeyProvider, SSE_C_SEGMENT_TAG_LEN,
};
use crate::system_metadata::SystemMetadata;

const TRACE_TARGET: &str = "server_core";
const COMPLETED_MULTIPART_UPLOADS_PER_BUCKET_LIMIT: usize = 10_000;

/// Maximum object size for single PUT or upload part (5 GiB, matches AWS S3).
pub const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// Fixed internal segment size for newly committed segmented payloads.
pub const INTERNAL_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

const LIFECYCLE_SWEEP_INTERVAL_MILLIS: u64 = 1000;

/// Hard cap on total records fetched across all PGs for a single list query.
/// Prevents unbounded memory when delimiter causes u32::MAX per-PG limits.
const MAX_LIST_RECORDS: usize = 100_000;
const S3_MAX_LIST_KEYS: u32 = 1_000;

#[derive(Debug)]
struct PayloadBufferPool {
    default_capacity: usize,
    max_cached: usize,
    cached: Mutex<Vec<Vec<u8>>>,
    #[cfg(test)]
    allocations: std::sync::atomic::AtomicUsize,
}

struct PooledPayloadBuffer {
    pool: Arc<PayloadBufferPool>,
    buf: Option<Vec<u8>>,
}

#[derive(Debug)]
struct SharedPayloadBuffer {
    pool: Option<Arc<PayloadBufferPool>>,
    buf: Vec<u8>,
}

struct EncodeScratchPool {
    scratch_len: usize,
    max_cached: usize,
    cached: Mutex<Vec<Vec<u8>>>,
    #[cfg(test)]
    allocations: std::sync::atomic::AtomicUsize,
}

struct EncodeScratch<'a> {
    pool: &'a EncodeScratchPool,
    buf: Option<Vec<u8>>,
}

struct WrittenShard {
    key: ShardKey,
    ack: storage::WriteAck,
}
/// Minimum part size for non-final parts (5 MiB).
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Maximum number of parts in a multipart upload (matches AWS S3).
const MAX_PARTS: usize = 10_000;

pub struct Coordinator {
    storage_node: Arc<SharedStorageNode>,
    bucket_policy_cache: RwLock<HashMap<BucketName, CachedBucketPolicy>>,
    bucket_lifecycle_cache: RwLock<HashMap<BucketName, CachedBucketLifecycle>>,
    pg_topology: PgTopology,
    ec_codec: Arc<ErasureCodec>,
    ec_config: EcConfig,
    encode_scratch_pool: EncodeScratchPool,
    payload_buffer_pool: Arc<PayloadBufferPool>,
    region: String,
    sse_c_validator: Option<SseCustomerValidatorConfig>,
    managed_key_provider: Option<StaticManagedKeyProvider>,
    _reclaim_sweeper: ReclaimSweeper,
    _lifecycle_sweeper: Arc<LifecycleSweeper>,
}

impl Coordinator {
    fn now_millis() -> u64 {
        storage::clock::current_time_millis()
    }
}

/// Compute an inline checksum value for the given algorithm and data.
fn compute_checksum(algo: ChecksumAlgorithm, data: &[u8]) -> RawChecksum {
    match algo {
        ChecksumAlgorithm::Crc32 => {
            RawChecksum::new(algo, checksum::crc32::checksum(data).to_be_bytes())
        }
        ChecksumAlgorithm::Crc32c => {
            RawChecksum::new(algo, checksum::crc32c::checksum(data).to_be_bytes())
        }
        ChecksumAlgorithm::Crc64nvme => {
            RawChecksum::new(algo, checksum::crc64::checksum(data).to_be_bytes())
        }
        ChecksumAlgorithm::Sha256 => RawChecksum::new(
            algo,
            ring::digest::digest(&ring::digest::SHA256, data).as_ref(),
        ),
        ChecksumAlgorithm::Sha1 => RawChecksum::new(
            algo,
            ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, data).as_ref(),
        ),
    }
    .expect("checksum helper produces bytes matching the requested algorithm")
}

enum StreamingChecksumAccumulator {
    Crc32(checksum::crc32::Hasher),
    Crc32c(checksum::crc32c::Hasher),
    Crc64(checksum::crc64::Hasher),
    Sha1(ring::digest::Context),
    Sha256(ring::digest::Context),
}

impl StreamingChecksumAccumulator {
    fn new(algo: ChecksumAlgorithm) -> Self {
        match algo {
            ChecksumAlgorithm::Crc32 => Self::Crc32(checksum::crc32::Hasher::new()),
            ChecksumAlgorithm::Crc32c => Self::Crc32c(checksum::crc32c::Hasher::new()),
            ChecksumAlgorithm::Crc64nvme => Self::Crc64(checksum::crc64::Hasher::new()),
            ChecksumAlgorithm::Sha1 => Self::Sha1(ring::digest::Context::new(
                &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
            )),
            ChecksumAlgorithm::Sha256 => {
                Self::Sha256(ring::digest::Context::new(&ring::digest::SHA256))
            }
        }
    }

    fn algorithm(&self) -> ChecksumAlgorithm {
        match self {
            Self::Crc32(_) => ChecksumAlgorithm::Crc32,
            Self::Crc32c(_) => ChecksumAlgorithm::Crc32c,
            Self::Crc64(_) => ChecksumAlgorithm::Crc64nvme,
            Self::Sha1(_) => ChecksumAlgorithm::Sha1,
            Self::Sha256(_) => ChecksumAlgorithm::Sha256,
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(hasher) => hasher.update(data),
            Self::Crc32c(hasher) => hasher.update(data),
            Self::Crc64(hasher) => hasher.update(data),
            Self::Sha1(hasher) => hasher.update(data),
            Self::Sha256(hasher) => hasher.update(data),
        }
    }

    fn finalize(self) -> RawChecksum {
        match self {
            Self::Crc32(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Crc32, hasher.finalize().to_be_bytes())
            }
            Self::Crc32c(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Crc32c, hasher.finalize().to_be_bytes())
            }
            Self::Crc64(hasher) => RawChecksum::new(
                ChecksumAlgorithm::Crc64nvme,
                hasher.finalize().to_be_bytes(),
            ),
            Self::Sha1(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Sha1, hasher.finish().as_ref())
            }
            Self::Sha256(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Sha256, hasher.finish().as_ref())
            }
        }
        .expect("streaming checksum accumulator produces bytes matching the algorithm")
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub mod test_helpers;

#[cfg(test)]
mod access_control_tests;
mod authz;
#[cfg(test)]
mod authz_model_tests;
mod authz_results;
mod authz_types;
mod bucket;
#[cfg(test)]
mod bucket_tests;
mod copy;
#[cfg(test)]
mod core_tests;
mod delete;
mod infra;
mod lifecycle;
mod listing;
mod multipart;
#[cfg(test)]
mod multipart_reclaim_trace_tests;
#[cfg(test)]
mod multipart_stateful_tests;
#[cfg(test)]
mod multipart_tests;
#[cfg(test)]
mod multipart_trace_tests;
mod object_metadata;
mod object_state;
#[cfg(test)]
mod object_state_tests;
mod payload;
mod pg_guards;
mod put;
mod read;
mod read_core;
#[cfg(test)]
mod read_tests;
mod request_types;
mod response_types;
mod runtime;
mod streaming;
#[cfg(test)]
mod test_hooks;
#[cfg(test)]
mod test_support;
