/// Fault injection framework for shard-level testing.
///
/// Wraps `MemoryPgStore` with policy-based fault injection. The policy
/// intercepts `ShardStore` operations and can return errors, corrupt data,
/// or drop writes. `PgMetadataStore` methods pass through unconditionally.
use std::cell::RefCell;
use std::collections::HashMap;

use crate::error::{MetadataError, StoreError};
use crate::memory_store::MemoryPgStore;
use crate::traits::{PgMetadataStore, ShardStore};
use crate::types::*;

// ── Fault types ─────────────────────────────────────────────────────

/// Which ShardStore operation is being intercepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShardFaultOp {
    Read,
    Write,
    Stat,
    Delete,
}

/// What to do when a fault rule matches.
#[derive(Debug, Clone)]
pub enum ShardFaultAction {
    /// No fault — pass through to inner store.
    Pass,
    /// Return `StoreError::NotFound`.
    ReturnNotFound,
    /// Return `StoreError::Io` with the given context message.
    ReturnIoError(&'static str),
    /// Write succeeds, then corrupt stored data so next read detects CRC mismatch.
    CorruptData { flip_count: u8, seed: u64 },
    /// Return `StoreError::IntegrityError` with synthetic values (expected=0, actual=1).
    ReturnIntegrityError,
    /// Return a fake `WriteAck` without actually writing. Subsequent read → NotFound.
    DropWrite,
}

// ── Policy trait ────────────────────────────────────────────────────

/// Decides what fault action to apply for a given operation.
pub trait ShardFaultPolicy {
    fn on_op(&self, op: ShardFaultOp, key: &ShardKey, attempt: u32) -> ShardFaultAction;
}

// ── ScriptedPolicy ─────────────────────────────────────────────────

/// A single fault rule. Fields set to `None` are wildcards.
pub struct FaultRule {
    /// Which operation to match. None = any.
    pub op: Option<ShardFaultOp>,
    /// Match shard index (last byte of ShardKey). None = any.
    pub shard_index: Option<u8>,
    /// Match attempt number (1-based). None = any.
    pub attempt: Option<u32>,
    /// Action to take when this rule matches.
    pub action: ShardFaultAction,
}

/// Ordered list of rules; first match wins. No match → Pass.
pub struct ScriptedPolicy {
    rules: Vec<FaultRule>,
}

impl ScriptedPolicy {
    pub fn new(rules: Vec<FaultRule>) -> Self {
        Self { rules }
    }
}

impl ShardFaultPolicy for ScriptedPolicy {
    fn on_op(&self, op: ShardFaultOp, key: &ShardKey, attempt: u32) -> ShardFaultAction {
        let shard_index = key.as_bytes()[SHARD_KEY_LEN - 1];
        for rule in &self.rules {
            if let Some(rule_op) = rule.op {
                if rule_op != op {
                    continue;
                }
            }
            if let Some(rule_idx) = rule.shard_index {
                if rule_idx != shard_index {
                    continue;
                }
            }
            if let Some(rule_attempt) = rule.attempt {
                if rule_attempt != attempt {
                    continue;
                }
            }
            return rule.action.clone();
        }
        ShardFaultAction::Pass
    }
}

// ── FaultyPgStore ──────────────────────────────────────────────────

/// Wraps `MemoryPgStore` with policy-based fault injection on `ShardStore` ops.
/// `PgMetadataStore` methods pass through unconditionally.
pub struct FaultyPgStore {
    inner: MemoryPgStore,
    policy: Box<dyn ShardFaultPolicy>,
    /// Per (op, key) attempt counter, 1-based.
    attempt_counts: RefCell<HashMap<(ShardFaultOp, Vec<u8>), u32>>,
}

impl FaultyPgStore {
    pub fn new(policy: Box<dyn ShardFaultPolicy>) -> Self {
        Self {
            inner: MemoryPgStore::new(),
            policy,
            attempt_counts: RefCell::new(HashMap::new()),
        }
    }

    /// Increment and return the attempt number for (op, key).
    fn next_attempt(&self, op: ShardFaultOp, key: &ShardKey) -> u32 {
        let map_key = (op, key.as_bytes().to_vec());
        let mut counts = self.attempt_counts.borrow_mut();
        let count = counts.entry(map_key).or_insert(0);
        *count += 1;
        *count
    }

    fn make_io_error(msg: &'static str) -> StoreError {
        StoreError::Io {
            context: msg,
            source: std::io::Error::other("fault injection"),
        }
    }
}

impl ShardStore for FaultyPgStore {
    fn write_shard(&self, key: &ShardKey, data: &[u8]) -> Result<WriteAck, StoreError> {
        let attempt = self.next_attempt(ShardFaultOp::Write, key);
        let action = self.policy.on_op(ShardFaultOp::Write, key, attempt);

        match action {
            ShardFaultAction::Pass => self.inner.write_shard(key, data),
            ShardFaultAction::ReturnNotFound => Err(StoreError::NotFound),
            ShardFaultAction::ReturnIoError(msg) => Err(Self::make_io_error(msg)),
            ShardFaultAction::ReturnIntegrityError => Err(StoreError::IntegrityError {
                expected: 0,
                actual: 1,
            }),
            ShardFaultAction::CorruptData { flip_count, seed } => {
                // Write normally, then corrupt the stored bytes (CRC unchanged).
                let ack = self.inner.write_shard(key, data)?;
                self.inner.corrupt_shard_data(key, flip_count, seed);
                Ok(ack)
            }
            ShardFaultAction::DropWrite => {
                // Return a fake WriteAck without writing anything.
                Ok(WriteAck {
                    crc64: crc64::checksum(data),
                    stored_size: data.len() as u64,
                })
            }
        }
    }

    fn read_shard(&self, key: &ShardKey) -> Result<ShardData, StoreError> {
        let attempt = self.next_attempt(ShardFaultOp::Read, key);
        let action = self.policy.on_op(ShardFaultOp::Read, key, attempt);

        match action {
            ShardFaultAction::Pass => self.inner.read_shard(key),
            ShardFaultAction::ReturnNotFound => Err(StoreError::NotFound),
            ShardFaultAction::ReturnIoError(msg) => Err(Self::make_io_error(msg)),
            ShardFaultAction::ReturnIntegrityError => Err(StoreError::IntegrityError {
                expected: 0,
                actual: 1,
            }),
            ShardFaultAction::CorruptData { flip_count, seed } => {
                // Corrupt then read (will fail CRC).
                self.inner.corrupt_shard_data(key, flip_count, seed);
                self.inner.read_shard(key)
            }
            ShardFaultAction::DropWrite => {
                // DropWrite on read doesn't make sense — treat as Pass.
                self.inner.read_shard(key)
            }
        }
    }

    fn delete_shard(&self, key: &ShardKey) -> Result<(), StoreError> {
        let attempt = self.next_attempt(ShardFaultOp::Delete, key);
        let action = self.policy.on_op(ShardFaultOp::Delete, key, attempt);

        match action {
            ShardFaultAction::Pass => self.inner.delete_shard(key),
            ShardFaultAction::ReturnNotFound => Err(StoreError::NotFound),
            ShardFaultAction::ReturnIoError(msg) => Err(Self::make_io_error(msg)),
            ShardFaultAction::ReturnIntegrityError => Err(StoreError::IntegrityError {
                expected: 0,
                actual: 1,
            }),
            ShardFaultAction::CorruptData { .. } | ShardFaultAction::DropWrite => {
                self.inner.delete_shard(key)
            }
        }
    }

    fn stat_shard(&self, key: &ShardKey) -> Result<ShardStat, StoreError> {
        let attempt = self.next_attempt(ShardFaultOp::Stat, key);
        let action = self.policy.on_op(ShardFaultOp::Stat, key, attempt);

        match action {
            ShardFaultAction::Pass => self.inner.stat_shard(key),
            ShardFaultAction::ReturnNotFound => Err(StoreError::NotFound),
            ShardFaultAction::ReturnIoError(msg) => Err(Self::make_io_error(msg)),
            ShardFaultAction::ReturnIntegrityError => Err(StoreError::IntegrityError {
                expected: 0,
                actual: 1,
            }),
            ShardFaultAction::CorruptData { .. } | ShardFaultAction::DropWrite => {
                self.inner.stat_shard(key)
            }
        }
    }
}

impl PgMetadataStore for FaultyPgStore {
    fn put_object_meta(&self, req: &PutObjectMetaReq) -> Result<(), MetadataError> {
        self.inner.put_object_meta(req)
    }

    fn get_object_meta(&self, bucket: &str, key: &str) -> Result<ObjectRecord, MetadataError> {
        self.inner.get_object_meta(bucket, key)
    }

    fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<ObjectRecord, MetadataError> {
        self.inner.get_object_version(bucket, key, version_id)
    }

    fn delete_object_meta(&self, bucket: &str, key: &str) -> Result<(), MetadataError> {
        self.inner.delete_object_meta(bucket, key)
    }

    fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<(), MetadataError> {
        self.inner.delete_object_version(bucket, key, version_id)
    }

    fn list_objects(&self, req: &ListObjectsReq) -> Result<ListObjectsResp, MetadataError> {
        self.inner.list_objects(req)
    }

    fn list_object_versions(
        &self,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, MetadataError> {
        self.inner.list_object_versions(req)
    }

    fn next_version_id(&self, bucket: &str, key: &str) -> Result<u64, MetadataError> {
        self.inner.next_version_id(bucket, key)
    }

    fn put_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
        tags: &str,
    ) -> Result<(), MetadataError> {
        self.inner.put_object_tags(bucket, key, version_id, tags)
    }

    fn get_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<Option<String>, MetadataError> {
        self.inner.get_object_tags(bucket, key, version_id)
    }

    fn delete_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<(), MetadataError> {
        self.inner.delete_object_tags(bucket, key, version_id)
    }

    fn create_multipart_upload(&self, req: &CreateMultipartUploadReq) -> Result<(), MetadataError> {
        self.inner.create_multipart_upload(req)
    }

    fn get_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Result<MultipartUploadRecord, MetadataError> {
        self.inner.get_multipart_upload(upload_id)
    }

    fn set_upload_state(
        &self,
        upload_id: &str,
        new_state: UploadState,
    ) -> Result<(), MetadataError> {
        self.inner.set_upload_state(upload_id, new_state)
    }

    fn delete_multipart_upload(&self, upload_id: &str) -> Result<(), MetadataError> {
        self.inner.delete_multipart_upload(upload_id)
    }

    fn list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, MetadataError> {
        self.inner.list_multipart_uploads(req)
    }

    fn upsert_multipart_part(
        &self,
        part: &MultipartPartRecord,
    ) -> Result<Option<u32>, MetadataError> {
        self.inner.upsert_multipart_part(part)
    }

    fn get_multipart_part(
        &self,
        upload_id: &str,
        part_number: u32,
    ) -> Result<MultipartPartRecord, MetadataError> {
        self.inner.get_multipart_part(upload_id, part_number)
    }

    fn list_multipart_parts(&self, req: &ListPartsReq) -> Result<ListPartsResp, MetadataError> {
        self.inner.list_multipart_parts(req)
    }

    fn commit_object_parts(&self, parts: &[ObjectPartRecord]) -> Result<(), MetadataError> {
        self.inner.commit_object_parts(parts)
    }

    fn get_object_parts(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<Vec<ObjectPartRecord>, MetadataError> {
        self.inner.get_object_parts(bucket, key, version_id)
    }

    fn delete_object_parts(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<(), MetadataError> {
        self.inner.delete_object_parts(bucket, key, version_id)
    }

    fn complete_multipart_commit(
        &self,
        upload_id: &str,
        obj: &PutObjectMetaReq,
        parts: &[ObjectPartRecord],
    ) -> Result<(), MetadataError> {
        self.inner
            .complete_multipart_commit(upload_id, obj, parts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key(shard_index: u8) -> ShardKey {
        ShardKey::new(&[0xAA; 16], 1, shard_index)
    }

    #[test]
    fn read_fault_returns_not_found() {
        let policy = ScriptedPolicy::new(vec![FaultRule {
            op: Some(ShardFaultOp::Read),
            shard_index: None,
            attempt: None,
            action: ShardFaultAction::ReturnNotFound,
        }]);
        let store = FaultyPgStore::new(Box::new(policy));
        let key = test_key(0);

        // Write succeeds (no read fault on write).
        store.write_shard(&key, b"hello").unwrap();

        // Read should return NotFound due to policy.
        let err = store.read_shard(&key).unwrap_err();
        assert!(matches!(err, StoreError::NotFound));
    }

    #[test]
    fn read_fault_returns_io_error() {
        let policy = ScriptedPolicy::new(vec![FaultRule {
            op: Some(ShardFaultOp::Read),
            shard_index: None,
            attempt: None,
            action: ShardFaultAction::ReturnIoError("fault injection"),
        }]);
        let store = FaultyPgStore::new(Box::new(policy));
        let key = test_key(0);

        store.write_shard(&key, b"hello").unwrap();
        let err = store.read_shard(&key).unwrap_err();
        assert!(matches!(err, StoreError::Io { .. }));
    }

    #[test]
    fn corrupt_data_on_write() {
        let policy = ScriptedPolicy::new(vec![FaultRule {
            op: Some(ShardFaultOp::Write),
            shard_index: None,
            attempt: None,
            action: ShardFaultAction::CorruptData {
                flip_count: 3,
                seed: 42,
            },
        }]);
        let store = FaultyPgStore::new(Box::new(policy));
        let key = test_key(0);

        // Write "succeeds" (returns Ok) but data is corrupted in store.
        let ack = store.write_shard(&key, b"hello world").unwrap();
        assert!(ack.stored_size > 0);

        // Read detects CRC mismatch — real production path.
        let err = store.read_shard(&key).unwrap_err();
        assert!(matches!(err, StoreError::IntegrityError { .. }));
    }

    #[test]
    fn return_integrity_error_direct() {
        let policy = ScriptedPolicy::new(vec![FaultRule {
            op: Some(ShardFaultOp::Read),
            shard_index: None,
            attempt: None,
            action: ShardFaultAction::ReturnIntegrityError,
        }]);
        let store = FaultyPgStore::new(Box::new(policy));
        let key = test_key(0);

        store.write_shard(&key, b"hello").unwrap();
        let err = store.read_shard(&key).unwrap_err();
        match err {
            StoreError::IntegrityError { expected, actual } => {
                assert_eq!(expected, 0);
                assert_eq!(actual, 1);
            }
            other => panic!("expected IntegrityError, got: {other:?}"),
        }
    }

    #[test]
    fn drop_write_causes_not_found() {
        let policy = ScriptedPolicy::new(vec![FaultRule {
            op: Some(ShardFaultOp::Write),
            shard_index: None,
            attempt: None,
            action: ShardFaultAction::DropWrite,
        }]);
        let store = FaultyPgStore::new(Box::new(policy));
        let key = test_key(0);

        // Write "succeeds" but nothing is stored.
        let ack = store.write_shard(&key, b"hello").unwrap();
        assert_eq!(ack.stored_size, 5);

        // Read finds nothing.
        let err = store.read_shard(&key).unwrap_err();
        assert!(matches!(err, StoreError::NotFound));
    }

    #[test]
    fn stat_fault_returns_not_found() {
        let policy = ScriptedPolicy::new(vec![FaultRule {
            op: Some(ShardFaultOp::Stat),
            shard_index: None,
            attempt: None,
            action: ShardFaultAction::ReturnNotFound,
        }]);
        let store = FaultyPgStore::new(Box::new(policy));
        let key = test_key(0);

        store.write_shard(&key, b"hello").unwrap();
        let err = store.stat_shard(&key).unwrap_err();
        assert!(matches!(err, StoreError::NotFound));
    }

    #[test]
    fn metadata_passthrough() {
        // Policy that faults every shard op — metadata should be unaffected.
        let policy = ScriptedPolicy::new(vec![FaultRule {
            op: None,
            shard_index: None,
            attempt: None,
            action: ShardFaultAction::ReturnNotFound,
        }]);
        let store = FaultyPgStore::new(Box::new(policy));

        store
            .put_object_meta(&PutObjectMetaReq {
                bucket: "b".to_string(),
                key: "k".to_string(),
                version_id: 0,
                status: 0,
                size: 100,
                total_size: 110,
                etag: vec![1, 2, 3],
                etag_kind: 0,
                ec_k: 4,
                ec_m: 2,
                data_layout: None,
                parts_count: None,
                metadata_blob: None,
            })
            .unwrap();

        let record = store.get_object_meta("b", "k").unwrap();
        assert_eq!(record.size, 100);
        assert_eq!(record.ec_k, 4);
    }

    #[test]
    fn shard_index_matching() {
        // Only fault shard index 2.
        let policy = ScriptedPolicy::new(vec![FaultRule {
            op: Some(ShardFaultOp::Read),
            shard_index: Some(2),
            attempt: None,
            action: ShardFaultAction::ReturnNotFound,
        }]);
        let store = FaultyPgStore::new(Box::new(policy));

        let key0 = test_key(0);
        let key2 = test_key(2);
        let key5 = test_key(5);

        store.write_shard(&key0, b"data0").unwrap();
        store.write_shard(&key2, b"data2").unwrap();
        store.write_shard(&key5, b"data5").unwrap();

        // Shard 0 and 5 read fine.
        assert!(store.read_shard(&key0).is_ok());
        assert!(store.read_shard(&key5).is_ok());

        // Shard 2 is faulted.
        assert!(matches!(
            store.read_shard(&key2).unwrap_err(),
            StoreError::NotFound
        ));
    }

    #[test]
    fn attempt_counter_per_key() {
        // Fail 1st read of shard 2, succeed on retry.
        let policy = ScriptedPolicy::new(vec![FaultRule {
            op: Some(ShardFaultOp::Read),
            shard_index: Some(2),
            attempt: Some(1),
            action: ShardFaultAction::ReturnNotFound,
        }]);
        let store = FaultyPgStore::new(Box::new(policy));

        let key0 = test_key(0);
        let key2 = test_key(2);

        store.write_shard(&key0, b"data0").unwrap();
        store.write_shard(&key2, b"data2").unwrap();

        // Shard 0: always works (rule only matches shard 2).
        assert!(store.read_shard(&key0).is_ok());

        // Shard 2, attempt 1: fails.
        assert!(matches!(
            store.read_shard(&key2).unwrap_err(),
            StoreError::NotFound
        ));

        // Shard 2, attempt 2: succeeds (rule only matches attempt 1).
        let data = store.read_shard(&key2).unwrap();
        assert_eq!(data.data, b"data2");

        // Shard 0 still works on second read.
        assert!(store.read_shard(&key0).is_ok());
    }

    #[test]
    fn wildcard_matching() {
        // Rule with all-None matches everything.
        let policy = ScriptedPolicy::new(vec![FaultRule {
            op: None,
            shard_index: None,
            attempt: None,
            action: ShardFaultAction::ReturnIoError("total fault"),
        }]);
        let store = FaultyPgStore::new(Box::new(policy));

        let key = test_key(0);
        assert!(matches!(
            store.write_shard(&key, b"x").unwrap_err(),
            StoreError::Io { .. }
        ));
        assert!(matches!(
            store.read_shard(&key).unwrap_err(),
            StoreError::Io { .. }
        ));
        assert!(matches!(
            store.stat_shard(&key).unwrap_err(),
            StoreError::Io { .. }
        ));
        assert!(matches!(
            store.delete_shard(&key).unwrap_err(),
            StoreError::Io { .. }
        ));
    }
}
