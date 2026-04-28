use super::test_helpers;
use super::test_topology::*;
use super::*;
use crate::conditional::ReadCondition;
use crate::metadata_blob::MetadataBlob;
use crate::sse::ManagedWrappingKeyConfig;
use crate::system_metadata::SystemMetadata;
use std::path::Path;
use std::sync::{Arc, Barrier, MutexGuard};
use storage::{
    MultipartPartSegmentRecord, MultipartUploadRecord, ObjectSegmentsReclaimRecord,
    ObjectSegmentsReclaimSegmentRecord, PayloadReclaimRoot, PgTopology, StreamUploadRecord,
    StreamUploadSegmentRecord,
};

const NO_READ: &ReadCondition = &ReadCondition {
    if_match: None,
    if_none_match: None,
    if_modified_since: None,
    if_unmodified_since: None,
};
const NO_PUT_OBJECT_ACL: PutObjectAcl<'static> = PutObjectAcl::None;
const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";

fn test_sse_s3_provider() -> StaticManagedKeyProvider {
    StaticManagedKeyProvider::single(
        ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
    )
}

fn setup_coordinator(dir: &Path) -> Coordinator {
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
    Coordinator::new_with_managed_key_provider(
        storage_node,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
    )
    .unwrap()
}

fn setup_coordinator_with_shared_storage(storage_node: Arc<SharedStorageNode>) -> Coordinator {
    let storage_cluster = StorageCluster::shared_single_node(storage_node);
    let shared_caches = shared_caches_for_storage_cluster(&storage_cluster);
    Coordinator::new_with_shared_caches_and_lifecycle_sweeper_factory(
        storage_cluster,
        shared_caches,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        LifecycleSweeper::acquire_shared,
    )
    .unwrap()
}

fn test_requester() -> Requester {
    test_helpers::requester("default-owner")
}

fn object_request<'a>(bucket: &'a str, key: &'a str, requester: Requester) -> ObjectRequest<'a> {
    ObjectRequest::new(
        trusted_bucket_name(bucket),
        trusted_object_key(key),
        requester,
        None,
    )
}

fn object_version_request<'a>(
    bucket: &'a str,
    key: &'a str,
    version_id: Option<VersionId>,
    requester: Requester,
) -> ObjectVersionRequest<'a> {
    ObjectVersionRequest::new(
        trusted_bucket_name(bucket),
        trusted_object_key(key),
        version_id,
        requester,
        None,
    )
}

trait MultipartUploadIdArg {
    fn into_test_upload_id(self) -> UploadId;
}

impl MultipartUploadIdArg for &str {
    fn into_test_upload_id(self) -> UploadId {
        UploadId::try_from(self).unwrap_or_else(|_| trusted_upload_id(self))
    }
}

impl MultipartUploadIdArg for &UploadId {
    fn into_test_upload_id(self) -> UploadId {
        self.clone()
    }
}

fn multipart_object_request<'a, I: MultipartUploadIdArg>(
    bucket: &'a str,
    key: &'a str,
    upload_id: I,
    requester: Requester,
) -> MultipartObjectRequest<'a> {
    MultipartObjectRequest::new(
        trusted_bucket_name(bucket),
        trusted_object_key(key),
        upload_id.into_test_upload_id(),
        requester,
        None,
    )
}

fn begin_stream_put_test(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
) -> Result<SessionId, ServerError> {
    let authorized = coord.authorize_put_object_write(&AuthorizePutObjectRequest {
        object: object_request(bucket, key, test_requester()),
        acl: NO_PUT_OBJECT_ACL.into(),
        policy_context: PutObjectPolicyContext::default(),
        object_lock: ObjectLockState::default(),
        tags: None,
        encryption: WriteEncryptionRequest::none(),
    })?;
    coord.begin_stream_put_session(&authorized)
}

fn begin_stream_part_test<I: MultipartUploadIdArg>(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    upload_id: I,
    part_number: u32,
) -> Result<BeginStreamPartResult, ServerError> {
    coord.begin_stream_part(&BeginStreamPartRequest {
        upload: multipart_object_request(bucket, key, upload_id, test_requester()),
        part_number,
        policy_context: PutObjectPolicyContext::default(),
        sse_customer: None,
    })
}

fn create_basic_multipart_upload(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
) -> CreateMultipartUploadResult {
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request(bucket, key, test_requester()),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap()
}

fn create_upload_with_parts(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    parts: &[(u32, &[u8])],
) -> (UploadId, Vec<CompletePart>) {
    let create = create_basic_multipart_upload(coord, bucket, key);
    let complete_parts = parts
        .iter()
        .map(|&(part_number, data)| {
            let result = test_helpers::upload_part(
                coord,
                &test_helpers::UploadPartRequest {
                    upload: multipart_object_request(
                        bucket,
                        key,
                        &create.upload_id,
                        test_requester(),
                    ),
                    part_number,
                    data,
                    claimed_checksum: None,
                    sse_customer: None,
                },
            )
            .unwrap();
            CompletePart {
                part_number,
                etag: result.etag,
                checksum: None,
            }
        })
        .collect();
    (create.upload_id, complete_parts)
}

fn make_test_read_runtime(dir: &Path) -> ReadRuntime {
    let storage_node = Arc::new(SharedStorageNode::open(dir, &[0]).unwrap());
    ReadRuntime {
        storage_node: StorageCluster::shared_single_node(Arc::clone(&storage_node)),
        #[cfg(test)]
        pg_topology: PgTopology::new(&[0]).unwrap(),
        payload_buffer_pool: PayloadBufferPool::new(storage_node.default_ec_shape()),
        sse_c_validator: None,
        managed_key_provider: None,
    }
}

fn read_all_body(mut body: ReadHandle) -> Result<Vec<u8>, ServerError> {
    let mut out = Vec::new();
    while let Some(chunk) = body.next_chunk(INTERNAL_SEGMENT_SIZE)? {
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

struct InvariantHarness<'a> {
    coord: &'a Coordinator,
}

impl<'a> InvariantHarness<'a> {
    fn new(coord: &'a Coordinator) -> Self {
        Self { coord }
    }

    fn active_stream_sessions(&self) -> Vec<StreamUploadRecord> {
        self.coord
            .storage_node
            .test_list_all_stream_uploads()
            .unwrap()
    }

    fn active_stream_sessions_for(&self, bucket: &str, key: &str) -> Vec<StreamUploadRecord> {
        let bucket = trusted_bucket_name(bucket);
        let key = trusted_object_key(key);
        self.active_stream_sessions()
            .into_iter()
            .filter(|session| session.bucket == bucket && session.key == key)
            .collect()
    }

    fn pending_multipart_uploads_for(&self, bucket: &str, key: &str) -> Vec<MultipartUploadRecord> {
        let bucket_name = trusted_bucket_name(bucket);
        let key_name = trusted_object_key(key);
        self.coord
            .storage_node
            .test_list_multipart_uploads_for_bucket(&bucket_name)
            .unwrap()
            .into_iter()
            .filter(|upload| upload.key == key_name)
            .collect()
    }

    fn pending_reclaim_roots_for(&self, bucket: &str, key: &str) -> Vec<PayloadReclaimRoot> {
        let bucket_name = trusted_bucket_name(bucket);
        let key_name = trusted_object_key(key);
        self.coord
            .storage_node
            .test_list_bucket_payload_reclaim_roots(&bucket_name)
            .unwrap()
            .into_iter()
            .filter(|root| root.key == key_name)
            .collect()
    }

    fn multipart_part_segments(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &UploadId,
    ) -> Vec<MultipartPartSegmentRecord> {
        self.coord
            .storage_node
            .test_get_all_multipart_part_segments_for_upload(
                &trusted_bucket_name(bucket),
                &trusted_object_key(key),
                upload_id,
            )
            .unwrap()
    }

    fn multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &UploadId,
    ) -> MultipartUploadRecord {
        self.coord
            .storage_node
            .test_get_multipart_upload(
                &trusted_bucket_name(bucket),
                &trusted_object_key(key),
                upload_id,
            )
            .unwrap()
    }

    fn stream_segments(
        &self,
        bucket: &str,
        key: &str,
        session_id: &SessionId,
    ) -> Vec<StreamUploadSegmentRecord> {
        self.coord
            .storage_node
            .test_list_stream_segments(
                &trusted_bucket_name(bucket),
                &trusted_object_key(key),
                session_id,
            )
            .unwrap()
    }

    fn force_stream_session_created_at(
        &self,
        bucket: &str,
        key: &str,
        session_id: &SessionId,
        created_at: u64,
    ) {
        self.coord
            .storage_node
            .test_force_stream_upload_created_at(
                &trusted_bucket_name(bucket),
                &trusted_object_key(key),
                session_id,
                created_at,
            )
            .unwrap();
    }

    fn assert_no_active_stream_sessions_for(&self, bucket: &str, key: &str, invariant: &str) {
        let sessions = self.active_stream_sessions_for(bucket, key);
        assert!(
            sessions.is_empty(),
            "{invariant}: expected no active stream sessions for {bucket}/{key}, found {sessions:?}"
        );
    }

    fn assert_no_pending_multipart_uploads_for(&self, bucket: &str, key: &str, invariant: &str) {
        let uploads = self.pending_multipart_uploads_for(bucket, key);
        assert!(
            uploads.is_empty(),
            "{invariant}: expected no pending multipart uploads for {bucket}/{key}, found {uploads:?}"
        );
    }

    fn assert_no_pending_reclaim_roots_for(&self, bucket: &str, key: &str, invariant: &str) {
        let roots = self.pending_reclaim_roots_for(bucket, key);
        assert!(
            roots.is_empty(),
            "{invariant}: expected no pending reclaim roots for {bucket}/{key}, found {roots:?}"
        );
    }
}

fn assert_segment_shards_exist(
    coord: &Coordinator,
    segments: &[StreamUploadSegmentRecord],
    invariant: &str,
    phase: &str,
) {
    for segment in segments {
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                coord
                    .storage_node
                    .test_shard_exists(segment.data_pg_id, &shard_key)
                    .unwrap(),
                "{invariant}: shard {i} should exist {phase}"
            );
        }
    }
}

fn assert_segment_shards_deleted(
    coord: &Coordinator,
    segments: &[StreamUploadSegmentRecord],
    invariant: &str,
    phase: &str,
) {
    for segment in segments {
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                !coord
                    .storage_node
                    .test_shard_exists(segment.data_pg_id, &shard_key)
                    .unwrap(),
                "{invariant}: shard {i} should be deleted {phase}"
            );
        }
    }
}

struct StreamAppendRaceSync {
    prepared_barrier: Arc<Barrier>,
    _serial_guard: MutexGuard<'static, ()>,
    _guard: StreamAppendTestHookGuard,
}

fn install_stream_append_race_hooks(
    session_id: &SessionId,
    segment_index: u32,
) -> StreamAppendRaceSync {
    let serial = STREAM_APPEND_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let prepared_barrier = Arc::new(Barrier::new(3));
    let prepared_barrier_hook = Arc::clone(&prepared_barrier);
    let guard = install_stream_append_test_hooks(StreamAppendTestHooks {
        target: Some((session_id.as_str().to_owned(), segment_index)),
        after_prepare: Some(Arc::new(move || {
            prepared_barrier_hook.wait();
        })),
    });
    StreamAppendRaceSync {
        prepared_barrier,
        _serial_guard: serial,
        _guard: guard,
    }
}

struct MultipartCompletePreCommitRaceSync {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
    _serial_guard: MutexGuard<'static, ()>,
    _guard: ReclamationTestHookGuard,
}

fn install_multipart_complete_pre_commit_race_hooks(
    bucket: &str,
    key: &str,
) -> MultipartCompletePreCommitRaceSync {
    let serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let reached_hook = Arc::clone(&reached);
    let resume_hook = Arc::clone(&resume);
    let guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_multipart_complete_pre_commit: Some(Arc::new(move || {
            reached_hook.wait();
            resume_hook.wait();
        })),
        ..ReclamationTestHooks::default()
    });
    MultipartCompletePreCommitRaceSync {
        reached,
        resume,
        _serial_guard: serial,
        _guard: guard,
    }
}

fn begin_stream_put_with_segment_path(
    coord: &Coordinator,
    bucket: &str,
    key_prefix: &str,
    require_cross_pg: bool,
) -> (String, SessionId) {
    for suffix in 0..256 {
        let key = format!("{key_prefix}-{suffix}");
        let session_id = begin_stream_put_test(coord, bucket, &key).unwrap();
        let has_cross_pg =
            stream_put_session_has_cross_pg_segments(coord, bucket, &key, &session_id);
        if has_cross_pg == require_cross_pg {
            return (key, session_id);
        }
        coord.abort_stream_put(bucket, &key, &session_id).unwrap();
    }
    panic!(
        "failed to find stream session for require_cross_pg={require_cross_pg} after 256 attempts"
    );
}

fn run_stream_duplicate_segment_race_invariant_test(pg_count: u32, require_cross_pg: bool) {
    let dir = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..pg_count).collect();
    let storage_node = Arc::new(SharedStorageNode::open(dir.path(), &pg_ids).unwrap());
    let admin = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
    let writer_a = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
    let writer_b = setup_coordinator_with_shared_storage(storage_node);
    let invariant =
        "duplicate stream appends at the same segment index must leave exactly one staged winner";
    let state = InvariantHarness::new(&admin);

    admin
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (key, session_id) =
        begin_stream_put_with_segment_path(&admin, "bucket", "stream-race", require_cross_pg);
    if require_cross_pg {
        assert!(
            stream_put_session_has_cross_pg_segments(&admin, "bucket", &key, &session_id),
            "{invariant}: expected the staged payloads to span PGs for the cross-PG case"
        );
    } else {
        assert!(
            !stream_put_session_has_cross_pg_segments(&admin, "bucket", &key, &session_id),
            "{invariant}: expected same-PG case to stage both payloads in the metadata PG"
        );
    }

    let sync = install_stream_append_race_hooks(&session_id, 0);
    let data_a = b"first-segment".to_vec();
    let data_b = b"second-segment".to_vec();
    let key_a = key.clone();
    let key_b = key.clone();
    let session_a = session_id.clone();
    let session_b = session_id.clone();
    let t_a = std::thread::spawn(move || {
        writer_a.append_plaintext_stream_segment_for_test("bucket", &key_a, &session_a, 0, &data_a)
    });
    let t_b = std::thread::spawn(move || {
        writer_b.append_plaintext_stream_segment_for_test("bucket", &key_b, &session_b, 0, &data_b)
    });

    sync.prepared_barrier.wait();

    let result_a = t_a.join().unwrap();
    let result_b = t_b.join().unwrap();
    let winner = match (&result_a, &result_b) {
        (Ok(()), Err(ServerError::InvalidRequest { .. })) => b"first-segment".as_slice(),
        (Err(ServerError::InvalidRequest { .. }), Ok(())) => b"second-segment".as_slice(),
        _ => panic!(
            "{invariant}: expected exactly one successful append and one duplicate rejection, got {result_a:?} and {result_b:?}"
        ),
    };

    let staged = admin
        .storage_node
        .test_list_stream_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key(&key),
            &session_id,
        )
        .unwrap();
    assert_eq!(
        staged.len(),
        1,
        "{invariant}: expected exactly one staged segment after duplicate append race"
    );
    assert_eq!(
        staged[0].segment_index, 0,
        "{invariant}: expected the winner to occupy segment index 0"
    );
    assert!(
        staged[0].segment_vid == GenerationId::new(1).unwrap()
            || staged[0].segment_vid == GenerationId::new(2).unwrap(),
        "{invariant}: expected the winner to retain one prepared payload generation"
    );
    admin
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", &key, test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(winner),
            total_size: winner.len() as u64,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let object = admin
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", &key, None, test_requester()),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        read_all_body(object.body).unwrap(),
        winner,
        "{invariant}: finalized object body did not match the winning duplicate append"
    );
    state.assert_no_active_stream_sessions_for("bucket", &key, invariant);
}

#[test]
fn streamed_part_reupload_replaces_displaced_shards_without_orphans() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant = "reuploading a streamed multipart part does not orphan displaced part shards";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let mpu = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let session_a = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;
    let data_a = b"streamed-reupload-a";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_a, 0, data_a)
        .unwrap();
    let result_a = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_a,
            part_number: 1,
            crc64: checksum::crc64::checksum(data_a),
            total_size: data_a.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();

    let segments_before = state.multipart_part_segments("bucket", "key", &mpu.upload_id);
    assert_eq!(
        segments_before.len(),
        1,
        "{invariant}: expected exactly one committed segment set before reupload"
    );
    for segment in &segments_before {
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                coord
                    .storage_node
                    .test_shard_exists(segment.data_pg_id, &shard_key)
                    .unwrap(),
                "{invariant}: displaced shard {i} should exist before the reupload commits"
            );
        }
    }

    let session_b = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;
    let data_b = b"streamed-reupload-b";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_b, 0, data_b)
        .unwrap();
    let result_b = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_b,
            part_number: 1,
            crc64: checksum::crc64::checksum(data_b),
            total_size: data_b.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();
    assert_ne!(result_a.etag, result_b.etag);

    let segments_after = state.multipart_part_segments("bucket", "key", &mpu.upload_id);
    assert_eq!(
        segments_after.len(),
        1,
        "{invariant}: expected exactly one current committed segment set after reupload"
    );
    assert_ne!(
        segments_before[0].segment_okh, segments_after[0].segment_okh,
        "{invariant}: reupload should replace the committed segment generation"
    );

    for segment in &segments_before {
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                !coord
                    .storage_node
                    .test_shard_exists(segment.data_pg_id, &shard_key)
                    .unwrap(),
                "{invariant}: displaced shard {i} should be deleted after the reupload commits"
            );
        }
    }
    for segment in &segments_after {
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                coord
                    .storage_node
                    .test_shard_exists(segment.data_pg_id, &shard_key)
                    .unwrap(),
                "{invariant}: current shard {i} should remain after reupload"
            );
        }
    }
    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);
}

#[test]
fn aborting_streamed_multipart_upload_cleans_committed_segments_and_shards() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant =
        "aborting a streamed multipart upload removes committed multipart segment rows and shards";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let mpu = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let session_id = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;
    let data = b"streamed-part-data-for-abort-test";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, data)
        .unwrap();
    coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_id,
            part_number: 1,
            crc64: checksum::crc64::checksum(data),
            total_size: data.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();

    let segments_before = state.multipart_part_segments("bucket", "key", &mpu.upload_id);
    assert!(
        !segments_before.is_empty(),
        "{invariant}: expected committed multipart segments before abort"
    );
    for segment in &segments_before {
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                coord
                    .storage_node
                    .test_shard_exists(segment.data_pg_id, &shard_key)
                    .unwrap(),
                "{invariant}: shard {i} should exist before abort"
            );
        }
    }

    coord
        .abort_multipart_upload(&multipart_object_request(
            "bucket",
            "key",
            &mpu.upload_id,
            test_requester(),
        ))
        .unwrap();

    assert!(
        state
            .multipart_part_segments("bucket", "key", &mpu.upload_id)
            .is_empty(),
        "{invariant}: expected no multipart segment rows after abort"
    );
    state.assert_no_pending_multipart_uploads_for("bucket", "key", invariant);
    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);

    for segment in &segments_before {
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                !coord
                    .storage_node
                    .test_shard_exists(segment.data_pg_id, &shard_key)
                    .unwrap(),
                "{invariant}: shard {i} should be deleted after abort"
            );
        }
    }
}

#[test]
fn scavenging_stale_sessions_removes_abandoned_streaming_state() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant = "scavenging a stale stream session removes the session and its staged writes";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"data")
        .unwrap();

    state.force_stream_session_created_at("bucket", "key", &session_id, 0);
    let count = coord.scavenge_stale_sessions(1);
    assert_eq!(
        count, 1,
        "{invariant}: expected exactly one stale session scavenged"
    );

    let err = coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 1, b"more")
        .unwrap_err();
    assert!(
        matches!(
            err,
            ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
        ),
        "{invariant}: expected session-not-found after scavenging, got {err:?}"
    );
    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);
    state.assert_no_pending_reclaim_roots_for("bucket", "key", invariant);
}

#[test]
fn scavenging_skips_committed_objects_and_their_payloads() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant =
        "scavenging stale sessions does not disturb committed objects or durable payloads";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"safe-data")
        .unwrap();
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b"safe-data"),
            total_size: 9,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let count = coord.scavenge_stale_sessions(0);
    assert_eq!(
        count, 0,
        "{invariant}: expected no stale sessions after finalize"
    );

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "key", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        read_all_body(result.body).unwrap(),
        b"safe-data",
        "{invariant}: committed object body changed after scavenging"
    );
    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);
    state.assert_no_pending_reclaim_roots_for("bucket", "key", invariant);
}

#[test]
fn staged_stream_object_is_not_visible_before_finalize() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant = "a staged streaming object is not externally visible before finalize succeeds";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "new-key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "new-key", &session_id, 0, b"pending")
        .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "new-key", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::ObjectNotFound { .. }),
        "{invariant}: staged object became readable before finalize, got {err:?}"
    );

    let err = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "new-key", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::ObjectNotFound { .. }),
        "{invariant}: staged object became head-visible before finalize, got {err:?}"
    );

    let sessions = state.active_stream_sessions_for("bucket", "new-key");
    assert_eq!(
        sessions.len(),
        1,
        "{invariant}: expected exactly one active session to hold the staged object state"
    );
    state.assert_no_pending_reclaim_roots_for("bucket", "new-key", invariant);
}

#[test]
fn duplicate_stream_append_race_same_pg_preserves_one_winner() {
    run_stream_duplicate_segment_race_invariant_test(1, false);
}

#[test]
fn duplicate_stream_append_race_cross_pg_preserves_one_winner() {
    run_stream_duplicate_segment_race_invariant_test(4, true);
}

#[test]
fn aborting_multipart_upload_rejects_late_list_parts_without_state_loss() {
    let dir = test_util::tempdir();
    let coord = super::test_support::setup_coordinator_without_lifecycle_sweeper(dir.path());
    let invariant = "once a multipart upload is aborting, later list-parts operations fail predictably without losing the upload state";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let create = create_basic_multipart_upload(&coord, "bucket", "key");
    test_helpers::upload_part(
        &coord,
        &test_helpers::UploadPartRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            part_number: 1,
            data: b"data",
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();

    coord
        .storage_node
        .test_set_upload_state(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &create.upload_id,
            UploadState::Aborting,
        )
        .unwrap();

    let err = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "{invariant}: expected NoSuchUpload once the upload is aborting, got {err:?}"
    );

    let upload = state.multipart_upload("bucket", "key", &create.upload_id);
    assert_eq!(
        upload.state,
        UploadState::Aborting,
        "{invariant}: late list-parts should not change the aborting terminal state"
    );
}

#[test]
fn completing_multipart_upload_rejects_late_abort_without_state_loss() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant = "once a multipart upload is completing, later abort attempts fail predictably without losing the upload state";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let create = create_basic_multipart_upload(&coord, "bucket", "key");

    coord
        .storage_node
        .test_set_upload_state(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &create.upload_id,
            UploadState::Completing,
        )
        .unwrap();

    let err = coord
        .abort_multipart_upload(&multipart_object_request(
            "bucket",
            "key",
            &create.upload_id,
            test_requester(),
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "{invariant}: expected NoSuchUpload once the upload is completing, got {err:?}"
    );

    let upload = state.multipart_upload("bucket", "key", &create.upload_id);
    assert_eq!(
        upload.state,
        UploadState::Completing,
        "{invariant}: late abort should not change the completing terminal state"
    );
}

#[test]
fn abort_wins_over_complete_after_snapshot_without_leaking_multipart_state() {
    let dir = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_node = Arc::new(SharedStorageNode::open(dir.path(), &pg_ids).unwrap());
    let admin = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
    let completer = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
    let aborter = setup_coordinator_with_shared_storage(storage_node);
    let invariant =
        "if abort wins after complete has snapshotted multipart state, the upload is removed without exposing a committed object or leaking multipart state";
    let state = InvariantHarness::new(&admin);

    admin
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let (upload_id, parts) = create_upload_with_parts(&admin, "bucket", "key", &[(1, b"part")]);

    let sync = install_multipart_complete_pre_commit_race_hooks("bucket", "key");
    let upload_id_for_complete = upload_id.clone();
    let parts_for_complete = parts.clone();
    let t_complete = std::thread::spawn(move || {
        completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(
                "bucket",
                "key",
                &upload_id_for_complete,
                test_requester(),
            ),
            parts: &parts_for_complete,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
    });

    sync.reached.wait();

    aborter
        .abort_multipart_upload(&multipart_object_request(
            "bucket",
            "key",
            &upload_id,
            test_requester(),
        ))
        .unwrap();

    sync.resume.wait();
    let complete_res = t_complete.join().unwrap();
    assert!(
        complete_res.is_err(),
        "{invariant}: complete should lose once abort deletes the upload, got {complete_res:?}"
    );

    let err = admin
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "key", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::ObjectNotFound { .. }),
        "{invariant}: aborted completion race should not expose a visible object, got {err:?}"
    );

    let err = admin
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request("bucket", "key", &upload_id, test_requester()),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "{invariant}: multipart upload should be gone after abort wins, got {err:?}"
    );

    state.assert_no_pending_multipart_uploads_for("bucket", "key", invariant);
    state.assert_no_pending_reclaim_roots_for("bucket", "key", invariant);
    assert!(
        state
            .multipart_part_segments("bucket", "key", &upload_id)
            .is_empty(),
        "{invariant}: abort winner should leave no committed multipart segment rows"
    );
}

#[test]
fn failed_stream_put_finalize_is_scavenged_without_visibility_or_orphans() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant =
        "a failed stream-put finalize leaves no visible object, and stale-session scavenging removes the abandoned staged shards";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"hello")
        .unwrap();

    let staged_segments = state.stream_segments("bucket", "key", &session_id);
    assert_eq!(
        staged_segments.len(),
        1,
        "{invariant}: expected one staged segment before finalize failure"
    );
    assert_segment_shards_exist(
        &coord,
        &staged_segments,
        invariant,
        "before finalize failure",
    );

    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b"hello"),
            total_size: 999,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::InvalidRequest { .. }),
        "{invariant}: expected InvalidRequest from failed finalize, got {err:?}"
    );

    let err = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "key", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::ObjectNotFound { .. }),
        "{invariant}: failed finalize should not make the object visible, got {err:?}"
    );

    let sessions = state.active_stream_sessions_for("bucket", "key");
    assert_eq!(
        sessions.len(),
        1,
        "{invariant}: failed finalize should leave exactly one stale session to scavenge"
    );
    state.assert_no_pending_reclaim_roots_for("bucket", "key", invariant);

    state.force_stream_session_created_at("bucket", "key", &session_id, 0);
    let count = coord.scavenge_stale_sessions(1);
    assert_eq!(
        count, 1,
        "{invariant}: expected stale-session scavenging to clean the failed finalize session"
    );

    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);
    state.assert_no_pending_reclaim_roots_for("bucket", "key", invariant);
    assert_segment_shards_deleted(&coord, &staged_segments, invariant, "after scavenging");
}

#[test]
fn failed_stream_part_finalize_is_scavenged_without_visible_part_or_orphans() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    let invariant =
        "a failed stream-part finalize leaves no visible multipart part, and stale-session scavenging removes the abandoned staged shards";
    let state = InvariantHarness::new(&coord);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let mpu = create_basic_multipart_upload(&coord, "bucket", "key");
    let session_id = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"part-data")
        .unwrap();

    let staged_segments = state.stream_segments("bucket", "key", &session_id);
    assert_eq!(
        staged_segments.len(),
        1,
        "{invariant}: expected one staged multipart segment before finalize failure"
    );
    assert_segment_shards_exist(
        &coord,
        &staged_segments,
        invariant,
        "before finalize failure",
    );

    let err = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_id,
            part_number: 1,
            crc64: checksum::crc64::checksum(b"part-data"),
            total_size: 999,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::InvalidRequest { .. }),
        "{invariant}: expected InvalidRequest from failed stream-part finalize, got {err:?}"
    );

    let parts = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert!(
        parts.parts.is_empty(),
        "{invariant}: failed finalize should not expose a committed multipart part"
    );

    let upload = state.multipart_upload("bucket", "key", &mpu.upload_id);
    assert_eq!(
        upload.state,
        UploadState::InProgress,
        "{invariant}: failed part finalize should not change the multipart upload state"
    );

    let sessions = state.active_stream_sessions_for("bucket", "key");
    assert_eq!(
        sessions.len(),
        1,
        "{invariant}: failed part finalize should leave exactly one stale session to scavenge"
    );

    state.force_stream_session_created_at("bucket", "key", &session_id, 0);
    let count = coord.scavenge_stale_sessions(1);
    assert_eq!(
        count, 1,
        "{invariant}: expected stale-session scavenging to clean the failed stream-part session"
    );

    state.assert_no_active_stream_sessions_for("bucket", "key", invariant);
    assert!(
        state
            .multipart_part_segments("bucket", "key", &mpu.upload_id)
            .is_empty(),
        "{invariant}: failed finalize should leave no committed multipart part segments"
    );
    assert_segment_shards_deleted(&coord, &staged_segments, invariant, "after scavenging");
}

#[test]
fn dropping_a_read_only_payload_lease_does_not_enqueue_reclaim_work() {
    let dir = test_util::tempdir();
    let runtime = make_test_read_runtime(dir.path());
    let invariant =
        "dropping a read-only payload lease without pending reclaim metadata must not enqueue reclaim work";
    let generation_id = GenerationId::new(1).unwrap();

    drop(runtime.acquire_object_payload_lease("bucket", "key", generation_id));

    assert!(
        runtime.storage_node.try_take_reclaim_work().is_none(),
        "{invariant}: unexpected reclaim work appeared after dropping a read-only lease"
    );
}

#[test]
fn final_payload_lease_drop_retries_only_when_reclaim_metadata_still_exists() {
    let dir = test_util::tempdir();
    let runtime = make_test_read_runtime(dir.path());
    let invariant =
        "the final payload lease drop retries reclaim exactly while durable reclaim metadata still exists";
    let generation_id = GenerationId::new(1).unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let data_pg_id = runtime
        .pg_topology
        .object_generation_segment_data_pg(&bucket, &key, generation_id, 0)
        .get();

    {
        runtime
            .storage_node
            .test_put_object_segments_reclaim(
                &bucket,
                &key,
                &ObjectSegmentsReclaimRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    generation_id,
                    created_at: 1,
                    segments: vec![ObjectSegmentsReclaimSegmentRecord {
                        segment_index: 0,
                        segment_okh: object_key_hash("bucket", "key"),
                        segment_vid: generation_id,
                        data_pg_id,
                        ec: EcShape { k: 4, m: 2 },
                    }],
                },
            )
            .unwrap();
    }

    let lease = runtime.acquire_object_payload_lease("bucket", "key", generation_id);
    runtime.enqueue_object_payload_reclaim("bucket", "key", generation_id);

    let stop = std::sync::atomic::AtomicBool::new(false);
    match runtime.storage_node.wait_for_reclaim_work(&stop) {
        Some(ReclaimWorkItem::ObjectPayload((bucket, key, queued_generation_id))) => {
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "key");
            assert_eq!(
                queued_generation_id, generation_id,
                "{invariant}: initial reclaim item targeted the wrong generation"
            );
        }
        Some(ReclaimWorkItem::BucketDelete(bucket)) => {
            panic!("{invariant}: expected object reclaim work, got bucket delete for {bucket}")
        }
        None => panic!("{invariant}: expected initial reclaim work, got none"),
    }

    runtime
        .try_reclaim_object_payload("bucket", "key", generation_id)
        .unwrap();
    assert!(
        runtime
            .storage_node
            .test_payload_reclaim_exists(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("key"),
                generation_id,
            )
            .unwrap(),
        "{invariant}: lease-gated reclaim retry should leave durable reclaim metadata in place"
    );

    drop(lease);

    match runtime.storage_node.wait_for_reclaim_work(&stop) {
        Some(ReclaimWorkItem::ObjectPayload((bucket, key, queued_generation_id))) => {
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "key");
            assert_eq!(
                queued_generation_id, generation_id,
                "{invariant}: retried reclaim item targeted the wrong generation"
            );
        }
        Some(ReclaimWorkItem::BucketDelete(bucket)) => {
            panic!(
                "{invariant}: expected retried object reclaim work, got bucket delete for {bucket}"
            )
        }
        None => panic!("{invariant}: expected retried reclaim work after dropping the final lease"),
    }

    runtime
        .try_reclaim_object_payload("bucket", "key", generation_id)
        .unwrap();
    assert!(
        !runtime
            .storage_node
            .test_payload_reclaim_exists(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("key"),
                generation_id,
            )
            .unwrap(),
        "{invariant}: successful reclaim after the final lease drop should clear durable reclaim metadata"
    );
}
