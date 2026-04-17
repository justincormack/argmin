use super::test_helpers;
use super::*;
use crate::conditional::ReadCondition;
use crate::metadata_blob::MetadataBlob;
use crate::sse::ManagedWrappingKeyConfig;
use crate::system_metadata::SystemMetadata;
use ec::EcConfig;
use std::path::Path;
use std::sync::{Arc, Barrier, MutexGuard};
use storage::{
    MultipartPartSegmentRecord, MultipartUploadRecord, PayloadReclaimRoot, PgMetadataStore,
    StreamUploadRecord,
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
    let ec_config = EcConfig::default();
    Coordinator::new_with_managed_key_provider(
        storage_node,
        ec_config,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
    )
    .unwrap()
}

fn setup_coordinator_with_shared_storage(storage_node: Arc<SharedStorageNode>) -> Coordinator {
    let ec_config = EcConfig::default();
    Coordinator::new_with_managed_key_provider(
        storage_node,
        ec_config,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
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
        let mut sessions = Vec::new();
        self.coord
            .pg_topology
            .for_each_pg(|pg_id| {
                let pg = self.coord.storage_node.get_pg(pg_id)?;
                sessions.extend(pg.list_all_stream_uploads()?);
                Ok::<(), ServerError>(())
            })
            .unwrap();
        sessions
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
        let mut uploads = Vec::new();
        self.coord
            .pg_topology
            .for_each_pg(|pg_id| {
                let pg = self.coord.storage_node.get_pg(pg_id)?;
                let listed = pg.list_multipart_uploads(&storage::ListMultipartUploadsReq {
                    bucket: bucket_name.clone(),
                    prefix: None,
                    key_marker: None,
                    upload_id_marker: None,
                    max_uploads: u32::MAX,
                })?;
                uploads.extend(
                    listed
                        .uploads
                        .into_iter()
                        .filter(|upload| upload.key == key_name),
                );
                Ok::<(), ServerError>(())
            })
            .unwrap();
        uploads
    }

    fn pending_reclaim_roots_for(&self, bucket: &str, key: &str) -> Vec<PayloadReclaimRoot> {
        let bucket_name = trusted_bucket_name(bucket);
        let key_name = trusted_object_key(key);
        let mut roots = Vec::new();
        self.coord
            .pg_topology
            .for_each_pg(|pg_id| {
                let pg = self.coord.storage_node.get_pg(pg_id)?;
                if let Some(root) =
                    PgMetadataStore::get_bucket_payload_reclaim_root(&*pg, &bucket_name)?
                {
                    if root.key == key_name {
                        roots.push(root);
                    }
                }
                Ok::<(), ServerError>(())
            })
            .unwrap();
        roots
    }

    fn multipart_part_segments(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &UploadId,
    ) -> Vec<MultipartPartSegmentRecord> {
        let meta_pg_id = self.coord.object_pg_id(bucket, key);
        let meta_pg = self.coord.storage_node.get_pg(meta_pg_id).unwrap();
        meta_pg
            .get_all_multipart_part_segments_for_upload(upload_id)
            .unwrap()
    }

    fn multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &UploadId,
    ) -> MultipartUploadRecord {
        let meta_pg_id = self.coord.object_pg_id(bucket, key);
        let meta_pg = self.coord.storage_node.get_pg(meta_pg_id).unwrap();
        meta_pg.get_multipart_upload(upload_id).unwrap()
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
        target: Some((session_id.to_string(), segment_index)),
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

fn begin_stream_put_with_segment_path(
    coord: &Coordinator,
    bucket: &str,
    key_prefix: &str,
    require_cross_pg: bool,
) -> (String, SessionId) {
    for suffix in 0..256 {
        let key = format!("{key_prefix}-{suffix}");
        let session_id = begin_stream_put_test(coord, bucket, &key).unwrap();
        let meta_pg_id = coord.object_pg_id(bucket, &key);
        let first_vid_pg = coord.shard_pg_id_raw(&format!("segment/{session_id}"), "0", 1);
        let second_vid_pg = coord.shard_pg_id_raw(&format!("segment/{session_id}"), "0", 2);
        let has_cross_pg = first_vid_pg != meta_pg_id || second_vid_pg != meta_pg_id;
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
    let meta_pg_id = admin.object_pg_id("bucket", &key);
    let first_vid_pg = admin.shard_pg_id_raw(&format!("segment/{session_id}"), "0", 1);
    let second_vid_pg = admin.shard_pg_id_raw(&format!("segment/{session_id}"), "0", 2);
    if require_cross_pg {
        assert!(
            first_vid_pg != meta_pg_id || second_vid_pg != meta_pg_id,
            "{invariant}: expected the staged payloads to span PGs for the cross-PG case"
        );
    } else {
        assert_eq!(
            first_vid_pg, meta_pg_id,
            "{invariant}: expected same-PG case to stage the first payload in the metadata PG"
        );
        assert_eq!(
            second_vid_pg, meta_pg_id,
            "{invariant}: expected same-PG case to stage the second payload in the metadata PG"
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

    let meta_pg = admin.storage_node.get_pg(meta_pg_id).unwrap();
    let staged = meta_pg.list_stream_segments(&session_id).unwrap();
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
    drop(meta_pg);

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
        let shard_pg = coord.storage_node.get_pg(segment.shard_pg_id).unwrap();
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                shard_pg.read_shard(&shard_key).is_ok(),
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
        let shard_pg = coord.storage_node.get_pg(segment.shard_pg_id).unwrap();
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                shard_pg.read_shard(&shard_key).is_err(),
                "{invariant}: displaced shard {i} should be deleted after the reupload commits"
            );
        }
    }
    for segment in &segments_after {
        let shard_pg = coord.storage_node.get_pg(segment.shard_pg_id).unwrap();
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                shard_pg.read_shard(&shard_key).is_ok(),
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
        let shard_pg = coord.storage_node.get_pg(segment.shard_pg_id).unwrap();
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                shard_pg.read_shard(&shard_key).is_ok(),
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
        let shard_pg = coord.storage_node.get_pg(segment.shard_pg_id).unwrap();
        let total_shards = usize::from(segment.ec_k) + usize::from(segment.ec_m);
        for i in 0..total_shards {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            assert!(
                shard_pg.read_shard(&shard_key).is_err(),
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

    std::thread::sleep(std::time::Duration::from_millis(5));
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
    let coord = setup_coordinator(dir.path());
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

    let meta_pg_id = coord.object_pg_id("bucket", "key");
    let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
    pg.set_upload_state(&create.upload_id, UploadState::Aborting)
        .unwrap();
    drop(pg);

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

    let meta_pg_id = coord.object_pg_id("bucket", "key");
    let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
    pg.set_upload_state(&create.upload_id, UploadState::Completing)
        .unwrap();
    drop(pg);

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
