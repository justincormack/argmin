use super::test_helpers;
use super::*;
use crate::conditional::ReadCondition;
use crate::metadata_blob::MetadataBlob;
use crate::sse::ManagedWrappingKeyConfig;
use crate::system_metadata::SystemMetadata;
use ec::EcConfig;
use std::path::Path;
use std::sync::Arc;
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
