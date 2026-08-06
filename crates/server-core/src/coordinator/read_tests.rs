use super::test_helpers;
use super::test_support::*;
use super::*;
use ec::EcConfig;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use storage::test_support::{
    StorageClusterPayloadTestSupport as _, StorageClusterSchedulingTestSupport as _,
};
use storage::StoreError;

#[test]
fn stream_put_get_object_readable() {
    // Stream-finalized objects use object segments for shard data.
    // GET reads from object_segments to reconstruct the object.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"hello")
        .unwrap();
    let crc = checksum::crc64::checksum(b"hello");
    let metadata = MetadataBlob::new();
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 5,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let payload = coord
        .storage_node()
        .test_capture_object_payload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            VersionId::Null,
        )
        .unwrap();
    assert_eq!(payload.segment_count(), 1);
    assert!(coord
        .storage_node()
        .test_object_payload_snapshot_is_fully_present(&payload)
        .unwrap());
    assert!(coord
        .storage_node()
        .test_object_payload_snapshot_places_each_shard_on_a_distinct_node(&payload)
        .unwrap());

    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.size, 5);

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"hello");
    assert_eq!(result.size, 5);
}

#[test]
fn get_object_payload_route_error_does_not_become_object_not_found() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"route error must not look missing",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let payload = coord
        .storage_node()
        .test_capture_object_payload(&bucket, &key, put.version_id)
        .unwrap();
    coord
        .storage_node()
        .test_inject_object_payload_first_segment_unknown_data_pg(&payload)
        .unwrap();

    let err = match coord.get_object(&GetObjectRequest {
        sse_customer: None,
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        cond: NO_READ,
    }) {
        Ok(result) => result.body.read_all().unwrap_err(),
        Err(err) => err,
    };

    assert!(
        matches!(
            err,
            ServerError::Store(ref failure)
                if failure.class() == storage::StoreOperationFailureClass::Other
        ),
        "expected typed route error, got {err:?}"
    );
}

#[test]
fn failed_stream_put_append_commit_cleans_placed_shards() {
    let _serial = STREAM_APPEND_TEST_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let hook_storage = Arc::clone(&coord.storage_node());
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_session_id = session_id.clone();
    let _guard = coord.install_stream_append_test_hooks(StreamAppendTestHooks {
        target: Some((session_id.as_str().to_owned(), 0)),
        after_prepare: Some(Arc::new(move || {
            hook_storage
                .abort_stream_upload_session(&hook_bucket, &hook_key, &hook_session_id)
                .unwrap();
        })),
    });

    let err = coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"orphan-me")
        .unwrap_err();
    assert!(
        matches!(err, ServerError::Metadata(_)),
        "expected missing stream session after hook abort, got {err:?}"
    );
}

#[test]
fn failed_stream_put_append_cleanup_failure_traces_allowed_orphan() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let _stream_serial = STREAM_APPEND_TEST_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let observed_cleanup_errors = Arc::new(Mutex::new(Vec::new()));
    let observed_cleanup_errors_for_hook = Arc::clone(&observed_cleanup_errors);
    let _cleanup_error_guard = coord
        .storage_node()
        .test_install_best_effort_payload_cleanup_error_hook(Arc::new(move |operation, error| {
            let context = match error {
                StoreError::Io { context, .. } => *context,
                other => panic!("expected injected IO cleanup error, got {other:?}"),
            };
            observed_cleanup_errors_for_hook
                .lock()
                .unwrap()
                .push((operation, context));
        }));
    let _placed_cleanup_guard = coord
        .storage_node()
        .test_install_before_placed_payload_shard_delete_hook(Arc::new(|_shard_key| {
            Err(StoreError::Io {
                context: "injected placed cleanup delete failure",
                source: std::io::Error::other("injected placed cleanup delete failure"),
            })
        }));
    let hook_storage = Arc::clone(&coord.storage_node());
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_session_id = session_id.clone();
    let _stream_guard = coord.install_stream_append_test_hooks(StreamAppendTestHooks {
        target: Some((session_id.as_str().to_owned(), 0)),
        after_prepare: Some(Arc::new(move || {
            hook_storage
                .abort_stream_upload_session(&hook_bucket, &hook_key, &hook_session_id)
                .unwrap();
        })),
    });

    let _trace = observability::AttachedTrace::new(observability::TraceContext::new_request());
    let err = coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"orphan-me")
        .unwrap_err();
    assert!(
        matches!(err, ServerError::Metadata(_)),
        "expected missing stream session after hook abort, got {err:?}"
    );

    let observed_cleanup_errors = observed_cleanup_errors.lock().unwrap();
    assert!(
        !observed_cleanup_errors.is_empty(),
        "placed shard cleanup failure should emit typed cleanup context"
    );
    assert!(observed_cleanup_errors.iter().all(|(operation, context)| {
        *operation == "delete placed payload shard"
            && *context == "injected placed cleanup delete failure"
    }));
}

#[test]
fn stream_put_abort_ack_cleanup_failure_traces_after_placed_cleanup() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"cleanup-me")
        .unwrap();

    let observed_cleanup_errors = Arc::new(Mutex::new(Vec::new()));
    let observed_cleanup_errors_for_hook = Arc::clone(&observed_cleanup_errors);
    let _cleanup_error_guard = coord
        .storage_node()
        .test_install_best_effort_payload_cleanup_error_hook(Arc::new(move |operation, error| {
            let context = match error {
                StoreError::Io { context, .. } => *context,
                other => panic!("expected injected IO cleanup error, got {other:?}"),
            };
            observed_cleanup_errors_for_hook
                .lock()
                .unwrap()
                .push((operation, context));
        }));
    let _ack_cleanup_guard = coord
        .storage_node()
        .test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(|_shard_key| {
            Err(StoreError::Io {
                context: "injected ack cleanup delete failure",
                source: std::io::Error::other("injected ack cleanup delete failure"),
            })
        }));

    let _trace = observability::AttachedTrace::new(observability::TraceContext::new_request());
    coord
        .abort_stream_put("bucket", "key", &session_id)
        .unwrap();

    let observed_cleanup_errors = observed_cleanup_errors.lock().unwrap();
    assert!(
        !observed_cleanup_errors.is_empty(),
        "ack cleanup failure should emit typed cleanup context"
    );
    assert!(observed_cleanup_errors.iter().all(|(operation, context)| {
        *operation == "delete payload ack" && *context == "injected ack cleanup delete failure"
    }));
}

#[test]
fn stream_put_abort_cleans_segment_committed_during_abort_window() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let _stream_serial = STREAM_APPEND_TEST_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment_index = 0;
    let abort_reached_storage = Arc::new(Barrier::new(2));
    let allow_abort_storage = Arc::new(Barrier::new(2));
    let abort_reached_storage_hook = Arc::clone(&abort_reached_storage);
    let allow_abort_storage_hook = Arc::clone(&allow_abort_storage);
    let _guard = coord
        .storage_node()
        .test_install_before_stream_abort_storage_hook(Arc::new(move || {
            abort_reached_storage_hook.wait();
            allow_abort_storage_hook.wait();
        }));

    let staged_payload = thread::scope(|scope| {
        let abort = scope.spawn(|| coord.abort_stream_put("bucket", "key", &session_id));
        abort_reached_storage.wait();
        coord
            .append_plaintext_stream_segment_for_test(
                "bucket",
                "key",
                &session_id,
                segment_index,
                b"race-data",
            )
            .unwrap();
        let staged_payload = coord
            .storage_node()
            .test_capture_stream_upload_payload(&bucket, &key, &session_id)
            .unwrap();
        assert_eq!(staged_payload.segment_count(), 1);
        assert!(coord
            .storage_node()
            .test_stream_upload_payload_snapshot_is_fully_present(&staged_payload)
            .unwrap());
        allow_abort_storage.wait();
        abort.join().unwrap().unwrap();
        staged_payload
    });
    assert!(coord
        .storage_node()
        .test_stream_upload_payload_snapshot_is_fully_absent(&staged_payload)
        .unwrap());
}

#[test]
fn stream_put_get_multi_segment() {
    // Stream-put with multiple segments: GET reconstructs all segments.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"aaaa")
        .unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 1, b"bbbb")
        .unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 2, b"cc")
        .unwrap();

    let full_data = b"aaaabbbbcc";
    let crc = checksum::crc64::checksum(full_data);
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 10,
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

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), full_data);
    assert_eq!(result.size, 10);
}

#[test]
fn segment_list_reader_next_chunk_moves_whole_loaded_segment() {
    let dir = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(dir.path(), &[0]);
    let ec_shape = storage_cluster.default_payload_ec_shape();
    let runtime = ReadRuntime {
        storage: super::read_core::ReadStorage::Cluster(storage_cluster),
        payload_buffer_pool: PayloadBufferPool::new(ec_shape),
        sse_c_validator: None,
        managed_key_provider: None,
    };

    let data = vec![1u8, 2, 3, 4];
    let ptr = data.as_ptr();
    let len = data.len();
    let mut reader = SegmentListReader::test_loaded_segment(
        runtime,
        "bucket".to_string(),
        "key".to_string(),
        data,
    );

    let chunk = reader.next_chunk(len).unwrap().unwrap();
    assert_eq!(chunk.as_ref(), [1u8, 2, 3, 4]);
    assert_eq!(chunk.as_ref().as_ptr(), ptr);
    assert!(reader.test_loaded_segment_is_none());
    assert!(reader.next_chunk(len).unwrap().is_none());
}

#[test]
fn get_object_reuses_payload_buffer_for_repeated_segment_reads() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let data = vec![7u8; INTERNAL_SEGMENT_SIZE];
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, &data)
        .unwrap();
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(&data),
            total_size: data.len() as u64,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();

    assert_eq!(coord.payload_buffer_pool.allocation_count(), 0);

    let first = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(first.body.read_all().unwrap(), data);
    assert_eq!(coord.payload_buffer_pool.allocation_count(), 1);

    let second = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(second.body.read_all().unwrap(), data);
    assert_eq!(coord.payload_buffer_pool.allocation_count(), 1);
}

#[test]
fn stream_put_range_read() {
    // Range reads on stream-put objects work correctly.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"AAAA")
        .unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 1, b"BBBB")
        .unwrap();

    let full_data = b"AAAABBBB";
    let crc = checksum::crc64::checksum(full_data);
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 8,
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

    let r1 = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Range { start: 0, end: 3 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(r1.body.read_all().unwrap(), b"AAAA");

    let r2 = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Range { start: 2, end: 5 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(r2.body.read_all().unwrap(), b"AABB");

    let r3 = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Range { start: 4, end: 7 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(r3.body.read_all().unwrap(), b"BBBB");

    let r4 = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Suffix { length: 3 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(r4.body.read_all().unwrap(), b"BBB");
}

#[test]
fn buffered_put_single_segment_skips_stream_session_rows() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let result = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"tiny-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let payload = coord
        .storage_node()
        .test_capture_object_payload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            result.version_id,
        )
        .unwrap();
    assert_eq!(payload.segment_count(), 1);
    assert!(coord
        .storage_node()
        .test_object_payload_snapshot_uses_transient_direct_put_layout(&payload)
        .unwrap());
    assert!(coord
        .storage_node()
        .test_object_payload_snapshot_is_fully_present(&payload)
        .unwrap());
    assert!(coord
        .storage_node()
        .test_object_payload_snapshot_places_each_shard_on_a_distinct_node(&payload)
        .unwrap());
    assert_eq!(
        storage::test_support::stream_upload_session_count(&coord.storage_node()).unwrap(),
        0
    );

    let get = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(get.body.read_all().unwrap(), b"tiny-data");
}

#[test]
fn failed_buffered_put_before_storage_mutation_allows_followup_put() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let oversized_value = "x".repeat(u16::MAX as usize + 1);
    let oversized_metadata =
        MetadataBlob::from_headers(&[("x-amz-meta-too-large", oversized_value.as_str())]).unwrap();
    let authorized = coord
        .authorize_put_object_write(&AuthorizePutObjectRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            tags: None,
            encryption: WriteEncryptionRequest::none(),
        })
        .unwrap();
    let reserved_snapshot_loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let reserved_snapshot_loads_for_hook = Arc::clone(&reserved_snapshot_loads);
    let _reservation_guard =
        coord.install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
            bucket: Some("bucket".to_string()),
            after_loaded: Some(Arc::new(move || {
                reserved_snapshot_loads_for_hook.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            })),
            ..BucketWriteHandleTestHooks::default()
        });
    let err = coord
        .put_object_from_authorized_write(
            &AuthorizedPutObjectCommitRequest {
                data: b"tiny-data",
                metadata: &oversized_metadata,
                system_metadata: &SystemMetadata::EMPTY,
                cond: &WriteCondition::default(),
            },
            &authorized,
        )
        .unwrap_err();
    assert!(
        matches!(err, ServerError::MetadataBlobError { .. }),
        "expected metadata serialization failure, got {err:?}"
    );
    assert_eq!(
        reserved_snapshot_loads.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "metadata serialization failure must precede the first durable storage mutation"
    );

    let result = coord
        .put_object_from_authorized_write(
            &AuthorizedPutObjectCommitRequest {
                data: b"tiny-data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                cond: &WriteCondition::default(),
            },
            &authorized,
        )
        .unwrap();
    assert_eq!(
        reserved_snapshot_loads.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "successful buffered PUT must traverse the reserved bucket-write snapshot path"
    );
    let get = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(result.version_id),
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(get.body.read_all().unwrap(), b"tiny-data");
}

#[test]
fn buffered_put_post_publish_error_keeps_committed_shards() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let _guard = coord
        .storage_node()
        .test_install_direct_put_post_publish_error(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            "post-publish direct put test failure".to_string(),
        );

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "crossed-key",
                test_requester(),
                None,
            ),
            data: b"crossed-key-must-not-trigger",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .expect("subject-bound post-publish fault must ignore another key");

    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"committed-despite-error",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            ServerError::InvalidRequest { ref reason }
                if reason == "post-publish direct put test failure"
        ),
        "expected injected post-publish failure, got {err:?}"
    );

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let payload = coord
        .storage_node()
        .test_capture_object_payload(&bucket, &key, VersionId::Null)
        .unwrap();
    assert_eq!(payload.segment_count(), 1);
    assert!(coord
        .storage_node()
        .test_object_payload_snapshot_uses_transient_direct_put_layout(&payload)
        .unwrap());
    assert!(coord
        .storage_node()
        .test_object_payload_snapshot_is_fully_present(&payload)
        .unwrap());

    let get = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(get.body.read_all().unwrap(), b"committed-despite-error");
}

#[test]
fn buffered_put_exact_segment_skips_stream_session_rows() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let data = vec![0xAB; INTERNAL_SEGMENT_SIZE];
    let result = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "exact", test_requester(), None),
            data: &data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let payload = coord
        .storage_node()
        .test_capture_object_payload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("exact"),
            result.version_id,
        )
        .unwrap();
    let layout = payload.layout();
    assert_eq!(layout.len(), 1);
    assert_eq!(layout[0].size, INTERNAL_SEGMENT_SIZE as u64);
    assert_eq!(
        storage::test_support::stream_upload_session_count(&coord.storage_node()).unwrap(),
        0
    );
}

#[test]
fn encode_parity_scratch_covers_max_sse_c_segment() {
    let ec_config = EcConfig::default();
    let k = ec_config.data_shards() as usize;
    let m = ec_config.parity_shards() as usize;
    let padded = (INTERNAL_SEGMENT_SIZE + SSE_C_SEGMENT_TAG_LEN).div_ceil(k) * k;
    let expected = (padded / k) * m;
    assert_eq!(
        encode_parity_scratch_len(storage::EcShape {
            k: ec_config.data_shards(),
            m: ec_config.parity_shards(),
        }),
        expected
    );
}

#[test]
fn buffered_put_writes_object_segments() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let data = vec![0x5A; (INTERNAL_SEGMENT_SIZE * 2) + 123];
    let result = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: &data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    {
        let payload = coord
            .storage_node()
            .test_capture_object_payload(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("key"),
                result.version_id,
            )
            .unwrap();
        let layout = payload.layout();
        assert_eq!(layout.len(), 3);
        assert_eq!(layout[0].segment_index, 0);
        assert_eq!(layout[0].size, INTERNAL_SEGMENT_SIZE as u64);
        assert_eq!(layout[1].segment_index, 1);
        assert_eq!(layout[1].size, INTERNAL_SEGMENT_SIZE as u64);
        assert_eq!(layout[2].segment_index, 2);
        assert_eq!(layout[2].size, 123);
        assert_eq!(
            storage::test_support::stream_upload_session_count(&coord.storage_node()).unwrap(),
            0
        );
    }

    let get = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(get.body.read_all().unwrap(), data);
}

#[test]
fn stream_put_zero_byte_get() {
    // Zero-byte stream-put objects are readable.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "empty").unwrap();
    let crc = checksum::crc64::checksum(b"");
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "empty", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 0,
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

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "empty",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"");
    assert_eq!(result.size, 0);
}

#[test]
fn stream_put_get_object_part() {
    // partNumber=1 on stream-put objects returns the full body.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"partdata")
        .unwrap();
    let crc = checksum::crc64::checksum(b"partdata");
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 8,
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

    let result = coord
        .get_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            part_number: 1,
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"partdata");
    assert_eq!(result.part_size, 8);
}
