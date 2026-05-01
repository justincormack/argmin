use super::test_helpers;
use super::test_support::*;
use super::*;
use ec::EcConfig;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use storage::{segment_key_hash, EcShape, GenerationId, PgTopology, ShardKey, StoreError};

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

    let segments = coord
        .storage_node
        .test_get_object_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            VersionId::Null,
        )
        .unwrap();
    assert_eq!(segments.len(), 1);
    let segment = &segments[0];
    let ec = EcShape {
        k: segment.ec_k,
        m: segment.ec_m,
    };
    let mut placed_node_dirs = BTreeSet::new();
    for shard_index in 0..ec.k + ec.m {
        let path = coord
            .storage_node
            .test_payload_shard_file_path(
                segment.data_pg_id,
                ec,
                &segment.segment_okh,
                segment.segment_vid,
                shard_index,
            )
            .unwrap();
        assert!(
            path.exists(),
            "stream PUT shard {shard_index} should exist at {}",
            path.display()
        );
        let node_dir = path
            .ancestors()
            .nth(4)
            .expect("payload shard path should include a node directory")
            .file_name()
            .unwrap()
            .to_owned();
        placed_node_dirs.insert(node_dir);
    }
    assert_eq!(placed_node_dirs.len(), usize::from(ec.k + ec.m));

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
    let mut segments = coord
        .storage_node
        .test_get_object_segments(&bucket, &key, put.version_id)
        .unwrap();
    assert_eq!(segments.len(), 1);
    segments[0].data_pg_id = 99;
    coord
        .storage_node
        .test_replace_live_object_segments(&bucket, &key, put.version_id, &segments)
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
    let err = result.body.read_all().unwrap_err();

    assert!(
        matches!(
            err,
            ServerError::Store(StoreError::ClusterPgNotFound {
                pg_id: 99,
                cluster_epoch: storage::ClusterEpoch::INITIAL,
            })
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
    let generation_id = coord
        .storage_node
        .test_object_generation_reservation_for(&bucket, &key, &session_id)
        .unwrap();
    let segment_okh = segment_key_hash("bucket", "key", generation_id, 0);
    let segment_vid = GenerationId::new(1).unwrap();
    let ec = coord.storage_node.default_payload_ec_shape();
    let data_pg_id = PgTopology::new(coord.storage_node.test_pg_ids())
        .unwrap()
        .object_generation_segment_data_pg(&bucket, &key, generation_id, 0)
        .get();
    let hook_storage = Arc::clone(&coord.storage_node);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_session_id = session_id.clone();
    let _guard = install_stream_append_test_hooks(StreamAppendTestHooks {
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

    for shard_index in 0..ec.k + ec.m {
        let shard_key = ShardKey::new(&segment_okh, segment_vid.get(), shard_index);
        assert!(
            !coord
                .storage_node
                .test_shard_exists(data_pg_id, &shard_key)
                .unwrap(),
            "failed stream append must remove shard metadata {shard_index}"
        );
        assert!(
            !coord
                .storage_node
                .test_payload_shard_file_exists(
                    data_pg_id,
                    ec,
                    &segment_okh,
                    segment_vid,
                    shard_index,
                )
                .unwrap(),
            "failed stream append must remove placed shard file {shard_index}"
        );
    }
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
    let generation_id = coord
        .storage_node
        .test_object_generation_reservation_for(&bucket, &key, &session_id)
        .unwrap();
    let segment_okh = segment_key_hash("bucket", "key", generation_id, 0);
    let segment_vid = GenerationId::new(1).unwrap();
    let ec = coord.storage_node.default_payload_ec_shape();
    let data_pg_id = PgTopology::new(coord.storage_node.test_pg_ids())
        .unwrap()
        .object_generation_segment_data_pg(&bucket, &key, generation_id, 0)
        .get();

    let observed_cleanup_errors = Arc::new(Mutex::new(Vec::new()));
    let observed_cleanup_errors_for_hook = Arc::clone(&observed_cleanup_errors);
    let _cleanup_error_guard = coord
        .storage_node
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
        .storage_node
        .test_install_before_placed_payload_shard_delete_hook(Arc::new(|_shard_key| {
            Err(StoreError::Io {
                context: "injected placed cleanup delete failure",
                source: std::io::Error::other("injected placed cleanup delete failure"),
            })
        }));
    let hook_storage = Arc::clone(&coord.storage_node);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_session_id = session_id.clone();
    let _stream_guard = install_stream_append_test_hooks(StreamAppendTestHooks {
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
    assert_eq!(
        observed_cleanup_errors.len(),
        usize::from(ec.k + ec.m),
        "each placed shard cleanup failure should emit typed cleanup context"
    );
    assert!(observed_cleanup_errors.iter().all(|(operation, context)| {
        *operation == "delete placed payload shard"
            && *context == "injected placed cleanup delete failure"
    }));
    drop(observed_cleanup_errors);

    let mut remaining_placed_files = 0usize;
    for shard_index in 0..ec.k + ec.m {
        let shard_key = ShardKey::new(&segment_okh, segment_vid.get(), shard_index);
        assert!(
            !coord
                .storage_node
                .test_shard_exists(data_pg_id, &shard_key)
                .unwrap(),
            "best-effort failure should still remove ack metadata {shard_index}"
        );
        if coord
            .storage_node
            .test_payload_shard_file_exists(data_pg_id, ec, &segment_okh, segment_vid, shard_index)
            .unwrap()
        {
            remaining_placed_files += 1;
        }
    }
    assert_eq!(
        remaining_placed_files,
        usize::from(ec.k + ec.m),
        "injected best-effort failure should leave every placed shard as orphan state"
    );
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
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let generation_id = coord
        .storage_node
        .test_object_generation_reservation_for(&bucket, &key, &session_id)
        .unwrap();
    let segment_okh = segment_key_hash("bucket", "key", generation_id, 0);
    let segment_vid = GenerationId::new(1).unwrap();
    let ec = coord.storage_node.default_payload_ec_shape();
    let data_pg_id = PgTopology::new(coord.storage_node.test_pg_ids())
        .unwrap()
        .object_generation_segment_data_pg(&bucket, &key, generation_id, 0)
        .get();

    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"cleanup-me")
        .unwrap();
    for shard_index in 0..ec.k + ec.m {
        let shard_key = ShardKey::new(&segment_okh, segment_vid.get(), shard_index);
        assert!(
            coord
                .storage_node
                .test_shard_exists(data_pg_id, &shard_key)
                .unwrap(),
            "staged stream append should publish ack row {shard_index}"
        );
        assert!(
            coord
                .storage_node
                .test_payload_shard_file_exists(
                    data_pg_id,
                    ec,
                    &segment_okh,
                    segment_vid,
                    shard_index,
                )
                .unwrap(),
            "staged stream append should publish placed file {shard_index}"
        );
    }

    let observed_cleanup_errors = Arc::new(Mutex::new(Vec::new()));
    let observed_cleanup_errors_for_hook = Arc::clone(&observed_cleanup_errors);
    let _cleanup_error_guard = coord
        .storage_node
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
        .storage_node
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
    assert_eq!(
        observed_cleanup_errors.len(),
        usize::from(ec.k + ec.m),
        "each ack cleanup failure should emit typed cleanup context"
    );
    assert!(observed_cleanup_errors.iter().all(|(operation, context)| {
        *operation == "delete payload ack" && *context == "injected ack cleanup delete failure"
    }));
    drop(observed_cleanup_errors);

    for shard_index in 0..ec.k + ec.m {
        let shard_key = ShardKey::new(&segment_okh, segment_vid.get(), shard_index);
        assert!(
            coord
                .storage_node
                .test_shard_exists(data_pg_id, &shard_key)
                .unwrap(),
            "ack cleanup failure should leave payload ack row {shard_index}"
        );
        assert!(
            !coord
                .storage_node
                .test_payload_shard_file_exists(
                    data_pg_id,
                    ec,
                    &segment_okh,
                    segment_vid,
                    shard_index,
                )
                .unwrap(),
            "ack cleanup failure should not prevent placed cleanup for shard {shard_index}"
        );
    }
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
    let generation_id = coord
        .storage_node
        .test_object_generation_reservation_for(&bucket, &key, &session_id)
        .unwrap();
    let segment_index = 0;
    let segment_okh = segment_key_hash("bucket", "key", generation_id, segment_index);
    let segment_vid = GenerationId::MIN;
    let ec = coord.storage_node.default_payload_ec_shape();
    let data_pg_id = PgTopology::new(coord.storage_node.test_pg_ids())
        .unwrap()
        .object_generation_segment_data_pg(&bucket, &key, generation_id, segment_index)
        .get();
    let hook_storage = Arc::clone(&coord.storage_node);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_session_id = session_id.clone();
    let _guard = coord
        .storage_node
        .test_install_before_stream_abort_storage_hook(Arc::new(move || {
            let data = b"race-data";
            let (_, segment_record) = hook_storage
                .prepare_stream_segment_append(
                    &hook_bucket,
                    &hook_key,
                    &storage::PrepareStreamUploadSegmentAppendReq {
                        session_id: hook_session_id.clone(),
                        segment_index,
                        size: data.len() as u64,
                        segment_crc64: Some(checksum::crc64::checksum(data)),
                        segment_okh: storage::stream_segment_key_hash(
                            &hook_session_id,
                            segment_index,
                        ),
                    },
                )
                .unwrap();
            let written_shards = hook_storage
                .write_stream_segment_payload_shards(&segment_record, data)
                .unwrap();
            let shard_batch: Vec<(&ShardKey, storage::WriteAck)> = written_shards
                .iter()
                .map(|written| (&written.key, written.ack))
                .collect();
            hook_storage
                .commit_stream_segment_append(
                    &hook_bucket,
                    &hook_key,
                    &hook_session_id,
                    segment_index,
                    &segment_record,
                    &shard_batch,
                )
                .unwrap();
        }));

    coord
        .abort_stream_put("bucket", "key", &session_id)
        .unwrap();

    for shard_index in 0..ec.k + ec.m {
        let shard_key = ShardKey::new(&segment_okh, segment_vid.get(), shard_index);
        assert!(
            !coord
                .storage_node
                .test_shard_exists(data_pg_id, &shard_key)
                .unwrap(),
            "abort must remove shard metadata committed during abort window {shard_index}"
        );
        assert!(
            !coord
                .storage_node
                .test_payload_shard_file_exists(
                    data_pg_id,
                    ec,
                    &segment_okh,
                    segment_vid,
                    shard_index,
                )
                .unwrap(),
            "abort must remove placed shard file committed during abort window {shard_index}"
        );
    }
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
        storage_node: storage_cluster,
        #[cfg(test)]
        pg_topology: PgTopology::new(&[0]).unwrap(),
        payload_buffer_pool: PayloadBufferPool::new(ec_shape),
        sse_c_validator: None,
        managed_key_provider: None,
    };

    let data = vec![1u8, 2, 3, 4];
    let ptr = data.as_ptr();
    let len = data.len();
    let mut reader = SegmentListReader {
        runtime,
        bucket: "bucket".to_string(),
        key: "key".to_string(),
        segments: vec![],
        next_segment_index: 0,
        loaded_segment: Some((Arc::new(SharedPayloadBuffer::from_unpooled(data)), 0, len)),
        sse_customer_request: None,
    };

    let chunk = reader.next_chunk(len).unwrap().unwrap();
    assert_eq!(chunk.as_ref(), [1u8, 2, 3, 4]);
    assert_eq!(chunk.as_ref().as_ptr(), ptr);
    assert!(reader.loaded_segment.is_none());
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

    let segments = coord
        .storage_node
        .test_get_object_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            result.version_id,
        )
        .unwrap();
    assert_eq!(segments.len(), 1);
    let live = coord
        .storage_node
        .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
        .unwrap()
        .as_live()
        .expect("buffered put should create a live object")
        .clone();
    let topology = PgTopology::new(coord.storage_node.test_pg_ids()).unwrap();
    assert_eq!(
        segments[0].segment_okh,
        segment_key_hash("bucket", "key", live.generation_id, 0)
    );
    assert_eq!(segments[0].segment_vid, live.generation_id);
    assert_eq!(
        segments[0].data_pg_id,
        topology
            .object_generation_segment_data_pg(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("key"),
                live.generation_id,
                0,
            )
            .get()
    );

    let segment = &segments[0];
    let ec = EcShape {
        k: segment.ec_k,
        m: segment.ec_m,
    };
    let mut placed_node_dirs = BTreeSet::new();
    for shard_index in 0..ec.k + ec.m {
        let path = coord
            .storage_node
            .test_payload_shard_file_path(
                segment.data_pg_id,
                ec,
                &segment.segment_okh,
                segment.segment_vid,
                shard_index,
            )
            .unwrap();
        assert!(
            path.exists(),
            "direct PUT shard {shard_index} should exist at {}",
            path.display()
        );
        let node_dir = path
            .ancestors()
            .nth(4)
            .expect("payload shard path should include a node directory")
            .file_name()
            .unwrap()
            .to_owned();
        placed_node_dirs.insert(node_dir);
    }
    assert_eq!(placed_node_dirs.len(), usize::from(ec.k + ec.m));
    assert!(coord
        .storage_node
        .test_list_all_stream_uploads()
        .unwrap()
        .is_empty());

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
fn failed_buffered_put_before_commit_leaves_no_generation_reservation_or_shards() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let oversized_value = "x".repeat(u16::MAX as usize + 1);
    let oversized_metadata =
        MetadataBlob::from_headers(&[("x-amz-meta-too-large", oversized_value.as_str())]).unwrap();
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"tiny-data",
            metadata: &oversized_metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::MetadataBlobError { .. }),
        "expected metadata serialization failure, got {err:?}"
    );

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let generation_id = GenerationId::MIN;
    let segment_okh = segment_key_hash("bucket", "key", generation_id, 0);
    let topology = PgTopology::new(coord.storage_node.test_pg_ids()).unwrap();
    let data_pg_id = topology
        .object_generation_segment_data_pg(&bucket, &key, generation_id, 0)
        .get();
    let ec = coord.storage_node.default_payload_ec_shape();
    for shard_index in 0..ec.k + ec.m {
        let shard_key = ShardKey::new(&segment_okh, generation_id.get(), shard_index);
        assert!(
            !coord
                .storage_node
                .test_shard_exists(data_pg_id, &shard_key)
                .unwrap(),
            "failed direct PUT must not leave shard {shard_index}"
        );
        assert!(
            !coord
                .storage_node
                .test_payload_shard_file_exists(
                    data_pg_id,
                    ec,
                    &segment_okh,
                    generation_id,
                    shard_index,
                )
                .unwrap(),
            "failed direct PUT must not leave placed shard file {shard_index}"
        );
    }

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
    let live = coord
        .storage_node
        .test_get_object_meta(&bucket, &key)
        .unwrap()
        .as_live()
        .expect("second put should create a live object")
        .clone();
    assert_eq!(live.version_id, result.version_id);
    assert_eq!(live.generation_id, GenerationId::MIN);
}

#[test]
fn buffered_put_post_publish_error_keeps_committed_shards() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let _guard = coord
        .storage_node
        .test_install_after_direct_put_metadata_publish_hook(Arc::new(|| {
            Err(storage::ObjectPgActionError::InvalidRequest {
                reason: "post-publish direct put test failure".to_string(),
            })
        }));

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
    let live = coord
        .storage_node
        .test_get_object_meta(&bucket, &key)
        .unwrap()
        .as_live()
        .expect("post-publish failure should leave the object visible")
        .clone();
    assert_eq!(live.generation_id, GenerationId::MIN);

    let segments = coord
        .storage_node
        .test_get_object_segments(&bucket, &key, VersionId::Null)
        .unwrap();
    assert_eq!(segments.len(), 1);
    let segment = &segments[0];
    let ec = EcShape {
        k: segment.ec_k,
        m: segment.ec_m,
    };
    for shard_index in 0..ec.k + ec.m {
        let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), shard_index);
        assert!(
            coord
                .storage_node
                .test_shard_exists(segment.data_pg_id, &shard_key)
                .unwrap(),
            "post-publish failure must keep shard metadata {shard_index}"
        );
        assert!(
            coord
                .storage_node
                .test_payload_shard_file_exists(
                    segment.data_pg_id,
                    ec,
                    &segment.segment_okh,
                    segment.segment_vid,
                    shard_index,
                )
                .unwrap(),
            "post-publish failure must keep placed shard file {shard_index}"
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

    let segments = coord
        .storage_node
        .test_get_object_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("exact"),
            result.version_id,
        )
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].size, INTERNAL_SEGMENT_SIZE as u64);
    assert!(coord
        .storage_node
        .test_list_all_stream_uploads()
        .unwrap()
        .is_empty());
}

#[test]
fn encode_parity_scratch_covers_max_sse_c_segment() {
    let ec_config = EcConfig::default();
    let k = ec_config.data_shards as usize;
    let m = ec_config.parity_shards as usize;
    let padded = (INTERNAL_SEGMENT_SIZE + SSE_C_SEGMENT_TAG_LEN).div_ceil(k) * k;
    let expected = (padded / k) * m;
    assert_eq!(
        encode_parity_scratch_len(storage::EcShape {
            k: ec_config.data_shards,
            m: ec_config.parity_shards,
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
        let segments = coord
            .storage_node
            .test_get_object_segments(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("key"),
                result.version_id,
            )
            .unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].segment_index, 0);
        assert_eq!(segments[0].size, INTERNAL_SEGMENT_SIZE as u64);
        assert_eq!(segments[1].segment_index, 1);
        assert_eq!(segments[1].size, INTERNAL_SEGMENT_SIZE as u64);
        assert_eq!(segments[2].segment_index, 2);
        assert_eq!(segments[2].size, 123);
        assert!(coord
            .storage_node
            .test_list_all_stream_uploads()
            .unwrap()
            .is_empty());
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
