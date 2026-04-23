use super::test_helpers;
use super::test_support::*;
use super::*;

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
    let ec_config = EcConfig::default();
    let runtime = ReadRuntime {
        storage_node: Arc::new(SharedStorageNode::open(dir.path(), &[0]).unwrap()),
        ec_codec: Arc::new(ErasureCodec::new(ec_config).unwrap()),
        ec_config,
        pg_topology: PgTopology::new(&[0]).unwrap(),
        payload_buffer_pool: PayloadBufferPool::new(ec_config),
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

    let meta_pg_id = coord.object_pg_id("bucket", "key");
    let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
    let segments = pg
        .get_object_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            result.version_id,
        )
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert!(pg.list_all_stream_uploads().unwrap().is_empty());
    drop(pg);

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

    let meta_pg_id = coord.object_pg_id("bucket", "exact");
    let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
    let segments = pg
        .get_object_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("exact"),
            result.version_id,
        )
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].size, INTERNAL_SEGMENT_SIZE as u64);
    assert!(pg.list_all_stream_uploads().unwrap().is_empty());
}

#[test]
fn encode_parity_scratch_covers_max_sse_c_segment() {
    let ec_config = EcConfig::default();
    let k = ec_config.data_shards as usize;
    let m = ec_config.parity_shards as usize;
    let padded = (INTERNAL_SEGMENT_SIZE + SSE_C_SEGMENT_TAG_LEN).div_ceil(k) * k;
    let expected = (padded / k) * m;

    assert_eq!(encode_parity_scratch_len(ec_config), expected);
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
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let segments = pg
            .get_object_segments(
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
        assert!(pg.list_all_stream_uploads().unwrap().is_empty());
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
