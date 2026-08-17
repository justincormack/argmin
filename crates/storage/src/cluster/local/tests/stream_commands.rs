// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cluster::request_ops::MetadataCommandApplyProgress;
use crate::cluster::{segment_payload_placement_key, StreamAppendCommitRequest};
use crate::metadata_command::ReleaseObjectGenerationCommand;
use crate::test_support::StorageClusterFailureTestSupport as _;
use crate::BucketSnapshotLoadError;

struct StreamPayloadCleanupFixture {
    _tmp: test_util::TempDir,
    cluster: Arc<crate::StorageCluster>,
    bucket: BucketName,
    key: ObjectKey,
    data_pg: u32,
    generation_id: GenerationId,
    ec: EcShape,
    session_id: crate::SessionId,
}

fn stream_payload_cleanup_fixture(session_byte: &str) -> StreamPayloadCleanupFixture {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec).unwrap();
    let (bucket, key, _, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from(session_byte.repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let generation_id = cluster
        .test_object_generation_reservation_for(&bucket, &key, &session_id)
        .unwrap();
    let data_pg = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .object_generation_segment_data_pg(&bucket, &key, generation_id, 0)
        .get();
    StreamPayloadCleanupFixture {
        _tmp: tmp,
        cluster,
        bucket,
        key,
        data_pg,
        generation_id,
        ec,
        session_id,
    }
}

fn assert_stream_payload_shard_state(
    fixture: &StreamPayloadCleanupFixture,
    expect_ack: bool,
    expect_file: bool,
) {
    let segment_okh = crate::segment_key_hash(
        fixture.bucket.as_str(),
        fixture.key.as_str(),
        fixture.generation_id,
        0,
    );
    let segment_vid = GenerationId::MIN;
    for shard_index in 0..fixture.ec.k + fixture.ec.m {
        let shard_key = ShardKey::new(&segment_okh, segment_vid.get(), shard_index);
        assert_eq!(
            fixture
                .cluster
                .test_shard_exists(fixture.data_pg, &shard_key)
                .unwrap(),
            expect_ack,
            "unexpected durable acknowledgement state for shard {shard_index}"
        );
        assert_eq!(
            fixture
                .cluster
                .test_payload_shard_file_exists(
                    fixture.data_pg,
                    fixture.ec,
                    &segment_okh,
                    segment_vid,
                    shard_index,
                )
                .unwrap(),
            expect_file,
            "unexpected placed-file state for shard {shard_index}"
        );
    }
}

#[test]
fn failed_stream_append_after_session_abort_removes_all_staged_payload() {
    let _serial = lock_payload_cleanup_hook_test();
    let fixture = stream_payload_cleanup_fixture("a1");
    let cleanup_attempts = Arc::new(AtomicUsize::new(0));
    let cleanup_attempts_for_hook = Arc::clone(&cleanup_attempts);
    let _cleanup_guard = fixture
        .cluster
        .test_install_before_placed_payload_shard_delete_hook(Arc::new(move |_| {
            cleanup_attempts_for_hook.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
    let _trace = observability::AttachedTrace::new(observability::TraceContext::new_request());

    let error = fixture
        .cluster
        .test_append_stream_segment_with_after_prepare(
            &fixture.bucket,
            &fixture.key,
            crate::StreamSegmentAppendInput {
                session_id: &fixture.session_id,
                segment_index: 0,
                payload_crc64: checksum::crc64::checksum(b"orphan-me"),
                storage_bytes: b"orphan-me",
            },
            || {
                fixture
                    .cluster
                    .abort_stream_upload_session(&fixture.bucket, &fixture.key, &fixture.session_id)
                    .unwrap();
            },
        )
        .unwrap_err();
    assert!(matches!(error, crate::ObjectPgActionError::Metadata(_)));
    assert_eq!(
        cleanup_attempts.load(Ordering::SeqCst),
        usize::from(fixture.ec.k + fixture.ec.m)
    );
    assert_stream_payload_shard_state(&fixture, false, false);
}

#[test]
fn failed_stream_append_placed_cleanup_failure_leaves_only_files() {
    let _serial = lock_payload_cleanup_hook_test();
    let fixture = stream_payload_cleanup_fixture("a2");
    let cleanup_failure = fixture.cluster.test_fail_placed_payload_shard_cleanup();
    let _trace = observability::AttachedTrace::new(observability::TraceContext::new_request());

    let error = fixture
        .cluster
        .test_append_stream_segment_with_after_prepare(
            &fixture.bucket,
            &fixture.key,
            crate::StreamSegmentAppendInput {
                session_id: &fixture.session_id,
                segment_index: 0,
                payload_crc64: checksum::crc64::checksum(b"orphan-me"),
                storage_bytes: b"orphan-me",
            },
            || {
                fixture
                    .cluster
                    .abort_stream_upload_session(&fixture.bucket, &fixture.key, &fixture.session_id)
                    .unwrap();
            },
        )
        .unwrap_err();
    assert!(matches!(error, crate::ObjectPgActionError::Metadata(_)));
    assert_eq!(
        cleanup_failure.invocation_count(),
        usize::from(fixture.ec.k + fixture.ec.m)
    );
    assert_stream_payload_shard_state(&fixture, false, true);
}

#[test]
fn stream_abort_ack_cleanup_failure_removes_files_and_retains_acknowledgements() {
    let _serial = lock_payload_cleanup_hook_test();
    let fixture = stream_payload_cleanup_fixture("a3");
    fixture
        .cluster
        .test_append_stream_segment_with_after_prepare(
            &fixture.bucket,
            &fixture.key,
            crate::StreamSegmentAppendInput {
                session_id: &fixture.session_id,
                segment_index: 0,
                payload_crc64: checksum::crc64::checksum(b"cleanup-me"),
                storage_bytes: b"cleanup-me",
            },
            || {},
        )
        .unwrap();
    assert_stream_payload_shard_state(&fixture, true, true);

    let cleanup_failure = fixture.cluster.test_fail_payload_ack_cleanup();
    let _trace = observability::AttachedTrace::new(observability::TraceContext::new_request());

    fixture
        .cluster
        .abort_stream_upload_session(&fixture.bucket, &fixture.key, &fixture.session_id)
        .unwrap();
    assert_eq!(
        cleanup_failure.invocation_count(),
        usize::from(fixture.ec.k + fixture.ec.m)
    );
    assert_stream_payload_shard_state(&fixture, true, false);
}

fn pending_release_command(
    cluster: &crate::StorageCluster,
    map: &LocalClusterMap,
    pg_id: PgId,
    bucket: &BucketName,
    key: &ObjectKey,
    reservation_id: &str,
) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReleaseObjectGeneration(ReleaseObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            crate::SessionId::try_from(reservation_id.repeat(16)).unwrap(),
        )),
    )
}

#[test]
fn applied_stream_create_matching_binds_requested_cleanup_deadline_for_both_targets() {
    let bucket = crate::tests::bucket_name("applied-stream-cleanup-bucket");
    let key = crate::tests::object_key("applied-stream-cleanup-key");
    let upload_id = crate::tests::multipart_upload_id("applied-stream-cleanup-upload");
    for (index, target, operation_kind) in [
        (
            1,
            crate::StreamUploadTarget::PutObject,
            crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        ),
        (
            2,
            crate::StreamUploadTarget::UploadPart {
                upload_id,
                part_number: 1,
            },
            crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        ),
    ] {
        let request = crate::CreateStreamUploadReq {
            session_id: crate::SessionId::try_from(format!("{index:02x}").repeat(16)).unwrap(),
            bucket: bucket.clone(),
            key: key.clone(),
            target,
            encryption: crate::ObjectEncryption::None,
        };
        let proof = crate::metadata_command::BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: format!("reservation-{index}"),
            owner_token: format!("owner-{index}"),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: operation_kind.to_string(),
            created_at: 10,
            lease_deadline: 20,
            target_context: Some(key.as_str().to_string()),
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(0),
                MetadataCommandLogIndex::new(index).unwrap(),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation_and_cleanup_deadline(
                    request.clone(),
                    10,
                    Some(100),
                    proof,
                ),
            )),
        );

        assert!(super::super::super::applied_stream_create_command(
            std::slice::from_ref(&command),
            &request,
            Some(100),
        )
        .is_some());
        assert!(
            super::super::super::applied_stream_create_command(
                std::slice::from_ref(&command),
                &request,
                Some(101),
            )
            .is_none(),
            "target {index} must not reuse a contender with a different cleanup deadline"
        );
    }
}

#[test]
fn stream_put_create_partial_apply_retry_reuses_existing_session() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("34".repeat(16)).unwrap();
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateStreamUpload(create)
                    if create.session.session_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected stream create metadata command replica apply failure",
                        source: std::io::Error::other(
                            "injected stream create metadata command replica apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .create_put_object_stream_session_raw(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    drop(hook_guard);

    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial stream create command must remain pending"
    );
    let primary_created_at = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        crate::PgMetadataStore::get_stream_upload(&*pg, &session_id)
            .unwrap()
            .created_at
    };
    {
        let failed_replica = map.node(NodeId::new(2)).unwrap().storage_node();
        let pg = failed_replica.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }

    let retry_value = cluster
        .create_put_object_stream_session_raw(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>((7_u8, create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(retry_value, 7);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
        assert_eq!(session.created_at, primary_created_at);
        assert!(matches!(
            session.target,
            crate::StreamUploadTarget::PutObject
        ));
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id,
            )
            .unwrap(),
            crate::GenerationId::MIN
        );
    }
}

#[test]
fn stream_put_create_retry_rejects_same_request_with_mismatched_created_at() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("7a".repeat(16)).unwrap();
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };

    cluster
        .create_put_object_stream_session_raw(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    {
        let primary = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        pg.test_force_stream_upload_created_at(&session_id, session.created_at.saturating_add(1))
            .unwrap();
    }

    let err = cluster
        .create_put_object_stream_session_raw(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::InvariantViolation {
                context: "create stream upload existing session mismatch",
                ..
            })
        ),
        "expected exact stream session row mismatch, got {err:?}"
    );
}

#[test]
fn stream_put_create_retry_rejects_same_request_with_mismatched_allocator_floor() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("7b".repeat(16)).unwrap();
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };

    cluster
        .create_put_object_stream_session_raw(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    {
        let primary = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        pg.test_force_stream_upload_next_segment_vid(
            &session_id,
            crate::GenerationId::new(2).unwrap(),
        )
        .unwrap();
    }

    let err = cluster
        .create_put_object_stream_session_raw(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::InvariantViolation {
                context: "create stream upload existing session mismatch",
                ..
            })
        ),
        "expected explicit initial allocator floor mismatch, got {err:?}"
    );
}

#[test]
fn stream_put_create_drains_unrelated_pending_create_before_new_session() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let first_session_id = crate::SessionId::try_from("37".repeat(16)).unwrap();
    let second_session_id = crate::SessionId::try_from("38".repeat(16)).unwrap();
    let first_create = crate::CreateStreamUploadReq {
        session_id: first_session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };
    let second_create = crate::CreateStreamUploadReq {
        session_id: second_session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = first_session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateStreamUpload(create)
                    if create.session.session_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "apply metadata command",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected stream create metadata command apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .create_put_object_stream_session_raw(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), first_create.clone()))
            },
        )
        .expect("published stream create must hand trailing convergence to recovery")
        .expect("stream-create preparation should produce an outcome");
    drop(hook_guard);

    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial first stream create command must remain pending"
    );

    let value = cluster
        .create_put_object_stream_session_raw(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>((9_u8, second_create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(value, 9);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        crate::PgMetadataStore::get_stream_upload(&*pg, &first_session_id).unwrap();
        crate::PgMetadataStore::get_stream_upload(&*pg, &second_session_id).unwrap();
    }
}

#[test]
fn stream_put_create_retries_after_pending_install_conflict() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("39".repeat(16)).unwrap();
    let unrelated_session_id = crate::SessionId::try_from("3a".repeat(16)).unwrap();
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };

    let pg_id = PgId::new(object_pg);
    let _serial = lock_metadata_command_apply_hook_test();
    let injected = Arc::new(AtomicBool::new(false));
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let injected_for_hook = Arc::clone(&injected);
    let map_for_hook = Arc::clone(&map);
    let bucket_for_hook = bucket.clone();
    let key_for_hook = key.clone();
    let session_for_hook = unrelated_session_id.clone();
    let proof_for_hook = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let command_epoch = cluster.operation_epoch();
    let _hook_guard = cluster.test_install_before_stream_put_create_pending_install_hook(
            Arc::new(move || {
                if injected_for_hook.swap(true, Ordering::SeqCst) {
                    return;
                }
                let command = MetadataCommandEnvelope::new(
                    crate::metadata_command::MetadataCommandId::new(
                        command_epoch,
                        pg_id,
                        map_for_hook.test_next_metadata_command_log_index(pg_id),
                    ),
                    MetadataCommandPayload::CreateStreamUpload(Box::new(
                        crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                            crate::CreateStreamUploadReq {
                                session_id: session_for_hook.clone(),
                                bucket: bucket_for_hook.clone(),
                                key: key_for_hook.clone(),
                                target: crate::StreamUploadTarget::PutObject,
                                encryption: crate::ObjectEncryption::None,
                            },
                            123,
                            proof_for_hook.clone(),
                        ),
                    )),
                );
                insert_pending_metadata_command_for_test(
                    &map_for_hook,
                    pg_id,
                    &bucket_for_hook,
                    &command,
                );
            }),
        );

    let attempts_for_action = Arc::clone(&attempts);
    let value = cluster
        .create_put_object_stream_session_raw(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                attempts_for_action.fetch_add(1, Ordering::SeqCst);
                assert!(existing_object.is_none());
                Ok::<_, ()>((13_u8, create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(value, 13);
    assert!(injected.load(Ordering::SeqCst));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "stream creation reruns request action after pending-slot contention"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
        let unrelated =
            crate::PgMetadataStore::get_stream_upload(&*pg, &unrelated_session_id).unwrap();
        assert_eq!(unrelated.bucket, bucket);
        assert_eq!(unrelated.key, key);
    }
}

#[test]
fn stream_abort_yields_after_one_pending_drain_before_budget_recheck() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let map = Arc::new(map);
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("73".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let pg_id = PgId::new(object_pg);
    let first = pending_release_command(&cluster, &map, pg_id, &bucket, &key, "74");
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &first);

    let inserted = Arc::new(Mutex::new(None));
    let hook_cluster = Arc::clone(&cluster);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let inserted_for_hook = Arc::clone(&inserted);
    let _hook = cluster.test_install_after_metadata_command_drain_hook(Arc::new(move || {
        let second = pending_release_command(
            &hook_cluster,
            &hook_map,
            pg_id,
            &hook_bucket,
            &hook_key,
            "75",
        );
        insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &second);
        *inserted_for_hook.lock().unwrap() = Some(second);
    }));

    let error = cluster
        .test_abort_stream_upload_session_with_max_attempts(&bucket, &key, &session_id, 1)
        .unwrap_err();
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
            context: "stream abort initial pending drain retry budget exhausted"
        })
    ));
    let inserted = inserted.lock().unwrap().clone().unwrap();
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(inserted),
        "stream abort must return to its budget boundary after one contender"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).is_ok());
    }
}

#[test]
fn stream_abort_missing_session_does_not_succeed_after_unrelated_pending_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let missing_session_id = crate::SessionId::try_from("35".repeat(16)).unwrap();
    let unrelated_session_id = crate::SessionId::try_from("36".repeat(16)).unwrap();
    let pg_id = PgId::new(object_pg);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                cluster.operation_epoch(),
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    crate::CreateStreamUploadReq {
                        session_id: unrelated_session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                    123,
                    proof,
                ),
            )),
        );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let error = cluster
        .abort_stream_upload_session(&bucket, &key, &missing_session_id)
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::ObjectPgActionError::Metadata(
                crate::MetadataError::StreamSessionNotFound { .. }
            )
        ),
        "unrelated pending command must not make missing stream abort idempotent: {error:?}"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let session =
            crate::PgMetadataStore::get_stream_upload(&*pg, &unrelated_session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
    }
}

#[test]
fn stream_put_append_published_log_gap_keeps_payload_for_pending_retry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("33".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append partial apply";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [89; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch: Vec<(&crate::ShardKey, crate::WriteAck)> = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AppendStreamSegment(append)
                    if append.segment.session_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::MetadataCommandLogGap {
                        node_id: node_id.as_u32(),
                        pg_id: command.id().pg_id().get(),
                        cluster_epoch: command.id().cluster_epoch(),
                        log_index: command.id().log_index().get(),
                        expected_log_index: command.id().log_index().get() - 1,
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial stream append command must remain pending"
    );
    {
        let failed_replica = map.node(NodeId::new(2)).unwrap().storage_node();
        let pg = failed_replica.get_pg(object_pg).unwrap();
        assert!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id)
                .unwrap()
                .is_empty()
        );
    }
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![segment.clone()]
        );
    }

    let pending_command = pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket)
        .expect("partial stream append command must remain pending");
    let drain_attempts = Arc::new(AtomicUsize::new(0));
    let drain_attempts_for_hook = Arc::clone(&drain_attempts);
    let pending_command_id = pending_command.id();
    let drain_hook = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if node_id != NodeId::new(2) || command.id() != pending_command_id {
                return Ok(());
            }
            let attempt = drain_attempts_for_hook.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                return Err(StoreError::MetadataCommandLogGap {
                    node_id: node_id.as_u32(),
                    pg_id: command.id().pg_id().get(),
                    cluster_epoch: command.id().cluster_epoch(),
                    log_index: command.id().log_index().get(),
                    expected_log_index: command.id().log_index().get() - 1,
                });
            }
            Err(StoreError::MetadataCommandLogChecksumMismatch {
                node_id: node_id.as_u32(),
                pg_id: command.id().pg_id().get(),
                cluster_epoch: command.id().cluster_epoch(),
                log_index: command.id().log_index().get(),
                stored_checksum: 1,
                computed_checksum: 2,
            })
        },
    ));
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id_from_label("publishedappendgap"),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    let error = cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, _existing_object| Ok::<_, ()>(((), create.clone())),
        )
        .expect_err("unrelated published command must defer multipart creation");
    drop(drain_hook);
    assert_eq!(drain_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        crate::BucketSnapshotLoadFailure::from(error).into_kind(),
        crate::BucketSnapshotLoadFailureKind::MetadataCommandContention,
        "unrelated published work must map through the bucket snapshot boundary as retryable contention"
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket),
        Some(pending_command),
        "published command must remain pending for trailing recovery"
    );

    let mut readback = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: payload.len(),
                segment_crc64: checksum::crc64::checksum(payload),
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            },
            &mut readback,
        )
        .unwrap();
    assert_eq!(readback, payload);

    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![segment.clone()]
        );
    }
}

#[test]
fn stream_append_publish_validation_fails_closed_when_acknowledged_shard_file_is_missing() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("37".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append publish validation";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [0xd4; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let data_pg_id = DataPgId::new_for_test(PgId::new(segment.data_pg_id));
    let placement_key = super::super::super::segment_payload_placement_key(
        &segment.segment_okh,
        segment.segment_vid,
    );
    let locations = cluster
        .place_payload_shards(
            data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &placement_key,
        )
        .unwrap();
    let missing_shard = written_shards[0].key.clone();
    let missing_location = locations[usize::from(missing_shard.shard_index().get())];
    map.node(missing_location.node_id())
        .unwrap()
        .storage_node()
        .delete_shard_file(missing_location.data_pg_id().get(), &missing_shard)
        .unwrap();

    let shard_batch: Vec<(&crate::ShardKey, crate::WriteAck)> = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    let err = cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap_err();
    assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::ShardStore {
                    ref source,
                    ..
                }) if matches!(**source, StoreError::NotFound)
            ),
            "missing acknowledged stream shard file should fail closed before segment publish, got {err:?}"
        );
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    let object_pg_store = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(object_pg))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    assert!(
        crate::PgMetadataStore::list_stream_segments(&*object_pg_store, &session_id)
            .unwrap()
            .is_empty(),
        "failed stream append publish validation must not publish segment metadata"
    );
}

#[test]
fn stream_put_append_command_id_race_drains_winner_before_ack_publish() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let contender = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("34".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append command id race";
    let (target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [98; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch: Vec<(&crate::ShardKey, crate::WriteAck)> = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    let hook_shard_batch: Vec<(crate::ShardKey, crate::WriteAck)> = written_shards
        .iter()
        .map(|written| (written.key.clone(), written.ack))
        .collect();

    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_segment = segment.clone();
    let hook_target = target.clone();
    let hook_once = Arc::new(AtomicBool::new(true));
    let hook_once_for_closure = Arc::clone(&hook_once);
    let _guard = cluster.test_install_before_stream_append_command_id_hook(Arc::new(move || {
        if !hook_once_for_closure.swap(false, Ordering::SeqCst) {
            return;
        }
        let pg_id = PgId::new(object_pg);
        let command_id = contender.next_object_metadata_command_id(pg_id).unwrap();
        let hook_shard_refs: Vec<(&crate::ShardKey, crate::WriteAck)> = hook_shard_batch
            .iter()
            .map(|(key, ack)| (key, *ack))
            .collect();
        contender
            .register_payload_shard_acks(hook_segment.data_pg_id, &hook_shard_refs)
            .unwrap();
        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
                bucket: hook_bucket.clone(),
                key: hook_key.clone(),
                target: hook_target.clone(),
                segment: hook_segment.clone(),
            })),
        );
        insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
    }));

    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    assert!(!hook_once.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![segment.clone()]
        );
    }
}

#[test]
fn stream_append_yields_after_one_pending_drain_before_budget_recheck() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let map = Arc::new(map);
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("70".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"single contender stream append drain";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [0x70; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    let pg_id = PgId::new(object_pg);
    let first = pending_release_command(&cluster, &map, pg_id, &bucket, &key, "71");
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &first);

    let inserted = Arc::new(Mutex::new(None));
    let hook_cluster = Arc::clone(&cluster);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let inserted_for_hook = Arc::clone(&inserted);
    let _hook = cluster.test_install_after_metadata_command_drain_hook(Arc::new(move || {
        let second = pending_release_command(
            &hook_cluster,
            &hook_map,
            pg_id,
            &hook_bucket,
            &hook_key,
            "72",
        );
        insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &second);
        *inserted_for_hook.lock().unwrap() = Some(second);
    }));

    let error = cluster
        .test_commit_stream_segment_append_with_max_attempts(
            StreamAppendCommitRequest {
                bucket: &bucket,
                key: &key,
                session_id: &session_id,
                segment_index: segment.segment_index,
                segment_record: &segment,
                shard_batch: &shard_batch,
            },
            1,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
            context: "stream append pending drain retry budget exhausted"
        })
    ));
    let inserted = inserted.lock().unwrap().clone().unwrap();
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(inserted),
        "stream append must return to its budget boundary after one contender"
    );
}

#[test]
fn stream_append_budget_exhaustion_after_competing_publish_preserves_payload() {
    let _cleanup_serial = lock_payload_cleanup_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let contender = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("35".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append competing publish timeout";
    let (target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [99; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    let hook_shard_batch = written_shards
        .iter()
        .map(|written| (written.key.clone(), written.ack))
        .collect::<Vec<_>>();

    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_segment = segment.clone();
    let hook_target = target.clone();
    let hook_once = Arc::new(AtomicBool::new(true));
    let hook_once_for_closure = Arc::clone(&hook_once);
    let _command_guard =
        cluster.test_install_before_stream_append_command_id_hook(Arc::new(move || {
            if !hook_once_for_closure.swap(false, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(object_pg);
            let command_id = contender.next_object_metadata_command_id(pg_id).unwrap();
            let hook_shard_refs = hook_shard_batch
                .iter()
                .map(|(key, ack)| (key, *ack))
                .collect::<Vec<_>>();
            contender
                .register_payload_shard_acks(hook_segment.data_pg_id, &hook_shard_refs)
                .unwrap();
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
                    bucket: hook_bucket.clone(),
                    key: hook_key.clone(),
                    target: hook_target.clone(),
                    segment: hook_segment.clone(),
                })),
            );
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
        }));

    let cleanup_attempts = Arc::new(AtomicUsize::new(0));
    let cleanup_attempts_hook = Arc::clone(&cleanup_attempts);
    let _cleanup_guard =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |_| {
            cleanup_attempts_hook.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));

    let error = cluster
        .test_commit_stream_segment_append_with_max_attempts(
            StreamAppendCommitRequest {
                bucket: &bucket,
                key: &key,
                session_id: &session_id,
                segment_index: segment.segment_index,
                segment_record: &segment,
                shard_batch: &shard_batch,
            },
            1,
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                context: "stream append pending retry budget exhausted"
            })
        ),
        "expected deterministic post-drain budget exhaustion, got {error:?}"
    );
    assert!(!hook_once.load(Ordering::SeqCst));
    assert_eq!(
        cleanup_attempts.load(Ordering::SeqCst),
        0,
        "the timed-out caller must not delete payload adopted by the competing command"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![segment.clone()]
        );
    }

    let placement_key = segment_payload_placement_key(&segment.segment_okh, segment.segment_vid);
    let locations = cluster
        .place_payload_shards(
            DataPgId::new_for_test(PgId::new(segment.data_pg_id)),
            ec_shape,
            &placement_key,
        )
        .unwrap();
    for (location, written) in locations.iter().zip(&written_shards) {
        cluster
            .read_payload_shard(*location, &written.key, written.ack)
            .unwrap();
    }
    let mut readback = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: payload.len(),
                segment_crc64: checksum::crc64::checksum(payload),
                ec: ec_shape,
            },
            &mut readback,
        )
        .unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn stream_append_install_collision_after_competing_publish_preserves_payload() {
    let _cleanup_serial = lock_payload_cleanup_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("36".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append cleared-slot install collision";
    let (target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [100; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();

    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_segment = segment.clone();
    let hook_target = target.clone();
    let hook_map = Arc::clone(&map);
    let hook_once = Arc::new(AtomicBool::new(true));
    let hook_once_for_closure = Arc::clone(&hook_once);
    let _command_guard = cluster.test_install_after_stream_append_command_id_allocated_hook(
        Arc::new(move |command_id| {
            assert!(
                hook_once_for_closure.swap(false, Ordering::SeqCst),
                "selected command-ID collision hook must run exactly once"
            );
            let command = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
                    bucket: hook_bucket.clone(),
                    key: hook_key.clone(),
                    target: hook_target.clone(),
                    segment: hook_segment.clone(),
                })),
            );
            let pg_id = command.id().pg_id();
            let mut nodes = hook_map
                .metadata_pg_acting_nodes(command.id().cluster_epoch(), pg_id)
                .unwrap();
            let primary_node_id = hook_map
                .metadata_pg_primary_node(command.id().cluster_epoch(), pg_id)
                .unwrap()
                .node_id();
            nodes.sort_by_key(|node| node.node_id() != primary_node_id);
            for node in nodes {
                node.metadata_command_client()
                    .apply_metadata_command_and_record(pg_id, &command)
                    .unwrap();
            }
        }),
    );

    let cleanup_attempts = Arc::new(AtomicUsize::new(0));
    let cleanup_attempts_hook = Arc::clone(&cleanup_attempts);
    let _cleanup_guard =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |_| {
            cleanup_attempts_hook.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));

    let error = cluster
        .test_commit_stream_segment_append_with_max_attempts(
            StreamAppendCommitRequest {
                bucket: &bucket,
                key: &key,
                session_id: &session_id,
                segment_index: segment.segment_index,
                segment_record: &segment,
                shard_batch: &shard_batch,
            },
            1,
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                context: "stream append log conflict retry budget exhausted"
            })
        ),
        "expected deterministic cleared-slot log conflict exhaustion, got {error:?}"
    );
    assert!(!hook_once.load(Ordering::SeqCst));
    assert_eq!(
        cleanup_attempts.load(Ordering::SeqCst),
        0,
        "the colliding caller must not delete payload already published without a pending slot"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![segment.clone()]
        );
    }

    let placement_key = segment_payload_placement_key(&segment.segment_okh, segment.segment_vid);
    let locations = cluster
        .place_payload_shards(
            DataPgId::new_for_test(PgId::new(segment.data_pg_id)),
            ec_shape,
            &placement_key,
        )
        .unwrap();
    for (location, written) in locations.iter().zip(&written_shards) {
        cluster
            .read_payload_shard(*location, &written.key, written.ack)
            .unwrap();
    }
    let mut readback = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: payload.len(),
                segment_crc64: checksum::crc64::checksum(payload),
                ec: ec_shape,
            },
            &mut readback,
        )
        .unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn stream_append_log_conflict_drain_failure_cleans_unreferenced_payload() {
    let _apply_serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("3b".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append failed log-conflict drain";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [104; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();

    let collision_reservation_id = crate::SessionId::try_from("3c".repeat(16)).unwrap();
    let pending_reservation_id = crate::SessionId::try_from("3d".repeat(16)).unwrap();
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_pending_reservation_id = pending_reservation_id.clone();
    let hook_once = Arc::new(AtomicBool::new(true));
    let hook_once_for_closure = Arc::clone(&hook_once);
    let _command_guard = cluster.test_install_after_stream_append_command_id_allocated_hook(
        Arc::new(move |command_id| {
            assert!(
                hook_once_for_closure.swap(false, Ordering::SeqCst),
                "selected command-ID collision hook must run exactly once"
            );
            let pg_id = command_id.pg_id();
            let collision = MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::ReleaseObjectGeneration(
                    ReleaseObjectGenerationCommand::new(
                        hook_bucket.clone(),
                        hook_key.clone(),
                        collision_reservation_id.clone(),
                    ),
                ),
            );
            let mut nodes = hook_map
                .metadata_pg_acting_nodes(command_id.cluster_epoch(), pg_id)
                .unwrap();
            let primary_node_id = hook_map
                .metadata_pg_primary_node(command_id.cluster_epoch(), pg_id)
                .unwrap()
                .node_id();
            nodes.sort_by_key(|node| node.node_id() != primary_node_id);
            for node in nodes {
                node.metadata_command_client()
                    .apply_metadata_command_and_record(pg_id, &collision)
                    .unwrap();
            }

            let pending = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    command_id.cluster_epoch(),
                    pg_id,
                    hook_map.test_next_metadata_command_log_index(pg_id),
                ),
                MetadataCommandPayload::ReleaseObjectGeneration(
                    ReleaseObjectGenerationCommand::new(
                        hook_bucket.clone(),
                        hook_key.clone(),
                        hook_pending_reservation_id.clone(),
                    ),
                ),
            );
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &pending);
        }),
    );

    let fail_once = Arc::new(AtomicBool::new(true));
    let fail_once_for_hook = Arc::clone(&fail_once);
    let failure_bucket = bucket.clone();
    let failure_key = key.clone();
    let failure_reservation_id = pending_reservation_id.clone();
    let _apply_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::ReleaseObjectGeneration(release)
                    if release.matches_request(
                        &failure_bucket,
                        &failure_key,
                        &failure_reservation_id,
                    )
            ) && node_id == NodeId::new(0)
                && fail_once_for_hook.swap(false, Ordering::SeqCst)
            {
                return Err(StoreError::Io {
                    context: "injected stream append log-conflict drain failure",
                    source: std::io::Error::other(
                        "injected stream append log-conflict drain failure",
                    ),
                });
            }
            Ok(())
        },
    ));

    let error = cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected stream append log-conflict drain failure",
                ..
            })
        ),
        "expected injected LogConflict drain failure, got {error:?}"
    );
    assert!(!hook_once.load(Ordering::SeqCst));
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "failed unrelated drain must retain its pending command for recovery"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id)
                .unwrap()
                .is_empty(),
            "failed unrelated drain must not publish the staged stream segment"
        );
    }

    let placement_key = segment_payload_placement_key(&segment.segment_okh, segment.segment_vid);
    let locations = cluster
        .place_payload_shards(
            DataPgId::new_for_test(PgId::new(segment.data_pg_id)),
            ec_shape,
            &placement_key,
        )
        .unwrap();
    for (location, written) in locations.iter().zip(&written_shards) {
        assert!(
            cluster
                .read_payload_shard(*location, &written.key, written.ack)
                .is_err(),
            "failed unrelated LogConflict drain must delete staged shard {}",
            written.key
        );
    }
}

#[test]
fn stream_append_unrelated_pending_duplicate_cleans_staged_payload() {
    let _cleanup_serial = lock_payload_cleanup_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("37".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let published_payload = b"published stream segment";
    let (_target, published_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: published_payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(published_payload),
                payload_crc64: checksum::crc64::checksum(published_payload),
                segment_okh: [101; 16],
            },
        )
        .unwrap();
    let conflicting_payload = b"unreferenced conflicting stream segment";
    let (_target, conflicting_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: conflicting_payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(conflicting_payload),
                payload_crc64: checksum::crc64::checksum(conflicting_payload),
                segment_okh: [102; 16],
            },
        )
        .unwrap();
    assert_ne!(
        published_segment.segment_vid,
        conflicting_segment.segment_vid
    );

    let published_shards = cluster
        .write_stream_segment_payload_shards(&published_segment, published_payload)
        .unwrap();
    let published_shard_batch = published_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            published_segment.segment_index,
            &published_segment,
            &published_shard_batch,
        )
        .unwrap();

    let conflicting_shards = cluster
        .write_stream_segment_payload_shards(&conflicting_segment, conflicting_payload)
        .unwrap();
    let conflicting_shard_batch = conflicting_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();

    let pg_id = PgId::new(object_pg);
    let unrelated = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReleaseObjectGeneration(ReleaseObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            crate::SessionId::try_from("38".repeat(16)).unwrap(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &unrelated);

    let cleanup_attempts = Arc::new(AtomicUsize::new(0));
    let cleanup_attempts_hook = Arc::clone(&cleanup_attempts);
    let _cleanup_guard =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |_| {
            cleanup_attempts_hook.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));

    let error = cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            conflicting_segment.segment_index,
            &conflicting_segment,
            &conflicting_shard_batch,
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason == "duplicate segment_index 0"
        ),
        "expected conflicting segment rejection, got {error:?}"
    );
    assert!(
        cleanup_attempts.load(Ordering::SeqCst) >= conflicting_shards.len(),
        "unrelated pending-command drainage must not suppress staged payload cleanup"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![published_segment.clone()]
        );
    }

    let placement_key = segment_payload_placement_key(
        &conflicting_segment.segment_okh,
        conflicting_segment.segment_vid,
    );
    let locations = cluster
        .place_payload_shards(
            DataPgId::new_for_test(PgId::new(conflicting_segment.data_pg_id)),
            ec_shape,
            &placement_key,
        )
        .unwrap();
    for (location, written) in locations.iter().zip(&conflicting_shards) {
        assert!(
            cluster
                .read_payload_shard(*location, &written.key, written.ack)
                .is_err(),
            "unreferenced conflicting shard {} must be deleted",
            written.key
        );
    }

    let mut readback = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: published_segment.data_pg_id,
                segment_okh: published_segment.segment_okh,
                segment_vid: published_segment.segment_vid,
                stored_size: published_payload.len(),
                segment_crc64: checksum::crc64::checksum(published_payload),
                ec: ec_shape,
            },
            &mut readback,
        )
        .unwrap();
    assert_eq!(readback, published_payload);
}

#[test]
fn stream_append_unrelated_install_contention_cleans_staged_payload() {
    let _cleanup_serial = lock_payload_cleanup_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("39".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append unrelated install contention";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [103; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();

    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_once = Arc::new(AtomicBool::new(true));
    let hook_once_for_closure = Arc::clone(&hook_once);
    let _command_guard =
        cluster.test_install_before_stream_append_command_id_hook(Arc::new(move || {
            if !hook_once_for_closure.swap(false, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(object_pg);
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    hook_map.test_next_metadata_command_log_index(pg_id),
                ),
                MetadataCommandPayload::ReleaseObjectGeneration(
                    ReleaseObjectGenerationCommand::new(
                        hook_bucket.clone(),
                        hook_key.clone(),
                        crate::SessionId::try_from("3a".repeat(16)).unwrap(),
                    ),
                ),
            );
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
        }));

    let cleanup_attempts = Arc::new(AtomicUsize::new(0));
    let cleanup_attempts_hook = Arc::clone(&cleanup_attempts);
    let _cleanup_guard =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |_| {
            cleanup_attempts_hook.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));

    let error = cluster
        .test_commit_stream_segment_append_with_max_attempts(
            StreamAppendCommitRequest {
                bucket: &bucket,
                key: &key,
                session_id: &session_id,
                segment_index: segment.segment_index,
                segment_record: &segment,
                shard_batch: &shard_batch,
            },
            1,
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                context: "stream append pending retry budget exhausted"
            })
        ),
        "expected deterministic unrelated contention exhaustion, got {error:?}"
    );
    assert!(!hook_once.load(Ordering::SeqCst));
    assert!(
        cleanup_attempts.load(Ordering::SeqCst) >= written_shards.len(),
        "unrelated install contention must clean staged payload"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id)
                .unwrap()
                .is_empty(),
            "unrelated contention must not publish the staged segment"
        );
    }

    let placement_key = segment_payload_placement_key(&segment.segment_okh, segment.segment_vid);
    let locations = cluster
        .place_payload_shards(
            DataPgId::new_for_test(PgId::new(segment.data_pg_id)),
            ec_shape,
            &placement_key,
        )
        .unwrap();
    for (location, written) in locations.iter().zip(&written_shards) {
        assert!(
            cluster
                .read_payload_shard(*location, &written.key, written.ack)
                .is_err(),
            "unreferenced staged shard {} must be deleted",
            written.key
        );
    }
}

#[test]
fn stream_abort_pending_drain_cleans_terminal_stream_session() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("45".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream abort pending drain allocator cleanup";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [0x45; 16],
            },
        )
        .unwrap();
    assert_stream_next_segment_vid(&map, NodeId::new(1), object_pg, &session_id, 2);
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AbortStreamUpload(abort)
                    if abort.session_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected stream abort metadata command replica apply failure",
                        source: std::io::Error::other(
                            "injected stream abort metadata command replica apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial abort command must remain pending"
    );
    assert_stream_next_segment_vid(&map, NodeId::new(2), object_pg, &session_id, 2);

    let next_reservation_id = crate::SessionId::try_from("46".repeat(16)).unwrap();
    cluster
        .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
}

#[test]
fn stream_abort_pending_install_race_rebuilds_staged_segments() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("4b".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let first_payload = b"first staged stream abort segment";
    let (_target, first_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: first_payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(first_payload),
                payload_crc64: checksum::crc64::checksum(first_payload),
                segment_okh: [0x4b; 16],
            },
        )
        .unwrap();
    let first_shards = cluster
        .write_stream_segment_payload_shards(&first_segment, first_payload)
        .unwrap();
    let first_shard_batch = first_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            first_segment.segment_index,
            &first_segment,
            &first_shard_batch,
        )
        .unwrap();

    let second_payload = b"raced stream append before abort install";
    let (second_target, second_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 1,
                size: second_payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(second_payload),
                payload_crc64: checksum::crc64::checksum(second_payload),
                segment_okh: [0x4c; 16],
            },
        )
        .unwrap();
    let second_shards = cluster
        .write_stream_segment_payload_shards(&second_segment, second_payload)
        .unwrap();

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_cluster = Arc::clone(&cluster);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_segment = second_segment.clone();
    let hook_target = second_target.clone();
    let hook_shards = second_shards.clone();
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(object_pg);
            let shard_batch = hook_shards
                .iter()
                .map(|written| (&written.key, written.ack))
                .collect::<Vec<_>>();
            hook_cluster
                .register_payload_shard_acks(hook_segment.data_pg_id, &shard_batch)
                .unwrap();
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let log_index = pg
                .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap()
                + 1;
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    MetadataCommandLogIndex::new(log_index).unwrap(),
                ),
                MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
                    bucket: hook_bucket.clone(),
                    key: hook_key.clone(),
                    target: hook_target.clone(),
                    segment: hook_segment.clone(),
                })),
            );
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }));

    cluster
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id)
                .unwrap()
                .is_empty()
        );
    }

    for (segment, payload) in [
        (&first_segment, first_payload.as_slice()),
        (&second_segment, second_payload.as_slice()),
    ] {
        let mut readback = Vec::new();
        let error = cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: segment.data_pg_id,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    stored_size: payload.len(),
                    segment_crc64: checksum::crc64::checksum(payload),
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                },
                &mut readback,
            )
            .unwrap_err();
        assert!(
            matches!(error, StoreError::NotFound),
            "abort must clean the staged payload after rebuilding from the raced append: {error:?}"
        );
    }
}

#[test]
fn stream_abort_matching_pending_install_race_returns_success() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("48".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let stream_create_bucket_write_reservation = {
        let node = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        crate::PgMetadataStore::get_stream_upload(&*pg, &session_id)
            .unwrap()
            .bucket_write_reservation
    };
    assert!(stream_create_bucket_write_reservation.is_some());

    let pg_id = PgId::new(object_pg);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::AbortStreamUpload(Box::new(AbortStreamUploadCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            session_id: session_id.clone(),
            staged_segments: Vec::new(),
            stream_create_bucket_write_reservation,
        })),
    );
    let inserted = Arc::new(AtomicBool::new(false));
    let inserted_for_hook = Arc::clone(&inserted);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_command = command.clone();
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if inserted_for_hook.swap(true, Ordering::SeqCst) {
                return;
            }
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &hook_command);
        }));
    cluster
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();

    assert!(inserted.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn stream_put_finalize_pending_drain_cleans_terminal_stream_session() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));
    let waiter_key = key_for_object_pg(
        map.nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        &bucket,
        object_pg,
        "stream-finalize-recovery-waiter-",
    );

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_pg = cluster.bucket_metadata_pg_id(&bucket);
    let session_id = crate::SessionId::try_from("49".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let waiter_session_id = crate::SessionId::try_from("4a".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &waiter_key,
            &waiter_session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream put finalize pending drain allocator cleanup";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: payload_crc64,
                payload_crc64,
                segment_okh: [0x49; 16],
            },
        )
        .unwrap();
    assert_stream_next_segment_vid(&map, NodeId::new(1), object_pg, &session_id, 2);
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.generation_reservation_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context:
                            "injected stream put finalize metadata command replica apply failure",
                        source: std::io::Error::other(
                            "injected stream put finalize metadata command replica apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let published = cluster
        .finalize_put_object_stream(&bucket, &key, &session_id, payload.len() as u64, |_| {
            Ok::<_, ()>(crate::PreparedStreamPutCommit {
                value: (),
                versioning: crate::BucketVersioningState::Disabled,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                etag_crc64: payload_crc64,
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(published.live_size, payload.len() as u64);
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial stream PUT finalize command must remain pending"
    );
    {
        let bucket_primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg))
            .unwrap();
        let bucket_pg_store = bucket_primary.storage_node().get_pg(bucket_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg_store, &bucket,)
                .unwrap()
                .len(),
            2
        );
    }
    assert_stream_next_segment_vid(&map, NodeId::new(2), object_pg, &session_id, 2);

    let pg_id = PgId::new(object_pg);
    let pending_command = pending_metadata_command_for_test(&map, pg_id, &bucket)
        .expect("partial stream PUT finalize command must remain pending");
    let timeout_selected = Arc::new(Barrier::new(2));
    let retry_selected = Arc::new(Barrier::new(2));
    cluster.test_install_metadata_command_recovery_wait_hook(
        pg_id,
        &pending_command,
        Arc::clone(&timeout_selected),
        Arc::clone(&retry_selected),
    );
    let owner_ready = Arc::new(Barrier::new(2));
    let owner_release = Arc::new(Barrier::new(2));
    let owner_ready_hook = Arc::clone(&owner_ready);
    let owner_release_hook = Arc::clone(&owner_release);
    let owner_command = pending_command.clone();
    let block_owner_once = Arc::new(AtomicBool::new(true));
    let block_owner_once_hook = Arc::clone(&block_owner_once);
    let owner_hook = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            if *command == owner_command && block_owner_once_hook.swap(false, Ordering::SeqCst) {
                owner_ready_hook.wait();
                owner_release_hook.wait();
            }
            Ok(())
        },
    ));
    let waiter_outcome = thread::scope(|scope| {
        let owner = scope.spawn(|| {
            cluster.drain_pending_metadata_command_with_recovery_gate(pg_id, &pending_command)
        });
        owner_ready.wait();
        let waiter = scope.spawn(|| {
            cluster.finalize_put_object_stream(&bucket, &waiter_key, &waiter_session_id, 0, |_| {
                Ok::<_, ()>(crate::PreparedStreamPutCommit {
                    value: "waiter",
                    versioning: crate::BucketVersioningState::Disabled,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    etag_crc64: checksum::crc64::checksum(&[]),
                    tags: None,
                    metadata_blob: crate::SerializedMetadataBlob::default(),
                    system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                })
            })
        });
        timeout_selected.wait();
        retry_selected.wait();
        owner_release.wait();
        assert_eq!(
            owner.join().unwrap().unwrap(),
            PendingMetadataCommandOutcome::Applied,
            "recovery owner must apply the pending stream PUT command"
        );
        waiter.join().unwrap().unwrap().unwrap()
    });
    drop(owner_hook);
    assert!(!block_owner_once.load(Ordering::SeqCst));
    assert_eq!(waiter_outcome.value, "waiter");
    assert_eq!(
        cluster.test_take_metadata_command_recovery_wait_hook_observation(),
        (2, 0),
        "stream PUT waiter must time out once, rejoin the active owner, and observe completion"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(live.size, payload.len() as u64);
        assert_eq!(live.generation_id, crate::GenerationId::MIN);
    }
    {
        let bucket_primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg))
            .unwrap();
        let bucket_pg_store = bucket_primary.storage_node().get_pg(bucket_pg).unwrap();
        assert!(crate::PgMetadataStore::durable_bucket_write_reservations(
            &*bucket_pg_store,
            &bucket,
        )
        .unwrap()
        .is_empty());
    }
}

fn assert_stream_put_finalize_retries_transient_unrelated_pending_drain_failure(
    mark_publication_started: bool,
    injected_action: crate::cluster::request_ops::StreamPutPendingDrainTestAction,
) {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap());
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-finalize-drain-retry-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::tests::stream_session_id("final-drain");
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let pg_id = PgId::new(2);
    let unrelated = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReleaseObjectGeneration(ReleaseObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            crate::tests::stream_session_id("unrelated-gen"),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &unrelated);
    if mark_publication_started {
        let primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
            .unwrap();
        primary
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap()
            .mark_pending_metadata_command_publication_started(
                primary.node_id().as_u32(),
                &unrelated,
            )
            .unwrap();
    }
    let owner = match map
        .runtime_state()
        .join_metadata_command_recovery(pg_id, &unrelated)
    {
        crate::cluster::MetadataCommandRecoveryAdmission::Leader(owner) => owner,
        _ => panic!("test must acquire the initial unrelated-command recovery flight"),
    };
    let owner = Arc::new(Mutex::new(Some(owner)));
    let owner_for_hook = Arc::clone(&owner);
    let injected = Arc::new(AtomicBool::new(false));
    let injected_for_hook = Arc::clone(&injected);
    let hook = cluster.test_install_stream_put_pending_drain_hook(Arc::new(move |event| {
        if event == crate::cluster::request_ops::StreamPutPendingDrainTestEvent::Initial
            && !injected_for_hook.swap(true, Ordering::SeqCst)
        {
            if matches!(
                injected_action,
                crate::cluster::request_ops::StreamPutPendingDrainTestAction::RetryableFailure
                    | crate::cluster::request_ops::StreamPutPendingDrainTestAction::IrrevocableFailure
            ) {
                drop(owner_for_hook.lock().unwrap().take());
            }
            injected_action
        } else {
            crate::cluster::request_ops::StreamPutPendingDrainTestAction::Continue
        }
    }));

    let action_calls = Arc::new(AtomicUsize::new(0));
    let result = cluster.finalize_put_object_stream(&bucket, &key, &session_id, 0, |_| {
        action_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<_, ()>(crate::PreparedStreamPutCommit {
            value: (),
            versioning: crate::BucketVersioningState::Disabled,
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            etag_crc64: checksum::crc64::checksum(&[]),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            object_lock: crate::ObjectLockState::default(),
            encryption: crate::ObjectEncryption::None,
        })
    });
    drop(hook);
    assert!(injected.load(Ordering::SeqCst));
    match injected_action {
        crate::cluster::request_ops::StreamPutPendingDrainTestAction::RetryableFailure
        | crate::cluster::request_ops::StreamPutPendingDrainTestAction::IrrevocableFailure => {
            result
                .expect("transient unrelated-command drain failure must be retried")
                .expect("stream PUT preparation should succeed");
            assert_eq!(action_calls.load(Ordering::SeqCst), 1);
            assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
            assert_bucket_write_reservations_released(&map, &bucket);
        }
        crate::cluster::request_ops::StreamPutPendingDrainTestAction::ExpireOuterBudget => {
            let error = result.expect_err("expired outer drain budget must stop finalization");
            assert!(matches!(
                error,
                crate::ObjectPgActionError::Store(ref source)
                    if source.operation_failure_class()
                        == crate::StoreOperationFailureClass::MetadataCommandContention
            ));
            assert_eq!(action_calls.load(Ordering::SeqCst), 0);
        }
        crate::cluster::request_ops::StreamPutPendingDrainTestAction::Continue => {
            unreachable!("test helper requires an injected stream drain action")
        }
    }
}

#[test]
fn stream_put_finalize_retries_transient_unrelated_pending_drain_failure() {
    assert_stream_put_finalize_retries_transient_unrelated_pending_drain_failure(
        false,
        crate::cluster::request_ops::StreamPutPendingDrainTestAction::RetryableFailure,
    );
}

#[test]
fn stream_put_finalize_retries_publication_started_pending_drain_failure() {
    assert_stream_put_finalize_retries_transient_unrelated_pending_drain_failure(
        true,
        crate::cluster::request_ops::StreamPutPendingDrainTestAction::IrrevocableFailure,
    );
}

#[test]
fn stream_put_finalize_initial_pending_drain_honors_outer_budget() {
    assert_stream_put_finalize_retries_transient_unrelated_pending_drain_failure(
        false,
        crate::cluster::request_ops::StreamPutPendingDrainTestAction::ExpireOuterBudget,
    );
}

#[test]
fn control_plane_peering_stream_put_finalize_old_primary_fails_closed_and_preserves_staging() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
        crate::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            node_ids
                .iter()
                .map(|node_id| {
                    (
                        *node_id,
                        tmp.path()
                            .join("sockets")
                            .join(format!("stream-node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();
    let configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("stream-storage")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "stream-finalize-peering-");
        let object_pg = 2;
        let key = key_for_object_pg(topology, &bucket, object_pg, "object-");
        (bucket, key, object_pg)
    };
    let source_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);
    let session_id = crate::tests::stream_session_id("peeringputfinal");
    source_cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"control-plane peering stale stream put finalize";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = source_cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: payload_crc64,
                payload_crc64,
                segment_okh: [0xd1; 16],
            },
        )
        .unwrap();
    let written_shards = source_cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    source_cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let stream_reservation_id = source_cluster
        .load_stream_upload_session(&bucket, &key, &session_id)
        .unwrap()
        .bucket_write_reservation
        .unwrap()
        .reservation_id;
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );
    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();

    let action_called = Arc::new(AtomicBool::new(false));
    let action_called_for_closure = Arc::clone(&action_called);
    let err = old_primary_cluster
        .finalize_put_object_stream(&bucket, &key, &session_id, payload.len() as u64, |_| {
            action_called_for_closure.store(true, Ordering::SeqCst);
            Ok::<_, ()>(crate::PreparedStreamPutCommit {
                value: (),
                versioning: crate::BucketVersioningState::Disabled,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                etag_crc64: payload_crc64,
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            })
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary stream PUT finalize should fail closed after control-plane Peering transition, got {err:?}"
    );
    assert!(
        !action_called.load(Ordering::SeqCst),
        "stale stream PUT finalize must not prepare a commit after route rejection"
    );

    for node_id in node_ids {
        let pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof::current(
            state.applied_log_index,
            state.applied_log_hash,
            state.state_digest,
        );
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary stream PUT finalize must not append an object-PG command on node {node_id:?}"
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "old-primary stream PUT finalize must not publish object metadata on node {node_id:?}"
        );
        assert!(
            pg.pending_metadata_command_envelope(node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary stream PUT finalize must not leave a source-epoch pending command on node {node_id:?}"
        );
        assert!(
            pg.pending_metadata_command_envelope(node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary stream PUT finalize must not leave a current-epoch pending command on node {node_id:?}"
        );
        assert!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).is_ok(),
            "old-primary stream PUT finalize must preserve stream session on node {node_id:?}"
        );
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![segment.clone()],
            "old-primary stream PUT finalize must preserve staged stream segment on node {node_id:?}"
        );
    }
    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    let bucket_primary = current_map
        .metadata_pg_primary_node(current_epoch, PgId::new(bucket_pg))
        .unwrap();
    let bucket_pg_store = bucket_primary.storage_node().get_pg(bucket_pg).unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg_store, &bucket)
            .unwrap()
            .iter()
            .any(|reservation| reservation.reservation_id == stream_reservation_id),
        "old-primary stream PUT finalize must preserve its durable stream reservation"
    );
}

#[test]
fn stream_put_finalize_missing_session_same_pg_does_not_call_action() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-finalize-missing-");
    let key = key_for_object_pg(topology, &bucket, 1, "object-");

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    assert_eq!(cluster.bucket_metadata_pg_id(&bucket), 1);
    assert_eq!(cluster.object_metadata_pg_id(&bucket, &key), 1);

    let session_id = crate::SessionId::try_from("7d".repeat(16)).unwrap();
    let action_called = Arc::new(AtomicBool::new(false));
    let action_called_for_closure = Arc::clone(&action_called);

    let err = cluster
        .finalize_put_object_stream(&bucket, &key, &session_id, 0, move |_| {
            action_called_for_closure.store(true, Ordering::SeqCst);
            Ok::<_, ()>(crate::PreparedStreamPutCommit {
                value: (),
                versioning: crate::BucketVersioningState::Disabled,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                etag_crc64: checksum::crc64::checksum(&[]),
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            })
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(
                crate::MetadataError::StreamSessionNotFound { .. }
            )
        ),
        "expected StreamSessionNotFound, got {err:?}"
    );
    assert!(!action_called.load(Ordering::SeqCst));

    let bucket_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap();
    let bucket_pg = bucket_primary.storage_node().get_pg(1).unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn stream_put_finalize_action_failure_preserves_stream_write_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-finalize-action-");
    let key = key_for_object_pg(topology, &bucket, 1, "object-");

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    assert_eq!(cluster.bucket_metadata_pg_id(&bucket), 1);
    assert_eq!(cluster.object_metadata_pg_id(&bucket, &key), 1);

    let session_id = crate::SessionId::try_from("7e".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let result: Result<crate::FinalizeStreamPutOutcome<()>, &str> = cluster
        .finalize_put_object_stream(&bucket, &key, &session_id, 0, |_| Err("condition failed"))
        .unwrap();
    assert!(matches!(result, Err("condition failed")));

    let bucket_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap();
    let bucket_pg = bucket_primary.storage_node().get_pg(1).unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket).unwrap();
    assert_eq!(reservations.len(), 1);
    let upload = crate::PgMetadataStore::get_stream_upload(&*bucket_pg, &session_id).unwrap();
    assert_eq!(
        upload.bucket_write_reservation.as_ref(),
        Some(&crate::metadata_command::BucketWriteReservationProof::from(
            &reservations[0]
        )),
        "failed finalize must preserve the live stream-create proof"
    );
}

#[test]
fn stream_put_finalize_releases_only_its_session_write_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-finalize-proof-binding-");
    let key = key_for_object_pg(topology, &bucket, 1, "object-");

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let first_session = crate::SessionId::try_from("7f".repeat(16)).unwrap();
    let second_session = crate::SessionId::try_from("80".repeat(16)).unwrap();
    for session_id in [&first_session, &second_session] {
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
    }

    let first_proof = cluster
        .load_stream_upload_session(&bucket, &key, &first_session)
        .unwrap()
        .bucket_write_reservation
        .expect("first stream session must retain its durable write proof");
    let second_proof = cluster
        .load_stream_upload_session(&bucket, &key, &second_session)
        .unwrap()
        .bucket_write_reservation
        .expect("second stream session must retain its durable write proof");
    assert_ne!(first_proof.reservation_id, second_proof.reservation_id);

    let bucket_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap();
    let bucket_pg = bucket_primary.storage_node().get_pg(1).unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket).unwrap();
    assert_eq!(reservations.len(), 2);
    assert!(reservations.iter().any(|record| {
        crate::metadata_command::BucketWriteReservationProof::from(record) == first_proof
    }));
    assert!(reservations.iter().any(|record| {
        crate::metadata_command::BucketWriteReservationProof::from(record) == second_proof
    }));
    drop(bucket_pg);

    cluster
        .finalize_put_object_stream(&bucket, &key, &first_session, 0, |_| {
            Ok::<_, ()>(crate::PreparedStreamPutCommit {
                value: (),
                versioning: crate::BucketVersioningState::Disabled,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                etag_crc64: checksum::crc64::checksum(&[]),
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            })
        })
        .unwrap()
        .unwrap();

    let remaining_session = cluster
        .load_stream_upload_session(&bucket, &key, &second_session)
        .expect("finalizing one stream must preserve the other same-key session");
    assert_eq!(
        remaining_session.bucket_write_reservation.as_ref(),
        Some(&second_proof)
    );
    assert!(matches!(
        cluster
            .load_stream_upload_session(&bucket, &key, &first_session)
            .map_err(|error| error.kind()),
        Err(crate::StreamUploadFailureKind::SessionNotFound)
    ));
    let bucket_pg = bucket_primary.storage_node().get_pg(1).unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket).unwrap();
    assert_eq!(reservations.len(), 1);
    assert_eq!(
        crate::metadata_command::BucketWriteReservationProof::from(&reservations[0]),
        second_proof,
        "successful finalize must release only the proof persisted in its own stream session"
    );
}

#[test]
fn generic_pending_drain_rejects_stream_put_with_substituted_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "substituted-stream-proof-drain-");
    let key = key_for_object_pg(topology, &bucket, 1, "object-");

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let first_session = crate::SessionId::try_from("81".repeat(16)).unwrap();
    let second_session = crate::SessionId::try_from("82".repeat(16)).unwrap();
    for session_id in [&first_session, &second_session] {
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
    }
    let first_proof = cluster
        .load_stream_upload_session(&bucket, &key, &first_session)
        .unwrap()
        .bucket_write_reservation
        .unwrap();
    let second_proof = cluster
        .load_stream_upload_session(&bucket, &key, &second_session)
        .unwrap()
        .bucket_write_reservation
        .unwrap();
    assert_ne!(first_proof.reservation_id, second_proof.reservation_id);

    let pg_id = PgId::new(1);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let (generation_id, write_sequence) = {
        let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
        (
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &first_session,
            )
            .unwrap(),
            pg.next_object_write_sequence(bucket.as_str(), key.as_str())
                .unwrap(),
        )
    };
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
            object: crate::PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: crate::VersionId::Null,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                generation_id,
                size: 0,
                etag: crate::ObjectEtag::single_part(checksum::crc64::checksum(&[])),
                ec: ec_shape,
                layout: crate::ObjectLayout::Standard,
                tags: None,
                metadata_blob: Some(crate::SerializedMetadataBlob::default()),
                system_metadata_blob: Some(crate::SerializedSystemMetadataBlob::default()),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            },
            segments: Vec::new(),
            generation_reservation_id: first_session.clone(),
            write_sequence,
            last_modified_millis: 1,
            stale_payload: None,
            bucket_write_reservation: second_proof.clone(),
        })),
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert_eq!(
            pg.metadata_command_acceptance(node_id.as_u32(), &command)
                .unwrap(),
            crate::metadata_command::MetadataCommandAcceptance::Apply,
            "the substituted proof must be the command's only invalid field"
        );
    }
    cluster
        .validate_metadata_command_bucket_write_reservation(&command)
        .expect("the substituted proof must itself be a live valid reservation");
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    let central_error = primary
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap()
        .apply_metadata_command(&command)
        .unwrap_err();
    assert!(matches!(
        central_error,
        crate::MetadataError::InvariantViolation {
            context: "commit direct put command stream reservation mismatch",
            ..
        }
    ));
    let states_before = node_ids.map(|node_id| {
        map.node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap()
            .metadata_command_replica_state()
            .unwrap()
    });

    let error = cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::ObjectPgActionError::Metadata(crate::MetadataError::InvariantViolation {
                context: "commit direct put command stream reservation mismatch",
                ..
            })
        ),
        "substituted stream proof must fail generic recovery, got {error:?}"
    );

    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let retained = primary_pg
        .pending_metadata_command_envelope(primary.node_id().as_u32(), ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(retained.as_ref(), Some(&command));
    drop(primary_pg);
    for (index, node_id) in node_ids.into_iter().enumerate() {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert_eq!(
            pg.metadata_command_replica_state().unwrap(),
            states_before[index]
        );
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(crate::PgMetadataStore::get_stream_upload(&*pg, &first_session).is_ok());
        assert!(crate::PgMetadataStore::get_stream_upload(&*pg, &second_session).is_ok());
    }
    let bucket_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let bucket_pg = bucket_primary.storage_node().get_pg(pg_id.get()).unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket).unwrap();
    assert_eq!(reservations.len(), 2);
    assert!(reservations.iter().any(|record| {
        crate::metadata_command::BucketWriteReservationProof::from(record) == first_proof
    }));
    assert!(reservations.iter().any(|record| {
        crate::metadata_command::BucketWriteReservationProof::from(record) == second_proof
    }));
}

#[test]
fn stream_put_finalize_rejects_unencrypted_etag_crc64_mismatch() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let session_id = crate::SessionId::try_from("8f".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let payloads: [&[u8]; 1] = [b"unencrypted stream segment"];
    let mut expected_crc64 = checksum::crc64::checksum(&[]);
    let mut total_size = 0;
    for (segment_index, payload) in payloads.iter().enumerate() {
        let segment_crc64 = checksum::crc64::checksum(payload);
        expected_crc64 =
            checksum::crc64::combine(expected_crc64, segment_crc64, payload.len() as u64);
        total_size += payload.len() as u64;
        let (_target, segment) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: segment_index as u32,
                    size: payload.len() as u64,
                    segment_crc64,
                    payload_crc64: segment_crc64,
                    segment_okh: [0x80 + segment_index as u8; 16],
                },
            )
            .unwrap();
        let written_shards = cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();
    }

    let err = cluster
        .finalize_put_object_stream(&bucket, &key, &session_id, total_size, |_| {
            Ok::<_, ()>(crate::PreparedStreamPutCommit {
                value: (),
                versioning: crate::BucketVersioningState::Disabled,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                etag_crc64: expected_crc64 ^ 1,
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            })
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason.contains("stream PUT etag CRC64 mismatch")
        ),
        "expected stream PUT etag CRC64 mismatch, got {err:?}"
    );
}

#[test]
fn stream_put_finalize_rejects_encrypted_payload_crc64_mismatch() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let encryption = crate::ObjectEncryption::SseS3(
        crate::SseS3ObjectState::new(
            9,
            [10; crate::SSE_S3_WRAP_NONCE_LEN],
            [11; crate::SSE_S3_WRAPPED_DEK_LEN],
            [12; crate::SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
        )
        .with_encrypted_checksum_metadata([13; crate::SSE_S3_CHECKSUM_NONCE_LEN], vec![14, 15, 16])
        .unwrap(),
    );
    let session_id = crate::SessionId::try_from("90".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(&bucket, &key, &session_id, encryption.clone())
        .unwrap();

    let payload = b"encrypted stream plaintext";
    let storage_bytes = b"encrypted stream ciphertext";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let segment_crc64 = checksum::crc64::checksum(storage_bytes);
    assert_ne!(payload_crc64, segment_crc64);
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64,
                payload_crc64,
                segment_okh: [0x91; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, storage_bytes)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let err = cluster
        .finalize_put_object_stream(&bucket, &key, &session_id, payload.len() as u64, |_| {
            Ok::<_, ()>(crate::PreparedStreamPutCommit {
                value: (),
                versioning: crate::BucketVersioningState::Disabled,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                etag_crc64: payload_crc64 ^ 1,
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                object_lock: crate::ObjectLockState::default(),
                encryption: encryption.clone(),
            })
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason.contains("stream PUT etag CRC64 mismatch")
        ),
        "expected encrypted stream PUT payload CRC64 mismatch, got {err:?}"
    );
}

#[test]
fn stream_part_finalize_rejects_staged_payload_crc64_mismatch() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("partcrcmismatch");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let session_id = crate::SessionId::try_from("92".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();

    let payload = b"stream part payload crc mismatch";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: payload_crc64,
                payload_crc64,
                segment_okh: [0x92; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        payload_crc64: payload_crc64 ^ 1,
        etag: vec![0x92; 8],
        etag_kind: crate::EtagKind::Crc64,
        part_vid: crate::GenerationId::MIN,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_456,
        checksum: None,
    };
    let err = cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(&upload_id, &session_id, 1, part.size, part.payload_crc64),
            |_| Ok::<_, ()>(prepared_stream_part((), &part)),
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason.contains("stream UploadPart etag CRC64 mismatch")
        ),
        "expected stream UploadPart etag CRC64 mismatch, got {err:?}"
    );
}

#[test]
fn stream_part_finalize_abandons_expired_install_then_retries_transported_contention() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec).unwrap());
    let (bucket, key, object_pg, _) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let upload_id = upload_id_from_label("partcontention");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let session_id = crate::tests::stream_session_id("part-contention");
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();

    let payload = b"UploadPartCopy finalization contention";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: payload_crc64,
                payload_crc64,
                segment_okh: [0xb2; 16],
            },
        )
        .unwrap();
    let written = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        payload_crc64,
        etag: payload_crc64.to_be_bytes().to_vec(),
        etag_kind: crate::EtagKind::Crc64,
        part_vid: crate::GenerationId::MIN,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_456,
        checksum: None,
    };
    let expired = Arc::new(AtomicBool::new(false));
    let hook_expired = Arc::clone(&expired);
    let hook_upload_id = upload_id.clone();
    let hook_session = session_id.clone();
    let hook =
        cluster.test_install_before_object_metadata_command_apply_hook(Arc::new(move |command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::CommitStreamPart(commit)
                    if commit.upload.upload_id == hook_upload_id
                        && commit.session_id == hook_session
                        && commit.part.part_number == 1
                        && !hook_expired.swap(true, Ordering::SeqCst)
            ) {
                return true;
            }
            false
        }));
    let attempts = Arc::new(AtomicUsize::new(0));
    let hook_attempts = Arc::clone(&attempts);
    let attempt_upload_id = upload_id.clone();
    let attempt_session = session_id.clone();
    let attempt_hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::CommitStreamPart(commit)
                    if commit.upload.upload_id == attempt_upload_id
                        && commit.session_id == attempt_session
                        && commit.part.part_number == 1
            ) && hook_attempts.fetch_add(1, Ordering::SeqCst) == 0
            {
                return Err(StoreError::StorageRpc {
                    node_id: 0,
                    operation: "apply metadata command",
                    failure: crate::storage_rpc::StorageRpcErrorCode::MetadataCommandContention,
                    detail: crate::error::StorageNodeFailureDetail::new(
                        "injected transported UploadPartCopy publication contention",
                    ),
                });
            }
            Ok(())
        }));

    let error = cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(&upload_id, &session_id, 1, part.size, part.payload_crc64),
            |_| Ok::<_, ()>(prepared_stream_part((), &part)),
        )
        .expect_err("expired inherited budget must not publish the stream part");
    drop(hook);

    assert!(expired.load(Ordering::SeqCst));
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention { .. })
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1),
            Err(crate::MetadataError::PartNotFound { .. })
        ));
    }

    let outcome = cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(&upload_id, &session_id, 1, part.size, part.payload_crc64),
            |_| Ok::<_, ()>(prepared_stream_part((), &part)),
        )
        .expect("a fresh retry must publish after the abandoned attempt")
        .expect("stream part preparation should succeed");
    assert_eq!(outcome.last_modified, part.last_modified);
    assert!(attempts.load(Ordering::SeqCst) >= 2);
    drop(attempt_hook);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap();
        assert_eq!(stored, part);
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn multipart_payload_snapshot_does_not_treat_shard_rows_without_files_as_absent() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("partpayloadrowwithoutfile");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();

    let session_id = crate::SessionId::try_from("93".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();

    let payload = b"multipart snapshot row without shard file";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: payload_crc64,
                payload_crc64,
                segment_okh: [0x93; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        payload_crc64,
        etag: payload_crc64.to_be_bytes().to_vec(),
        etag_kind: crate::EtagKind::Crc64,
        part_vid: crate::GenerationId::MIN,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_456,
        checksum: None,
    };
    let _ = cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(&upload_id, &session_id, 1, part.size, part.payload_crc64),
            |_| Ok::<_, ()>(prepared_stream_part((), &part)),
        )
        .unwrap();

    let snapshot = cluster
        .test_capture_multipart_part_payload(&bucket, &key, &upload_id, 1)
        .unwrap();
    assert!(cluster
        .test_multipart_part_payload_snapshot_is_fully_present(&snapshot)
        .unwrap());

    for shard_index in 0..segment.ec_k + segment.ec_m {
        let shard_path = cluster
            .test_payload_shard_file_path(
                segment.data_pg_id,
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
                &segment.segment_okh,
                segment.segment_vid,
                shard_index,
            )
            .unwrap();
        std::fs::remove_file(shard_path).unwrap();
    }

    assert!(!cluster
        .test_multipart_part_payload_snapshot_is_fully_present(&snapshot)
        .unwrap());
    assert!(
        !cluster
            .test_multipart_part_payload_snapshot_is_fully_absent(&snapshot)
            .unwrap(),
        "durable shard rows without files must not be classified as fully absent"
    );
}

#[test]
fn stream_put_finalize_matching_pending_install_race_returns_success() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("4c".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream put same pending install race";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: payload_crc64,
                payload_crc64,
                segment_okh: [0x4c; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let pg_id = PgId::new(object_pg);
    let (generation_id, write_sequence) = {
        let primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
            .unwrap();
        let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
        let generation_id = crate::traits::PgMetadataStore::get_object_generation_reservation(
            &*pg,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap();
        let write_sequence = pg
            .next_object_write_sequence(bucket.as_str(), key.as_str())
            .unwrap();
        (generation_id, write_sequence)
    };
    let object = crate::PutLiveObjectReq {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: crate::VersionId::Null,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        generation_id,
        size: payload.len() as u64,
        etag: crate::ObjectEtag::single_part(payload_crc64),
        ec: EcShape {
            k: segment.ec_k,
            m: segment.ec_m,
        },
        layout: crate::ObjectLayout::Standard,
        tags: None,
        metadata_blob: Some(crate::SerializedMetadataBlob::default()),
        system_metadata_blob: Some(crate::SerializedSystemMetadataBlob::default()),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
    };
    let segments = vec![crate::ObjectSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: crate::VersionId::Null,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];
    let command_proof = cluster
        .load_stream_upload_session(&bucket, &key, &session_id)
        .unwrap()
        .bucket_write_reservation
        .unwrap();
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
            object,
            segments,
            generation_reservation_id: session_id.clone(),
            write_sequence,
            last_modified_millis: 123_460,
            stale_payload: None,
            bucket_write_reservation: command_proof,
        })),
    );
    let inserted = Arc::new(AtomicBool::new(false));
    let inserted_for_hook = Arc::clone(&inserted);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_command = command.clone();
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if inserted_for_hook.swap(true, Ordering::SeqCst) {
                return;
            }
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &hook_command);
        }));

    let outcome = cluster
        .finalize_put_object_stream(&bucket, &key, &session_id, payload.len() as u64, |_| {
            Ok::<_, ()>(crate::PreparedStreamPutCommit {
                value: "ok",
                versioning: crate::BucketVersioningState::Disabled,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                etag_crc64: payload_crc64,
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            })
        })
        .unwrap()
        .unwrap();

    assert!(inserted.load(Ordering::SeqCst));
    assert_eq!(outcome.value, "ok");
    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_size, payload.len() as u64);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().size, payload.len() as u64);
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn versioned_stream_put_finalize_reserves_object_version_through_command_stream() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    let session_id = crate::SessionId::try_from("76".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"versioned stream put finalization";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: payload_crc64,
                payload_crc64,
                segment_okh: [0x76; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let reserve_apply_count = Arc::new(AtomicUsize::new(0));
    let fail_commit_replica_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_session_id = session_id.clone();
    let reserve_apply_count_hook = Arc::clone(&reserve_apply_count);
    let fail_commit_replica_once_hook = Arc::clone(&fail_commit_replica_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if let MetadataCommandPayload::ReserveObjectVersion(reservation) = command.payload() {
                if reservation.bucket == hook_bucket && reservation.key == hook_key {
                    reserve_apply_count_hook.fetch_add(1, Ordering::SeqCst);
                }
            }
            if matches!(
                command.payload(),
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.generation_reservation_id == hook_session_id
                        && node_id == NodeId::new(0)
                        && fail_commit_replica_once_hook.swap(false, Ordering::SeqCst)
            ) {
                return Err(StoreError::StorageRpc {
                    node_id: node_id.as_u32(),
                    operation: "apply metadata command",
                    failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                    detail: crate::error::StorageNodeFailureDetail::new(
                        "injected replica response timeout after primary apply",
                    ),
                });
            }
            Ok(())
        },
    ));

    let outcome = cluster
        .finalize_put_object_stream(&bucket, &key, &session_id, payload.len() as u64, |_| {
            Ok::<_, ()>(crate::PreparedStreamPutCommit {
                value: (),
                versioning: crate::BucketVersioningState::Enabled,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                etag_crc64: payload_crc64,
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            })
        })
        .unwrap()
        .unwrap();
    drop(hook_guard);
    assert!(
        !fail_commit_replica_once.load(Ordering::SeqCst),
        "versioned stream PUT must exercise the post-primary transport retry"
    );
    assert_eq!(outcome.version_id, crate::VersionId::from_u64(1));
    assert_eq!(
        reserve_apply_count.load(Ordering::SeqCst),
        node_ids.len(),
        "versioned stream PUT finalization must reserve through the metadata command stream"
    );

    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 2);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(live.version_id, outcome.version_id);
        assert_eq!(live.size, payload.len() as u64);
        assert_eq!(
            pg.object_write_sequence(bucket.as_str(), key.as_str(), outcome.version_id,)
                .unwrap(),
            Some(1),
        );
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn stream_put_finalize_converges_after_zero_apply_exact_witness_conflict() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("77".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let conflict_once = Arc::new(AtomicBool::new(true));
    let conflict_once_for_hook = Arc::clone(&conflict_once);
    let hook_map = Arc::clone(&map);
    let hook_session_id = session_id.clone();
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if node_id == NodeId::new(0)
                && matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.generation_reservation_id == hook_session_id
                )
                && conflict_once_for_hook.swap(false, Ordering::SeqCst)
            {
                std::thread::sleep(std::time::Duration::from_millis(1_100));
                let pg = hook_map
                    .node(node_id)
                    .unwrap()
                    .storage_node()
                    .get_pg(command.id().pg_id().get())?;
                pg.apply_metadata_command_and_record(node_id.as_u32(), command)
                    .map_err(|error| match error {
                        crate::BucketSnapshotLoadError::Store(error) => error,
                        crate::BucketSnapshotLoadError::Metadata(error) => {
                            panic!("manual stream commit apply failed: {error}")
                        }
                    })?;
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id: node_id.as_u32(),
                    pg_id: command.id().pg_id().get(),
                    cluster_epoch: command.id().cluster_epoch(),
                    log_index: command.id().log_index().get(),
                });
            }
            Ok(())
        },
    ));

    let outcome = cluster
        .finalize_put_object_stream(&bucket, &key, &session_id, 0, |_| {
            Ok::<_, ()>(crate::PreparedStreamPutCommit {
                value: "published",
                versioning: crate::BucketVersioningState::Disabled,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                etag_crc64: checksum::crc64::checksum(&[]),
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            })
        })
        .unwrap()
        .unwrap();
    drop(hook_guard);

    assert_eq!(outcome.value, "published");
    assert!(
        !conflict_once.load(Ordering::SeqCst),
        "stream finalization must exercise the zero-apply exact witness conflict"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().size, 0);
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn stream_part_finalize_pending_drain_cleans_terminal_stream_session() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("partfinalizedrain");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let session_id = crate::SessionId::try_from("47".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();
    let payload = b"stream part finalize pending drain allocator cleanup";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [0x47; 16],
            },
        )
        .unwrap();
    assert_stream_next_segment_vid(&map, NodeId::new(1), object_pg, &session_id, 2);
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitStreamPart(commit)
                    if commit.session_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected stream part metadata command replica apply failure",
                        source: std::io::Error::other(
                            "injected stream part metadata command replica apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let expected_part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        payload_crc64: segment.payload_crc64,
        etag: segment.payload_crc64.to_be_bytes().to_vec(),
        etag_kind: crate::EtagKind::Crc64,
        part_vid: crate::GenerationId::MIN,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_456,
        checksum: None,
    };
    let expected_segments = vec![crate::MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];
    let published = cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(
                &upload_id,
                &session_id,
                1,
                expected_part.size,
                expected_part.payload_crc64,
            ),
            |_| Ok::<_, ()>(prepared_stream_part((), &expected_part)),
        )
        .unwrap()
        .unwrap();
    assert_eq!(published.last_modified, expected_part.last_modified);
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial stream part command must remain pending"
    );
    assert_stream_next_segment_vid(&map, NodeId::new(2), object_pg, &session_id, 2);

    cluster
        .drain_pending_object_metadata_commands_for_bucket(PgId::new(object_pg), &bucket)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
            expected_part
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part_segments_for_upload_part(
                &*pg, &bucket, &key, &upload_id, 1
            )
            .unwrap(),
            expected_segments
        );
    }
}

#[test]
fn stream_part_finalize_matching_pending_install_race_returns_success() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("partfinalizematch");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let session_id = crate::SessionId::try_from("4b".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload.clone()),
            1,
            &session_id,
        )
        .unwrap();
    assert_bucket_write_reservations_released(&map, &bucket);
    let payload = b"stream part same pending install race";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [0x4b; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let expected_part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        payload_crc64: segment.payload_crc64,
        etag: segment.payload_crc64.to_be_bytes().to_vec(),
        etag_kind: crate::EtagKind::Crc64,
        part_vid: crate::GenerationId::MIN,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_459,
        checksum: None,
    };
    let expected_segments = vec![crate::MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];

    let inserted = Arc::new(AtomicBool::new(false));
    let inserted_for_hook = Arc::clone(&inserted);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_upload = upload.clone();
    let hook_session_id = session_id.clone();
    let hook_part = expected_part.clone();
    let hook_segments = expected_segments.clone();
    let hook_proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let pg_id = PgId::new(object_pg);
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if inserted_for_hook.swap(true, Ordering::SeqCst) {
                return;
            }
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    hook_map.test_next_metadata_command_log_index(pg_id),
                ),
                MetadataCommandPayload::CommitStreamPart(Box::new(CommitStreamPartCommand {
                    bucket: hook_bucket.clone(),
                    key: hook_key.clone(),
                    session_id: hook_session_id.clone(),
                    upload: hook_upload.clone(),
                    part: hook_part.clone(),
                    segments: hook_segments.clone(),
                    existing_part: None,
                    displaced_segments: Vec::new(),
                    bucket_write_reservation: hook_proof.clone(),
                })),
            );
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
        }));

    let outcome = cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(
                &upload_id,
                &session_id,
                1,
                expected_part.size,
                expected_part.payload_crc64,
            ),
            |_| Ok::<_, ()>(prepared_stream_part("ok", &expected_part)),
        )
        .unwrap()
        .unwrap();

    assert!(inserted.load(Ordering::SeqCst));
    assert_eq!(outcome.value, "ok");
    assert_eq!(outcome.last_modified, expected_part.last_modified);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
            expected_part
        );
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn upload_part_stream_finalize_partial_apply_reopens_and_converges() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(0));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("partfinalizereopen");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let session_id = crate::SessionId::try_from("4e".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();
    let payload = b"stream part finalize partial apply survives reopen";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [0x4e; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let expected_part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        payload_crc64: segment.payload_crc64,
        etag: segment.payload_crc64.to_be_bytes().to_vec(),
        etag_kind: crate::EtagKind::Crc64,
        part_vid: crate::GenerationId::MIN,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_459,
        checksum: None,
    };
    let expected_segments = vec![crate::MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitStreamPart(commit)
                    if commit.session_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "apply metadata command",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected stream part reopen apply failure".to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));
    cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(
                &upload_id,
                &session_id,
                1,
                expected_part.size,
                expected_part.payload_crc64,
            ),
            |_| Ok::<_, ()>(prepared_stream_part((), &expected_part)),
        )
        .expect("published stream-part finalize must hand trailing convergence to recovery")
        .expect("stream-part finalize preparation should produce an outcome");
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial stream part command must remain durable before reopen"
    );
    drop(cluster);
    drop(map);

    let mut reopened_map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("reopen local map");
    set_route_primary(&mut reopened_map, object_pg, NodeId::new(0));
    set_route_primary(&mut reopened_map, data_pg, NodeId::new(2));
    let reopened_map = Arc::new(reopened_map);
    assert!(
        pending_metadata_command_for_test(&reopened_map, PgId::new(object_pg), &bucket).is_none(),
        "open-time recovery should converge and clear the partial stream part command"
    );

    for node_id in node_ids {
        let node = reopened_map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
            expected_part
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part_segments_for_upload_part(
                &*pg, &bucket, &key, &upload_id, 1
            )
            .unwrap(),
            expected_segments
        );
    }
    assert_clean_metadata_command_stream(&reopened_map, &[object_pg]);
}

#[test]
fn upload_part_stream_create_does_not_expose_foreign_terminal_pending_upload() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, foreign_key, object_pg, data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let target_key = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        key_for_object_pg(topology, &bucket, object_pg, "foreign-terminal-target-")
    };
    assert_ne!(foreign_key, target_key);
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let create_upload = |key: &ObjectKey, upload_id: crate::UploadId| {
        let request = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: crate::OwnerIdentity::from_principal("initiator"),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        cluster
            .create_multipart_upload(
                &bucket,
                key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), request.clone()))
                },
            )
            .unwrap()
            .unwrap();
        upload_id
    };
    let foreign_upload_id = create_upload(
        &foreign_key,
        upload_id_from_label("foreignterminalpendingupload"),
    );
    let target_upload_id = create_upload(
        &target_key,
        upload_id_from_label("foreignterminaltargetupload"),
    );
    assert!(cluster
        .abort_multipart_upload(&bucket, &foreign_key, &foreign_upload_id)
        .unwrap());

    let stale_session_id = crate::SessionId::try_from("8a".repeat(16)).unwrap();
    let stale_proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        Some(foreign_key.as_str()),
    );
    let pg_id = PgId::new(object_pg);
    let stale = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::CreateStreamUpload(Box::new(
            crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                crate::CreateStreamUploadReq {
                    session_id: stale_session_id.clone(),
                    bucket: bucket.clone(),
                    key: foreign_key.clone(),
                    target: crate::StreamUploadTarget::UploadPart {
                        upload_id: foreign_upload_id.clone(),
                        part_number: 1,
                    },
                    encryption: crate::ObjectEncryption::None,
                },
                123,
                stale_proof.clone(),
            ),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &stale);

    #[derive(Clone, Copy, Debug)]
    enum RejectedTerminalEvidence {
        PublicationStarted,
        Witnessed,
        Applied,
        AmbiguousDispatch,
        MismatchedUpload,
        IntegrityFailure,
    }

    for evidence in [
        RejectedTerminalEvidence::PublicationStarted,
        RejectedTerminalEvidence::Witnessed,
        RejectedTerminalEvidence::Applied,
        RejectedTerminalEvidence::AmbiguousDispatch,
        RejectedTerminalEvidence::MismatchedUpload,
        RejectedTerminalEvidence::IntegrityFailure,
    ] {
        let hook_stale = stale.clone();
        let hook_guard = cluster.test_install_terminal_stream_apply_failure_hook(Arc::new(
            move |command, failure| {
                if command != &hook_stale {
                    return;
                }
                match evidence {
                    RejectedTerminalEvidence::PublicationStarted => {
                        failure.progress = MetadataCommandApplyProgress::PublicationStarted;
                    }
                    RejectedTerminalEvidence::Witnessed => {
                        failure.progress = MetadataCommandApplyProgress::Witnessed;
                    }
                    RejectedTerminalEvidence::Applied => {
                        failure.applied_nodes = 1;
                    }
                    RejectedTerminalEvidence::AmbiguousDispatch => {
                        failure.may_have_applied = true;
                    }
                    RejectedTerminalEvidence::MismatchedUpload => {
                        failure.source =
                            BucketSnapshotLoadError::Metadata(MetadataError::NoSuchUpload {
                                upload_id: "different-upload".to_owned(),
                            });
                    }
                    RejectedTerminalEvidence::IntegrityFailure => {
                        failure.source =
                            BucketSnapshotLoadError::Metadata(MetadataError::InvariantViolation {
                                context:
                                    "injected terminal stream classification integrity failure",
                                reason: "test integrity evidence must remain fail-closed"
                                    .to_owned(),
                            });
                    }
                }
            },
        ));
        cluster
            .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
            .expect_err("non-definitive terminal evidence must remain fail-closed");
        drop(hook_guard);

        assert_eq!(
            pending_metadata_command_for_test(&map, pg_id, &bucket),
            Some(stale.clone()),
            "{evidence:?} must retain the exact pending command"
        );
        for node_id in node_ids {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            assert!(
                !pg.metadata_command_abandoned(node_id.as_u32(), &stale)
                    .unwrap(),
                "{evidence:?} must not record an abandonment tombstone"
            );
        }
        let bucket_pg_id = PgId::new(
            map.node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology()
                .bucket_pg_for(&bucket),
        );
        let bucket_primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, bucket_pg_id)
            .unwrap();
        let bucket_pg = bucket_primary
            .storage_node()
            .get_pg(bucket_pg_id.get())
            .unwrap();
        let reservations =
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
                .unwrap();
        assert!(
            reservations.iter().any(|record| {
                crate::metadata_command::BucketWriteReservationProof::from(record) == stale_proof
            }),
            "{evidence:?} must retain the pending command reservation"
        );
    }

    let target_upload = cluster
        .load_in_progress_multipart_upload(&bucket, &target_key, &target_upload_id)
        .unwrap();
    let target_session_id = crate::SessionId::try_from("8b".repeat(16)).unwrap();
    let created_session = cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(target_upload),
            1,
            &target_session_id,
        )
        .unwrap();

    assert_eq!(created_session, target_session_id);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(pg
            .metadata_command_abandoned(node_id.as_u32(), &stale)
            .unwrap());
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &foreign_upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &stale_session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &target_session_id)
                .unwrap()
                .target,
            crate::StreamUploadTarget::UploadPart {
                upload_id: target_upload_id.clone(),
                part_number: 1,
            }
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);

    let terminal_session_id = crate::SessionId::try_from("8c".repeat(16)).unwrap();
    let terminal_target = crate::StreamUploadTarget::UploadPart {
        upload_id: target_upload_id.clone(),
        part_number: 2,
    };
    let terminal_segment = crate::StreamUploadSegmentRecord {
        session_id: terminal_session_id.clone(),
        segment_index: 0,
        size: 1,
        segment_crc64: 1,
        payload_crc64: 1,
        segment_okh: [0x8c; 16],
        segment_vid: crate::GenerationId::MIN,
        data_pg_id: data_pg,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: ec_shape.k,
        ec_m: ec_shape.m,
    };
    let terminal_append = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
            bucket: bucket.clone(),
            key: target_key.clone(),
            target: terminal_target.clone(),
            segment: terminal_segment.clone(),
        })),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &terminal_append);

    let mismatched_append = terminal_append.clone();
    let mismatch_guard = cluster.test_install_terminal_stream_apply_failure_hook(Arc::new(
        move |command, failure| {
            if command == &mismatched_append {
                failure.source =
                    BucketSnapshotLoadError::Metadata(MetadataError::StreamSessionNotFound {
                        session_id: "different-session".to_owned(),
                    });
            }
        },
    ));
    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .expect_err("a mismatched terminal session must remain fail-closed");
    drop(mismatch_guard);
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(terminal_append.clone()),
        "a mismatched terminal session must retain the exact pending command"
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(!pg
            .metadata_command_abandoned(node_id.as_u32(), &terminal_append)
            .unwrap());
    }

    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .expect("the matching terminal session must authorize abandonment");
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(pg
            .metadata_command_abandoned(node_id.as_u32(), &terminal_append)
            .unwrap());
    }

    let terminal_state_session_id = crate::SessionId::try_from("8d".repeat(16)).unwrap();
    let terminal_state_append = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
            bucket: bucket.clone(),
            key: target_key,
            target: terminal_target,
            segment: crate::StreamUploadSegmentRecord {
                session_id: terminal_state_session_id,
                ..terminal_segment
            },
        })),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &terminal_state_append);
    let hooked_terminal_state_append = terminal_state_append.clone();
    let terminal_state_guard = cluster.test_install_terminal_stream_apply_failure_hook(Arc::new(
        move |command, failure| {
            if command == &hooked_terminal_state_append {
                failure.source =
                    BucketSnapshotLoadError::Metadata(MetadataError::StreamSessionNotInProgress {
                        state: 1,
                    });
            }
        },
    ));
    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .expect("a terminal stream-session state must authorize abandonment");
    drop(terminal_state_guard);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(pg
            .metadata_command_abandoned(node_id.as_u32(), &terminal_state_append)
            .unwrap());
    }
}

#[test]
fn upload_part_stream_finalize_finishes_terminal_pending_slot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let crossed_bucket = crate::tests::bucket_name("stream-part-finalize-crossed-bucket");
    create_test_bucket(&cluster, &crossed_bucket);
    let upload_id = upload_id_from_label("terminalslot");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let session_id = crate::SessionId::try_from("4d".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload.clone()),
            1,
            &session_id,
        )
        .unwrap();
    let payload = b"stream part terminal pending slot";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [0x4d; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        payload_crc64: segment.payload_crc64,
        etag: segment.payload_crc64.to_be_bytes().to_vec(),
        etag_kind: crate::EtagKind::Crc64,
        part_vid: crate::GenerationId::MIN,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_458,
        checksum: None,
    };
    let segments = vec![crate::MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];
    let pg_id = PgId::new(object_pg);
    let correct_proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let crossed_proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let crossed_target_proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
        Some("stream-part-finalize-crossed-key"),
    );
    let crossed_bucket_proof = acquire_test_bucket_write_proof(
        &cluster,
        &crossed_bucket,
        crate::metadata_command::UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let command = |proof| {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CommitStreamPart(Box::new(CommitStreamPartCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: session_id.clone(),
                upload: upload.clone(),
                part: part.clone(),
                segments: segments.clone(),
                existing_part: None,
                displaced_segments: Vec::new(),
                bucket_write_reservation: proof,
            })),
        )
    };
    cluster
        .validate_metadata_command_bucket_write_reservation(&command(correct_proof.clone()))
        .unwrap();
    for (case, proof) in [
        ("operation", crossed_proof.clone()),
        ("target", crossed_target_proof.clone()),
        ("bucket", crossed_bucket_proof.clone()),
    ] {
        let crossed_error = cluster
            .validate_metadata_command_bucket_write_reservation(&command(proof))
            .unwrap_err();
        assert!(
            matches!(
                crossed_error,
                crate::BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::BucketWriteReservationConflict { .. }
                )
            ),
            "crossed stream-part {case} proof must fail: {crossed_error:?}"
        );
    }
    let mut co_crossed_payload = command(crossed_bucket_proof.clone()).payload().clone();
    let MetadataCommandPayload::CommitStreamPart(co_crossed) = &mut co_crossed_payload else {
        panic!("expected stream-part commit command");
    };
    co_crossed.bucket = crossed_bucket.clone();
    let co_crossed_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        co_crossed_payload,
    );
    let co_crossed_error = cluster
        .validate_metadata_command_bucket_write_reservation(&co_crossed_command)
        .unwrap_err();
    assert!(
        matches!(
            co_crossed_error,
            crate::BucketSnapshotLoadError::Metadata(
                crate::MetadataError::BucketWriteReservationConflict { .. }
            )
        ),
        "co-crossed stream-part command and proof must fail internal subject validation: {co_crossed_error:?}"
    );
    let reservations_before = node_ids.map(|node_id| {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket).unwrap()
    });
    let sessions_before = node_ids.map(|node_id| {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap()
    });
    let malformed = command(crossed_proof.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &malformed);
    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for ((node_id, expected_reservations), expected_session) in node_ids
        .into_iter()
        .zip(&reservations_before)
        .zip(&sessions_before)
    {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap(),
            *expected_session,
            "crossed stream-part recovery must preserve the session on node {node_id:?}"
        );
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1),
            Err(crate::MetadataError::PartNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket).unwrap(),
            *expected_reservations,
            "crossed stream-part recovery must not release the unrelated reservation on node {node_id:?}"
        );
    }
    cluster
        .release_bucket_write_reservation_proof(&crossed_proof)
        .unwrap();
    cluster
        .release_bucket_write_reservation_proof(&crossed_target_proof)
        .unwrap();
    cluster
        .release_bucket_write_reservation_proof(&crossed_bucket_proof)
        .unwrap();

    let command = command(correct_proof);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();
    assert!(
        matches!(
            pending_metadata_command_for_test(&map, pg_id, &bucket)
                .as_ref()
                .map(MetadataCommandEnvelope::payload),
            Some(MetadataCommandPayload::CommitStreamPart(commit))
                if commit.matches_request(&bucket, &key, &upload_id, &session_id, 1)
        ),
        "terminal pending slot must survive before retry"
    );

    let err = cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(&upload_id, &session_id, 1, part.size, part.payload_crc64),
            |_| -> Result<crate::PreparedStreamPartCommit<()>, ()> {
                panic!("terminal pending slot should finish before rerunning action")
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(
                crate::MetadataError::StreamSessionNotFound { .. }
            )
        ),
        "expected retry to finish terminal slot then report missing stream session, got {err:?}"
    );
    let leftover = pending_metadata_command_for_test(&map, pg_id, &bucket);
    assert!(leftover.is_none(), "leftover pending command: {leftover:?}");
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
            part
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part_segments_for_upload_part(
                &*pg, &bucket, &key, &upload_id, 1
            )
            .unwrap(),
            segments
        );
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn upload_part_stream_finalize_committed_response_loss_retry_sees_terminal_part() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("partlostresp");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let session_id = crate::SessionId::try_from("5b".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();
    let payload = b"stream part committed response loss";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [0x5b; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();
    let staged_shard_keys = written_shards
        .iter()
        .map(|written| written.key.clone())
        .collect::<Vec<_>>();

    let expected_part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        payload_crc64: segment.payload_crc64,
        etag: segment.payload_crc64.to_be_bytes().to_vec(),
        etag_kind: crate::EtagKind::Crc64,
        part_vid: crate::GenerationId::MIN,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_460,
        checksum: None,
    };
    let expected_segments = vec![crate::MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        placement_cluster_epoch: segment.placement_cluster_epoch,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];

    let _serial = lock_metadata_command_apply_hook_test();
    let hook_guard =
        cluster.test_install_after_object_metadata_command_publish_hook(Arc::new(|| {
            Err(crate::ObjectPgActionError::InvalidRequest {
                reason: "injected stream part finalize response loss".to_string(),
            })
        }));

    let first_err = cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(
                &upload_id,
                &session_id,
                1,
                expected_part.size,
                expected_part.payload_crc64,
            ),
            |_| Ok::<_, ()>(prepared_stream_part((), &expected_part)),
        )
        .unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason == "injected stream part finalize response loss"
        ),
        "expected injected post-commit stream part finalize response-loss error, got {first_err:?}"
    );
    drop(hook_guard);

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
            expected_part
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part_segments_for_upload_part(
                &*pg, &bucket, &key, &upload_id, 1
            )
            .unwrap(),
            expected_segments
        );
    }
    for (shard_index, key) in staged_shard_keys.iter().enumerate() {
        assert!(
            cluster
                .test_payload_shard_file_exists(
                    segment.data_pg_id,
                    ec_shape,
                    &segment.segment_okh,
                    segment.segment_vid,
                    shard_index as u8
                )
                .unwrap(),
            "committed stream part response loss must preserve placed shard {key:?}"
        );
    }

    let retried = cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(
                &upload_id,
                &session_id,
                1,
                expected_part.size,
                expected_part.payload_crc64,
            ),
            |_| -> Result<crate::PreparedStreamPartCommit<()>, ()> {
                panic!("committed stream part retry should fail before rerunning action")
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            retried,
            crate::ObjectPgActionError::Metadata(
                crate::MetadataError::StreamSessionNotFound { .. }
            )
        ),
        "expected retry to observe missing terminal stream session, got {retried:?}"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for (shard_index, key) in staged_shard_keys.iter().enumerate() {
        assert!(
            cluster
                .test_payload_shard_file_exists(
                    segment.data_pg_id,
                    ec_shape,
                    &segment.segment_okh,
                    segment.segment_vid,
                    shard_index as u8
                )
                .unwrap(),
            "committed stream part retry must preserve placed shard {key:?}"
        );
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn upload_part_stream_finalize_pending_install_race_reloads_after_abort() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("open first local map");
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "upload-part-finalize-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("open second local map");
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);
    let upload_id = upload_id_from_label("partfinalizeabort");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    first_cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();

    let session_id = crate::SessionId::try_from("59".repeat(16)).unwrap();
    let upload = first_cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    first_cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();

    let payload = b"stream part finalize loses the pending slot to abort";
    let (_target, segment) = first_cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [0x59; 16],
            },
        )
        .unwrap();
    let written_shards = first_cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    first_cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let pg_id = PgId::new(2);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_upload_id = upload_id.clone();
    let hook_bucket_write_reservation = acquire_test_bucket_write_proof(
        &first_cluster,
        &bucket,
        "abort-multipart-upload",
        Some(key.as_str()),
    );
    let _hook_guard = first_cluster.test_install_before_metadata_command_pending_install_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let cleanup = pg
                .prepare_abort_multipart_upload_cleanup(&hook_bucket, &hook_key, &hook_upload_id)
                .unwrap()
                .expect("upload is still in progress");
            let log_index = pg
                .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap()
                + 1;
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    MetadataCommandLogIndex::new(log_index).unwrap(),
                ),
                MetadataCommandPayload::AbortMultipartUpload(Box::new(
                    AbortMultipartUploadCommand {
                        bucket: hook_bucket.clone(),
                        key: hook_key.clone(),
                        upload_id: hook_upload_id.clone(),
                        cleanup,
                        bucket_write_reservation: hook_bucket_write_reservation.clone(),
                    },
                )),
            );
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }),
    );

    let calls_for_action = Arc::clone(&action_calls);
    let err = first_cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(
                &upload_id,
                &session_id,
                1,
                payload.len() as u64,
                segment.payload_crc64,
            ),
            |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                assert_eq!(snapshot.staged_size, payload.len() as u64);
                assert_eq!(snapshot.staged_payload_crc64, segment.payload_crc64);
                Ok::<_, ()>(crate::PreparedStreamPartCommit {
                    value: (),
                    last_modified: 123_456,
                    checksum: None,
                })
            },
        )
        .unwrap_err();

    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(crate::MetadataError::NoSuchUpload {
                upload_id: ref missing
            }) if missing == upload_id.as_str()
        ),
        "expected finalize to reload after abort removed the upload, got {err:?}"
    );
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(action_calls.load(Ordering::SeqCst), 1);
    assert!(pending_metadata_command_for_test(&first_map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let node = first_map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1),
            Err(crate::MetadataError::PartNotFound { .. })
        ));
    }

    let mut readback = Vec::new();
    let error = first_cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: payload.len(),
                segment_crc64: checksum::crc64::checksum(payload),
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            },
            &mut readback,
        )
        .unwrap_err();
    assert!(
        matches!(error, StoreError::NotFound),
        "abort winner must clean staged payload after finalize contention: {error:?}"
    );
    assert_clean_metadata_command_stream(&first_map, &[pg_id.get()]);
    assert_bucket_write_reservations_released(&first_map, &bucket);
}

#[test]
fn upload_part_copy_staged_segments_are_cleaned_when_complete_wins_finalize_slot() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "upload-part-finalize-complete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, mut expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "completewinsfinalize");
    let pg_id = PgId::new(2);

    let session_id = crate::SessionId::try_from("5a".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &req.upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            2,
            &session_id,
        )
        .unwrap();
    // UploadPartCopy stores copied source bytes as ordinary UploadPart stream
    // segments. Use two staged segments so terminal MPU cleanup proves it
    // removes every copied segment payload when completion wins the slot.
    let first_payload = b"copied source segment one";
    let (_target, first_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: first_payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(first_payload),
                payload_crc64: checksum::crc64::checksum(first_payload),
                segment_okh: [0x5a; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&first_segment, first_payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            first_segment.segment_index,
            &first_segment,
            &shard_batch,
        )
        .unwrap();
    let second_payload = b"copied source segment two";
    let (_target, second_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 1,
                size: second_payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(second_payload),
                payload_crc64: checksum::crc64::checksum(second_payload),
                segment_okh: [0x5d; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&second_segment, second_payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            second_segment.segment_index,
            &second_segment,
            &shard_batch,
        )
        .unwrap();

    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_req = req.clone();
    let hook_session_id = session_id.clone();
    let hook_proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-complete-multipart-race",
        Some(key.as_str()),
    );
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let parts_count =
                std::num::NonZeroU32::new(u32::try_from(hook_req.part_records.len()).unwrap())
                    .unwrap();
            let object_parts = crate::node_client::complete_multipart_expected_object_parts(
                &hook_req,
                crate::VersionId::Null,
                primary.storage_node().pg_topology(),
            );
            let mut selected_streaming_segments =
                crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(
                    &*pg,
                    &hook_req.upload_id,
                )
                .unwrap();
            for selected in &mut selected_streaming_segments {
                selected.version_id = crate::VersionId::Null.to_u64();
            }
            let active_session = crate::PgMetadataStore::get_stream_upload(&*pg, &hook_session_id)
                .expect("active UploadPart stream session");
            let stream_upload_segments =
                crate::PgMetadataStore::list_stream_segments(&*pg, &hook_session_id)
                    .expect("active UploadPart stream segments");
            let write_sequence = pg
                .next_object_write_sequence(hook_bucket.as_str(), hook_key.as_str())
                .unwrap();
            let log_index = pg
                .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap()
                + 1;
            let last_modified_millis = 987_655;
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    MetadataCommandLogIndex::new(log_index).unwrap(),
                ),
                MetadataCommandPayload::CommitMultipartObject(Box::new(
                    CommitMultipartObjectCommand {
                        upload_id: hook_req.upload_id.clone(),
                        completion_fingerprint: hook_req.completion_fingerprint,
                        bucket_write_reservation: hook_proof.clone(),
                        object: crate::PutLiveObjectReq {
                            bucket: hook_bucket.clone(),
                            key: hook_key.clone(),
                            version_id: crate::VersionId::Null,
                            owner: hook_req.owner.clone(),
                            acl_grants: hook_req.acl_grants.clone(),
                            public_read: hook_req.public_read,
                            generation_id: hook_req.generation_id,
                            size: hook_req.size,
                            etag: crate::ObjectEtag::MultipartComposite {
                                crc64: hook_req.etag_crc64,
                                parts: parts_count,
                            },
                            ec: EcShape { k: 0, m: 0 },
                            layout: crate::ObjectLayout::MultipartManifest { parts_count },
                            tags: hook_req.tags.clone(),
                            metadata_blob: hook_req.metadata_blob.clone(),
                            system_metadata_blob: hook_req.system_metadata_blob.clone(),
                            object_lock: hook_req.object_lock,
                            encryption: hook_req.encryption.clone(),
                        },
                        parts: object_parts,
                        selected_streaming_segments,
                        omitted_parts: Vec::new(),
                        omitted_streaming_segments: Vec::new(),
                        stream_uploads: vec![crate::TerminalStreamCleanupRecord::from(
                            &active_session,
                        )],
                        stream_upload_segments,
                        write_sequence,
                        last_modified_millis,
                        stale_payload: None,
                    },
                )),
            );
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }));

    let calls_for_action = Arc::clone(&action_calls);
    let staged_size = first_segment.size + second_segment.size;
    let staged_payload_crc64 = checksum::crc64::combine(
        first_segment.payload_crc64,
        second_segment.payload_crc64,
        second_segment.size,
    );
    let err = cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(
                &req.upload_id,
                &session_id,
                2,
                staged_size,
                staged_payload_crc64,
            ),
            |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                assert_eq!(snapshot.staged_size, staged_size);
                assert_eq!(snapshot.staged_payload_crc64, staged_payload_crc64);
                Ok::<_, ()>(crate::PreparedStreamPartCommit {
                    value: (),
                    last_modified: 123_457,
                    checksum: None,
                })
            },
        )
        .unwrap_err();

    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(crate::MetadataError::NoSuchUpload {
                upload_id: ref missing
            }) if missing == req.upload_id.as_str()
        ),
        "expected finalize to reload after complete removed the upload, got {err:?}"
    );
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(action_calls.load(Ordering::SeqCst), 1);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    expected_segment.version_id = crate::VersionId::Null.to_u64();
    let outcome = crate::CompleteMultipartCommitOutcome {
        version_id: crate::VersionId::Null,
        stale_payload_generation_id: None,
        live_tags: req.tags.clone(),
        live_size: req.size,
        live_last_modified: 987_655,
    };
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        pg_id.get(),
        &req,
        &expected_segment,
        &outcome,
        1,
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &req.upload_id, 2),
            Err(crate::MetadataError::PartNotFound { .. })
        ));
    }

    for (segment, payload) in [
        (&first_segment, first_payload.as_slice()),
        (&second_segment, second_payload.as_slice()),
    ] {
        let mut readback = Vec::new();
        let error = cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: segment.data_pg_id,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    stored_size: payload.len(),
                    segment_crc64: checksum::crc64::checksum(payload),
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                },
                &mut readback,
            )
            .unwrap_err();
        assert!(
            matches!(error, StoreError::NotFound),
            "complete winner must clean copied staged payload after finalize contention: {error:?}"
        );
    }
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        pg_id.get(),
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn upload_part_stream_finalize_replaces_same_part_with_displaced_cleanup() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "upload-part-replace-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("partreplace");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();

    let first_payload = b"first streamed multipart part";
    let (_first_shards, first_part, first_segment) = upload_streamed_test_multipart_part(
        &cluster,
        &bucket,
        &key,
        &upload_id,
        1,
        [0x5b; 16],
        first_payload,
    );
    assert_eq!(first_part.generation, 0);

    let second_payload = b"replacement streamed multipart part";
    let (_second_shards, second_part, second_segment) = upload_streamed_test_multipart_part(
        &cluster,
        &bucket,
        &key,
        &upload_id,
        1,
        [0x5c; 16],
        second_payload,
    );
    assert_eq!(second_part.generation, 1);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(2).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
            second_part
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part_segments_for_upload_part(
                &*pg, &bucket, &key, &upload_id, 1
            )
            .unwrap(),
            vec![second_segment.clone()]
        );
    }

    let mut first_readback = Vec::new();
    let first_error = cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: first_segment.data_pg_id,
                segment_okh: first_segment.segment_okh,
                segment_vid: first_segment.segment_vid,
                stored_size: first_payload.len(),
                segment_crc64: checksum::crc64::checksum(first_payload),
                ec: EcShape {
                    k: first_segment.ec_k,
                    m: first_segment.ec_m,
                },
            },
            &mut first_readback,
        )
        .unwrap_err();
    assert!(
        matches!(first_error, StoreError::NotFound),
        "replacement finalize must clean displaced part payload: {first_error:?}"
    );

    let mut second_readback = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: second_segment.data_pg_id,
                segment_okh: second_segment.segment_okh,
                segment_vid: second_segment.segment_vid,
                stored_size: second_payload.len(),
                segment_crc64: checksum::crc64::checksum(second_payload),
                ec: EcShape {
                    k: second_segment.ec_k,
                    m: second_segment.ec_m,
                },
            },
            &mut second_readback,
        )
        .unwrap();
    assert_eq!(second_readback, second_payload);

    assert_clean_metadata_command_stream(&map, &[2]);
}

#[test]
fn stream_segment_prepare_uses_durable_session_vid_allocator() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, _object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("7b".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let req = crate::PrepareStreamUploadSegmentAppendReq {
        session_id: session_id.clone(),
        segment_index: 0,
        size: 16,
        segment_crc64: 1,
        payload_crc64: 1,
        segment_okh: [42; 16],
    };

    let (_target, first) = cluster
        .prepare_stream_segment_append(&bucket, &key, &req)
        .unwrap();
    let (_target, second) = cluster
        .prepare_stream_segment_append(&bucket, &key, &req)
        .unwrap();

    assert_eq!(first.segment_vid, crate::GenerationId::MIN);
    assert_eq!(second.segment_vid, crate::GenerationId::new(2).unwrap());
    assert_eq!(first.segment_okh, second.segment_okh);
    assert_eq!(first.segment_index, second.segment_index);
}

#[test]
fn stream_segment_prepare_allocates_vid_after_validation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, _object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("7c".repeat(16)).unwrap();
    let req = crate::PrepareStreamUploadSegmentAppendReq {
        session_id: session_id.clone(),
        segment_index: 0,
        size: 16,
        segment_crc64: 1,
        payload_crc64: 1,
        segment_okh: [42; 16],
    };

    let err = cluster
        .prepare_stream_segment_append(&bucket, &key, &req)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Metadata(crate::MetadataError::StreamSessionNotFound { .. })
    ));

    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let (_target, segment) = cluster
        .prepare_stream_segment_append(&bucket, &key, &req)
        .unwrap();

    assert_eq!(segment.segment_vid, crate::GenerationId::MIN);
}
