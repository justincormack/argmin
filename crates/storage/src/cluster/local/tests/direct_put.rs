// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone, Copy)]
struct DirectPayloadTestIdentity {
    data_pg_id: u32,
    ec: EcShape,
    segment_okh: [u8; 16],
    segment_vid: GenerationId,
}

fn direct_payload_test_identity(
    payload: &crate::DirectPutPayloadWrite<'_>,
) -> DirectPayloadTestIdentity {
    DirectPayloadTestIdentity {
        data_pg_id: payload.written.data_pg_id,
        ec: payload.written.ec,
        segment_okh: payload.segment_okh,
        segment_vid: payload.segment_vid,
    }
}

fn assert_direct_payload_shards_exist(
    cluster: &crate::StorageCluster,
    identity: DirectPayloadTestIdentity,
) {
    for shard_index in 0..identity.ec.k + identity.ec.m {
        assert!(cluster
            .test_payload_shard_file_exists(
                identity.data_pg_id,
                identity.ec,
                &identity.segment_okh,
                identity.segment_vid,
                shard_index,
            )
            .unwrap());
    }
}

fn assert_direct_payload_staging_cleaned(
    map: &LocalClusterMap,
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    reservation_id: &crate::SessionId,
    identity: DirectPayloadTestIdentity,
) {
    for shard_index in 0..identity.ec.k + identity.ec.m {
        assert!(!cluster
            .test_payload_shard_file_exists(
                identity.data_pg_id,
                identity.ec,
                &identity.segment_okh,
                identity.segment_vid,
                shard_index,
            )
            .unwrap());
    }
    let object_pg_id = cluster.object_metadata_pg(bucket, key).pg_id().get();
    for node in map.nodes.values() {
        let node = node.storage_node();
        let pg = node.get_pg(object_pg_id).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                bucket,
                key,
                reservation_id,
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn opaque_direct_put_payload_rejects_crossed_object_routes() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("direct-put-handle-bucket");
    let other_bucket = crate::tests::bucket_name("direct-put-handle-other-bucket");
    let key = crate::tests::object_key("direct-put-handle-key");
    let other_key = crate::tests::object_key("direct-put-handle-other-key");
    create_test_bucket(&cluster, &bucket);
    create_test_bucket(&cluster, &other_bucket);

    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let commit_reservation_id = crate::tests::stream_session_id("opaque-commit");
    let commit_generation_id = route.reserve_generation(&commit_reservation_id).unwrap();
    let commit_payload = route
        .write_direct_object_payload(
            &commit_reservation_id,
            commit_generation_id,
            21,
            b"opaque direct payload",
        )
        .unwrap();
    let commit_identity = direct_payload_test_identity(&commit_payload);
    assert_direct_payload_shards_exist(&cluster, commit_identity);
    let prepared = crate::PreparedDirectPutObjectCommit {
        versioning: crate::BucketVersioningState::Disabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        etag_crc64: checksum::crc64::checksum(b"opaque direct payload"),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        bucket_write_reservation: acquire_test_bucket_write_proof(
            &cluster,
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        ),
    };

    let crossed_key_route = admission
        .active_put_object_route(&bucket, &other_key)
        .unwrap();
    let error = crossed_key_route
        .commit_direct_object(commit_payload, &prepared, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert_eq!(error.kind(), crate::DirectPutFailureKind::InternalError);
    assert_eq!(error.diagnostic_cause_label(), "invalid_request");
    assert_direct_payload_staging_cleaned(
        &map,
        &cluster,
        &bucket,
        &key,
        &commit_reservation_id,
        commit_identity,
    );

    let discard_reservation_id = crate::tests::stream_session_id("opaque-discard");
    let discard_generation_id = route.reserve_generation(&discard_reservation_id).unwrap();
    let discard_payload = route
        .write_direct_object_payload(
            &discard_reservation_id,
            discard_generation_id,
            21,
            b"opaque direct payload",
        )
        .unwrap();
    let discard_identity = direct_payload_test_identity(&discard_payload);
    assert_direct_payload_shards_exist(&cluster, discard_identity);
    let crossed_bucket_route = admission
        .active_put_object_route(&other_bucket, &key)
        .unwrap();
    let error = crossed_bucket_route
        .discard_direct_object_payload(discard_payload)
        .unwrap_err();
    assert_eq!(error.kind(), crate::DirectPutFailureKind::InternalError);
    assert_eq!(error.diagnostic_cause_label(), "invalid_request");
    assert_direct_payload_staging_cleaned(
        &map,
        &cluster,
        &bucket,
        &key,
        &discard_reservation_id,
        discard_identity,
    );
}

#[test]
fn opaque_direct_put_payload_rejects_same_epoch_cross_cluster_commit_and_cleans_owner() {
    let issuer_tmp = test_util::tempdir();
    let receiver_tmp = test_util::tempdir();
    let issuer_map = Arc::new(
        LocalClusterMap::open(
            issuer_tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let receiver_map = Arc::new(
        LocalClusterMap::open(
            receiver_tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let issuer = current_cluster(&issuer_map);
    let receiver = current_cluster(&receiver_map);
    assert_eq!(issuer.cluster_epoch(), receiver.cluster_epoch());
    let bucket = crate::tests::bucket_name("cross-cluster-direct-put");
    let key = crate::tests::object_key("same-subject");
    create_test_bucket(&issuer, &bucket);
    create_test_bucket(&receiver, &bucket);

    let issuer_handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&issuer));
    let receiver_handle =
        crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&receiver));
    let issuer_admission = issuer_handle.admit_current_route().unwrap();
    let receiver_admission = receiver_handle.admit_current_route().unwrap();
    let issuer_route = issuer_admission
        .active_put_object_route(&bucket, &key)
        .unwrap();
    let receiver_route = receiver_admission
        .active_put_object_route(&bucket, &key)
        .unwrap();
    let reservation_id = crate::tests::stream_session_id("cross-cluster");
    let generation_id = issuer_route.reserve_generation(&reservation_id).unwrap();
    let payload = issuer_route
        .write_direct_object_payload(
            &reservation_id,
            generation_id,
            26,
            b"same epoch issuer payload",
        )
        .unwrap();
    let payload_identity = direct_payload_test_identity(&payload);
    assert_direct_payload_shards_exist(&issuer, payload_identity);
    let prepared = crate::PreparedDirectPutObjectCommit {
        versioning: crate::BucketVersioningState::Disabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        etag_crc64: checksum::crc64::checksum(b"same epoch issuer payload"),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        bucket_write_reservation: acquire_test_bucket_write_proof(
            &receiver,
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        ),
    };

    let error = receiver_route
        .commit_direct_object(payload, &prepared, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert_eq!(error.kind(), crate::DirectPutFailureKind::InternalError);
    assert_eq!(error.diagnostic_cause_label(), "invalid_request");
    assert_direct_payload_staging_cleaned(
        &issuer_map,
        &issuer,
        &bucket,
        &key,
        &reservation_id,
        payload_identity,
    );
    let receiver_pg_id = receiver.object_metadata_pg(&bucket, &key).pg_id().get();
    for node_id in trace_node_ids() {
        let pg = receiver_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(receiver_pg_id)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
}

#[test]
fn dropping_armed_direct_put_payload_cleans_issuer_staging() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("dropped-direct-put");
    let key = crate::tests::object_key("uncommitted");
    create_test_bucket(&cluster, &bucket);
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let reservation_id = crate::tests::stream_session_id("drop-direct");
    let generation_id = route.reserve_generation(&reservation_id).unwrap();
    let payload = route
        .write_direct_object_payload(&reservation_id, generation_id, 20, b"drop cleans payload")
        .unwrap();
    let payload_identity = direct_payload_test_identity(&payload);
    assert_direct_payload_shards_exist(&cluster, payload_identity);

    drop(payload);

    assert_direct_payload_staging_cleaned(
        &map,
        &cluster,
        &bucket,
        &key,
        &reservation_id,
        payload_identity,
    );
}

#[test]
fn matching_pending_direct_put_retries_transient_abandonment_scan_and_preserves_fatal_error() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("direct-put-inspection-failure");
    let key = crate::tests::object_key("pending-command");
    create_test_bucket(&cluster, &bucket);
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let reservation_id = crate::tests::stream_session_id("inspect-failure");
    let generation_id = route.reserve_generation(&reservation_id).unwrap();
    let data = b"payload owned by a matching pending command";
    let payload = route
        .write_direct_object_payload(&reservation_id, generation_id, data.len() as u64, data)
        .unwrap();
    let payload_identity = direct_payload_test_identity(&payload);
    let prepared = crate::PreparedDirectPutObjectCommit {
        versioning: crate::BucketVersioningState::Disabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        etag_crc64: checksum::crc64::checksum(data),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        bucket_write_reservation: acquire_test_bucket_write_proof(
            &cluster,
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        ),
    };
    let request = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id,
            payload: data,
            segment_okh: payload.segment_okh,
            written: &payload.written,
        },
        prepared.bucket_write_reservation.clone(),
    );
    let shard_batch: Vec<(&ShardKey, WriteAck)> = payload
        .written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    cluster
        .register_payload_shard_acks(request.data_pg_id, &shard_batch)
        .unwrap();
    let pg_id = cluster.object_metadata_pg(&bucket, &key).pg_id();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let command = cluster
        .prepare_commit_direct_put_object_command(
            pg_id,
            &pg,
            &request,
            crate::VersionId::Null,
            request.bucket_write_reservation.clone(),
        )
        .unwrap();
    drop(pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_for_hook = Arc::clone(&hook_calls);
    let hook_guard = cluster.test_install_before_direct_put_abandoned_log_inspection_hook(
        Arc::new(move |command| {
            assert!(matches!(
                command.payload(),
                MetadataCommandPayload::CommitDirectPutObject(_)
            ));
            if hook_calls_for_hook.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(StoreError::StorageRpc {
                    node_id: 2,
                    operation: "injected direct PUT abandonment observation",
                    failure: crate::storage_rpc::StorageRpcErrorCode::TransportClosed,
                    detail: crate::StorageNodeFailureDetail::new(
                        "transient response loss before fatal observation",
                    ),
                }
                .into());
            }
            Err(StoreError::MetadataCommandLogChecksumMismatch {
                node_id: 2,
                pg_id: command.id().pg_id().get(),
                cluster_epoch: command.id().cluster_epoch(),
                log_index: command.id().log_index().get(),
                stored_checksum: 1,
                computed_checksum: 2,
            }
            .into())
        }),
    );
    let error = route
        .commit_direct_object(payload, &prepared, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert_eq!(error.kind(), crate::DirectPutFailureKind::InternalError);
    assert_eq!(error.diagnostic_cause_label(), "store_integrity_failure");
    assert!(hook_calls.load(Ordering::SeqCst) >= 2);

    let retained = pending_metadata_command_for_test(&map, pg_id, &bucket)
        .expect("matching durable command must remain pending");
    assert_eq!(retained.id(), command.id());
    assert_direct_payload_shards_exist(&cluster, payload_identity);
    for node_id in trace_node_ids() {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id,
            )
            .unwrap(),
            generation_id,
            "durable pending ownership must retain the generation reservation"
        );
    }

    drop(hook_guard);
}

#[test]
fn matching_published_direct_put_returns_before_trailing_payload_ack_work() {
    let tmp = test_util::tempdir();
    let node_ids = trace_node_ids();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &node_ids,
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("direct-put-published-handoff");
    let key = crate::tests::object_key("pending-command");
    create_test_bucket(&cluster, &bucket);
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let reservation_id = crate::tests::stream_session_id("published");
    let generation_id = route.reserve_generation(&reservation_id).unwrap();
    let data = b"payload owned by a published pending command";
    let payload = route
        .write_direct_object_payload(&reservation_id, generation_id, data.len() as u64, data)
        .unwrap();
    let prepared = crate::PreparedDirectPutObjectCommit {
        versioning: crate::BucketVersioningState::Disabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        etag_crc64: checksum::crc64::checksum(data),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        bucket_write_reservation: acquire_test_bucket_write_proof(
            &cluster,
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        ),
    };
    let request = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload: data,
            segment_okh: payload.segment_okh,
            written: &payload.written,
        },
        prepared.bucket_write_reservation.clone(),
    );
    let pg_id = cluster.object_metadata_pg(&bucket, &key).pg_id();
    let primary = map
        .metadata_pg_primary_node(cluster.operation_epoch(), pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let command = cluster
        .prepare_commit_direct_put_object_command(
            pg_id,
            &primary_pg,
            &request,
            crate::VersionId::Null,
            request.bucket_write_reservation.clone(),
        )
        .unwrap();
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    primary
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap()
        .mark_pending_metadata_command_publication_started(primary.node_id().as_u32(), &command)
        .unwrap();
    let witness = node_ids
        .iter()
        .copied()
        .find(|node_id| *node_id != primary.node_id())
        .unwrap();
    for node_id in [witness, primary.node_id()] {
        map.node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap()
            .apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
    }
    let trailing = node_ids
        .iter()
        .copied()
        .find(|node_id| *node_id != witness && *node_id != primary.node_id())
        .unwrap();

    let scan_hook =
        cluster.test_install_before_direct_put_abandoned_log_inspection_hook(Arc::new(|_| {
            Err(StoreError::StorageRpc {
                node_id: 2,
                operation: "injected published direct PUT abandonment observation",
                failure: crate::storage_rpc::StorageRpcErrorCode::TransportClosed,
                detail: crate::StorageNodeFailureDetail::new(
                    "injected response loss after direct PUT publication",
                ),
            }
            .into())
        }));
    let budget_command = command.clone();
    let budget_hook = cluster.test_install_direct_put_abandonment_observation_budget_hook(
        Arc::new(move |command| command == &budget_command),
    );
    let ack_registration_calls = Arc::new(AtomicUsize::new(0));
    let ack_registration_calls_for_hook = Arc::clone(&ack_registration_calls);
    let ack_registration_hook = cluster
        .test_install_before_direct_put_payload_ack_registration_hook(Arc::new(move || {
            ack_registration_calls_for_hook.fetch_add(1, Ordering::SeqCst);
            Err(crate::ObjectPgActionError::InvalidRequest {
                reason: "published direct PUT continued into payload acknowledgement registration"
                    .to_owned(),
            })
        }));
    let outcome = route
        .commit_direct_object(payload, &prepared, |_| -> Result<(), ()> {
            panic!("matching published direct PUT must not rerun its precondition")
        })
        .unwrap()
        .unwrap();
    drop(ack_registration_hook);
    drop(budget_hook);
    drop(scan_hook);

    assert_eq!(
        ack_registration_calls.load(Ordering::SeqCst),
        0,
        "confirmed publication must return before payload acknowledgement work"
    );
    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_size, data.len() as u64);
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(command.clone()),
        "confirmed publication must leave trailing convergence to recovery"
    );
    let trailing_pg = map
        .node(trailing)
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_meta(&*trailing_pg, &bucket, &key),
        Err(crate::MetadataError::ObjectNotFound)
    ));
}

#[test]
fn physically_mismatched_pending_direct_put_partitions_mixed_overlap_cleanup() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("direct-put-physical-mismatch");
    let key = crate::tests::object_key("pending-command");
    create_test_bucket(&cluster, &bucket);
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let reservation_id = crate::tests::stream_session_id("phys-mismatch");
    let generation_id = route.reserve_generation(&reservation_id).unwrap();
    let data = b"payload whose staging keys overlap the pending command";
    let pending_payload = route
        .write_direct_object_payload(&reservation_id, generation_id, data.len() as u64, data)
        .unwrap();
    let payload_identity = direct_payload_test_identity(&pending_payload);
    let prepared = crate::PreparedDirectPutObjectCommit {
        versioning: crate::BucketVersioningState::Disabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        etag_crc64: checksum::crc64::checksum(data),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        bucket_write_reservation: acquire_test_bucket_write_proof(
            &cluster,
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        ),
    };
    let mut pending_request = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id,
            payload: data,
            segment_okh: pending_payload.segment_okh,
            written: &pending_payload.written,
        },
        prepared.bucket_write_reservation.clone(),
    );
    pending_request.ec = EcShape { k: 1, m: 1 };
    let command_shard_count = pending_request.ec.k + pending_request.ec.m;
    assert!(
        command_shard_count < payload_identity.ec.k + payload_identity.ec.m,
        "the regression requires caller shards beyond the command's overlapping prefix"
    );
    let shard_batch: Vec<(&ShardKey, WriteAck)> = pending_payload
        .written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    cluster
        .register_payload_shard_acks(pending_request.data_pg_id, &shard_batch)
        .unwrap();
    let pg_id = cluster.object_metadata_pg(&bucket, &key).pg_id();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let command = cluster
        .prepare_commit_direct_put_object_command(
            pg_id,
            &pg,
            &pending_request,
            crate::VersionId::Null,
            pending_request.bucket_write_reservation.clone(),
        )
        .unwrap();
    drop(pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    pending_payload.disarm();

    let mismatched_payload = route
        .write_direct_object_payload(&reservation_id, generation_id, data.len() as u64, data)
        .unwrap();
    assert_eq!(
        direct_payload_test_identity(&mismatched_payload).segment_okh,
        payload_identity.segment_okh,
        "the regression must exercise overlapping command staging keys"
    );
    let error = route
        .commit_direct_object(mismatched_payload, &prepared, |_| -> Result<(), ()> {
            panic!("a physically mismatched pending command must not enter recovery")
        })
        .unwrap_err();
    assert_eq!(
        error.kind(),
        crate::DirectPutFailureKind::MetadataCommandContention
    );
    assert_eq!(
        error.diagnostic_cause_label(),
        "store_metadata_command_contention"
    );

    let retained = pending_metadata_command_for_test(&map, pg_id, &bucket)
        .expect("physically mismatched command must remain pending");
    assert_eq!(retained.id(), command.id());
    for shard_index in 0..payload_identity.ec.k + payload_identity.ec.m {
        assert_eq!(
            cluster
                .test_payload_shard_file_exists(
                    payload_identity.data_pg_id,
                    payload_identity.ec,
                    &payload_identity.segment_okh,
                    payload_identity.segment_vid,
                    shard_index,
                )
                .unwrap(),
            shard_index < command_shard_count,
            "command-owned overlap must survive while caller-only shards are cleaned"
        );
    }
    for node_id in trace_node_ids() {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id,
            )
            .unwrap(),
            generation_id,
            "the pending command must retain its generation reservation"
        );
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
}

#[test]
fn direct_put_fanout_rejects_live_crossed_reservation_subjects() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::tests::bucket_name("direct-put-crossed-proof-bucket");
    let key = crate::tests::object_key("direct-put-crossed-proof-key");
    create_test_bucket(&cluster, &bucket);

    let reservation_id = crate::tests::stream_session_id("crossed-proof");
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let operation_reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let operation_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&operation_reservation.record);
    let target_reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some("direct-put-other-key"),
        )
        .unwrap();
    let target_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&target_reservation.record);

    let pg_id = PgId::new(0);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let write_sequence = primary
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap()
        .next_object_write_sequence(bucket.as_str(), key.as_str())
        .unwrap();
    let command_with_proof = |proof| {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(
                crate::metadata_command::CommitDirectPutObjectCommand {
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
                    generation_reservation_id: reservation_id.clone(),
                    write_sequence,
                    last_modified_millis: 1,
                    stale_payload: None,
                    bucket_write_reservation: proof,
                },
            )),
        )
    };

    for (case, proof) in [
        ("operation", operation_proof.clone()),
        ("target", target_proof),
    ] {
        let command = command_with_proof(proof);
        let error = cluster
            .validate_metadata_command_bucket_write_reservation(&command)
            .unwrap_err();
        assert!(
            matches!(
                error,
                crate::BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::BucketWriteReservationConflict { .. }
                )
            ),
            "crossed direct PUT proof {case} must fail central fanout validation, got {error:?}"
        );
    }

    let command = command_with_proof(operation_proof);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
}

#[test]
fn direct_put_terminal_install_conflict_reinspects_before_draining_newer_command() {
    let _guard = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-pending-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    let newer_key = key_for_object_pg(topology, &bucket, 2, "newer-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);

    let loser_payload = b"loser direct put";
    let loser_reservation_id =
        crate::SessionId::try_from("11111111111111111111111111111111".to_string()).unwrap();
    let loser_generation_id = first_cluster
        .reserve_put_object_generation(&bucket, &key, &loser_reservation_id)
        .unwrap();
    let loser_written = first_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            loser_generation_id,
            0,
            &[0x91; 16],
            loser_payload,
        )
        .unwrap();
    let loser_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: loser_reservation_id,
            generation_id: loser_generation_id,
            payload: loser_payload,
            segment_okh: [0x91; 16],
            written: &loser_written,
        },
    );

    let winner_payload = b"winner direct put";
    let winner_reservation_id =
        crate::SessionId::try_from("22222222222222222222222222222222".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            winner_generation_id,
            0,
            &[0x92; 16],
            winner_payload,
        )
        .unwrap();
    let winner_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: winner_reservation_id,
            generation_id: winner_generation_id,
            payload: winner_payload,
            segment_okh: [0x92; 16],
            written: &winner_written,
        },
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_cluster = Arc::clone(&second_cluster);
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_req = winner_req.clone();
    let hook_written_shards = winner_written.written_shards.clone();
    let hook_newer_key = newer_key;
    let newer_command = Arc::new(Mutex::new(None));
    let newer_command_for_hook = Arc::clone(&newer_command);
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = first_cluster.test_install_before_metadata_command_pending_install_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(2);
            let shard_batch: Vec<(&ShardKey, WriteAck)> = hook_written_shards
                .iter()
                .map(|written| (&written.key, written.ack))
                .collect();
            hook_cluster
                .register_payload_shard_acks(hook_req.data_pg_id, &shard_batch)
                .unwrap();
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let command = hook_cluster
                .prepare_commit_direct_put_object_command(
                    pg_id,
                    &pg,
                    &hook_req,
                    crate::VersionId::Null,
                    hook_req.bucket_write_reservation.clone(),
                )
                .unwrap();
            drop(pg);
            hook_cluster
                .test_apply_metadata_command_to_acting_set_from_origin(primary.node_id(), &command)
                .unwrap();
            let newer = MetadataCommandEnvelope::new(
                hook_cluster.next_object_metadata_command_id(pg_id).unwrap(),
                MetadataCommandPayload::ReserveObjectGeneration(
                    ReserveObjectGenerationCommand::new(
                        hook_bucket.clone(),
                        hook_newer_key.clone(),
                        crate::SessionId::try_from("23".repeat(16)).unwrap(),
                        GenerationId::new(203).unwrap(),
                        crate::clock::current_time_millis(),
                    ),
                ),
            );
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &newer,
                Some(&hook_bucket),
            )
            .unwrap();
            *newer_command_for_hook.lock().unwrap() = Some(newer);
        }),
    );

    let snapshot_read_attempts = Arc::new(AtomicUsize::new(0));
    let snapshot_read_attempts_for_hook = Arc::clone(&snapshot_read_attempts);
    let map_for_snapshot_hook = Arc::clone(&first_map);
    let bucket_for_snapshot_hook = bucket.clone();
    let newer_command_for_snapshot_hook = Arc::clone(&newer_command);
    let _snapshot_read_hook = first_cluster.test_install_direct_put_snapshot_read_hook(Arc::new(
        move || {
            match snapshot_read_attempts_for_hook.fetch_add(1, Ordering::SeqCst) {
                0 => Ok(()),
                1 => Err(crate::ObjectPgActionError::Store(
                    crate::StoreError::MetadataCommandContention {
                        context: "injected direct PUT snapshot read contention",
                    },
                )),
                2 => {
                    assert_eq!(
                        pending_metadata_command_for_test(
                            &map_for_snapshot_hook,
                            PgId::new(2),
                            &bucket_for_snapshot_hook,
                        ),
                        newer_command_for_snapshot_hook.lock().unwrap().clone(),
                        "a transient snapshot read failure must not permit the newer command to be drained",
                    );
                    Ok(())
                }
                _ => Ok(()),
            }
        },
    ));

    let calls_for_action = Arc::clone(&action_calls);
    let map_for_action = Arc::clone(&first_map);
    let bucket_for_action = bucket.clone();
    let newer_command_for_action = Arc::clone(&newer_command);
    let pending_install_backoffs = || {
        observability::metadata_command_backoff_dimension_snapshot()
            .into_iter()
            .filter(|sample| {
                sample.pg_id == Some(2)
                    && sample.operation == "commit_direct_put_metadata"
                    && sample.context == "direct PUT pending install retry budget exhausted"
            })
            .map(|sample| sample.count)
            .sum::<u64>()
    };
    let backoffs_before = pending_install_backoffs();
    let result = first_cluster
        .commit_direct_put_object_from_payload_shards(
            &loser_req,
            &loser_written.written_shards,
            move |snapshot| {
                let call = calls_for_action.fetch_add(1, Ordering::SeqCst);
                if call == 1 {
                    assert_eq!(
                        pending_metadata_command_for_test(
                            &map_for_action,
                            PgId::new(2),
                            &bucket_for_action,
                        ),
                        newer_command_for_action.lock().unwrap().clone(),
                        "snapshot reinspection must precede draining a newer pending command"
                    );
                }
                if snapshot.existing_etag.is_some() {
                    Err("object already exists")
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
    assert!(matches!(result, Err("object already exists")));
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        pending_install_backoffs(),
        backoffs_before,
        "draining a predecessor is progress and must immediately re-enter FIFO admission"
    );
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "direct PUT precondition must be rerun after slot contention changes object state"
    );
    assert_eq!(
        snapshot_read_attempts.load(Ordering::SeqCst),
        3,
        "snapshot reinspection must remain latched across one transient read failure"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().expect("winner object is live");
        assert_eq!(live.generation_id, winner_generation_id);
        assert_eq!(live.size, winner_payload.len() as u64);
    }
}

#[test]
fn direct_put_transferred_contender_exits_without_retrying_drain() {
    let _guard = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-contender-convergence-");
    let key = key_for_object_pg(topology, &bucket, 2, "candidate-");
    let contender_key = key_for_object_pg(topology, &bucket, 2, "contender-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id = crate::SessionId::try_from("23".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let segment_okh = [0x93; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            b"uninstalled direct PUT candidate",
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload: b"uninstalled direct PUT candidate",
            segment_okh,
            written: &written,
        },
    );

    let pg_id = PgId::new(2);
    let contender = MetadataCommandEnvelope::new(
        cluster.next_object_metadata_command_id(pg_id).unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            contender_key,
            crate::SessionId::try_from("24".repeat(16)).unwrap(),
            GenerationId::new(24).unwrap(),
            crate::clock::current_time_millis(),
        )),
    );
    let contender_id = contender.id();
    let install_ran = Arc::new(AtomicBool::new(false));
    let install_ran_for_hook = Arc::clone(&install_ran);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_contender = contender.clone();
    let _install_hook =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if install_ran_for_hook.swap(true, Ordering::SeqCst) {
                return;
            }
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &hook_contender,
                Some(&hook_bucket),
            )
            .unwrap();
        }));

    let drain_attempts = Arc::new(AtomicUsize::new(0));
    let drain_attempts_for_hook = Arc::clone(&drain_attempts);
    let _drain_hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |command| {
            if command.id() == contender_id {
                drain_attempts_for_hook.fetch_add(1, Ordering::SeqCst);
                return Err(StoreError::RouteMapExpired {
                    cluster_epoch: contender_id.cluster_epoch(),
                    valid_until_ms: 1,
                    now_ms: 2,
                });
            }
            Ok(())
        }));

    let started = Instant::now();
    let error = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "transferred contender took {:?} to leave the request path",
        started.elapsed()
    );
    assert!(matches!(
        &error,
        crate::ObjectPgActionError::MetadataCommandRecoveryTransferred
    ));
    assert!(install_ran.load(Ordering::SeqCst));
    assert_eq!(drain_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        crate::DirectPutFailure::from_object_pg_action(error).kind(),
        crate::DirectPutFailureKind::MetadataCommandContention
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(contender),
        "the unrelated converging command must remain available to recovery"
    );
    assert_bucket_write_reservations_released(&map, &bucket);
    for shard_index in 0..written.ec.k + written.ec.m {
        assert!(!cluster
            .test_payload_shard_file_exists(
                written.data_pg_id,
                written.ec,
                &segment_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
        Err(crate::MetadataError::ObjectNotFound)
    ));
}

#[test]
fn direct_put_pending_install_uncertainty_without_durable_command_reinspects() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-install-uncertain-no-slot-");
    let key = key_for_object_pg(topology, &bucket, 2, "candidate-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(2);
    let mut command = MetadataCommandEnvelope::new(
        cluster.next_object_metadata_command_id(pg_id).unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key,
            crate::SessionId::try_from("67".repeat(16)).unwrap(),
            GenerationId::new(101).unwrap(),
            crate::clock::current_time_millis().saturating_add(60_000),
        )),
    );

    let (observed_tx, observed_rx) = std::sync::mpsc::sync_channel(1);
    let observation_signalled = Arc::new(AtomicBool::new(false));
    let observation_signalled_for_hook = Arc::clone(&observation_signalled);
    let _inspection_hook =
        cluster.test_install_post_budget_metadata_command_inspection_hook(Arc::new(move |_, _| {
            if !observation_signalled_for_hook.swap(true, Ordering::SeqCst) {
                observed_tx.send(()).unwrap();
            }
            None
        }));
    let pg_lock = map.runtime_state().metadata_command_pg_lock(pg_id);
    let pg_guard = pg_lock.lock();
    let finish_cluster = Arc::clone(&cluster);
    let finish_bucket = bucket.clone();
    let worker = std::thread::spawn(move || {
        finish_cluster.finish_direct_put_after_pending_install_uncertainty(
            pg_id,
            &finish_bucket,
            &mut command,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                context: "test pending install uncertainty",
            }),
        )
    });
    observed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("publication inspection must observe the busy PG serialization boundary");
    drop(pg_guard);
    let (outcome, command_owned) = worker.join().unwrap().unwrap();

    assert!(!command_owned);
    assert!(matches!(
        outcome,
        crate::cluster::request_ops::NewObjectMetadataCommandApplyOutcome::Reinspect(
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention { .. })
        )
    ));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn snapshot_sensitive_install_drains_only_the_observed_contender() {
    let _guard = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "snapshot-install-one-drain-");
    let first_key = key_for_object_pg(topology, &bucket, 2, "first-");
    let candidate_key = key_for_object_pg(topology, &bucket, 2, "candidate-");
    let second_key = key_for_object_pg(topology, &bucket, 2, "second-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(2);
    let first_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let first = MetadataCommandEnvelope::new(
        first_id,
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            first_key,
            crate::SessionId::try_from("61".repeat(16)).unwrap(),
            GenerationId::new(101).unwrap(),
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &first);

    let next_log_index = MetadataCommandLogIndex::new(first_id.log_index().get() + 1).unwrap();
    let candidate = MetadataCommandEnvelope::new(
        MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, next_log_index),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            candidate_key,
            crate::SessionId::try_from("62".repeat(16)).unwrap(),
            GenerationId::new(102).unwrap(),
            crate::clock::current_time_millis(),
        )),
    );

    let inserted_after_drain = Arc::new(Mutex::new(None));
    let hook_cluster = Arc::clone(&cluster);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let inserted_after_drain_for_hook = Arc::clone(&inserted_after_drain);
    let _hook_guard = cluster.test_install_after_metadata_command_drain_hook(Arc::new(move || {
        let command_id = hook_cluster.next_object_metadata_command_id(pg_id).unwrap();
        assert_eq!(command_id.log_index(), next_log_index);
        let second = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                hook_bucket.clone(),
                second_key.clone(),
                crate::SessionId::try_from("63".repeat(16)).unwrap(),
                GenerationId::new(103).unwrap(),
                crate::clock::current_time_millis(),
            )),
        );
        insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &second);
        *inserted_after_drain_for_hook.lock().unwrap() = Some(second);
    }));

    let outcome = cluster
        .install_snapshot_sensitive_metadata_command_or_drain(
            crate::metadata_command::metadata_command_publisher!(
                CommitDirectPutObjectFromPayloadShards
            ),
            pg_id,
            &bucket,
            &candidate,
            None,
            &mut crate::cluster::SnapshotSensitiveRetryPhase::default().snapshot_evaluated(),
        )
        .unwrap();
    assert_eq!(
        outcome,
        crate::cluster::SnapshotSensitiveInstallOutcome::ContenderDrained
    );
    let inserted_after_drain = inserted_after_drain.lock().unwrap().clone().unwrap();
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(inserted_after_drain),
        "typed installation must return after draining its observed contender"
    );
}

#[test]
fn snapshot_sensitive_install_recognizes_its_exact_terminal_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "snapshot-install-terminal-");
    let key = key_for_object_pg(topology, &bucket, 2, "terminal-");
    let contender_key = key_for_object_pg(topology, &bucket, 2, "contender-");
    let candidate_key = key_for_object_pg(topology, &bucket, 2, "candidate-");
    let newer_key = key_for_object_pg(topology, &bucket, 2, "newer-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    // Put the primary after the deterministic witness in publication order.
    // Terminal replay must not depend on which actor reports the conflict.
    set_route_primary(&mut map, 2, NodeId::new(0));

    let map = Arc::new(map);
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(2);
    let command = MetadataCommandEnvelope::new(
        cluster.next_object_metadata_command_id(pg_id).unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key,
            crate::SessionId::try_from("64".repeat(16)).unwrap(),
            GenerationId::new(104).unwrap(),
            crate::clock::current_time_millis(),
        )),
    );
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &command)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.record_current_metadata_command_checkpoint(node_id.as_u32(), ClusterEpoch::INITIAL)
            .unwrap();
        assert!(matches!(
            pg.compact_metadata_command_log(ClusterEpoch::INITIAL)
                .unwrap(),
            crate::pg_store::MetadataCommandLogCompactionStatus::Compacted {
                deleted_entries: 1,
                ..
            }
        ));
        assert_eq!(
            pg.metadata_command_log_stats(ClusterEpoch::INITIAL)
                .unwrap()
                .retained_entries,
            0
        );
    }

    let outcome = cluster
        .install_snapshot_sensitive_metadata_command_or_drain(
            crate::metadata_command::metadata_command_publisher!(
                CommitDirectPutObjectFromPayloadShards
            ),
            pg_id,
            &bucket,
            &command,
            None,
            &mut crate::cluster::SnapshotSensitiveRetryPhase::default().snapshot_evaluated(),
        )
        .unwrap();

    assert_eq!(
        outcome,
        crate::cluster::SnapshotSensitiveInstallOutcome::Installed,
        "an exact command applied while its insert response was lost must not be treated as a contender"
    );
    let mut work_budget = crate::cluster::RequestWorkBudget::new(Duration::from_secs(1), None)
        .for_operation("test_exact_terminal_pending_install")
        .for_pg(pg_id);
    assert!(matches!(
        cluster
            .apply_new_object_metadata_command_for_bucket_or_reinspect(
                pg_id,
                &bucket,
                &command,
                &mut work_budget,
            )
            .unwrap(),
        crate::cluster::request_ops::NewObjectMetadataCommandApplyOutcome::Applied
    ));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    let contender_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let contender = MetadataCommandEnvelope::new(
        contender_id,
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            contender_key,
            crate::SessionId::try_from("65".repeat(16)).unwrap(),
            GenerationId::new(105).unwrap(),
            crate::clock::current_time_millis(),
        )),
    );
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &contender)
        .unwrap();
    let candidate = MetadataCommandEnvelope::new(
        contender_id,
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            candidate_key,
            crate::SessionId::try_from("66".repeat(16)).unwrap(),
            GenerationId::new(106).unwrap(),
            crate::clock::current_time_millis(),
        )),
    );
    let newer = MetadataCommandEnvelope::new(
        cluster.next_object_metadata_command_id(pg_id).unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            newer_key,
            crate::SessionId::try_from("67".repeat(16)).unwrap(),
            GenerationId::new(107).unwrap(),
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &newer);
    assert_eq!(
        cluster
            .install_snapshot_sensitive_metadata_command_or_drain(
                crate::metadata_command::metadata_command_publisher!(
                    CommitDirectPutObjectFromPayloadShards
                ),
                pg_id,
                &bucket,
                &candidate,
                None,
                &mut crate::cluster::SnapshotSensitiveRetryPhase::default().snapshot_evaluated(),
            )
            .unwrap(),
        crate::cluster::SnapshotSensitiveInstallOutcome::ReinspectSnapshot,
        "a different terminal command at the candidate index must trigger reinspection"
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(newer),
        "a stale candidate must not drain a newer command before snapshot reinspection"
    );
    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    assert_clean_metadata_command_stream(&map, &[2]);
}

#[test]
fn direct_put_pending_install_race_keeps_bucket_write_proof_for_retry() {
    let _guard = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-proof-race-");
    let loser_key = key_for_object_pg(topology, &bucket, 2, "loser-");
    let winner_key = key_for_object_pg(topology, &bucket, 2, "winner-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);

    let loser_payload = b"loser direct put with command proof";
    let loser_reservation_id =
        crate::SessionId::try_from("51515151515151515151515151515151".to_string()).unwrap();
    let loser_generation_id = first_cluster
        .reserve_put_object_generation(&bucket, &loser_key, &loser_reservation_id)
        .unwrap();
    let loser_written = first_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &loser_key,
            loser_generation_id,
            0,
            &[0xb1; 16],
            loser_payload,
        )
        .unwrap();
    let command_reservation = first_cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(loser_key.as_str()),
        )
        .unwrap();
    let loser_req = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &loser_key,
            reservation_id: loser_reservation_id,
            generation_id: loser_generation_id,
            payload: loser_payload,
            segment_okh: [0xb1; 16],
            written: &loser_written,
        },
        crate::metadata_command::BucketWriteReservationProof::from(&command_reservation.record),
    );

    let winner_payload = b"winner unrelated direct put";
    let winner_reservation_id =
        crate::SessionId::try_from("52525252525252525252525252525252".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &winner_key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &winner_key,
            winner_generation_id,
            0,
            &[0xb2; 16],
            winner_payload,
        )
        .unwrap();
    let winner_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &winner_key,
            reservation_id: winner_reservation_id,
            generation_id: winner_generation_id,
            payload: winner_payload,
            segment_okh: [0xb2; 16],
            written: &winner_written,
        },
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_cluster = Arc::clone(&second_cluster);
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_req = winner_req.clone();
    let hook_written_shards = winner_written.written_shards.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = first_cluster.test_install_before_metadata_command_pending_install_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(2);
            let shard_batch: Vec<(&ShardKey, WriteAck)> = hook_written_shards
                .iter()
                .map(|written| (&written.key, written.ack))
                .collect();
            hook_cluster
                .register_payload_shard_acks(hook_req.data_pg_id, &shard_batch)
                .unwrap();
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let command = hook_cluster
                .prepare_commit_direct_put_object_command(
                    pg_id,
                    &pg,
                    &hook_req,
                    crate::VersionId::Null,
                    hook_req.bucket_write_reservation.clone(),
                )
                .unwrap();
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }),
    );

    let calls_for_action = Arc::clone(&action_calls);
    first_cluster
        .commit_direct_put_object_from_payload_shards(
            &loser_req,
            &loser_written.written_shards,
            move |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                assert!(snapshot.existing_etag.is_none());
                Ok::<(), ()>(())
            },
        )
        .unwrap()
        .unwrap();
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "direct PUT must rerun after install contention while keeping its write proof"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());
    let object_pg = first_map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    let stored = crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &loser_key).unwrap();
    assert_eq!(stored.as_live().unwrap().generation_id, loser_generation_id);
    drop(object_pg);
    let bucket_pg = first_map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty()
    );
    drop(bucket_pg);
    assert_clean_metadata_command_stream(&first_map, &[2]);
}

#[test]
fn direct_put_committed_response_loss_retry_returns_existing_commit() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-response-loss-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("53535353535353535353535353535353".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put committed response loss retry";
    let segment_okh = [0xb3; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let hook_guard =
        cluster.test_install_after_direct_put_metadata_publish_hook(Arc::new(|_, _| {
            Err(crate::ObjectPgActionError::InvalidRequest {
                reason: "injected direct PUT response loss".to_string(),
            })
        }));

    let first_err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason == "injected direct PUT response loss"
        ),
        "expected injected post-commit direct PUT response-loss error, got {first_err:?}"
    );
    drop(hook_guard);

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            |_| -> Result<(), ()> { panic!("committed direct PUT retry must not rerun action") },
        )
        .unwrap()
        .unwrap();

    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_size, payload.len() as u64);
    assert!(pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[2]);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().expect("direct PUT object should be live");
        assert_eq!(live.generation_id, generation_id);
        assert_eq!(live.size, payload.len() as u64);
    }
}

#[test]
fn direct_put_budget_expiry_after_pending_install_returns_snapshot_conflict_and_cleans_staging() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .node(NodeId::new(0))
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

    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let reservation_id = crate::tests::stream_session_id("install-expiry");
    let generation_id = route.reserve_generation(&reservation_id).unwrap();
    let data = b"direct PUT inherited budget expiry after pending install";
    let payload = route
        .write_direct_object_payload(&reservation_id, generation_id, data.len() as u64, data)
        .unwrap();
    let payload_identity = direct_payload_test_identity(&payload);
    let prepared = crate::PreparedDirectPutObjectCommit {
        versioning: crate::BucketVersioningState::Enabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        etag_crc64: checksum::crc64::checksum(data),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        bucket_write_reservation: acquire_test_bucket_write_proof(
            &cluster,
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        ),
    };

    let _serial = lock_metadata_command_apply_hook_test();
    let expired = Arc::new(AtomicBool::new(false));
    let expired_hook = Arc::clone(&expired);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard =
        cluster.test_install_direct_put_pending_installed_hook(Arc::new(move |command| {
            matches!(
                command.payload(),
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                && !expired_hook.swap(true, Ordering::SeqCst)
            )
        }));
    let reinspection_hook =
        cluster.test_install_snapshot_reinspection_hook(Arc::new(|| Duration::from_millis(250)));
    let before_action_hook =
        cluster.test_install_before_snapshot_reinspection_action_hook(Arc::new(|| {
            thread::sleep(Duration::from_millis(300))
        }));
    let action_calls = AtomicUsize::new(0);
    let error = route
        .commit_direct_object(payload, &prepared, |_| {
            action_calls.fetch_add(1, Ordering::SeqCst);
            Ok::<(), ()>(())
        })
        .unwrap_err();
    drop(before_action_hook);
    drop(reinspection_hook);
    drop(hook_guard);

    assert!(expired.load(Ordering::SeqCst));
    assert_eq!(
        error.kind(),
        crate::DirectPutFailureKind::SnapshotReinspectionConflict
    );
    assert_eq!(action_calls.load(Ordering::SeqCst), 1);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    assert_direct_payload_staging_cleaned(
        &map,
        &cluster,
        &bucket,
        &key,
        &reservation_id,
        payload_identity,
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
}

#[test]
fn direct_put_retry_reconstructs_matching_witness_before_reservation_validation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .node(NodeId::new(0))
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

    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let reservation_id = crate::tests::stream_session_id("adopt-witness");
    let generation_id = route.reserve_generation(&reservation_id).unwrap();
    let data = b"direct PUT matching pending witness adoption";
    let payload = route
        .write_direct_object_payload(&reservation_id, generation_id, data.len() as u64, data)
        .unwrap();
    let prepared = crate::PreparedDirectPutObjectCommit {
        versioning: crate::BucketVersioningState::Enabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        etag_crc64: checksum::crc64::checksum(data),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        bucket_write_reservation: acquire_test_bucket_write_proof(
            &cluster,
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        ),
    };
    let request = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id,
            payload: data,
            segment_okh: payload.segment_okh,
            written: &payload.written,
        },
        prepared.bucket_write_reservation.clone(),
    );
    let shard_batch = payload
        .written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .register_payload_shard_acks(request.data_pg_id, &shard_batch)
        .unwrap();
    let pg_id = PgId::new(object_pg);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(object_pg).unwrap();
    let command = cluster
        .prepare_commit_direct_put_object_command(
            pg_id,
            &primary_pg,
            &request,
            crate::VersionId::from_u64(1),
            prepared.bucket_write_reservation.clone(),
        )
        .unwrap();
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &command)
        .unwrap();
    cluster
        .release_bucket_write_reservation_proof(&prepared.bucket_write_reservation)
        .unwrap();

    let outcome = route
        .commit_direct_object(payload, &prepared, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();
    assert_eq!(outcome.version_id, crate::VersionId::from_u64(1));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let live = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
            .unwrap()
            .into_live()
            .expect("recovered direct PUT must publish a live object");
        assert_eq!(live.version_id, outcome.version_id);
        assert_eq!(live.generation_id, generation_id);
    }
}

#[test]
fn versioned_direct_put_hands_transported_trailing_contention_to_recovery() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .node(NodeId::new(0))
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

    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let reservation_id = crate::tests::stream_session_id("direct-timeout");
    let generation_id = route.reserve_generation(&reservation_id).unwrap();
    let data = b"versioned direct PUT replica timeout";
    let payload = route
        .write_direct_object_payload(&reservation_id, generation_id, data.len() as u64, data)
        .unwrap();
    let prepared = crate::PreparedDirectPutObjectCommit {
        versioning: crate::BucketVersioningState::Enabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        etag_crc64: checksum::crc64::checksum(data),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        bucket_write_reservation: acquire_test_bucket_write_proof(
            &cluster,
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        ),
    };

    let _serial = lock_metadata_command_apply_hook_test();
    let trailing_replica_attempts = Arc::new(AtomicUsize::new(0));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let trailing_replica_attempts_hook = Arc::clone(&trailing_replica_attempts);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(2)
            ) {
                trailing_replica_attempts_hook.fetch_add(1, Ordering::SeqCst);
                return Err(StoreError::StorageRpc {
                    node_id: node_id.as_u32(),
                    operation: "apply metadata command",
                    failure: crate::storage_rpc::StorageRpcErrorCode::MetadataCommandContention,
                    detail: crate::error::StorageNodeFailureDetail::new(
                        "injected transported direct PUT trailing-replica contention",
                    ),
                });
            }
            Ok(())
        },
    ));
    let outcome = route
        .commit_direct_object(payload, &prepared, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();
    drop(hook_guard);
    assert_eq!(
        trailing_replica_attempts.load(Ordering::SeqCst),
        1,
        "published direct PUT must hand the first failed trailing apply to recovery"
    );
    assert_eq!(outcome.version_id, crate::VersionId::from_u64(1));
    let pending = pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket)
        .expect("published command must retain its exact recovery slot");
    let expected_identity = (
        pending.id().cluster_epoch(),
        pending.id().log_index(),
        pending.checksum_crc64(),
    );

    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().expect("direct PUT object should be live");
        assert_eq!(live.version_id, outcome.version_id);
        assert_eq!(live.generation_id, generation_id);
        assert_eq!(live.size, data.len() as u64);
    }
    let trailing_pg = map
        .node(NodeId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_meta(&*trailing_pg, &bucket, &key),
        Err(crate::MetadataError::ObjectNotFound)
    ));
    drop(trailing_pg);

    let recovered_identities = Arc::new(Mutex::new(Vec::new()));
    let recovered_identities_hook = Arc::clone(&recovered_identities);
    let recovery_bucket = bucket.clone();
    let recovery_key = key.clone();
    let recovery_hook = cluster.test_install_after_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if node_id == NodeId::new(2)
                        && commit.object.bucket == recovery_bucket
                        && commit.object.key == recovery_key
            ) {
                recovered_identities_hook
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push((
                        command.id().cluster_epoch(),
                        command.id().log_index(),
                        command.checksum_crc64(),
                    ));
            }
            Ok(())
        },
    ));
    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_route(
                PgId::new(object_pg),
                &pending,
                &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    drop(recovery_hook);
    let recovered_identities = recovered_identities
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    assert_eq!(
        recovered_identities.as_slice(),
        &[expected_identity],
        "recovery must replay the exact published command"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn versioned_direct_put_converges_primary_apply_response_loss_before_success() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .node(NodeId::new(0))
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

    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let reservation_id = crate::tests::stream_session_id("response-loss");
    let generation_id = route.reserve_generation(&reservation_id).unwrap();
    let data = b"versioned direct PUT apply response loss";
    let payload = route
        .write_direct_object_payload(&reservation_id, generation_id, data.len() as u64, data)
        .unwrap();
    let prepared = crate::PreparedDirectPutObjectCommit {
        versioning: crate::BucketVersioningState::Enabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        etag_crc64: checksum::crc64::checksum(data),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        bucket_write_reservation: acquire_test_bucket_write_proof(
            &cluster,
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        ),
    };

    let _serial = lock_metadata_command_apply_hook_test();
    let primary_response_lost = Arc::new(AtomicBool::new(false));
    let witness_response_lost = Arc::new(AtomicBool::new(false));
    let observed_command_identities = Arc::new(Mutex::new(Vec::new()));
    let attempted_command_identities = Arc::new(Mutex::new(Vec::new()));
    let attempt_count = Arc::new(AtomicUsize::new(0));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let primary_response_lost_hook = Arc::clone(&primary_response_lost);
    let witness_response_lost_hook = Arc::clone(&witness_response_lost);
    let observed_command_identities_hook = Arc::clone(&observed_command_identities);
    let attempted_command_identities_hook = Arc::clone(&attempted_command_identities);
    let attempt_count_hook = Arc::clone(&attempt_count);
    let attempt_hook_bucket = bucket.clone();
    let attempt_hook_key = key.clone();
    let attempt_hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == attempt_hook_bucket
                        && commit.object.key == attempt_hook_key
            ) {
                attempted_command_identities_hook
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push((
                        command.id().cluster_epoch(),
                        command.id().log_index(),
                        command.checksum_crc64(),
                    ));
                if attempt_count_hook.fetch_add(1, Ordering::SeqCst) == 1 {
                    return Err(StoreError::RouteMapExpired {
                        cluster_epoch: ClusterEpoch::INITIAL,
                        valid_until_ms: 0,
                        now_ms: 1,
                    });
                }
            }
            Ok(())
        }));
    let hook_guard = cluster.test_install_after_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() {
                if commit.object.bucket == hook_bucket && commit.object.key == hook_key {
                    observed_command_identities_hook
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .push((
                            command.id().cluster_epoch(),
                            command.id().log_index(),
                            command.checksum_crc64(),
                        ));
                    if node_id == NodeId::new(1)
                        && !primary_response_lost_hook.swap(true, Ordering::SeqCst)
                    {
                        return Err(StoreError::StorageRpc {
                            node_id: node_id.as_u32(),
                            operation: "apply metadata command",
                            failure: crate::storage_rpc::StorageRpcErrorCode::TransportClosed,
                            detail: crate::error::StorageNodeFailureDetail::new(
                                "injected response loss after durable primary apply",
                            ),
                        });
                    }
                    if node_id == NodeId::new(0)
                        && !witness_response_lost_hook.swap(true, Ordering::SeqCst)
                    {
                        return Err(StoreError::StorageRpc {
                            node_id: node_id.as_u32(),
                            operation: "apply metadata command",
                            failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                            detail: crate::error::StorageNodeFailureDetail::new(
                                "injected response loss after durable replica apply",
                            ),
                        });
                    }
                }
            }
            Ok(())
        },
    ));
    let outcome = route
        .commit_direct_object(payload, &prepared, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();
    drop(hook_guard);
    drop(attempt_hook);
    assert!(
        primary_response_lost.load(Ordering::SeqCst),
        "direct PUT must cross the durable primary response-loss boundary"
    );
    assert!(
        witness_response_lost.load(Ordering::SeqCst),
        "direct PUT must cross the durable witness response-loss boundary"
    );
    let observed_command_identities = observed_command_identities
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let expected_identity = observed_command_identities[0];
    assert!(
        observed_command_identities
            .iter()
            .all(|identity| *identity == expected_identity),
        "every publication-confirmation replay must retain epoch, index, and checksum"
    );
    drop(observed_command_identities);
    let attempted_command_identities = attempted_command_identities
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    assert!(
        attempted_command_identities.len() >= 3,
        "witness loss, an unrelated route expiry, and primary loss must all preserve exact-command confirmation"
    );
    assert!(
        attempted_command_identities
            .iter()
            .all(|identity| *identity == expected_identity),
        "every apply attempt must retain the exact epoch, index, and checksum"
    );
    assert_eq!(outcome.version_id, crate::VersionId::from_u64(1));
    let pending = pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket)
        .expect("primary response loss must retain the exact recovery slot");
    assert_eq!(
        (
            pending.id().cluster_epoch(),
            pending.id().log_index(),
            pending.checksum_crc64(),
        ),
        expected_identity,
        "recovery handoff must retain the published command identity"
    );
    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_route(
                PgId::new(object_pg),
                &pending,
                &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().expect("direct PUT object should be live");
        assert_eq!(live.version_id, outcome.version_id);
        assert_eq!(live.generation_id, generation_id);
        assert_eq!(live.size, data.len() as u64);
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn direct_put_overwrite_committed_response_loss_retry_preserves_reclaim_generation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-overwrite-response-loss-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let first_reservation_id =
        crate::SessionId::try_from("54545454545454545454545454545454".to_string()).unwrap();
    let first_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &first_reservation_id)
        .unwrap();
    let first_payload = b"original direct put object";
    let first_segment_okh = [0xc4; 16];
    let first_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            first_generation_id,
            0,
            &first_segment_okh,
            first_payload,
        )
        .unwrap();
    let first_commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: first_reservation_id,
            generation_id: first_generation_id,
            payload: first_payload,
            segment_okh: first_segment_okh,
            written: &first_written,
        },
    );
    let first_outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &first_commit_req,
            &first_written.written_shards,
            |_| Ok::<(), ()>(()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(first_outcome.stale_generation_id, None);

    let overwrite_reservation_id =
        crate::SessionId::try_from("55555555555555555555555555555555".to_string()).unwrap();
    let overwrite_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &overwrite_reservation_id)
        .unwrap();
    let overwrite_payload = b"replacement direct put object after response loss";
    let overwrite_segment_okh = [0xc5; 16];
    let overwrite_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            overwrite_generation_id,
            0,
            &overwrite_segment_okh,
            overwrite_payload,
        )
        .unwrap();
    let overwrite_commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: overwrite_reservation_id,
            generation_id: overwrite_generation_id,
            payload: overwrite_payload,
            segment_okh: overwrite_segment_okh,
            written: &overwrite_written,
        },
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let hook_guard =
        cluster.test_install_after_direct_put_metadata_publish_hook(Arc::new(|_, _| {
            Err(crate::ObjectPgActionError::InvalidRequest {
                reason: "injected direct PUT overwrite response loss".to_string(),
            })
        }));

    let first_err = cluster
        .commit_direct_put_object_from_payload_shards(
            &overwrite_commit_req,
            &overwrite_written.written_shards,
            |_| Ok::<(), ()>(()),
        )
        .unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason == "injected direct PUT overwrite response loss"
        ),
        "expected injected post-commit direct PUT response-loss error, got {first_err:?}"
    );
    drop(hook_guard);

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &overwrite_commit_req,
            &overwrite_written.written_shards,
            |_| -> Result<(), ()> {
                panic!("committed direct PUT overwrite retry must not rerun action")
            },
        )
        .unwrap()
        .unwrap();

    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_size, overwrite_payload.len() as u64);
    assert_eq!(outcome.stale_generation_id, Some(first_generation_id));
    assert!(cluster
        .payload_reclaim_exists(&bucket, &key, first_generation_id)
        .unwrap());
    assert!(pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[2]);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored
            .as_live()
            .expect("direct PUT overwrite object should be live");
        assert_eq!(live.generation_id, overwrite_generation_id);
        assert_eq!(live.size, overwrite_payload.len() as u64);
    }
}

#[test]
fn copy_object_destination_committed_response_loss_retry_returns_existing_commit() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "copy-object-response-loss-");
    let source_key = key_for_object_pg(topology, &bucket, 2, "source-");
    let dst_key = key_for_object_pg(topology, &bucket, 2, "dest-");
    for pg_id in pg_ids {
        set_route_primary(&mut map, pg_id, NodeId::new(1));
    }

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let source_payload = b"copied object payload after response loss";
    let source_segment = write_committed_direct_segment_for_with_okh(
        &cluster,
        &bucket,
        &source_key,
        [0xcb; 16],
        source_payload,
    );

    let reservation_id =
        crate::SessionId::try_from("56565656565656565656565656565656".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &dst_key, &reservation_id)
        .unwrap();
    let dst_segment_okh = [0xcc; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &dst_key,
            generation_id,
            0,
            &dst_segment_okh,
            &source_segment.payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &dst_key,
            reservation_id,
            generation_id,
            payload: &source_segment.payload,
            segment_okh: dst_segment_okh,
            written: &written,
        },
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let hook_guard =
        cluster.test_install_after_direct_put_metadata_publish_hook(Arc::new(|_, _| {
            Err(crate::ObjectPgActionError::InvalidRequest {
                reason: "injected CopyObject destination response loss".to_string(),
            })
        }));

    let first_err = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            |snapshot| {
                assert!(
                    snapshot.existing_etag.is_none(),
                    "copy destination should not exist before first publish"
                );
                Ok::<(), ()>(())
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason == "injected CopyObject destination response loss"
        ),
        "expected injected post-commit CopyObject response-loss error, got {first_err:?}"
    );
    drop(hook_guard);

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            |_| -> Result<(), ()> {
                panic!("committed CopyObject destination retry must not rerun action")
            },
        )
        .unwrap()
        .unwrap();

    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_size, source_segment.payload.len() as u64);
    assert_eq!(outcome.stale_generation_id, None);
    let dst_object_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .object_pg_for(&bucket, &dst_key);
    assert_direct_put_metadata_on_acting_nodes(
        &map,
        &node_ids,
        dst_object_pg,
        &commit_req,
        &outcome,
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(dst_object_pg), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[dst_object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
    let source_object_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .object_pg_for(&bucket, &source_key);
    for node_id in node_ids {
        let source_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(source_object_pg)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_meta(&*source_pg, &bucket, &source_key).unwrap();
        assert_eq!(
            stored.as_live().unwrap().generation_id,
            source_segment.generation_id,
            "committed CopyObject retry must preserve source object on node {node_id:?}"
        );
    }
}

#[test]
fn stale_direct_put_reservation_cannot_resurrect_deleted_null_version() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stale-direct-put-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let stale_payload = b"stale direct put";
    let stale_reservation_id =
        crate::SessionId::try_from("61616161616161616161616161616161".to_string()).unwrap();
    let stale_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &stale_reservation_id)
        .unwrap();
    let stale_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            stale_generation_id,
            0,
            &[0xa1; 16],
            stale_payload,
        )
        .unwrap();
    let stale_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: stale_reservation_id,
            generation_id: stale_generation_id,
            payload: stale_payload,
            segment_okh: [0xa1; 16],
            written: &stale_written,
        },
    );
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap();
    let object_pg_store = primary.storage_node().get_pg(2).unwrap();
    let stale_command = cluster
        .prepare_commit_direct_put_object_command(
            PgId::new(2),
            &object_pg_store,
            &stale_req,
            crate::VersionId::Null,
            stale_req.bucket_write_reservation.clone(),
        )
        .unwrap();
    drop(object_pg_store);

    for (label, payload, segment_byte) in [
        ("newer-a", b"newer direct put a".as_slice(), 0xa2),
        ("newer-b", b"newer direct put b".as_slice(), 0xa3),
    ] {
        let reservation_id =
            crate::SessionId::try_from(format!("{segment_byte:02x}").repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let written = cluster
            .write_direct_put_segment_payload_shards(
                &bucket,
                &key,
                generation_id,
                0,
                &[segment_byte; 16],
                payload,
            )
            .unwrap();
        let req = direct_put_commit_req(
            &cluster,
            DirectPutCommitReqFixture {
                bucket: &bucket,
                key: &key,
                reservation_id,
                generation_id,
                payload,
                segment_okh: [segment_byte; 16],
                written: &written,
            },
        );
        let outcome = cluster
            .commit_direct_put_object_from_payload_shards(&req, &written.written_shards, |_| {
                Ok::<_, ()>(())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            outcome.live_size,
            payload.len() as u64,
            "{label} commit should be live before cleanup"
        );
    }

    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap();

    let stale_late_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(2),
            map.test_next_metadata_command_log_index(PgId::new(2)),
        ),
        stale_command.payload().clone(),
    );
    let stale_result = cluster.test_apply_metadata_command_to_acting_set_from_origin(
        primary.node_id(),
        &stale_late_command,
    );
    assert!(
        matches!(
            stale_result,
            Err(crate::BucketSnapshotLoadError::Metadata(
                crate::MetadataError::StaleObjectWriteCommand {
                    ref bucket,
                    ref key,
                    write_sequence: 1,
                    generation_id: Some(1),
                }
            )) if bucket == &stale_req.bucket && key == &stale_req.key
        ),
        "expected stale object write command after newer writes and delete, got {stale_result:?}"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "stale direct PUT must not resurrect object on node {node_id:?}"
        );
    }
}

#[test]
fn direct_put_pre_command_route_error_releases_bucket_write_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = local_map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut local_map, object_pg, NodeId::new(1));
    set_route_primary(&mut local_map, data_pg, NodeId::new(2));

    let mut map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let reservation_id =
        crate::SessionId::try_from("53535353535353535353535353535353".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put route failure";
    let segment_okh = [0xb3; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let command_reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let commit_req = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
        crate::metadata_command::BucketWriteReservationProof::from(&command_reservation.record),
    );
    drop(cluster);

    Arc::get_mut(&mut map)
        .unwrap()
        .pg_routes
        .get_mut(&PgId::new(object_pg))
        .unwrap()
        .state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::PgNotActive {
            pg_id,
            state: PgState::Peering,
            ..
        }) if pg_id == object_pg
    ));

    let bucket_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg_id))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "pre-command storage errors must release caller-owned bucket write proof"
    );
}

#[test]
fn non_current_epoch_direct_put_commit_fails_closed_and_cleans_unowned_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = local_map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut local_map, object_pg, NodeId::new(1));
    set_route_primary(&mut local_map, data_pg, NodeId::new(2));

    let map = Arc::new(local_map);
    let current_cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&current_cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("54545454545454545454545454545454".to_string()).unwrap();
    let generation_id = current_cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let payload = b"stale epoch direct put";
    let segment_okh = [0xb4; 16];
    let written = current_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let bucket_write_reservation = current_cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let commit_req = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
        crate::metadata_command::BucketWriteReservationProof::from(
            &bucket_write_reservation.record,
        ),
    );
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();

    let err = stale_cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
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
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale direct PUT commit should fail closed at the metadata-primary boundary, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale direct PUT commit must not append an object-PG command"
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "stale direct PUT must not publish object metadata on node {node_id:?}"
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
    for shard_index in 0..written.ec.k + written.ec.m {
        assert!(!current_cluster
            .test_payload_shard_file_exists(
                written.data_pg_id,
                written.ec,
                &segment_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
}

#[test]
fn control_plane_peering_direct_put_old_primary_fails_closed_and_cleans_unowned_state() {
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
                            .join(format!("node-{}.sock", node_id.as_u32()))
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
                    .join("storage")
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
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("67676767676767676767676767676767".to_string()).unwrap();
    let generation_id = source_cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let payload = b"control-plane peering stale direct put";
    let segment_okh = [0xc7; 16];
    let written = source_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    source_cluster
        .test_register_payload_shard_acks(written.data_pg_id, &written.written_shards)
        .unwrap();
    {
        let source_data_pg_primary = source_map
            .node(
                source_map
                    .pg_route(PgId::new(written.data_pg_id))
                    .unwrap()
                    .primary_node_id(),
            )
            .unwrap()
            .storage_node()
            .get_pg(written.data_pg_id)
            .unwrap();
        for written_shard in &written.written_shards {
            source_data_pg_primary
                .validate_written_shard_ack(&written_shard.key, written_shard.ack)
                .unwrap();
        }
    }
    let bucket_write_reservation = source_cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let commit_req = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
        crate::metadata_command::BucketWriteReservationProof::from(
            &bucket_write_reservation.record,
        ),
    );
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

    let err = old_primary_cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
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
        "old-primary direct PUT commit should fail closed after control-plane Peering transition, got {err:?}"
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
            "old-primary direct PUT must not append an object-PG command on node {node_id:?}"
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "old-primary direct PUT must not publish object metadata on node {node_id:?}"
        );
        assert!(
            pg.pending_metadata_command_envelope(node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary direct PUT must not leave a source-epoch pending command on node {node_id:?}"
        );
        assert!(
            pg.pending_metadata_command_envelope(node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary direct PUT must not leave a current-epoch pending command on node {node_id:?}"
        );
    }
    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for node_id in node_ids {
        let pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(bucket_pg)
            .unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket)
                .unwrap()
                .is_empty(),
            "old-primary direct PUT must release bucket write reservations on node {node_id:?}"
        );
    }
    let current_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&current_map)).unwrap();
    for shard_index in 0..written.ec.k + written.ec.m {
        assert!(!current_cluster
            .test_payload_shard_file_exists(
                written.data_pg_id,
                written.ec,
                &segment_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
    let source_data_pg_route = current_map
        .reconstructed_pg_route_at_epoch(PgId::new(written.data_pg_id), source_epoch)
        .unwrap();
    let source_data_pg_primary = current_map
        .node(source_data_pg_route.primary_node_id())
        .unwrap()
        .storage_node()
        .get_pg(written.data_pg_id)
        .unwrap();
    for written_shard in &written.written_shards {
        assert!(
            matches!(
                source_data_pg_primary
                    .validate_written_shard_ack(&written_shard.key, written_shard.ack),
                Err(StoreError::NotFound)
            ),
            "old-primary direct PUT must delete retained data-PG ack row for shard {}",
            written_shard.key
        );
    }
}

#[test]
fn control_plane_peering_copy_object_destination_old_primary_fails_closed_and_cleans_staging() {
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
                            .join(format!("copy-node-{}.sock", node_id.as_u32()))
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
                    .join("copy-storage")
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
    let (bucket, dst_key, dst_object_pg, _dst_data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_key = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let source_pg = pg_ids
            .iter()
            .copied()
            .find(|pg_id| *pg_id != dst_object_pg)
            .unwrap();
        key_for_object_pg(topology, &bucket, source_pg, "copy-source-")
    };
    let source_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);
    let source_payload = b"control-plane peering stale copy source payload";
    let source_segment = write_committed_direct_segment_for_with_okh(
        &source_cluster,
        &bucket,
        &source_key,
        [0xc9; 16],
        source_payload,
    );

    let reservation_id =
        crate::SessionId::try_from("69696969696969696969696969696969".to_string()).unwrap();
    let generation_id = source_cluster
        .reserve_put_object_generation(&bucket, &dst_key, &reservation_id)
        .unwrap();
    let before_dst_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &dst_key)
        .unwrap();
    let dst_segment_okh = [0xca; 16];
    let written = source_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &dst_key,
            generation_id,
            0,
            &dst_segment_okh,
            &source_segment.payload,
        )
        .unwrap();
    source_cluster
        .test_register_payload_shard_acks(written.data_pg_id, &written.written_shards)
        .unwrap();
    let bucket_write_reservation = source_cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(dst_key.as_str()),
        )
        .unwrap();
    let commit_req = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &dst_key,
            reservation_id,
            generation_id,
            payload: &source_segment.payload,
            segment_okh: dst_segment_okh,
            written: &written,
        },
        crate::metadata_command::BucketWriteReservationProof::from(
            &bucket_write_reservation.record,
        ),
    );
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(
            PgId::new(dst_object_pg),
            vec![NodeId::new(1), NodeId::new(2)],
        )
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == dst_object_pg {
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
        current_map
            .pg_route(PgId::new(dst_object_pg))
            .unwrap()
            .state(),
        PgState::Peering,
        "control-plane acting-set change should put the destination object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(dst_object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the destination acting set"
    );
    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();

    let err = old_primary_cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == dst_object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary CopyObject destination commit should fail closed after control-plane Peering transition, got {err:?}"
    );

    for node_id in node_ids {
        let dst_pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(dst_object_pg)
            .unwrap();
        let state = dst_pg.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof::current(
            state.applied_log_index,
            state.applied_log_hash,
            state.state_digest,
        );
        assert_eq!(
            proof, before_dst_object_pg_proof,
            "old-primary CopyObject destination must not append an object-PG command on node {node_id:?}"
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*dst_pg, &bucket, &dst_key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "old-primary CopyObject destination must not publish destination metadata on node {node_id:?}"
        );
        assert!(
            dst_pg
                .pending_metadata_command_envelope(node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary CopyObject destination must not leave a source-epoch pending command on node {node_id:?}"
        );
        assert!(
            dst_pg
                .pending_metadata_command_envelope(node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary CopyObject destination must not leave a current-epoch pending command on node {node_id:?}"
        );
    }
    let source_object_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .object_pg_for(&bucket, &source_key);
    for node_id in node_ids {
        let source_pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(source_object_pg)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_meta(&*source_pg, &bucket, &source_key).unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(
            live.generation_id, source_segment.generation_id,
            "failed CopyObject destination commit must preserve source object on node {node_id:?}"
        );
    }
    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for node_id in node_ids {
        let pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(bucket_pg)
            .unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket)
                .unwrap()
                .is_empty(),
            "old-primary CopyObject destination must release bucket write reservations on node {node_id:?}"
        );
    }
    let current_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&current_map)).unwrap();
    for shard_index in 0..written.ec.k + written.ec.m {
        assert!(!current_cluster
            .test_payload_shard_file_exists(
                written.data_pg_id,
                written.ec,
                &dst_segment_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
    let source_data_pg_route = current_map
        .reconstructed_pg_route_at_epoch(PgId::new(written.data_pg_id), source_epoch)
        .unwrap();
    let source_data_pg_primary = current_map
        .node(source_data_pg_route.primary_node_id())
        .unwrap()
        .storage_node()
        .get_pg(written.data_pg_id)
        .unwrap();
    for written_shard in &written.written_shards {
        assert!(
            matches!(
                source_data_pg_primary
                    .validate_written_shard_ack(&written_shard.key, written_shard.ack),
                Err(StoreError::NotFound)
            ),
            "old-primary CopyObject destination must delete retained data-PG ack row for shard {}",
            written_shard.key
        );
    }
}

#[test]
fn direct_put_publish_validation_fails_closed_when_acknowledged_shard_file_is_missing() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-publish-validation-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut local_map, 1, NodeId::new(1));
    set_route_primary(&mut local_map, 2, NodeId::new(1));
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("64646464646464646464646464646464".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put publish validation";
    let segment_okh = [0xd1; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let data_pg_id = DataPgId::new_for_test(PgId::new(written.data_pg_id));
    let placement_key =
        super::super::super::segment_payload_placement_key(&segment_okh, generation_id);
    let locations = cluster
        .place_payload_shards(data_pg_id, written.ec, &placement_key)
        .unwrap();
    let missing_shard = written.written_shards[0].key.clone();
    let missing_location = locations[usize::from(missing_shard.shard_index().get())];
    let hook_map = Arc::clone(&map);
    let hook_missing_shard = missing_shard.clone();
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = cluster.test_install_before_direct_put_command_id_hook(Arc::new(move || {
        if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        hook_map
            .node(missing_location.node_id())
            .unwrap()
            .storage_node()
            .delete_shard_file(missing_location.data_pg_id().get(), &hook_missing_shard)
            .unwrap();
        Ok(())
    }));

    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::ShardStore {
                ref source,
                ..
            }) if matches!(**source, StoreError::NotFound)
        ),
        "missing acknowledged shard file should fail closed before metadata publish, got {err:?}"
    );
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_bucket_write_reservations_released(&map, &bucket);
    assert_clean_metadata_command_stream(&map, &[2]);
    let object_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key),
        Err(crate::MetadataError::ObjectNotFound)
    ));
}

#[test]
fn direct_put_publish_validation_rejects_truncated_shard_batch() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-truncated-shards-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut local_map, 1, NodeId::new(1));
    set_route_primary(&mut local_map, 2, NodeId::new(1));
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("65656565656565656565656565656565".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put truncated shard batch";
    let segment_okh = [0xd3; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let truncated = written.written_shards[..written.written_shards.len() - 1].to_vec();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &truncated, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::PayloadShardSetMismatch { .. })
        ),
        "truncated shard batch should fail closed before metadata publish, got {err:?}"
    );
    assert_bucket_write_reservations_released(&map, &bucket);
    assert_clean_metadata_command_stream(&map, &[2]);
    let object_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key),
        Err(crate::MetadataError::ObjectNotFound)
    ));
}

#[test]
fn direct_put_retries_transient_precommand_observation_within_shared_budget() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut local_map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-observation-retry-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut local_map, 1, NodeId::new(1));
    set_route_primary(&mut local_map, 2, NodeId::new(1));
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("67676767676767676767676767676767".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put transient observation retry";
    let segment_okh = [0xd7; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_for_closure = Arc::clone(&hook_calls);
    let _hook = cluster.test_install_before_direct_put_command_id_hook(Arc::new(move || {
        if hook_calls_for_closure.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(crate::ObjectPgActionError::Store(
                StoreError::MetadataCommandContention {
                    context: "injected direct PUT pre-command observation contention",
                },
            ));
        }
        Ok(())
    }));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_calls_for_closure = Arc::clone(&action_calls);
    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            move |_| {
                action_calls_for_closure.fetch_add(1, Ordering::SeqCst);
                Ok::<(), ()>(())
            },
        )
        .unwrap()
        .unwrap();

    assert_eq!(outcome.live_size, payload.len() as u64);
    assert_eq!(hook_calls.load(Ordering::SeqCst), 2);
    assert_eq!(action_calls.load(Ordering::SeqCst), 2);
    assert!(pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[2]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn direct_put_does_not_evaluate_condition_after_local_snapshot_crosses_deadline() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut local_map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-local-snapshot-deadline-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut local_map, 1, NodeId::new(1));
    set_route_primary(&mut local_map, 2, NodeId::new(1));
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("68686868686868686868686868686868".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put local snapshot deadline";
    let segment_okh = [0xd8; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let mut commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    commit_req.versioning = crate::BucketVersioningState::Enabled;

    let _snapshot_hook =
        cluster.test_install_after_direct_put_snapshot_loaded_hook(Arc::new(|| true));
    let allocator_calls = Arc::new(AtomicUsize::new(0));
    let allocator_calls_for_hook = Arc::clone(&allocator_calls);
    let _allocator_hook =
        cluster.test_install_before_object_version_command_id_hook(Arc::new(move || {
            allocator_calls_for_hook.fetch_add(1, Ordering::SeqCst);
        }));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_calls_for_closure = Arc::clone(&action_calls);
    let error = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            move |_| {
                action_calls_for_closure.fetch_add(1, Ordering::SeqCst);
                Ok::<(), ()>(())
            },
        )
        .unwrap_err();

    assert!(matches!(
        error,
        crate::ObjectPgActionError::SnapshotReinspectionConflict
    ));
    assert_eq!(action_calls.load(Ordering::SeqCst), 0);
    assert_eq!(allocator_calls.load(Ordering::SeqCst), 0);
    assert!(pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[2]);
    assert_bucket_write_reservations_released(&map, &bucket);
    let object_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key),
        Err(crate::MetadataError::ObjectNotFound)
    ));
}

#[derive(Clone, Copy)]
enum DirectPutCommandIdRaceDrainFailure {
    Contention,
    AwaitingAuthorizedRecovery,
}

fn assert_direct_put_command_id_race_drains_winner_and_reruns_precondition_action(
    injected_failure: DirectPutCommandIdRaceDrainFailure,
) {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-command-id-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);

    let loser_payload = b"loser direct put command id";
    let loser_reservation_id =
        crate::SessionId::try_from("31313131313131313131313131313131".to_string()).unwrap();
    let loser_generation_id = first_cluster
        .reserve_put_object_generation(&bucket, &key, &loser_reservation_id)
        .unwrap();
    let loser_written = first_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            loser_generation_id,
            0,
            &[0xa1; 16],
            loser_payload,
        )
        .unwrap();
    let loser_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: loser_reservation_id,
            generation_id: loser_generation_id,
            payload: loser_payload,
            segment_okh: [0xa1; 16],
            written: &loser_written,
        },
    );

    let winner_payload = b"winner direct put command id";
    let winner_reservation_id =
        crate::SessionId::try_from("32323232323232323232323232323232".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            winner_generation_id,
            0,
            &[0xa2; 16],
            winner_payload,
        )
        .unwrap();
    let winner_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: winner_reservation_id,
            generation_id: winner_generation_id,
            payload: winner_payload,
            segment_okh: [0xa2; 16],
            written: &winner_written,
        },
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_cluster = Arc::clone(&second_cluster);
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_req = winner_req.clone();
    let hook_written_shards = winner_written.written_shards.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard =
        first_cluster.test_install_before_direct_put_command_id_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            let pg_id = PgId::new(2);
            let shard_batch: Vec<(&ShardKey, WriteAck)> = hook_written_shards
                .iter()
                .map(|written| (&written.key, written.ack))
                .collect();
            hook_cluster
                .register_payload_shard_acks(hook_req.data_pg_id, &shard_batch)
                .unwrap();
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let command = hook_cluster
                .prepare_commit_direct_put_object_command(
                    pg_id,
                    &pg,
                    &hook_req,
                    crate::VersionId::Null,
                    hook_req.bucket_write_reservation.clone(),
                )
                .unwrap();
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
            Ok(())
        }));

    let transient_drain_failure = Arc::new(AtomicBool::new(true));
    let transient_drain_failure_for_hook = Arc::clone(&transient_drain_failure);
    let _drain_hook = first_cluster
        .test_install_pending_object_metadata_command_drain_attempt_hook(Arc::new(
            move |command, _work_budget| {
                if matches!(
                    command.payload(),
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.object.generation_id == winner_generation_id
                ) && transient_drain_failure_for_hook.swap(false, Ordering::SeqCst)
                {
                    return Err(match injected_failure {
                        DirectPutCommandIdRaceDrainFailure::Contention => {
                            crate::ObjectPgActionError::Store(
                                StoreError::MetadataCommandContention {
                                    context: "injected direct PUT contender drain contention",
                                },
                            )
                        }
                        DirectPutCommandIdRaceDrainFailure::AwaitingAuthorizedRecovery => {
                            crate::ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery
                        }
                    });
                }
                Ok(())
            },
        ));

    let calls_for_action = Arc::clone(&action_calls);
    let result = first_cluster
        .commit_direct_put_object_from_payload_shards(
            &loser_req,
            &loser_written.written_shards,
            move |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                if snapshot.existing_etag.is_some() {
                    Err("object already exists")
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
    assert!(matches!(result, Err("object already exists")));
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(!transient_drain_failure.load(Ordering::SeqCst));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "direct PUT precondition must be rerun after command-id contention changes object state"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().expect("winner object is live");
        assert_eq!(live.generation_id, winner_generation_id);
        assert_eq!(live.size, winner_payload.len() as u64);
    }
}

#[test]
fn direct_put_command_id_race_drains_winner_and_reruns_precondition_action() {
    assert_direct_put_command_id_race_drains_winner_and_reruns_precondition_action(
        DirectPutCommandIdRaceDrainFailure::Contention,
    );
}

#[test]
fn direct_put_command_id_race_retries_awaiting_authorized_recovery() {
    assert_direct_put_command_id_race_drains_winner_and_reruns_precondition_action(
        DirectPutCommandIdRaceDrainFailure::AwaitingAuthorizedRecovery,
    );
}

#[test]
fn direct_put_retries_irreversible_uncertainty_on_the_active_route() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .node(NodeId::new(0))
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

    let reservation_id = crate::tests::stream_session_id("same-route");
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct PUT same-route recovery";
    let segment_okh = [0x6d; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );

    let inject_once = Arc::new(AtomicBool::new(true));
    let inject_once_for_hook = Arc::clone(&inject_once);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let _apply_hook =
        cluster.test_install_direct_put_metadata_apply_uncertainty_hook(Arc::new(move |command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket && commit.object.key == hook_key
            ) && inject_once_for_hook.swap(false, Ordering::SeqCst)
            {
                crate::cluster::request_ops::DirectPutMetadataApplyUncertaintyTestAction::Inject
            } else {
                crate::cluster::request_ops::DirectPutMetadataApplyUncertaintyTestAction::None
            }
        }));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_calls_for_commit = Arc::clone(&action_calls);
    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            move |_| {
                action_calls_for_commit.fetch_add(1, Ordering::SeqCst);
                Ok::<(), ()>(())
            },
        )
        .unwrap()
        .unwrap();

    assert!(!inject_once.load(Ordering::SeqCst));
    assert_eq!(action_calls.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.live_size, payload.len() as u64);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_eq!(cluster.test_metadata_command_recovery_flight_count(), 0);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn direct_put_terminal_cleanup_handoff_projects_same_object_and_rejects_unrelated() {
    struct CleanupGateRelease(Option<std::sync::mpsc::SyncSender<()>>);

    impl CleanupGateRelease {
        fn release(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }

    impl Drop for CleanupGateRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let other_key = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        key_for_object_pg(topology, &bucket, object_pg, "cleanup-handoff-other-")
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id = crate::tests::stream_session_id("cleanup-handoff");
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct PUT terminal cleanup handoff";
    let segment_okh = [0x4f; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );

    let contender_reservation_id = crate::tests::stream_session_id("cleanup-contendr");
    let contender_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &contender_reservation_id)
        .unwrap();
    let contender_payload = b"same-object direct PUT contender";
    let contender_segment_okh = [0x50; 16];
    let contender_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            contender_generation_id,
            0,
            &contender_segment_okh,
            contender_payload,
        )
        .unwrap();
    let contender_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: contender_reservation_id.clone(),
            generation_id: contender_generation_id,
            payload: contender_payload,
            segment_okh: contender_segment_okh,
            written: &contender_written,
        },
    );

    let cleanup_attempts = Arc::new(AtomicUsize::new(0));
    let cleanup_attempts_for_hook = Arc::clone(&cleanup_attempts);
    let (cleanup_reached_tx, cleanup_reached_rx) = std::sync::mpsc::sync_channel(1);
    let (cleanup_release_tx, cleanup_release_rx) = std::sync::mpsc::sync_channel(1);
    let cleanup_release_rx = Arc::new(Mutex::new(cleanup_release_rx));
    let cleanup_release_rx_for_hook = Arc::clone(&cleanup_release_rx);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let cleanup_hook = cluster.test_install_global_metadata_command_terminal_slot_removal_hook(
        Arc::new(move |command| {
            let matches = matches!(
                command.payload(),
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket && commit.object.key == hook_key
            );
            if matches {
                let attempt = cleanup_attempts_for_hook.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    cleanup_reached_tx.send(()).unwrap();
                    cleanup_release_rx_for_hook
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .recv_timeout(Duration::from_secs(5))
                        .expect("terminal cleanup gate was not released");
                }
            }
            matches
        }),
    );

    let other_reservation_id = crate::tests::stream_session_id("cleanup-other");
    let same_object_reservation_id = crate::tests::stream_session_id("cleanup-same");
    let mut cleanup_release = CleanupGateRelease(Some(cleanup_release_tx));
    let (outcome, waiter_error, same_object_generation_id, command) = thread::scope(|scope| {
        let (owner_result_tx, owner_result_rx) = std::sync::mpsc::sync_channel(1);
        let owner_cluster = &cluster;
        let owner_request = &commit_req;
        let owner_shards = &written.written_shards;
        scope.spawn(move || {
            owner_result_tx
                .send(owner_cluster.commit_direct_put_object_from_payload_shards(
                    owner_request,
                    owner_shards,
                    |_| Ok::<(), ()>(()),
                ))
                .unwrap();
        });
        cleanup_reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("direct PUT did not reach terminal cleanup");

        let pg_id = PgId::new(object_pg);
        let command = pending_metadata_command_for_test(&map, pg_id, &bucket)
            .expect("deferred terminal cleanup must retain the direct PUT command");

        let contender_action_calls = Arc::new(AtomicUsize::new(0));
        let (contender_action_tx, contender_action_rx) = std::sync::mpsc::channel();
        let (contender_result_tx, contender_result_rx) = std::sync::mpsc::sync_channel(1);
        let contender_cluster = &cluster;
        let contender_request = &contender_req;
        let contender_shards = &contender_written.written_shards;
        let contender_action_calls_for_thread = Arc::clone(&contender_action_calls);
        scope.spawn(move || {
            contender_result_tx
                .send(
                    contender_cluster.commit_direct_put_object_from_payload_shards(
                        contender_request,
                        contender_shards,
                        |snapshot| {
                            contender_action_calls_for_thread.fetch_add(1, Ordering::SeqCst);
                            contender_action_tx.send(()).unwrap();
                            if snapshot.existing_etag.is_some() {
                                Err("object already exists")
                            } else {
                                Ok(())
                            }
                        },
                    ),
                )
                .unwrap();
        });
        contender_action_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("same-object contender did not inspect the published winner");

        let (waiter_result_tx, waiter_result_rx) = std::sync::mpsc::sync_channel(1);
        let waiter_cluster = &cluster;
        let waiter_bucket = &bucket;
        let waiter_key = &other_key;
        let waiter_reservation_id = &other_reservation_id;
        scope.spawn(move || {
            waiter_result_tx
                .send(waiter_cluster.reserve_put_object_generation(
                    waiter_bucket,
                    waiter_key,
                    waiter_reservation_id,
                ))
                .unwrap();
        });
        let waiter_selection_deadline = Instant::now() + Duration::from_secs(5);
        while !map
            .runtime_state()
            .test_metadata_command_recovery_handoff_requested(pg_id, &command)
        {
            assert!(
                Instant::now() < waiter_selection_deadline,
                "unrelated reservation did not select the direct PUT recovery flight"
            );
            thread::sleep(Duration::from_millis(1));
        }
        let waiter_error = waiter_result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("unrelated reservation waiter remained blocked by terminal cleanup")
            .unwrap_err();
        assert!(matches!(
            cluster.reserve_put_object_generation(&bucket, &other_key, &other_reservation_id),
            Err(crate::ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery)
        ));
        assert_eq!(
            cleanup_attempts.load(Ordering::SeqCst),
            1,
            "an unrelated request must not retry terminal cleanup on its request budget"
        );

        cleanup_release.release();
        let outcome = owner_result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("direct PUT owner did not finish after cleanup release")
            .unwrap()
            .unwrap();
        let retry_observed = Arc::new(AtomicBool::new(false));
        let retry_observed_for_hook = Arc::clone(&retry_observed);
        let retry_command_id = command.id();
        let _retry_hook = cluster
            .test_install_pending_object_metadata_command_recovery_transferred_hook(Arc::new(
                move |candidate| {
                    if candidate.id() == retry_command_id {
                        retry_observed_for_hook.store(true, Ordering::SeqCst);
                    }
                },
            ));
        let (same_object_result_tx, same_object_result_rx) = std::sync::mpsc::sync_channel(1);
        let same_object_cluster = &cluster;
        let same_object_bucket = &bucket;
        let same_object_key = &key;
        let same_object_reservation_id = &same_object_reservation_id;
        scope.spawn(move || {
            same_object_result_tx
                .send(same_object_cluster.reserve_put_object_generation(
                    same_object_bucket,
                    same_object_key,
                    same_object_reservation_id,
                ))
                .unwrap();
        });
        let retry_deadline = Instant::now() + Duration::from_secs(5);
        while !retry_observed.load(Ordering::SeqCst) {
            match same_object_result_rx.try_recv() {
                Ok(result) => panic!(
                    "same-object reservation returned before reobserving authorized recovery: {result:?}"
                ),
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("same-object reservation result channel disconnected")
                }
            }
            assert!(
                Instant::now() < retry_deadline,
                "same-object reservation did not reobserve authorized recovery"
            );
            thread::sleep(Duration::from_millis(1));
        }
        drop(cleanup_hook);
        assert_eq!(
            cluster
                .drain_pending_metadata_command_with_authorized_recovery_route(
                    pg_id, &command, &cluster,
                )
                .unwrap(),
            PendingMetadataCommandOutcome::Applied
        );
        let same_object_generation_id = same_object_result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("same-object reservation did not finish after recovery")
            .expect("same-object reservation must retry the published direct PUT");
        let contender_result = contender_result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("same-object contender did not finish cleanup after recovery")
            .expect("same-object contender must project the published winner");
        assert!(matches!(contender_result, Err("object already exists")));
        assert_eq!(contender_action_calls.load(Ordering::SeqCst), 1);
        (outcome, waiter_error, same_object_generation_id, command)
    });
    assert_eq!(outcome.live_size, payload.len() as u64);
    assert!(matches!(
        waiter_error,
        crate::ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery
    ));
    assert!(same_object_generation_id.get() > contender_generation_id.get());
    assert_eq!(cleanup_attempts.load(Ordering::SeqCst), 1);

    let pg_id = PgId::new(object_pg);
    assert!(matches!(
        command.payload(),
        MetadataCommandPayload::CommitDirectPutObject(commit)
            if commit.object.key == key && commit.object.generation_id == generation_id
    ));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    assert_direct_payload_staging_cleaned(
        &map,
        &cluster,
        &bucket,
        &key,
        &contender_reservation_id,
        DirectPayloadTestIdentity {
            data_pg_id: contender_written.data_pg_id,
            ec: contender_written.ec,
            segment_okh: contender_segment_okh,
            segment_vid: contender_generation_id,
        },
    );
}

#[test]
fn direct_put_retains_irreversible_handoff_until_authorized_recovery() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .node(NodeId::new(0))
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

    let reservation_id = crate::tests::stream_session_id("route-shift");
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct PUT route transition";
    let segment_okh = [0x9d; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );

    let inject_once = Arc::new(AtomicBool::new(true));
    let inject_once_for_hook = Arc::clone(&inject_once);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let _apply_hook =
        cluster.test_install_direct_put_metadata_apply_uncertainty_hook(Arc::new(move |command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket && commit.object.key == hook_key
            ) && inject_once_for_hook.swap(false, Ordering::SeqCst)
            {
                crate::cluster::request_ops::DirectPutMetadataApplyUncertaintyTestAction::InjectAfterBudgetExpiry
            } else {
                crate::cluster::request_ops::DirectPutMetadataApplyUncertaintyTestAction::None
            }
        }));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_calls_for_commit = Arc::clone(&action_calls);
    let error = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            move |_| {
                action_calls_for_commit.fetch_add(1, Ordering::SeqCst);
                Ok::<(), ()>(())
            },
        )
        .unwrap_err();

    assert!(!inject_once.load(Ordering::SeqCst));
    assert_eq!(action_calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::MetadataCommandOutcomeUnconfirmed { .. })
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some());
    assert_eq!(cluster.test_metadata_command_recovery_flight_count(), 1);
}

#[test]
fn direct_put_log_conflict_pending_visibility_error_cleans_new_payload() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-pending-read-error-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    let wrong_scope_bucket = bucket_for_pg(topology, 1, "wrong-pending-scope-");
    set_route_primary(&mut local_map, 1, NodeId::new(1));
    set_route_primary(&mut local_map, 2, NodeId::new(1));

    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("36363636363636363636363636363636".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put pending visibility error";
    let segment_okh = [0xc6; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_wrong_scope_bucket = wrong_scope_bucket.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = cluster.test_install_before_direct_put_command_id_hook(Arc::new(move || {
        if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let pg_id = PgId::new(2);
        let command = create_bucket_metadata_command(pg_id, 2, hook_wrong_scope_bucket.clone());
        force_insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
        Ok(())
    }));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandPendingConflict { .. })
        ),
        "expected malformed pending slot visibility error, got {err:?}"
    );

    assert_bucket_write_reservations_released(&map, &bucket);
    // The unreadable pending slot still blocks command-log-preserving
    // generation-reservation release. This regression pins the cleanup that
    // must not be bypassed by the pending-visibility error: the caller-owned
    // bucket write proof and unowned direct PUT payload shards.
    for shard_index in 0..written.ec.k + written.ec.m {
        assert!(!cluster
            .test_payload_shard_file_exists(
                written.data_pg_id,
                written.ec,
                &segment_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
}

#[test]
fn direct_put_stale_commit_snapshot_reruns_precondition_action() {
    const STALE_SNAPSHOTS: usize = 17;

    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-stale-snapshot-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);

    let loser_payload = b"loser direct put stale snapshot";
    let loser_reservation_id =
        crate::SessionId::try_from("41414141414141414141414141414141".to_string()).unwrap();
    let loser_generation_id = first_cluster
        .reserve_put_object_generation(&bucket, &key, &loser_reservation_id)
        .unwrap();
    let loser_written = first_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            loser_generation_id,
            0,
            &[0xb1; 16],
            loser_payload,
        )
        .unwrap();
    let loser_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: loser_reservation_id,
            generation_id: loser_generation_id,
            payload: loser_payload,
            segment_okh: [0xb1; 16],
            written: &loser_written,
        },
    );

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_saw_existing = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&first_map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_calls_for_closure = Arc::clone(&hook_calls);
    let _hook_guard =
        first_cluster.test_install_before_direct_put_command_id_hook(Arc::new(move || {
            let call = hook_calls_for_closure.fetch_add(1, Ordering::SeqCst);
            if call >= STALE_SNAPSHOTS {
                return Ok(());
            }
            if call == 0 {
                // The conditional mutation owns a ten-second operation budget. Crossing the
                // former one-second stale-snapshot sub-budget must not expose contention.
                thread::sleep(Duration::from_millis(1_100));
            }
            let pg_id = PgId::new(2);
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let generation_id = crate::GenerationId::new(10_000 + call as u64).unwrap();
            let size = 100 + call as u64;
            crate::PgMetadataStore::put_object_meta(
                &*pg,
                &crate::PutObjectReq::Live(crate::PutLiveObjectReq {
                    bucket: hook_bucket.clone(),
                    key: hook_key.clone(),
                    version_id: crate::VersionId::Null,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    generation_id,
                    size,
                    etag: crate::ObjectEtag::single_part(10_000 + call as u64),
                    ec: EcShape { k: 1, m: 0 },
                    layout: crate::ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: Some(crate::SerializedMetadataBlob::default()),
                    system_metadata_blob: Some(crate::SerializedSystemMetadataBlob::default()),
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                }),
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            Ok(())
        }));

    let calls_for_action = Arc::clone(&action_calls);
    let saw_existing_for_action = Arc::clone(&action_saw_existing);
    let hook_calls_for_action = Arc::clone(&hook_calls);
    let result = first_cluster
        .commit_direct_put_object_from_payload_shards(
            &loser_req,
            &loser_written.written_shards,
            move |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                if snapshot.existing_etag.is_some() {
                    saw_existing_for_action.store(true, Ordering::SeqCst);
                }
                if hook_calls_for_action.load(Ordering::SeqCst) >= STALE_SNAPSHOTS {
                    Err("object changed repeatedly")
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
    assert!(matches!(result, Err("object changed repeatedly")));
    assert_eq!(hook_calls.load(Ordering::SeqCst), STALE_SNAPSHOTS);
    assert_eq!(action_calls.load(Ordering::SeqCst), STALE_SNAPSHOTS + 1);
    assert!(action_saw_existing.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    assert_bucket_write_reservations_released(&first_map, &bucket);
}

#[test]
fn direct_put_stale_retry_and_pending_drain_share_operation_budget() {
    let _guard = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-shared-budget-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let loser_payload = b"loser direct put shared budget";
    let loser_reservation_id =
        crate::SessionId::try_from("42424242424242424242424242424242".to_string()).unwrap();
    let loser_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &loser_reservation_id)
        .unwrap();
    let loser_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            loser_generation_id,
            0,
            &[0xc1; 16],
            loser_payload,
        )
        .unwrap();
    let loser_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: loser_reservation_id,
            generation_id: loser_generation_id,
            payload: loser_payload,
            segment_okh: [0xc1; 16],
            written: &loser_written,
        },
    );

    let contender_payload = b"pending contender shared budget";
    let contender_reservation_id =
        crate::SessionId::try_from("43434343434343434343434343434343".to_string()).unwrap();
    let contender_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &contender_reservation_id)
        .unwrap();
    let contender_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            contender_generation_id,
            0,
            &[0xc2; 16],
            contender_payload,
        )
        .unwrap();
    let contender_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: contender_reservation_id,
            generation_id: contender_generation_id,
            payload: contender_payload,
            segment_okh: [0xc2; 16],
            written: &contender_written,
        },
    );

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let pending_installed = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_cluster = Arc::clone(&cluster);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_contender_req = contender_req.clone();
    let hook_contender_shards = contender_written.written_shards.clone();
    let hook_calls_for_closure = Arc::clone(&hook_calls);
    let pending_installed_for_closure = Arc::clone(&pending_installed);
    let _command_id_hook =
        cluster.test_install_before_direct_put_command_id_hook(Arc::new(move || {
            let call = hook_calls_for_closure.fetch_add(1, Ordering::SeqCst);
            let pg_id = PgId::new(2);
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            if call == 0 {
                thread::sleep(Duration::from_millis(1_100));
                let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
                crate::PgMetadataStore::put_object_meta(
                    &*pg,
                    &crate::PutObjectReq::Live(crate::PutLiveObjectReq {
                        bucket: hook_bucket.clone(),
                        key: hook_key.clone(),
                        version_id: crate::VersionId::Null,
                        owner: crate::OwnerIdentity::from_principal("owner"),
                        acl_grants: crate::AclGrants::default(),
                        public_read: false,
                        generation_id: crate::GenerationId::new(44_000).unwrap(),
                        size: 44,
                        etag: crate::ObjectEtag::single_part(44_000),
                        ec: EcShape { k: 1, m: 0 },
                        layout: crate::ObjectLayout::Standard,
                        tags: None,
                        metadata_blob: Some(crate::SerializedMetadataBlob::default()),
                        system_metadata_blob: Some(crate::SerializedSystemMetadataBlob::default()),
                        object_lock: crate::ObjectLockState::default(),
                        encryption: crate::ObjectEncryption::None,
                    }),
                )
                .unwrap();
                pg.refresh_metadata_command_state_digest().unwrap();
                return Ok(());
            }
            if call != 1 {
                return Ok(());
            }
            let shard_batch: Vec<(&ShardKey, WriteAck)> = hook_contender_shards
                .iter()
                .map(|written| (&written.key, written.ack))
                .collect();
            hook_cluster
                .register_payload_shard_acks(hook_contender_req.data_pg_id, &shard_batch)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let command = hook_cluster
                .prepare_commit_direct_put_object_command(
                    pg_id,
                    &pg,
                    &hook_contender_req,
                    crate::VersionId::Null,
                    hook_contender_req.bucket_write_reservation.clone(),
                )
                .unwrap();
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
            pending_installed_for_closure.store(true, Ordering::SeqCst);
            Ok(())
        }));

    let drain_hook_ran = Arc::new(AtomicBool::new(false));
    let drain_hook_ran_for_closure = Arc::clone(&drain_hook_ran);
    let _drain_hook = cluster.test_install_direct_put_pending_drain_hook(Arc::new(move || {
        !drain_hook_ran_for_closure.swap(true, Ordering::SeqCst)
    }));

    let error = cluster
        .commit_direct_put_object_from_payload_shards(
            &loser_req,
            &loser_written.written_shards,
            |_| Ok::<(), ()>(()),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        crate::ObjectPgActionError::MetadataCommandAwaitingAuthorizedRecovery
    ));
    assert_eq!(hook_calls.load(Ordering::SeqCst), 2);
    assert!(pending_installed.load(Ordering::SeqCst));
    assert!(drain_hook_ran.load(Ordering::SeqCst));

    let pending = pending_metadata_command_for_test(&map, PgId::new(2), &bucket)
        .expect("expired direct PUT budget must not drain the pending contender");
    assert!(matches!(
        pending.payload(),
        MetadataCommandPayload::CommitDirectPutObject(commit)
            if commit.object.generation_id == contender_generation_id
    ));
    assert!(cluster.test_metadata_command_recovery_awaiting_authorized(PgId::new(2), &pending));
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap();
    let pg = primary.storage_node().get_pg(2).unwrap();
    let live = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
        .unwrap()
        .as_live()
        .cloned()
        .unwrap();
    assert_eq!(
        live.generation_id,
        crate::GenerationId::new(44_000).unwrap()
    );
}
