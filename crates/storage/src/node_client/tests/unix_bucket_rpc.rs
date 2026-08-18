// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::control_plane::{PendingMetadataCommandObservation, PendingMetadataCommandRecovery};
use crate::storage_rpc::{
    encode_aborting_multipart_upload_buckets_response, encode_bucket_delete_begin_roots_response,
    encode_bucket_delete_finalize_claim_optional_record_response,
    encode_bucket_delete_finalize_roots_response, encode_bucket_execution_generations_response,
    encode_bucket_fast_path_identities_response, encode_bucket_info_outcome_response,
    encode_bucket_list_response, encode_bucket_snapshot_response,
    encode_bucket_subresource_get_response, encode_bucket_write_reservations_list_response,
    encode_lifecycle_sweep_buckets_response, encode_lifecycle_sweep_roots_response,
    StorageRpcAbortingMultipartUploadBucketsResponse, StorageRpcBucketDeleteBeginRootsResponse,
    StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse,
    StorageRpcBucketDeleteFinalizeRootsResponse, StorageRpcBucketExecutionGenerationsResponse,
    StorageRpcBucketFastPathIdentitiesResponse, StorageRpcBucketInfoOutcome,
    StorageRpcBucketInfoOutcomeResponse, StorageRpcBucketListResponse,
    StorageRpcBucketSnapshotOutcome, StorageRpcBucketSnapshotResponse,
    StorageRpcBucketSubresourceGetOutcome, StorageRpcBucketSubresourceGetResponse,
    StorageRpcBucketWriteReservationsListResponse, StorageRpcLifecycleSweepBucketsResponse,
    StorageRpcLifecycleSweepRootsResponse,
};

#[derive(Clone, Copy)]
enum HistoricalReplicaHeadAuthorization {
    Matching,
    Missing,
    WrongEpoch,
}

fn historical_bucket_delete_replica_head(
    authorization: HistoricalReplicaHeadAuthorization,
) -> Result<BucketInfo, BucketSnapshotLoadError> {
    let tmp = test_util::tempdir();
    let source_epoch = ClusterEpoch::new(1).unwrap();
    let other_epoch = ClusterEpoch::new(2).unwrap();
    let current_epoch = ClusterEpoch::new(3).unwrap();
    let mut config = test_config(&tmp);
    config.node_id = NodeId::new(8);
    config.cluster_epoch = current_epoch;
    config.route_map_validity =
        RouteMapValidity::until_ms(crate::clock::current_time_millis().saturating_add(60_000))
            .unwrap();
    config.pg_routes[0] = StorageNodePgRoute {
        pg_id: 0,
        cluster_epoch: current_epoch,
        state: crate::types::PgState::Peering,
        primary_node_id: NodeId::new(7),
        metadata_transfer_destination_epoch: None,
        metadata_read_route: None,
        acting_set: vec![NodeId::new(7), NodeId::new(8)],
    };
    let retained_route = |cluster_epoch| StorageNodePgRoute {
        pg_id: 0,
        cluster_epoch,
        state: crate::types::PgState::Active,
        primary_node_id: NodeId::new(7),
        metadata_transfer_destination_epoch: None,
        metadata_read_route: None,
        acting_set: vec![NodeId::new(7), NodeId::new(8)],
    };
    config
        .historical_pg_routes
        .push(retained_route(source_epoch));
    let authorized_epoch = match authorization {
        HistoricalReplicaHeadAuthorization::Matching => Some(source_epoch),
        HistoricalReplicaHeadAuthorization::Missing => None,
        HistoricalReplicaHeadAuthorization::WrongEpoch => {
            config
                .historical_pg_routes
                .push(retained_route(other_epoch));
            Some(other_epoch)
        }
    };
    if let Some(authorized_epoch) = authorized_epoch {
        config.pending_metadata_command_recoveries.push((
            PgId::new(0),
            PendingMetadataCommandRecovery::new(
                NodeId::new(7),
                PendingMetadataCommandObservation::new(
                    authorized_epoch,
                    std::num::NonZeroU64::MIN,
                    0x1234,
                ),
            ),
        ));
    }
    let bucket = crate::tests::bucket_name("historical-delete-replica-head-bucket");
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client =
        UnixStorageNodeClient::new(config.node_id, source_epoch, config.socket_path.clone())
            .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let result = client
        .open_bucket_delete_replica_metadata_route(source_epoch, bucket_pg_id_for_test(0), &bucket)
        .and_then(|route| route.head_bucket_replica_for_delete());
    server_thread.join().unwrap();
    result
}

fn bucket_pg_id_for_test(pg_id: u32) -> BucketPgId {
    BucketPgId::new_for_test(PgId::new(pg_id))
}

fn bucket_for_pg(topology: &PgTopology, target_pg: u32, prefix: &str) -> BucketName {
    (0..10_000)
        .map(|suffix| crate::tests::bucket_name(format!("{prefix}-{suffix}")))
        .find(|bucket| topology.bucket_pg_for(bucket) == target_pg)
        .expect("test topology must route a generated bucket to the target PG")
}

fn key_for_object_pg(
    topology: &PgTopology,
    bucket: &BucketName,
    target_pg: u32,
    prefix: &str,
) -> ObjectKey {
    (0..10_000)
        .map(|suffix| crate::tests::object_key(format!("{prefix}-{suffix}")))
        .find(|key| topology.object_pg_for(bucket, key) == target_pg)
        .expect("test topology must route a generated key to the target PG")
}

fn bucket_metadata_scan_route<'a>(
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
) -> Box<dyn BucketMetadataScanRoute + 'a> {
    client
        .open_bucket_metadata_scan_route(route_cluster_epoch, pg_id)
        .unwrap()
}

fn bucket_metadata_route<'a>(
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
    bucket: &BucketName,
) -> Box<dyn BucketMetadataRoute + 'a> {
    client
        .open_bucket_metadata_route(route_cluster_epoch, pg_id, bucket)
        .unwrap()
}

#[test]
fn unix_exact_bucket_route_rejects_crossed_builder_subjects_before_transport() {
    let tmp = test_util::tempdir();
    let route_epoch = ClusterEpoch::new(7).unwrap();
    let topology = Arc::new(PgTopology::new(&[0, 1]).unwrap());
    let bucket = bucket_for_pg(&topology, 0, "exact-route-subject");
    let other_bucket = bucket_for_pg(&topology, 0, "exact-route-other-subject");
    let client = UnixStorageNodeClient::new(
        NodeId::new(9),
        route_epoch,
        tmp.path().join("missing-storage-node.sock"),
    )
    .with_pg_topology(topology);
    let requests_started = rpc_requests_started_for_test(&client);
    let route = bucket_metadata_route(&client, route_epoch, bucket_pg_id_for_test(0), &bucket);
    let mutation = BucketSubresourceMutation::Delete {
        kind: BucketSubresourceKind::Cors,
    };

    for command_id in [
        MetadataCommandId::new(
            ClusterEpoch::new(route_epoch.get() + 1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandId::new(
            route_epoch,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
    ] {
        let error = route
            .build_put_bucket_subresource_command(command_id, &mutation)
            .unwrap_err();
        assert!(matches!(
            error,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            })
        ));
    }

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let wrong_config = crate::CreateBucketConfig {
        name: other_bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner,
        acl_grants: &acl_grants,
        public_read: false,
        public_write: false,
        versioning: BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };
    let error = route
        .build_create_bucket_command(
            MetadataCommandId::new(
                route_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            &wrong_config,
        )
        .expect_err("crossed create-bucket config must be rejected");
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let reservation = BucketWriteReservationRecord {
        bucket: bucket.clone(),
        reservation_id: "exact-route-barrier-reservation".to_string(),
        owner_token: "exact-route-barrier-owner".to_string(),
        cluster_epoch: route_epoch,
        bucket_execution_generation: 2,
        bucket_incarnation_generation: 3,
        operation_kind: COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string(),
        created_at: 10,
        lease_deadline: 20,
        target_context: Some("object-key".to_string()),
    };
    let valid_proof = BucketWriteReservationProof::from(&reservation);
    let mut crossed_bucket = valid_proof.clone();
    crossed_bucket.bucket = other_bucket;
    let mut crossed_operation = valid_proof.clone();
    crossed_operation.operation_kind = "abort-multipart-upload".to_string();
    let mut crossed_target = valid_proof.clone();
    crossed_target.target_context = Some("another-object-key".to_string());
    let mut crossed_epoch = valid_proof;
    crossed_epoch.cluster_epoch = ClusterEpoch::new(route_epoch.get() + 1).unwrap();
    for proof in [
        crossed_bucket,
        crossed_operation,
        crossed_target,
        crossed_epoch,
    ] {
        let error = route
            .build_advance_multipart_completion_barrier_command(
                MetadataCommandId::new(
                    route_epoch,
                    PgId::new(0),
                    MetadataCommandLogIndex::new(3).unwrap(),
                ),
                "object-key",
                &proof,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationConflict { .. })
        ));
    }
    assert_eq!(rpc_requests_started_for_test(&client), requests_started);
}

fn assert_bucket_metadata_payload_decode(error: BucketSnapshotLoadError) {
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
}

#[test]
fn unix_exact_bucket_routes_bind_not_found_response_subjects() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let topology = Arc::new(PgTopology::new(&config.pg_ids).unwrap());
    let source_bucket = crate::tests::bucket_name("exact-not-found-source");
    let foreign_bucket = crate::tests::bucket_name("exact-not-found-foreign");

    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let source_for_server = source_bucket.clone();
    let server_thread = thread::spawn(move || {
        for (expected_kind, payload) in [
            (
                StorageRpcMessageKind::BucketHeadRaw,
                encode_bucket_info_outcome_response(&StorageRpcBucketInfoOutcomeResponse {
                    outcome: StorageRpcBucketInfoOutcome::BucketNotFound {
                        name: foreign_bucket.clone(),
                    },
                }),
            ),
            (
                StorageRpcMessageKind::BucketSnapshotLoad,
                encode_bucket_snapshot_response(&StorageRpcBucketSnapshotResponse {
                    outcome: StorageRpcBucketSnapshotOutcome::BucketNotFound {
                        name: foreign_bucket.clone(),
                    },
                }),
            ),
            (
                StorageRpcMessageKind::BucketSnapshotLoad,
                encode_bucket_snapshot_response(&StorageRpcBucketSnapshotResponse {
                    outcome: StorageRpcBucketSnapshotOutcome::BucketNotFound {
                        name: source_for_server.clone(),
                    },
                }),
            ),
            (
                StorageRpcMessageKind::BucketSubresourceGet,
                encode_bucket_subresource_get_response(&StorageRpcBucketSubresourceGetResponse {
                    outcome: StorageRpcBucketSubresourceGetOutcome::BucketNotFound {
                        name: foreign_bucket,
                    },
                }),
            ),
            (
                StorageRpcMessageKind::BucketSubresourceGet,
                encode_bucket_subresource_get_response(&StorageRpcBucketSubresourceGetResponse {
                    outcome: StorageRpcBucketSubresourceGetOutcome::BucketNotFound {
                        name: source_for_server,
                    },
                }),
            ),
        ] {
            let (mut connection, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut connection).unwrap();
            assert_eq!(request.kind, expected_kind);
            write_storage_rpc_frame_to(
                &mut connection,
                &StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                },
            )
            .unwrap();
        }
    });

    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(topology);
    let route = bucket_metadata_route(
        &client,
        config.cluster_epoch,
        bucket_pg_id_for_test(0),
        &source_bucket,
    );
    assert_bucket_metadata_payload_decode(route.head_bucket_raw().unwrap_err());
    assert_bucket_metadata_payload_decode(
        route
            .load_bucket_snapshot(BucketSnapshotRequest::default())
            .unwrap_err(),
    );
    assert!(matches!(
        route
            .load_bucket_snapshot(BucketSnapshotRequest::default())
            .unwrap_err(),
        BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name })
            if name == source_bucket
    ));
    assert_bucket_metadata_payload_decode(
        route
            .get_bucket_subresource(BucketSubresourceKind::Cors)
            .unwrap_err(),
    );
    assert!(matches!(
        route
            .get_bucket_subresource(BucketSubresourceKind::Cors)
            .unwrap_err(),
        BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name })
            if name == source_bucket
    ));
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_metadata_scan_route_rejects_foreign_epoch_and_request_subject_before_rpc() {
    let tmp = test_util::tempdir();
    let topology = Arc::new(PgTopology::new(&[0, 1]).unwrap());
    let foreign_bucket = bucket_for_pg(&topology, 1, "bucket-scan-foreign-request");
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    )
    .with_pg_topology(Arc::clone(&topology));
    let requests_started = rpc_requests_started_for_test(&client);

    let route = bucket_metadata_scan_route(
        &client,
        ClusterEpoch::new(1).unwrap(),
        bucket_pg_id_for_test(0),
    );
    assert_bucket_metadata_payload_decode(
        route
            .load_bucket_execution_generations(std::slice::from_ref(&foreign_bucket))
            .unwrap_err(),
    );
    assert_bucket_metadata_payload_decode(
        route
            .load_bucket_fast_path_identities(std::slice::from_ref(&foreign_bucket))
            .unwrap_err(),
    );
    drop(route);

    match client
        .open_bucket_metadata_scan_route(ClusterEpoch::new(2).unwrap(), bucket_pg_id_for_test(0))
    {
        Err(BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation { .. })) => {}
        Err(error) => panic!("unexpected foreign-epoch route error: {error}"),
        Ok(_) => panic!("foreign epoch opened a bucket metadata scan route"),
    }
    assert_eq!(rpc_requests_started_for_test(&client), requests_started);
}

#[test]
fn unix_bucket_metadata_scan_route_rejects_foreign_response_subjects() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    let topology = Arc::new(PgTopology::new(&config.pg_ids).unwrap());
    let requested_bucket = bucket_for_pg(&topology, 0, "bucket-scan-requested");
    let foreign_bucket = bucket_for_pg(&topology, 1, "bucket-scan-foreign-response");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl = crate::AclGrants::default();
    let foreign_info = test_bucket_info(foreign_bucket.clone(), &owner, &acl);

    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let foreign_for_server = foreign_bucket.clone();
    let server_thread = thread::spawn(move || {
        for (expected_kind, payload) in [
            (
                StorageRpcMessageKind::BucketList,
                encode_bucket_list_response(&StorageRpcBucketListResponse {
                    buckets: vec![foreign_info],
                })
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::BucketExecutionGenerations,
                encode_bucket_execution_generations_response(
                    &StorageRpcBucketExecutionGenerationsResponse {
                        generations: HashMap::from([(foreign_for_server.clone(), 11)]),
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::BucketFastPathIdentities,
                encode_bucket_fast_path_identities_response(
                    &StorageRpcBucketFastPathIdentitiesResponse {
                        identities: HashMap::from([(
                            foreign_for_server.clone(),
                            BucketFastPathIdentity {
                                bucket_execution_generation: 11,
                                bucket_incarnation_generation: 17,
                            },
                        )]),
                    },
                )
                .unwrap(),
            ),
        ] {
            let (mut connection, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut connection).unwrap();
            assert_eq!(request.kind, expected_kind);
            write_storage_rpc_frame_to(
                &mut connection,
                &StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                },
            )
            .unwrap();
        }
    });

    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(topology);
    let route = bucket_metadata_scan_route(&client, config.cluster_epoch, bucket_pg_id_for_test(0));
    assert_bucket_metadata_payload_decode(route.list_buckets(owner.as_str()).unwrap_err());
    assert_bucket_metadata_payload_decode(
        route
            .load_bucket_execution_generations(std::slice::from_ref(&requested_bucket))
            .unwrap_err(),
    );
    assert_bucket_metadata_payload_decode(
        route
            .load_bucket_fast_path_identities(std::slice::from_ref(&requested_bucket))
            .unwrap_err(),
    );
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_metadata_scan_route_rejects_duplicate_bucket_list_rows() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let topology = Arc::new(PgTopology::new(&config.pg_ids).unwrap());
    let bucket = bucket_for_pg(&topology, 0, "bucket-scan-duplicate-response");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let bucket_info = test_bucket_info(bucket, &owner, &crate::AclGrants::default());

    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let server_thread = thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        let request = read_storage_rpc_frame_from(&mut connection).unwrap();
        assert_eq!(request.kind, StorageRpcMessageKind::BucketList);
        let payload = encode_bucket_list_response(&StorageRpcBucketListResponse {
            buckets: vec![bucket_info.clone(), bucket_info],
        })
        .unwrap();
        write_storage_rpc_frame_to(
            &mut connection,
            &StorageRpcFrame {
                request_id: request.request_id,
                kind: request.kind,
                payload: encode_storage_rpc_success_response(&payload),
            },
        )
        .unwrap();
    });

    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(topology);
    let route = bucket_metadata_scan_route(&client, config.cluster_epoch, bucket_pg_id_for_test(0));

    assert_bucket_metadata_payload_decode(route.list_buckets(owner.as_str()).unwrap_err());
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_metadata_scan_rejects_misplaced_durable_bucket_as_payload_decode() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes.push(StorageNodePgRoute {
        pg_id: 1,
        cluster_epoch: config.cluster_epoch,
        state: crate::types::PgState::Active,
        primary_node_id: config.node_id,
        metadata_transfer_destination_epoch: None,
        metadata_read_route: None,
        acting_set: vec![config.node_id],
    });
    let topology = Arc::new(PgTopology::new(&config.pg_ids).unwrap());
    let misplaced_bucket = bucket_for_pg(&topology, 1, "bucket-scan-misplaced-durable");
    let owner = crate::CanonicalUserId::from_principal("owner");
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let wrong_pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*wrong_pg,
            &misplaced_bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        wrong_pg.refresh_metadata_command_state_digest().unwrap();
    }

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(topology);
    let route = bucket_metadata_scan_route(&client, config.cluster_epoch, bucket_pg_id_for_test(0));

    assert_bucket_metadata_payload_decode(route.list_buckets(owner.as_str()).unwrap_err());
    server_thread.join().unwrap();
}

fn retained_bucket_write_route<'a>(
    client: &'a UnixStorageNodeClient,
    pg_id: BucketPgId,
    bucket: &BucketName,
) -> Box<dyn RetainedBucketWriteReservationRoute + 'a> {
    client
        .open_retained_bucket_write_reservation_route(pg_id, bucket)
        .unwrap()
}

fn bucket_write_reservation_route<'a>(
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
    bucket: &BucketName,
) -> Box<dyn BucketWriteReservationRoute + 'a> {
    client
        .open_bucket_write_reservation_route(route_cluster_epoch, pg_id, bucket)
        .unwrap()
}

fn bucket_write_reservation_scan_route<'a>(
    client: &'a UnixStorageNodeClient,
    route_cluster_epoch: ClusterEpoch,
    pg_id: BucketPgId,
) -> Box<dyn BucketWriteReservationScanRoute + 'a> {
    client
        .open_bucket_write_reservation_scan_route(route_cluster_epoch, pg_id)
        .unwrap()
}

#[test]
fn unix_bucket_write_reservation_scan_route_rejects_foreign_epoch_and_marker_before_rpc() {
    let tmp = test_util::tempdir();
    let topology = Arc::new(PgTopology::new(&[0, 1]).unwrap());
    let foreign_bucket = bucket_for_pg(&topology, 1, "write-scan-foreign-marker");
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    )
    .with_pg_topology(Arc::clone(&topology));
    let requests_started = rpc_requests_started_for_test(&client);

    let route = bucket_write_reservation_scan_route(
        &client,
        ClusterEpoch::new(1).unwrap(),
        bucket_pg_id_for_test(0),
    );
    assert_bucket_metadata_payload_decode(
        route
            .get_bucket_delete_begin_roots(10, Some(&foreign_bucket), 16)
            .unwrap_err(),
    );
    drop(route);

    match client.open_bucket_write_reservation_scan_route(
        ClusterEpoch::new(2).unwrap(),
        bucket_pg_id_for_test(0),
    ) {
        Err(BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation { .. })) => {}
        Err(error) => panic!("unexpected foreign-epoch route error: {error}"),
        Ok(_) => panic!("foreign epoch opened a bucket write reservation scan route"),
    }
    assert_eq!(rpc_requests_started_for_test(&client), requests_started);
}

#[test]
fn unix_bucket_write_reservation_scan_route_rejects_foreign_response_subjects() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    let topology = Arc::new(PgTopology::new(&config.pg_ids).unwrap());
    let foreign_bucket = bucket_for_pg(&topology, 1, "write-scan-foreign-response");
    let foreign_key = key_for_object_pg(&topology, &foreign_bucket, 1, "foreign-key");
    let foreign_info = test_bucket_info(
        foreign_bucket.clone(),
        &crate::CanonicalUserId::from_principal("owner"),
        &crate::AclGrants::default(),
    );

    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let server_thread = thread::spawn(move || {
        for (expected_kind, payload) in [
            (
                StorageRpcMessageKind::BucketDeleteFinalizeRoots,
                encode_bucket_delete_finalize_roots_response(
                    &StorageRpcBucketDeleteFinalizeRootsResponse {
                        roots: vec![BucketDeleteFinalizeRoot {
                            bucket: foreign_bucket.clone(),
                            bucket_incarnation_generation: 3,
                        }],
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::BucketDeleteBeginRoots,
                encode_bucket_delete_begin_roots_response(
                    &StorageRpcBucketDeleteBeginRootsResponse {
                        roots: vec![BucketDeleteBeginRoot {
                            bucket: foreign_bucket.clone(),
                            bucket_execution_generation: 2,
                            bucket_incarnation_generation: 3,
                        }],
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::LifecycleSweepRoots,
                encode_lifecycle_sweep_roots_response(&StorageRpcLifecycleSweepRootsResponse {
                    roots: vec![LifecycleSweepRoot {
                        bucket: foreign_bucket.clone(),
                        bucket_incarnation_generation: 3,
                        source: crate::types::LifecycleSweepRootSource::LifecycleConfig,
                    }],
                })
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::LifecycleSweepBucketsList,
                encode_lifecycle_sweep_buckets_response(&StorageRpcLifecycleSweepBucketsResponse {
                    buckets: vec![foreign_info],
                })
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::ObjectAbortingMultipartUploadBucketsList,
                encode_aborting_multipart_upload_buckets_response(
                    &StorageRpcAbortingMultipartUploadBucketsResponse {
                        witnesses: vec![AbortingMultipartUploadBucketWitness {
                            bucket: foreign_bucket,
                            key: foreign_key,
                        }],
                    },
                )
                .unwrap(),
            ),
        ] {
            let (mut connection, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut connection).unwrap();
            assert_eq!(request.kind, expected_kind);
            write_storage_rpc_frame_to(
                &mut connection,
                &StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                },
            )
            .unwrap();
        }
    });

    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(topology);
    let route = bucket_write_reservation_scan_route(
        &client,
        config.cluster_epoch,
        bucket_pg_id_for_test(0),
    );
    assert_bucket_metadata_payload_decode(
        route.get_bucket_delete_finalize_roots(10, 16).unwrap_err(),
    );
    assert_bucket_metadata_payload_decode(
        route
            .get_bucket_delete_begin_roots(10, None, 16)
            .unwrap_err(),
    );
    assert_bucket_metadata_payload_decode(route.get_lifecycle_sweep_roots(10, 16).unwrap_err());
    assert_bucket_metadata_payload_decode(route.list_buckets_with_lifecycle().unwrap_err());
    let object_route = client
        .open_object_mutation_scan_metadata_route(
            config.cluster_epoch,
            crate::ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
        )
        .unwrap();
    assert!(matches!(
        object_route
            .list_aborting_multipart_upload_bucket_witnesses()
            .unwrap_err(),
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_write_reservation_scan_route_rejects_duplicate_or_unordered_responses() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let topology = Arc::new(PgTopology::new(&config.pg_ids).unwrap());
    let bucket = bucket_for_pg(&topology, 0, "write-scan-duplicate-response");
    let bucket_info = test_bucket_info(
        bucket.clone(),
        &crate::CanonicalUserId::from_principal("owner"),
        &crate::AclGrants::default(),
    );
    let finalize_root = BucketDeleteFinalizeRoot {
        bucket: bucket.clone(),
        bucket_incarnation_generation: 3,
    };
    let begin_root = BucketDeleteBeginRoot {
        bucket: bucket.clone(),
        bucket_execution_generation: 2,
        bucket_incarnation_generation: 3,
    };
    let lifecycle_root = LifecycleSweepRoot {
        bucket: bucket.clone(),
        bucket_incarnation_generation: 3,
        source: crate::types::LifecycleSweepRootSource::LifecycleConfig,
    };
    let witness = AbortingMultipartUploadBucketWitness {
        bucket: bucket.clone(),
        key: key_for_object_pg(&topology, &bucket, 0, "duplicate-key"),
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let server_thread = thread::spawn(move || {
        for (expected_kind, payload) in [
            (
                StorageRpcMessageKind::BucketDeleteFinalizeRoots,
                encode_bucket_delete_finalize_roots_response(
                    &StorageRpcBucketDeleteFinalizeRootsResponse {
                        roots: vec![finalize_root.clone(), finalize_root],
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::BucketDeleteBeginRoots,
                encode_bucket_delete_begin_roots_response(
                    &StorageRpcBucketDeleteBeginRootsResponse {
                        roots: vec![begin_root.clone(), begin_root],
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::LifecycleSweepRoots,
                encode_lifecycle_sweep_roots_response(&StorageRpcLifecycleSweepRootsResponse {
                    roots: vec![lifecycle_root.clone(), lifecycle_root],
                })
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::LifecycleSweepBucketsList,
                encode_lifecycle_sweep_buckets_response(&StorageRpcLifecycleSweepBucketsResponse {
                    buckets: vec![bucket_info.clone(), bucket_info],
                })
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::ObjectAbortingMultipartUploadBucketsList,
                encode_aborting_multipart_upload_buckets_response(
                    &StorageRpcAbortingMultipartUploadBucketsResponse {
                        witnesses: vec![witness.clone(), witness],
                    },
                )
                .unwrap(),
            ),
        ] {
            let (mut connection, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut connection).unwrap();
            assert_eq!(request.kind, expected_kind);
            write_storage_rpc_frame_to(
                &mut connection,
                &StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                },
            )
            .unwrap();
        }
    });

    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(topology);
    let route = bucket_write_reservation_scan_route(
        &client,
        config.cluster_epoch,
        bucket_pg_id_for_test(0),
    );
    assert_bucket_metadata_payload_decode(
        route.get_bucket_delete_finalize_roots(10, 16).unwrap_err(),
    );
    assert_bucket_metadata_payload_decode(
        route
            .get_bucket_delete_begin_roots(10, None, 16)
            .unwrap_err(),
    );
    assert_bucket_metadata_payload_decode(route.get_lifecycle_sweep_roots(10, 16).unwrap_err());
    assert_bucket_metadata_payload_decode(route.list_buckets_with_lifecycle().unwrap_err());
    let object_route = client
        .open_object_mutation_scan_metadata_route(
            config.cluster_epoch,
            crate::ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
        )
        .unwrap();
    assert!(matches!(
        object_route
            .list_aborting_multipart_upload_bucket_witnesses()
            .unwrap_err(),
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    server_thread.join().unwrap();
}

#[test]
fn unix_retained_bucket_write_route_rejects_foreign_subject_before_rpc() {
    let client = test_unix_storage_node_client();
    let bound_bucket = crate::tests::bucket_name("unix-retained-route-bound-bucket");
    let foreign_bucket = crate::tests::bucket_name("unix-retained-route-foreign-bucket");
    let pg_id = bucket_pg_id_for_test(0);
    let route = retained_bucket_write_route(&client, pg_id, &bound_bucket);
    let foreign = test_retained_bucket_write_subjects(foreign_bucket, pg_id.get());

    for (error, operation) in [
        (
            route
                .release_durable_bucket_write_reservation(&foreign.reservation)
                .unwrap_err(),
            "release durable bucket write reservation",
        ),
        (
            route
                .release_metadata_command_bucket_write_reservation(&foreign.proof)
                .unwrap_err(),
            "release metadata command bucket write reservation",
        ),
        (
            route
                .clear_durable_bucket_write_drain(&foreign.drain)
                .unwrap_err(),
            "clear durable bucket write drain",
        ),
        (
            route
                .release_bucket_delete_finalize_claim(&foreign.delete_claim)
                .unwrap_err(),
            "release bucket delete finalize claim",
        ),
        (
            route
                .release_lifecycle_sweep_claim(&foreign.lifecycle_claim)
                .unwrap_err(),
            "release lifecycle sweep claim",
        ),
    ] {
        assert_route_subject_mismatch(error, operation);
    }

    let mut wrong_pg = test_retained_bucket_write_subjects(bound_bucket, pg_id.get());
    wrong_pg.delete_claim.pg_id = pg_id.get().saturating_add(1);
    wrong_pg.lifecycle_claim.pg_id = pg_id.get().saturating_add(1);
    assert_route_subject_mismatch(
        route
            .release_bucket_delete_finalize_claim(&wrong_pg.delete_claim)
            .unwrap_err(),
        "release bucket delete finalize claim",
    );
    assert_route_subject_mismatch(
        route
            .release_lifecycle_sweep_claim(&wrong_pg.lifecycle_claim)
            .unwrap_err(),
        "release lifecycle sweep claim",
    );
}

#[test]
fn unix_bucket_write_reservation_route_rejects_foreign_subjects_before_rpc() {
    let client = test_unix_storage_node_client();
    let bound_bucket = crate::tests::bucket_name("unix-write-route-bound-bucket");
    let foreign_bucket = crate::tests::bucket_name("unix-write-route-foreign-bucket");
    let pg_id = bucket_pg_id_for_test(0);
    let route =
        bucket_write_reservation_route(&client, ClusterEpoch::INITIAL, pg_id, &bound_bucket);
    let foreign = test_retained_bucket_write_subjects(foreign_bucket.clone(), pg_id.get());
    let requests_started = rpc_requests_started_for_test(&client);
    let assert_payload_decode = |error: BucketSnapshotLoadError, operation: &str| {
        assert!(
            matches!(
                error,
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ),
            "foreign {operation} must fail with PayloadDecode"
        );
    };

    assert_payload_decode(
        route
            .acquire_durable_bucket_write_reservation(DurableBucketWriteReservationAcquire {
                name: &foreign_bucket,
                reservation_id: "foreign-reservation",
                owner_token: "foreign-owner",
                cluster_epoch: ClusterEpoch::INITIAL,
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key"),
            })
            .unwrap_err(),
        "reservation acquire",
    );
    assert_payload_decode(
        route
            .validate_bucket_write_reservation_proof(&foreign.proof)
            .unwrap_err(),
        "reservation validation",
    );
    assert_payload_decode(
        route
            .heartbeat_durable_bucket_write_drain(&foreign.drain, 30)
            .unwrap_err(),
        "drain heartbeat",
    );
    assert_payload_decode(
        route
            .heartbeat_lifecycle_sweep_claim(&foreign.lifecycle_claim, 30, Some(40))
            .unwrap_err(),
        "lifecycle claim heartbeat",
    );
    assert_payload_decode(
        route
            .record_lifecycle_sweep_claim_error(&foreign.lifecycle_claim, "foreign")
            .unwrap_err(),
        "lifecycle claim error",
    );

    assert_payload_decode(
        route
            .acquire_durable_bucket_write_reservation(DurableBucketWriteReservationAcquire {
                name: &bound_bucket,
                reservation_id: "wrong-epoch-reservation",
                owner_token: "wrong-epoch-owner",
                cluster_epoch: ClusterEpoch::new(2).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key"),
            })
            .unwrap_err(),
        "reservation acquire epoch",
    );
    assert_eq!(rpc_requests_started_for_test(&client), requests_started);
}

#[test]
fn unix_bucket_write_reservation_route_rejects_foreign_or_duplicate_response_subjects() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("write-route-response-bucket");
    let foreign_bucket = crate::tests::bucket_name("write-route-response-foreign");
    let reservation = BucketWriteReservationRecord {
        bucket: bucket.clone(),
        reservation_id: "duplicate-reservation".to_string(),
        owner_token: "reservation-owner".to_string(),
        cluster_epoch: config.cluster_epoch,
        bucket_execution_generation: 2,
        bucket_incarnation_generation: 3,
        operation_kind: "put-object".to_string(),
        created_at: 10,
        lease_deadline: 20,
        target_context: Some("key".to_string()),
    };
    let foreign_claim = BucketDeleteFinalizeClaimRecord {
        bucket: foreign_bucket,
        bucket_incarnation_generation: 3,
        claim_id: "foreign-claim".to_string(),
        owner_token: "claim-owner".to_string(),
        cluster_epoch: config.cluster_epoch,
        pg_id: 0,
        claimed_at: 10,
        lease_deadline: Some(20),
        attempt_count: 1,
        last_error: None,
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let server_thread = thread::spawn(move || {
        for (expected_kind, payload) in [
            (
                StorageRpcMessageKind::BucketWriteReservationsList,
                encode_bucket_write_reservations_list_response(
                    &StorageRpcBucketWriteReservationsListResponse {
                        records: vec![reservation.clone(), reservation],
                    },
                )
                .unwrap(),
            ),
            (
                StorageRpcMessageKind::BucketDeleteFinalizeClaimGet,
                encode_bucket_delete_finalize_claim_optional_record_response(
                    &StorageRpcBucketDeleteFinalizeClaimOptionalRecordResponse {
                        record: Some(foreign_claim),
                    },
                )
                .unwrap(),
            ),
        ] {
            let (mut connection, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut connection).unwrap();
            assert_eq!(request.kind, expected_kind);
            write_storage_rpc_frame_to(
                &mut connection,
                &StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                },
            )
            .unwrap();
        }
    });

    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let route = bucket_write_reservation_route(
        &client,
        config.cluster_epoch,
        bucket_pg_id_for_test(0),
        &bucket,
    );
    assert_bucket_metadata_payload_decode(route.durable_bucket_write_reservations().unwrap_err());
    assert_bucket_metadata_payload_decode(route.bucket_delete_finalize_claim().unwrap_err());
    server_thread.join().unwrap();
}

#[test]
fn unix_object_list_response_requires_truncated_marker_identity() {
    let client = test_unix_storage_node_client();
    let pg_id = ObjectMetadataScanPgId::new_for_test(PgId::new(0));
    let pg_topology = PgTopology::new(&[0]).unwrap();
    let bucket = crate::tests::bucket_name("list-marker-bucket");
    let key = crate::tests::object_key("list-marker-key");
    let object = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        ObjectLayout::Standard,
    );
    let req = ListObjectsReq {
        bucket,
        prefix: None,
        start_after: None,
        start_at: None,
        max_keys: 1,
    };
    let mut response = ListObjectsResp {
        objects: vec![object],
        is_truncated: true,
        next_start_after: None,
    };
    assert!(validate_list_objects_response(&client, pg_id, &pg_topology, &response, &req).is_err());

    response.next_start_after = Some(crate::tests::object_key("wrong-marker"));
    assert!(validate_list_objects_response(&client, pg_id, &pg_topology, &response, &req).is_err());

    response.next_start_after = Some(key);
    validate_list_objects_response(&client, pg_id, &pg_topology, &response, &req).unwrap();
}

#[test]
fn unix_object_version_list_response_requires_truncated_marker_identity() {
    let client = test_unix_storage_node_client();
    let pg_id = ObjectMetadataScanPgId::new_for_test(PgId::new(0));
    let pg_topology = PgTopology::new(&[0]).unwrap();
    let bucket = crate::tests::bucket_name("version-list-marker-bucket");
    let key = crate::tests::object_key("version-list-marker-key");
    let version_id = VersionId::from_u64(44);
    let mut object = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        ObjectLayout::Standard,
    );
    let StoredObject::Live(record) = &mut object else {
        unreachable!("test helper always builds a live object");
    };
    record.version_id = version_id;
    let req = ListObjectVersionsReq {
        bucket,
        prefix: None,
        key_marker: None,
        version_id_marker: None,
        start_at: None,
        max_keys: 1,
    };
    let mut response = ListObjectVersionsResp {
        versions: vec![object],
        is_truncated: true,
        next_key_marker: Some(key.clone()),
        next_version_id_marker: None,
    };
    assert!(
        validate_list_object_versions_response(&client, pg_id, &pg_topology, &response, &req)
            .is_err()
    );

    response.next_version_id_marker = Some(VersionId::from_u64(45));
    assert!(
        validate_list_object_versions_response(&client, pg_id, &pg_topology, &response, &req)
            .is_err()
    );

    response.next_version_id_marker = Some(version_id);
    validate_list_object_versions_response(&client, pg_id, &pg_topology, &response, &req).unwrap();
}

#[test]
fn unix_multipart_upload_list_response_requires_final_upload_marker_identity() {
    let client = test_unix_storage_node_client();
    let pg_id = ObjectMetadataScanPgId::new_for_test(PgId::new(0));
    let pg_topology = PgTopology::new(&[0]).unwrap();
    let bucket = crate::tests::bucket_name("mpu-list-marker-bucket");
    let key = crate::tests::object_key("mpu-list-marker-key");
    let upload_id = UploadId::try_from("u".repeat(crate::UPLOAD_ID_LEN)).unwrap();
    let upload = test_multipart_upload_record(
        bucket.clone(),
        key.clone(),
        upload_id.clone(),
        UploadState::InProgress,
    );
    let req = ListMultipartUploadsReq {
        bucket,
        prefix: None,
        page_start: None,
        max_uploads: 1,
    };
    let mut response = ListMultipartUploadsResp {
        uploads: vec![upload],
        is_truncated: false,
        next_key_marker: Some(key.clone()),
        next_upload_id_marker: None,
    };
    assert!(validate_list_multipart_uploads_response(
        &client,
        pg_id,
        &pg_topology,
        &response,
        &req
    )
    .is_err());

    response.next_upload_id_marker =
        Some(UploadId::try_from("v".repeat(crate::UPLOAD_ID_LEN)).unwrap());
    assert!(validate_list_multipart_uploads_response(
        &client,
        pg_id,
        &pg_topology,
        &response,
        &req
    )
    .is_err());

    response.next_upload_id_marker = Some(upload_id.clone());
    validate_list_multipart_uploads_response(&client, pg_id, &pg_topology, &response, &req)
        .unwrap();

    response.is_truncated = true;
    validate_list_multipart_uploads_response(&client, pg_id, &pg_topology, &response, &req)
        .unwrap();

    let mut empty = ListMultipartUploadsResp {
        uploads: Vec::new(),
        is_truncated: false,
        next_key_marker: None,
        next_upload_id_marker: None,
    };
    validate_list_multipart_uploads_response(&client, pg_id, &pg_topology, &empty, &req).unwrap();

    empty.next_key_marker = Some(key);
    empty.next_upload_id_marker = Some(upload_id);
    assert!(
        validate_list_multipart_uploads_response(&client, pg_id, &pg_topology, &empty, &req)
            .is_err()
    );

    empty.next_key_marker = None;
    empty.next_upload_id_marker = None;
    empty.is_truncated = true;
    assert!(
        validate_list_multipart_uploads_response(&client, pg_id, &pg_topology, &empty, &req)
            .is_err()
    );
}

#[test]
fn unix_listing_response_validators_reject_foreign_scan_pg_rows() {
    let client = test_unix_storage_node_client();
    let pg_id = ObjectMetadataScanPgId::new_for_test(PgId::new(0));
    let pg_topology = PgTopology::new(&[0, 1]).unwrap();
    let bucket = crate::tests::bucket_name("foreign-listing-row-bucket");
    let key = (0..10_000)
        .map(|index| crate::tests::object_key(format!("foreign-listing-row-{index}")))
        .find(|key| pg_topology.object_pg_for(&bucket, key) == 1)
        .expect("test must find a key placed on the foreign scan PG");
    let object = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        ObjectLayout::Standard,
    );
    let object_request = ListObjectsReq {
        bucket: bucket.clone(),
        prefix: None,
        start_after: None,
        start_at: None,
        max_keys: 1,
    };
    let object_error = validate_list_objects_response(
        &client,
        pg_id,
        &pg_topology,
        &ListObjectsResp {
            objects: vec![object.clone()],
            is_truncated: false,
            next_start_after: None,
        },
        &object_request,
    )
    .unwrap_err();

    let version_request = ListObjectVersionsReq {
        bucket: bucket.clone(),
        prefix: None,
        key_marker: None,
        version_id_marker: None,
        start_at: None,
        max_keys: 1,
    };
    let version_error = validate_list_object_versions_response(
        &client,
        pg_id,
        &pg_topology,
        &ListObjectVersionsResp {
            versions: vec![object],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
        },
        &version_request,
    )
    .unwrap_err();

    let upload_id = UploadId::try_from("u".repeat(crate::UPLOAD_ID_LEN)).unwrap();
    let upload = test_multipart_upload_record(
        bucket.clone(),
        key.clone(),
        upload_id.clone(),
        UploadState::InProgress,
    );
    let upload_request = ListMultipartUploadsReq {
        bucket,
        prefix: None,
        page_start: None,
        max_uploads: 1,
    };
    let upload_error = validate_list_multipart_uploads_response(
        &client,
        pg_id,
        &pg_topology,
        &ListMultipartUploadsResp {
            uploads: vec![upload],
            is_truncated: false,
            next_key_marker: Some(key),
            next_upload_id_marker: Some(upload_id),
        },
        &upload_request,
    )
    .unwrap_err();

    for error in [object_error, version_error, upload_error] {
        assert!(matches!(
            error,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            })
        ));
    }
}

fn test_bucket_info(
    name: BucketName,
    owner: &crate::CanonicalUserId,
    acl_grants: &crate::AclGrants,
) -> BucketInfo {
    BucketInfo {
        name,
        owner_principal: "owner".to_string(),
        owner_canonical_id: owner.clone(),
        created_at: 123,
        region: 0,
        state: BucketState::Active,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        acl_grants: acl_grants.clone(),
        public_read: false,
        public_write: false,
        public_access_block: None,
        ownership_controls: None,
        bucket_policy_present: false,
        bucket_policy_public: false,
        bucket_policy_generation: 0,
        bucket_lifecycle_present: false,
        bucket_lifecycle_generation: 0,
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        multipart_upload_id_key: crate::types::MultipartUploadIdKey::from_bytes([1; 32]),
        bucket_abac_enabled: false,
        encryption: crate::types::EffectiveBucketEncryptionConfig::default(),
    }
}

#[test]
fn unix_create_bucket_build_response_rejects_mismatched_identity() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("create-bucket-rpc-expected");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let config = crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner,
        acl_grants: &acl_grants,
        public_read: false,
        public_write: false,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };
    let command_id = MetadataCommandId::new(
        ClusterEpoch::new(1).unwrap(),
        PgId::new(0),
        MetadataCommandLogIndex::new(1).unwrap(),
    );

    let wrong_bucket = crate::tests::bucket_name("create-bucket-rpc-wrong");
    let err = client
        .validate_create_bucket_command_build_outcome(
            StorageRpcCreateBucketCommandBuildOutcome::Exists(test_bucket_info(
                wrong_bucket,
                &owner,
                &acl_grants,
            )),
            &bucket,
            command_id,
            &config,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate create-bucket command build response",
            ..
        })
    ));

    let other_owner = crate::CanonicalUserId::from_principal("other-owner");
    let bad_config = crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "other-owner",
        owner_canonical_id: &other_owner,
        acl_grants: &acl_grants,
        public_read: false,
        public_write: false,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };
    let bad_command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::CreateBucket(
            CreateBucketCommand::from_config_for_test(&bad_config, 123, 1).unwrap(),
        ),
    );
    let err = client
        .validate_create_bucket_command_build_outcome(
            StorageRpcCreateBucketCommandBuildOutcome::Command(Box::new(bad_command)),
            &bucket,
            command_id,
            &config,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate create-bucket command build response",
            ..
        })
    ));
}

#[test]
fn unix_multipart_completion_barrier_build_response_rejects_mismatched_identity() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("completed-order-rpc-expected");
    let wrong_bucket = crate::tests::bucket_name("completed-order-rpc-wrong");
    let command_id = MetadataCommandId::new(
        ClusterEpoch::new(1).unwrap(),
        PgId::new(0),
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
            AdvanceMultipartCompletionBarrierCommand {
                bucket: wrong_bucket,
                barrier_sequence: 3,
            },
        ),
    );

    let err = client
        .validate_multipart_completion_barrier_command_build_response(
            3, command, &bucket, command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate multipart completion barrier command build response",
            ..
        })
    ));

    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
            AdvanceMultipartCompletionBarrierCommand {
                bucket: bucket.clone(),
                barrier_sequence: 0,
            },
        ),
    );
    let err = client
        .validate_multipart_completion_barrier_command_build_response(
            0, command, &bucket, command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate multipart completion barrier command build response",
            ..
        })
    ));

    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
            AdvanceMultipartCompletionBarrierCommand {
                bucket: bucket.clone(),
                barrier_sequence: 4,
            },
        ),
    );
    let err = client
        .validate_multipart_completion_barrier_command_build_response(
            3, command, &bucket, command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate multipart completion barrier command build response",
            ..
        })
    ));
}

#[test]
fn unix_mark_bucket_deleting_build_response_rejects_mismatched_identity() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("mark-deleting-rpc-expected");
    let wrong_bucket = crate::tests::bucket_name("mark-deleting-rpc-wrong");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let command_id = MetadataCommandId::new(
        ClusterEpoch::new(1).unwrap(),
        PgId::new(0),
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    let wrong_bucket_record = BucketRecord::from_create_config(
        &crate::CreateBucketConfig {
            name: wrong_bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        },
        123,
        1,
    )
    .unwrap();
    let wrong_bucket_command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            wrong_bucket_record,
        )),
    );

    let err = client
        .validate_mark_bucket_deleting_command_build_response(
            wrong_bucket_command,
            &bucket,
            command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate bucket mark-deleting command build response",
            ..
        })
    ));

    let active_bucket_record = BucketRecord::from_create_config(
        &crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        },
        123,
        1,
    )
    .unwrap();
    let active_state_command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand {
            bucket: active_bucket_record,
        }),
    );
    let err = client
        .validate_mark_bucket_deleting_command_build_response(
            active_state_command,
            &bucket,
            command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate bucket mark-deleting command build response",
            ..
        })
    ));

    let create_bucket_config = crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner,
        acl_grants: &acl_grants,
        public_read: false,
        public_write: false,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };
    let wrong_payload_command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::CreateBucket(
            CreateBucketCommand::from_config_for_test(&create_bucket_config, 123, 1).unwrap(),
        ),
    );

    let err = client
        .validate_mark_bucket_deleting_command_build_response(
            wrong_payload_command,
            &bucket,
            command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate bucket mark-deleting command build response",
            ..
        })
    ));

    let mut active_info = test_bucket_info(bucket.clone(), &owner, &acl_grants);
    active_info.state = BucketState::Active;
    let err = client
        .validate_mark_bucket_deleting_already_deleting_response(&active_info, &bucket)
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate bucket mark-deleting command build response",
            ..
        })
    ));

    let mut deleting_info = test_bucket_info(wrong_bucket, &owner, &acl_grants);
    deleting_info.state = BucketState::Deleting;
    let err = client
        .validate_mark_bucket_deleting_already_deleting_response(&deleting_info, &bucket)
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate bucket mark-deleting command build response",
            ..
        })
    ));
}

#[test]
fn unix_object_read_snapshot_response_rejects_missing_multipart_parts() {
    let bucket = crate::tests::bucket_name("object-read-missing-part-rpc");
    let key = crate::tests::object_key("object-read-missing-part-rpc-key");
    let stored = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        ObjectLayout::MultipartManifest {
            parts_count: std::num::NonZeroU32::new(2).unwrap(),
        },
    );
    let snapshot = ObjectReadSnapshot::from_records(
        stored,
        Vec::new(),
        vec![test_object_read_multipart_part(&bucket, &key, 1)],
        Vec::new(),
    )
    .unwrap();

    assert_object_read_snapshot_rejected(&snapshot, ObjectReadSnapshotMode::MultipartParts);
}

#[test]
fn object_read_snapshot_construction_rejects_crossed_segment_subject() {
    let bucket = crate::tests::bucket_name("object-read-crossed-segment");
    let key = crate::tests::object_key("object-read-crossed-segment-key");
    let stored = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        test_multipart_layout(),
    );
    let mut segment = test_object_read_multipart_segment(&bucket, &key, 1);
    segment.key = crate::tests::object_key("other-key");

    let error = ObjectReadSnapshot::from_records(
        stored,
        Vec::new(),
        vec![test_object_read_multipart_part(&bucket, &key, 1)],
        vec![segment],
    )
    .unwrap_err();

    assert_eq!(error, "multipart segment does not match snapshot subject");
}

#[test]
fn unix_object_read_snapshot_response_rejects_orphan_multipart_segments() {
    let bucket = crate::tests::bucket_name("object-read-orphan-seg-rpc");
    let key = crate::tests::object_key("object-read-orphan-seg-rpc-key");
    let stored = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        test_multipart_layout(),
    );
    let snapshot = ObjectReadSnapshot::from_records(
        stored,
        Vec::new(),
        vec![test_object_read_multipart_part(&bucket, &key, 1)],
        vec![test_object_read_multipart_segment(&bucket, &key, 2)],
    )
    .unwrap();

    assert_object_read_snapshot_rejected(&snapshot, ObjectReadSnapshotMode::FullPayloadLayout);
}

#[test]
fn unix_object_read_snapshot_response_accepts_zero_byte_multipart_part_without_segments() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("object-read-zero-part-rpc");
    let key = crate::tests::object_key("object-read-zero-part-rpc-key");
    let stored = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        ObjectLayout::MultipartManifest {
            parts_count: std::num::NonZeroU32::new(2).unwrap(),
        },
    );
    let mut zero_part = test_object_read_multipart_part(&bucket, &key, 2);
    zero_part.size = 0;
    let snapshot = ObjectReadSnapshot::from_records(
        stored,
        Vec::new(),
        vec![test_object_read_multipart_part(&bucket, &key, 1), zero_part],
        vec![test_object_read_multipart_segment(&bucket, &key, 1)],
    )
    .unwrap();
    let identity = ObjectReadAuthSubjectIdentity::for_stored(&snapshot.stored);

    client
        .validate_object_read_snapshot_response(
            &snapshot,
            &bucket,
            &key,
            Some(VersionId::Null),
            &identity,
            ObjectReadSnapshotMode::FullPayloadLayout,
        )
        .unwrap();
}

#[test]
fn unix_direct_put_snapshot_response_rejects_mismatched_auth_etag() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-validate");
    let key = crate::tests::object_key("direct-put-snapshot-validate-key");
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: Some("unexpected-etag".to_string()),
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: None,
        stale_payload: None,
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_mismatched_stale_payload_identity() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-stale");
    let key = crate::tests::object_key("direct-put-snapshot-stale-key");
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: None,
        stale_payload: Some(ObjectPayloadReclaimCommand::Segments(
            ObjectSegmentsReclaimRecord {
                bucket: crate::tests::bucket_name("wrong-direct-put-snapshot-stale"),
                key: key.clone(),
                generation_id: GenerationId::new(9).unwrap(),
                created_at: 0,
                segments: Vec::new(),
            },
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_stale_payload_without_source() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-stale-shape");
    let key = crate::tests::object_key("direct-put-snapshot-stale-shape-key");
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: None,
        stale_payload: Some(ObjectPayloadReclaimCommand::Segments(
            ObjectSegmentsReclaimRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                generation_id: GenerationId::new(9).unwrap(),
                created_at: 0,
                segments: Vec::new(),
            },
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_stale_source_without_payload() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-source-only");
    let key = crate::tests::object_key("direct-put-snapshot-source-only-key");
    let generation_id = GenerationId::new(9).unwrap();
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(test_live_stored_object(
            bucket.clone(),
            key.clone(),
            generation_id,
            ObjectLayout::Standard,
        )),
        stale_payload: None,
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_stale_payload_delete_marker_source() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-delete-marker-source");
    let key = crate::tests::object_key("direct-put-snapshot-delete-marker-source-key");
    let generation_id = GenerationId::new(9).unwrap();
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(test_delete_marker_stored_object(
            bucket.clone(),
            key.clone(),
        )),
        stale_payload: Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            generation_id,
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_stale_payload_generation_mismatch() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-generation");
    let key = crate::tests::object_key("direct-put-snapshot-generation-key");
    let source_generation = GenerationId::new(9).unwrap();
    let reclaim_generation = GenerationId::new(10).unwrap();
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(test_live_stored_object(
            bucket.clone(),
            key.clone(),
            source_generation,
            ObjectLayout::Standard,
        )),
        stale_payload: Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            reclaim_generation,
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_numbered_stale_source() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-numbered-source");
    let key = crate::tests::object_key("direct-put-snapshot-numbered-source-key");
    let generation_id = GenerationId::new(9).unwrap();
    let mut source = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        generation_id,
        ObjectLayout::Standard,
    );
    let StoredObject::Live(source_record) = &mut source else {
        unreachable!("test helper always builds a live object");
    };
    source_record.version_id = VersionId::from_u64(2);
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(source),
        stale_payload: Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            generation_id,
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_stale_payload_layout_mismatch() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-layout");
    let key = crate::tests::object_key("direct-put-snapshot-layout-key");
    let generation_id = GenerationId::new(9).unwrap();
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(test_live_stored_object(
            bucket.clone(),
            key.clone(),
            generation_id,
            test_multipart_layout(),
        )),
        stale_payload: Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            generation_id,
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_accepts_stale_null_source_under_numbered_current() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-null-source");
    let key = crate::tests::object_key("direct-put-snapshot-null-source-key");
    let current_generation = GenerationId::new(10).unwrap();
    let stale_generation = GenerationId::new(9).unwrap();
    let mut current = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        current_generation,
        ObjectLayout::Standard,
    );
    let StoredObject::Live(current_record) = &mut current else {
        unreachable!("test helper always builds a live object");
    };
    current_record.version_id = VersionId::from_u64(2);
    let expected_etag = current_record.etag.format();
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: Some(expected_etag),
        },
        current: Some(current),
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(test_live_stored_object(
            bucket.clone(),
            key.clone(),
            stale_generation,
            ObjectLayout::Standard,
        )),
        stale_payload: Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            stale_generation,
        )),
    };

    assert_direct_put_snapshot_accepted(&snapshot, &bucket, &key);
}

#[test]
fn unix_delete_specific_command_response_rejects_wrong_reclaim_target() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("delete-specific-target-rpc");
    let key = crate::tests::object_key("delete-specific-target-rpc-key");
    let generation_id = GenerationId::new(9).unwrap();
    let stored = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        generation_id,
        ObjectLayout::Standard,
    );
    let expected_target = DeleteObjectVersionTarget::Live {
        generation_id,
        layout: ObjectLayout::Standard,
        payload: test_segments_reclaim(bucket.clone(), key.clone(), generation_id),
    };
    let bad_generation_id = GenerationId::new(10).unwrap();
    let bad_target = DeleteObjectVersionTarget::Live {
        generation_id: bad_generation_id,
        layout: ObjectLayout::Standard,
        payload: test_segments_reclaim(bucket.clone(), key.clone(), bad_generation_id),
    };
    let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
            bucket_write_reservation: proof.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            mode: crate::metadata_command::DeleteObjectVersionMode::Specific,
            target: bad_target,
        })),
    );

    let err = client
        .validate_delete_specific_object_command_response(
            &command,
            ClusterEpoch::new(1).unwrap(),
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            &BuildDeleteSpecificObjectVersionCommandReq {
                version_id: VersionId::Null,
                expected_stored: Some(&stored),
                expected_target: Some(&expected_target),
                expected_version_list: None,
                bucket_write_reservation: &proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate delete-specific object command build response",
            ..
        })
    ));
}

#[test]
fn unix_insert_delete_marker_response_rejects_wrong_snapshot_stale_payload() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("insert-marker-stale-rpc");
    let key = crate::tests::object_key("insert-marker-stale-rpc-key");
    let generation_id = GenerationId::new(9).unwrap();
    let stored = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        generation_id,
        ObjectLayout::Standard,
    );
    let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal("owner"),
            write_sequence: 1,
            last_modified_millis: 1,
            stale_payload: Some(test_segments_reclaim(
                crate::tests::bucket_name("wrong-insert-marker-stale-rpc"),
                key.clone(),
                generation_id,
            )),
        }),
    );
    let owner = OwnerIdentity::from_principal("owner");

    let err = client
        .validate_insert_delete_marker_command_response(
            &command,
            ClusterEpoch::new(1).unwrap(),
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            &BuildInsertDeleteMarkerCommandReq {
                expected_current: Some(&stored),
                version_id: VersionId::Null,
                owner: &owner,
                stale_payload: InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                    created_at: 1,
                },
                expected_stale_payload_source: Some(&stored),
                bucket_write_reservation: &proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate insert-delete-marker command build response",
            ..
        })
    ));
}

#[test]
fn unix_proof_release_response_rejects_non_empty_payload() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );

    client.validate_proof_release_response(&[]).unwrap();
    let err = client
        .validate_proof_release_response(b"unexpected")
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "decode proof release response",
            ..
        })
    ));
}

#[test]
fn unix_bucket_write_reservation_client_acquires_validates_and_releases() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-write-reservation-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..5)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let route = bucket_write_reservation_route(
        &client,
        ClusterEpoch::new(1).unwrap(),
        bucket_pg_id_for_test(0),
        &bucket,
    );

    let lease_deadline = crate::clock::current_time_millis().saturating_add(60_000);
    let record = route
        .acquire_durable_bucket_write_reservation(
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-remote-1",
                owner_token: "owner-token-remote-1",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline,
                target_context: Some("key=a"),
            },
        )
        .unwrap();
    assert_eq!(record.bucket, bucket);
    assert_eq!(record.reservation_id, "reservation-remote-1");

    route
        .validate_bucket_write_reservation_proof(&BucketWriteReservationProof::from(&record))
        .unwrap();
    let mut conflicting_proof = BucketWriteReservationProof::from(&record);
    conflicting_proof.owner_token = "wrong-owner-token".to_string();
    let conflict = route
        .validate_bucket_write_reservation_proof(&conflicting_proof)
        .unwrap_err();
    assert!(matches!(
        conflict,
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationConflict {
            reservation_id
        }) if reservation_id == record.reservation_id
    ));
    let renewed_deadline = lease_deadline.saturating_add(60_000);
    let renewed = route
        .heartbeat_durable_bucket_write_reservation_with_effect_fence(
            &BucketWriteReservationProof::from(&record),
            renewed_deadline,
            crate::types::AdmittedRouteEffectFence::bounded(
                ClusterEpoch::new(1).unwrap(),
                crate::clock::current_time_millis().saturating_add(120_000),
                crate::clock::monotonic_time_millis().saturating_add(119_000),
            ),
        )
        .unwrap();
    assert_eq!(renewed.lease_deadline, renewed_deadline);
    retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .release_durable_bucket_write_reservation(&renewed)
        .unwrap();
    for thread in server_threads {
        thread.join().unwrap();
    }

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(PgMetadataStore::durable_bucket_write_reservation(
        &*pg,
        &bucket,
        "reservation-remote-1",
    )
    .unwrap()
    .is_none());
}

#[test]
fn unix_bucket_write_reservation_identity_uses_current_route_after_epoch_change() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    let acquire_epoch = config.cluster_epoch;
    let route_epoch = ClusterEpoch::new(acquire_epoch.get() + 1).unwrap();
    config.cluster_epoch = route_epoch;
    config.pg_routes[0].cluster_epoch = route_epoch;
    let bucket = crate::tests::bucket_name("bucket-write-reservation-old-epoch-release-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let lease_deadline = crate::clock::current_time_millis().saturating_add(60_000);
    let record = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        let record = PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-old-epoch-release",
                owner_token: "owner-token-old-epoch-release",
                cluster_epoch: acquire_epoch,
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline,
                target_context: Some("key=a"),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        record
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..3)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client =
        UnixStorageNodeClient::new(config.node_id, route_epoch, config.socket_path.clone())
            .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let route =
        bucket_write_reservation_route(&client, route_epoch, bucket_pg_id_for_test(0), &bucket);

    let proof = BucketWriteReservationProof::from(&record);
    route
        .validate_bucket_write_reservation_proof(&proof)
        .unwrap();
    let renewed = route
        .heartbeat_durable_bucket_write_reservation_with_effect_fence(
            &proof,
            lease_deadline.saturating_add(60_000),
            crate::types::AdmittedRouteEffectFence::bounded(
                route_epoch,
                crate::clock::current_time_millis().saturating_add(120_000),
                crate::clock::monotonic_time_millis().saturating_add(119_000),
            ),
        )
        .unwrap();
    retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .release_durable_bucket_write_reservation(&renewed)
        .unwrap();
    for thread in server_threads {
        thread.join().unwrap();
    }

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(PgMetadataStore::durable_bucket_write_reservation(
        &*pg,
        &bucket,
        &record.reservation_id,
    )
    .unwrap()
    .is_none());
}

#[test]
fn unix_bucket_write_reservation_client_preserves_draining_signal() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-write-reservation-draining-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::begin_durable_bucket_write_drain(
            &*pg,
            &bucket,
            "drain-remote-1",
            "drain-owner-remote-1",
            ClusterEpoch::new(1).unwrap(),
            10,
            20,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let route = bucket_write_reservation_route(
        &client,
        ClusterEpoch::new(1).unwrap(),
        bucket_pg_id_for_test(0),
        &bucket,
    );

    let err = route
        .acquire_durable_bucket_write_reservation(
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-remote-1",
                owner_token: "owner-token-remote-1",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 30,
                lease_deadline: 40,
                target_context: Some("key=a"),
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)
    ));
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_write_reservation_client_preserves_bucket_not_found() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-write-reservation-missing-rpc");
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let route = bucket_write_reservation_route(
        &client,
        ClusterEpoch::new(1).unwrap(),
        bucket_pg_id_for_test(0),
        &bucket,
    );

    let err = route
        .acquire_durable_bucket_write_reservation(
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-remote-1",
                owner_token: "owner-token-remote-1",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 30,
                lease_deadline: 40,
                target_context: Some("key=a"),
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name })
            if name == bucket
    ));
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_write_reservation_client_routes_drain_and_finalize_coordination() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-drain-finalize-rpc");
    let finalize_bucket = crate::tests::bucket_name("bucket-finalize-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let finalize_bucket_incarnation_generation = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-for-drain-list",
                owner_token: "reservation-owner-for-drain-list",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key=a"),
            },
        )
        .unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &finalize_bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::mark_bucket_deleting(&*pg, &finalize_bucket).unwrap();
        let generation = PgMetadataStore::head_bucket_raw(&*pg, &finalize_bucket)
            .unwrap()
            .bucket_incarnation_generation;
        pg.refresh_metadata_command_state_digest().unwrap();
        generation
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..22)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let route = bucket_write_reservation_route(
        &client,
        ClusterEpoch::new(1).unwrap(),
        bucket_pg_id_for_test(0),
        &bucket,
    );
    let scan_route = bucket_write_reservation_scan_route(
        &client,
        ClusterEpoch::new(1).unwrap(),
        bucket_pg_id_for_test(0),
    );

    let reservations = route.durable_bucket_write_reservations().unwrap();
    assert_eq!(reservations.len(), 1);
    assert_eq!(reservations[0].reservation_id, "reservation-for-drain-list");

    let drain = route
        .begin_durable_bucket_write_drain("drain-rpc-1", "drain-owner-rpc-1", 30, 40)
        .unwrap();
    assert_eq!(drain.bucket, bucket);
    assert_eq!(drain.drain_id, "drain-rpc-1");
    assert!(route.durable_bucket_write_drain_exists().unwrap());
    assert_eq!(
        route
            .durable_bucket_write_drain()
            .unwrap()
            .as_ref()
            .map(|record| record.drain_id.as_str()),
        Some("drain-rpc-1")
    );
    retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .clear_durable_bucket_write_drain(&drain)
        .unwrap();
    assert!(!route.durable_bucket_write_drain_exists().unwrap());
    assert!(route.durable_bucket_write_drain().unwrap().is_none());

    route
        .begin_durable_bucket_write_drain(
            "expired-drain-rpc-1",
            "expired-drain-owner-rpc-1",
            50,
            55,
        )
        .unwrap();
    let expired = route
        .clear_expired_durable_bucket_write_drain(60)
        .unwrap()
        .expect("expired drain should clear");
    assert_eq!(expired.drain_id, "expired-drain-rpc-1");

    let live_begin_drain = route
        .begin_durable_bucket_write_drain(
            "active-begin-drain-rpc-1",
            "active-begin-drain-owner-rpc-1",
            61,
            120,
        )
        .unwrap();
    let begin_roots = scan_route
        .get_bucket_delete_begin_roots(90, None, 16)
        .unwrap();
    assert!(begin_roots.is_empty());
    retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .clear_durable_bucket_write_drain(&live_begin_drain)
        .unwrap();

    let expired_begin_drain = route
        .begin_durable_bucket_write_drain(
            "expired-begin-drain-rpc-1",
            "expired-begin-drain-owner-rpc-1",
            61,
            80,
        )
        .unwrap();
    let begin_roots = scan_route
        .get_bucket_delete_begin_roots(90, None, 16)
        .unwrap();
    assert_eq!(begin_roots.len(), 1);
    assert_eq!(begin_roots[0].bucket, bucket);
    retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .clear_durable_bucket_write_drain(&expired_begin_drain)
        .unwrap();

    let finalize_exact_route = bucket_write_reservation_route(
        &client,
        ClusterEpoch::new(1).unwrap(),
        bucket_pg_id_for_test(0),
        &finalize_bucket,
    );
    let claim = finalize_exact_route
        .acquire_bucket_delete_finalize_claim(
            finalize_bucket_incarnation_generation,
            "finalize-claim-rpc-1",
            "finalize-claim-owner-rpc-1",
            70,
            Some(80),
            70,
        )
        .unwrap()
        .expect("finalize claim should acquire");
    assert_eq!(claim.bucket, finalize_bucket);
    assert_eq!(claim.claim_id, "finalize-claim-rpc-1");
    let replacement_claim = finalize_exact_route
        .acquire_bucket_delete_finalize_claim(
            finalize_bucket_incarnation_generation,
            "finalize-claim-rpc-2",
            "finalize-claim-owner-rpc-2",
            81,
            Some(100),
            81,
        )
        .unwrap()
        .expect("expired finalizer claim should be stealable");
    let observed_claim = finalize_exact_route
        .bucket_delete_finalize_claim()
        .unwrap()
        .expect("finalize claim read should return current claim");
    assert_eq!(observed_claim.bucket, finalize_bucket);
    assert_eq!(observed_claim.claim_id, replacement_claim.claim_id);
    assert_eq!(
        observed_claim.bucket_incarnation_generation,
        finalize_bucket_incarnation_generation
    );
    let finalize_route =
        retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &finalize_bucket);
    let stale_release = finalize_route
        .release_bucket_delete_finalize_claim(&claim)
        .unwrap_err();
    assert!(matches!(
        stale_release,
        BucketSnapshotLoadError::Metadata(MetadataError::ReclaimClaimConflict { .. })
    ));
    finalize_route
        .release_bucket_delete_finalize_claim(&replacement_claim)
        .unwrap();
    assert!(finalize_exact_route
        .bucket_delete_finalize_claim()
        .unwrap()
        .is_none());

    let roots = scan_route.get_bucket_delete_finalize_roots(90, 16).unwrap();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].bucket, finalize_bucket);

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_write_reservation_client_routes_lifecycle_sweep_coordination() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-lifecycle-sweep-rpc");
    let aborting_key = crate::tests::object_key("aborting-upload");
    let aborting_upload_id = crate::tests::multipart_upload_id("lifecycle-aborting-upload");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let bucket_incarnation_generation = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::put_bucket_subresource(
            &*pg,
            &bucket,
            crate::types::PutBucketSubresource {
                kind: BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::types::BucketSubresourceAux::None,
            },
        )
        .unwrap();
        PgMetadataStore::create_multipart_upload(
            &*pg,
            &CreateMultipartUploadReq {
                upload_id: aborting_upload_id.clone(),
                bucket: bucket.clone(),
                key: aborting_key.clone(),
                tags: None,
                metadata_blob: SerializedMetadataBlob::default(),
                system_metadata_blob: SerializedSystemMetadataBlob::default(),
                initiator: OwnerIdentity::from_principal("owner"),
                owner: OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                object_lock: ObjectLockState::default(),
                checksum: None,
                encryption: ObjectEncryption::None,
            },
        )
        .unwrap();
        PgMetadataStore::set_upload_state(&*pg, &aborting_upload_id, UploadState::Aborting)
            .unwrap();
        let generation = PgMetadataStore::head_bucket_raw(&*pg, &bucket)
            .unwrap()
            .bucket_incarnation_generation;
        pg.refresh_metadata_command_state_digest().unwrap();
        generation
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    // One connection for each lifecycle RPC below: bucket list, aborting-upload
    // list, roots, acquire, heartbeat, record-error, and release.
    let server_threads: Vec<_> = (0..7)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let route = bucket_write_reservation_route(
        &client,
        ClusterEpoch::new(1).unwrap(),
        bucket_pg_id_for_test(0),
        &bucket,
    );
    let scan_route = bucket_write_reservation_scan_route(
        &client,
        ClusterEpoch::new(1).unwrap(),
        bucket_pg_id_for_test(0),
    );

    let buckets = scan_route.list_buckets_with_lifecycle().unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].name, bucket);

    let object_route = client
        .open_object_mutation_scan_metadata_route(
            config.cluster_epoch,
            ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
        )
        .unwrap();
    let witnesses = object_route
        .list_aborting_multipart_upload_bucket_witnesses()
        .unwrap();
    assert_eq!(
        witnesses,
        vec![AbortingMultipartUploadBucketWitness {
            bucket: bucket.clone(),
            key: aborting_key,
        }]
    );

    let roots = scan_route.get_lifecycle_sweep_roots(10, 16).unwrap();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].bucket, bucket);
    assert_eq!(
        roots[0].source,
        crate::types::LifecycleSweepRootSource::LifecycleConfig
    );

    let claim = route
        .acquire_lifecycle_sweep_claim(
            bucket_incarnation_generation,
            "lifecycle-claim-rpc-1",
            "lifecycle-owner-rpc-1",
            20,
            Some(40),
            20,
        )
        .unwrap()
        .expect("lifecycle claim should acquire");
    assert_eq!(claim.bucket, bucket);
    assert_eq!(claim.claim_id, "lifecycle-claim-rpc-1");

    let heartbeat = route
        .heartbeat_lifecycle_sweep_claim(&claim, 30, Some(50))
        .unwrap();
    assert_eq!(heartbeat.heartbeat_at, 30);
    assert_eq!(heartbeat.lease_deadline, Some(50));

    let error_record = route
        .record_lifecycle_sweep_claim_error(&heartbeat, "transient lifecycle error")
        .unwrap();
    assert_eq!(
        error_record.last_error.as_deref(),
        Some("transient lifecycle error")
    );

    retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .release_lifecycle_sweep_claim(&error_record)
        .unwrap();

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_metadata_client_loads_bucket_snapshot() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-snapshot-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::put_bucket_subresource(
            &*pg,
            &bucket,
            crate::types::PutBucketSubresource {
                kind: BucketSubresourceKind::Policy,
                body: "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
                aux: crate::types::BucketSubresourceAux::policy(false),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let route_epoch = ClusterEpoch::new(1).unwrap();
    let client =
        UnixStorageNodeClient::new(NodeId::new(7), route_epoch, config.socket_path.clone())
            .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));

    let request = BucketSnapshotRequest {
        policy: true,
        tags: BucketSnapshotTagsRequest::Always,
        lifecycle: false,
        cors: true,
    };
    let route = bucket_metadata_route(
        &client,
        route_epoch,
        BucketPgId::new_for_test(PgId::new(0)),
        &bucket,
    );
    let snapshot = route.load_bucket_snapshot(request).unwrap();
    assert_eq!(snapshot.bucket.name, bucket);
    assert_eq!(snapshot.request, request);
    assert_eq!(
        snapshot.policy,
        crate::types::LoadedBucketSubresource::Loaded(
            "{\"Version\":\"2012-10-17\",\"Statement\":[]}".to_string()
        )
    );
    assert_eq!(
        snapshot.tags,
        crate::types::LoadedBucketSubresource::Missing
    );
    assert_eq!(
        snapshot.lifecycle,
        crate::types::LoadedBucketSubresource::NotRequested
    );
    assert_eq!(
        snapshot.cors,
        crate::types::LoadedBucketSubresource::Missing
    );
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_metadata_client_builds_multipart_completion_barrier_command() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("completed-order-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let bucket_write_reservation;
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        let reservation = PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "completed-order-reservation",
                owner_token: "completed-order-owner",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                created_at: 1_000,
                lease_deadline: crate::clock::current_time_millis() + 60_000,
                target_context: Some("object-key"),
            },
        )
        .unwrap();
        bucket_write_reservation = BucketWriteReservationProof::from(&reservation);
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let route_epoch = ClusterEpoch::new(1).unwrap();
    let client =
        UnixStorageNodeClient::new(NodeId::new(7), route_epoch, config.socket_path.clone())
            .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let command_id = MetadataCommandId::new(
        ClusterEpoch::new(1).unwrap(),
        PgId::new(0),
        MetadataCommandLogIndex::new(1).unwrap(),
    );

    let route = bucket_metadata_route(
        &client,
        route_epoch,
        BucketPgId::new_for_test(PgId::new(0)),
        &bucket,
    );
    let (barrier_sequence, command) = route
        .build_advance_multipart_completion_barrier_command(
            command_id,
            "object-key",
            &bucket_write_reservation,
        )
        .unwrap();

    assert_eq!(barrier_sequence, 1);
    assert_eq!(command.id(), command_id);
    match command.payload() {
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(advance) => {
            assert_eq!(advance.bucket, bucket);
            assert_eq!(advance.barrier_sequence, barrier_sequence);
        }
        other => panic!("unexpected command payload: {other:?}"),
    }
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_metadata_client_routes_bucket_control_operations() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-control-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::put_bucket_subresource(
            &*pg,
            &bucket,
            crate::types::PutBucketSubresource {
                kind: BucketSubresourceKind::Policy,
                body: "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
                aux: crate::types::BucketSubresourceAux::policy(false),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..6)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let route_epoch = ClusterEpoch::new(1).unwrap();
    let client =
        UnixStorageNodeClient::new(NodeId::new(7), route_epoch, config.socket_path.clone())
            .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let command_id = |log_index| {
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(log_index).unwrap(),
        )
    };

    let route = bucket_metadata_route(
        &client,
        route_epoch,
        BucketPgId::new_for_test(PgId::new(0)),
        &bucket,
    );
    let versioning = route
        .build_put_bucket_versioning_command(command_id(1), BucketVersioningState::Enabled)
        .unwrap();
    let MetadataCommandPayload::PutBucketVersioning(versioning_command) = versioning.payload()
    else {
        panic!("unexpected versioning command payload");
    };
    assert!(route
        .pending_put_bucket_versioning_command_matches_current(
            versioning_command,
            BucketVersioningState::Enabled,
        )
        .unwrap());

    let acl = route
        .build_put_bucket_acl_command(
            command_id(2),
            &crate::AclGrants::default(),
            crate::BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap();
    match acl.payload() {
        MetadataCommandPayload::PutBucketAcl(command) => {
            assert_eq!(command.bucket.name, bucket);
            assert!(command.bucket.public_read);
            assert!(!command.bucket.public_write);
        }
        other => panic!("unexpected ACL command payload: {other:?}"),
    }

    let property = route
        .build_put_bucket_property_command(
            command_id(3),
            &BucketPropertyMutation::AbacEnabled(true),
        )
        .unwrap();
    match property.payload() {
        MetadataCommandPayload::PutBucketProperty(command) => {
            assert_eq!(command.bucket.name, bucket);
            assert!(command.bucket.bucket_abac_enabled);
        }
        other => panic!("unexpected property command payload: {other:?}"),
    }

    let subresource =
        BucketSubresourceMutation::PutLifecycle("<LifecycleConfiguration/>".to_string());
    let subresource_command = route
        .build_put_bucket_subresource_command(command_id(4), &subresource)
        .unwrap();
    match subresource_command.payload() {
        MetadataCommandPayload::PutBucketSubresource(command) => {
            assert!(command.matches_mutation(&bucket, &subresource));
        }
        other => panic!("unexpected subresource command payload: {other:?}"),
    }

    let policy = route
        .get_bucket_subresource(BucketSubresourceKind::Policy)
        .unwrap();
    assert_eq!(
        policy.as_deref(),
        Some("{\"Version\":\"2012-10-17\",\"Statement\":[]}")
    );

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_property_pending_match_binds_same_generation_to_requested_mutation() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-property-pending-match-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..4)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let route_epoch = ClusterEpoch::new(1).unwrap();
    let client =
        UnixStorageNodeClient::new(NodeId::new(7), route_epoch, config.socket_path.clone())
            .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let route = bucket_metadata_route(
        &client,
        route_epoch,
        BucketPgId::new_for_test(PgId::new(0)),
        &bucket,
    );
    let config = crate::PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: true,
        block_public_policy: false,
        restrict_public_buckets: true,
    };
    let put = BucketPropertyMutation::PublicAccessBlock(Some(config));
    let delete = BucketPropertyMutation::PublicAccessBlock(None);
    let command = route
        .build_put_bucket_property_command(
            MetadataCommandId::new(
                route_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            &put,
        )
        .unwrap();
    MetadataCommandNodeClient::apply_metadata_command_and_record(&client, PgId::new(0), &command)
        .unwrap();
    let MetadataCommandPayload::PutBucketProperty(property) = command.payload() else {
        panic!("unexpected property command payload")
    };
    assert!(route
        .pending_put_bucket_property_command_matches_current(property, &put)
        .unwrap());
    assert!(
        !route
            .pending_put_bucket_property_command_matches_current(property, &delete)
            .unwrap(),
        "an applied PUT command must not satisfy a same-effect DELETE request"
    );

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_metadata_client_releases_bucket_write_proof_after_route_expiry() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("proof-release-rpc-bucket");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let reservation = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        let reservation = PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-1",
                owner_token: "owner-token-1",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key=a"),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        reservation
    };
    config.route_map_validity = crate::RouteMapValidity::until_ms(1).unwrap();
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .release_metadata_command_bucket_write_reservation(&BucketWriteReservationProof::from(
            &reservation,
        ))
        .unwrap();
    server_thread.join().unwrap();

    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .release_metadata_command_bucket_write_reservation(&BucketWriteReservationProof::from(
            &reservation,
        ))
        .unwrap();
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(
        PgMetadataStore::durable_bucket_write_reservation(&*pg, &bucket, "reservation-1")
            .unwrap()
            .is_none()
    );
}

#[test]
fn unix_bucket_write_reservation_client_clears_exact_drain_after_route_expiry() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("drain-clear-expired-route-rpc-bucket");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let drain = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        let drain = PgMetadataStore::begin_durable_bucket_write_drain(
            &*pg,
            &bucket,
            "expired-route-drain",
            "expired-route-drain-owner",
            config.cluster_epoch,
            10,
            20,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        drain
    };
    config.route_map_validity = crate::RouteMapValidity::until_ms(1).unwrap();
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .clear_durable_bucket_write_drain(&drain)
        .unwrap();
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(
        PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .is_none(),
        "retained drain clear must remove the exact drain after active route expiry"
    );
}

#[test]
fn unix_bucket_write_reservation_client_releases_finalizer_claim_after_route_expiry() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("finalizer-claim-release-expired-route-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let claim = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::mark_bucket_deleting(&*pg, &bucket).unwrap();
        let generation = PgMetadataStore::head_bucket_raw(&*pg, &bucket)
            .unwrap()
            .bucket_incarnation_generation;
        let claim = PgMetadataStore::acquire_bucket_delete_finalize_claim(
            &*pg,
            &bucket,
            generation,
            "expired-route-finalizer-claim",
            "expired-route-finalizer-owner",
            config.cluster_epoch,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("finalizer claim should be acquired");
        pg.refresh_metadata_command_state_digest().unwrap();
        claim
    };
    config.route_map_validity = crate::RouteMapValidity::until_ms(1).unwrap();
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .release_bucket_delete_finalize_claim(&claim)
        .unwrap();
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(
        PgMetadataStore::bucket_delete_finalize_claim(&*pg)
            .unwrap()
            .is_none(),
        "retained finalizer-claim release must work after active route expiry"
    );
}

#[test]
fn unix_bucket_write_reservation_client_releases_lifecycle_claim_after_route_expiry() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("lifecycle-claim-release-expired-route-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let (bucket_incarnation_generation, claim) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::put_bucket_subresource(
            &*pg,
            &bucket,
            crate::types::PutBucketSubresource {
                kind: BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::types::BucketSubresourceAux::None,
            },
        )
        .unwrap();
        let generation = PgMetadataStore::head_bucket_raw(&*pg, &bucket)
            .unwrap()
            .bucket_incarnation_generation;
        let claim = PgMetadataStore::acquire_lifecycle_sweep_claim(
            &*pg,
            &bucket,
            generation,
            "expired-route-lifecycle-claim",
            "expired-route-lifecycle-owner",
            config.cluster_epoch,
            10,
            None,
            10,
        )
        .unwrap()
        .expect("lifecycle claim should be acquired");
        pg.refresh_metadata_command_state_digest().unwrap();
        (generation, claim)
    };
    config.route_map_validity = crate::RouteMapValidity::until_ms(1).unwrap();
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .release_lifecycle_sweep_claim(&claim)
        .unwrap();
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    let replacement = PgMetadataStore::acquire_lifecycle_sweep_claim(
        &*pg,
        &bucket,
        bucket_incarnation_generation,
        "post-release-lifecycle-claim",
        "post-release-lifecycle-owner",
        config.cluster_epoch,
        20,
        None,
        20,
    )
    .unwrap()
    .expect("retained lifecycle-claim release must work after active route expiry");
    PgMetadataStore::release_lifecycle_sweep_claim(
        &*pg,
        &replacement.bucket,
        replacement.bucket_incarnation_generation,
        &replacement.claim_id,
        &replacement.owner_token,
        replacement.cluster_epoch,
    )
    .unwrap();
}

#[test]
fn unix_bucket_metadata_client_preserves_proof_release_conflict() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("proof-release-conflict-bucket");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let reservation = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        let reservation = PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-1",
                owner_token: "owner-token-1",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key=a"),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        reservation
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let mut proof = BucketWriteReservationProof::from(&reservation);
    proof.owner_token = "wrong-owner-token".to_string();
    let err = retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .release_metadata_command_bucket_write_reservation(&proof)
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationConflict {
            reservation_id
        }) if reservation_id == "reservation-1"
    ));
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(
        PgMetadataStore::durable_bucket_write_reservation(&*pg, &bucket, "reservation-1")
            .unwrap()
            .is_some()
    );
}

#[test]
fn unix_bucket_clients_reject_wrong_bucket_pg_before_bucket_access() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes = vec![
        StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![NodeId::new(7)],
        },
        StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![NodeId::new(7)],
        },
    ];
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let (bucket, correct_pg_id, wrong_pg_id, wrong_pg_release_record) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let (bucket, correct_pg_id) = (0..100)
            .map(|index| crate::tests::bucket_name(format!("bucket-rpc-wrong-pg-{index}")))
            .map(|bucket| {
                let pg_id = node.pg_topology().bucket_pg_for(&bucket);
                (bucket, pg_id)
            })
            .find(|(_, pg_id)| *pg_id < 2)
            .expect("two-PG topology must place a test bucket");
        for pg_id in [correct_pg_id, if correct_pg_id == 0 { 1 } else { 0 }] {
            let pg = node.get_pg(pg_id).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &acl_grants,
                false,
                false,
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
        let wrong_pg = node.get_pg(wrong_pg_id).unwrap();
        let wrong_pg_release_record = PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*wrong_pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "wrong-pg-release-reservation",
                owner_token: "wrong-pg-release-owner",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key"),
            },
        )
        .unwrap();
        wrong_pg.refresh_metadata_command_state_digest().unwrap();
        (bucket, correct_pg_id, wrong_pg_id, wrong_pg_release_record)
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    // Exact bucket capabilities reject crossed PG subjects before transport.
    // Only the correct snapshot and the retained reservation release below
    // reach the server.
    let server_threads: Vec<_> = (0..2)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let route_epoch = ClusterEpoch::new(1).unwrap();
    let topology = Arc::new(PgTopology::new(&config.pg_ids).unwrap());
    let client =
        UnixStorageNodeClient::new(NodeId::new(7), route_epoch, config.socket_path.clone())
            .with_pg_topology(topology);
    let wrong_bucket_pg = BucketPgId::new_for_test(PgId::new(wrong_pg_id));

    let head_error = client
        .open_bucket_metadata_route(route_epoch, wrong_bucket_pg, &bucket)
        .err()
        .expect("wrong-PG exact route must be rejected");
    assert!(matches!(
        head_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let correct_bucket_pg = BucketPgId::new_for_test(PgId::new(correct_pg_id));
    let correct_snapshot = bucket_metadata_route(&client, route_epoch, correct_bucket_pg, &bucket)
        .load_bucket_snapshot(crate::BucketSnapshotRequest::default())
        .unwrap();
    assert_eq!(correct_snapshot.bucket.name, bucket);

    let snapshot_error = client
        .open_bucket_metadata_route(route_epoch, wrong_bucket_pg, &bucket)
        .err()
        .expect("wrong-PG snapshot route must be rejected");
    assert!(matches!(
        snapshot_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let create_error = client
        .open_bucket_metadata_route(route_epoch, wrong_bucket_pg, &bucket)
        .err()
        .expect("wrong-PG create route must be rejected");
    assert!(matches!(
        create_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let reservation_error = client
        .open_bucket_write_reservation_route(route_epoch, wrong_bucket_pg, &bucket)
        .err()
        .expect("wrong-PG reservation route must be rejected");
    assert!(
        matches!(
            reservation_error,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            })
        ),
        "unexpected wrong-PG reservation error: {reservation_error:?}"
    );

    let release_error =
        retained_bucket_write_route(&client, wrong_bucket_pg, &wrong_pg_release_record.bucket)
            .release_durable_bucket_write_reservation(&wrong_pg_release_record)
            .unwrap_err();
    assert!(
        matches!(
            release_error,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            })
        ),
        "unexpected wrong-PG reservation release error: {release_error:?}"
    );

    for thread in server_threads {
        thread.join().unwrap();
    }

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    for pg_id in [correct_pg_id, wrong_pg_id] {
        let pg = node.get_pg(pg_id).unwrap();
        assert!(
            PgMetadataStore::durable_bucket_write_reservation(
                &*pg,
                &bucket,
                "wrong-pg-reservation",
            )
            .unwrap()
            .is_none(),
            "wrong-PG reservation must not mutate PG {pg_id}"
        );
    }
    let wrong_pg = node.get_pg(wrong_pg_id).unwrap();
    assert_eq!(
        PgMetadataStore::durable_bucket_write_reservation(
            &*wrong_pg,
            &bucket,
            &wrong_pg_release_record.reservation_id,
        )
        .unwrap(),
        Some(wrong_pg_release_record),
        "wrong-PG retained release must not mutate the equivalent durable subject"
    );
}

#[test]
fn unix_bucket_metadata_client_rejects_proof_release_wrong_bucket_pg() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes = vec![
        StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![NodeId::new(7)],
        },
        StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![NodeId::new(7)],
        },
    ];
    let owner = crate::CanonicalUserId::from_principal("owner");
    let (bucket, correct_pg_id, wrong_pg_id, reservation) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let (bucket, correct_pg_id, wrong_pg_id) = (0..100)
            .map(|index| crate::tests::bucket_name(format!("proof-release-wrong-pg-{index}")))
            .find_map(|bucket| {
                let correct_pg_id = node.pg_topology().bucket_pg_for(&bucket);
                (correct_pg_id < 2).then(|| {
                    let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
                    (bucket, correct_pg_id, wrong_pg_id)
                })
            })
            .expect("two-PG topology must place a test bucket");
        let mut reservations = Vec::new();
        for pg_id in [correct_pg_id, wrong_pg_id] {
            let pg = node.get_pg(pg_id).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            reservations.push(
                PgMetadataStore::acquire_durable_bucket_write_reservation(
                    &*pg,
                    crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                        name: &bucket,
                        reservation_id: "reservation-1",
                        owner_token: "owner-token-1",
                        cluster_epoch: ClusterEpoch::new(1).unwrap(),
                        operation_kind: "put-object",
                        created_at: 10,
                        lease_deadline: 20,
                        target_context: Some("key=a"),
                    },
                )
                .unwrap(),
            );
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        assert_eq!(
            reservations[0], reservations[1],
            "wrong-PG proof-release canary must have equivalent durable state"
        );
        let reservation = reservations.remove(0);
        (bucket, correct_pg_id, wrong_pg_id, reservation)
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let err = retained_bucket_write_route(&client, bucket_pg_id_for_test(wrong_pg_id), &bucket)
        .release_metadata_command_bucket_write_reservation(&BucketWriteReservationProof::from(
            &reservation,
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "proof release",
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    for pg_id in [correct_pg_id, wrong_pg_id] {
        let pg = node.get_pg(pg_id).unwrap();
        assert_eq!(
            PgMetadataStore::durable_bucket_write_reservation(&*pg, &bucket, "reservation-1")
                .unwrap(),
            Some(reservation.clone()),
            "wrong-PG proof release must reject before mutating either durable canary"
        );
    }
}

#[test]
fn unix_bucket_write_drain_operations_reject_wrong_bucket_pg_before_access() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes = vec![
        StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: config.cluster_epoch,
            state: crate::types::PgState::Active,
            primary_node_id: config.node_id,
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![config.node_id],
        },
        StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: config.cluster_epoch,
            state: crate::types::PgState::Active,
            primary_node_id: config.node_id,
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![config.node_id],
        },
    ];
    let owner = crate::CanonicalUserId::from_principal("owner");
    let (bucket, correct_pg_id, wrong_pg_id, drain) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let (bucket, correct_pg_id) = (0..100)
            .map(|index| crate::tests::bucket_name(format!("drain-rpc-wrong-pg-{index}")))
            .map(|bucket| {
                let pg_id = node.pg_topology().bucket_pg_for(&bucket);
                (bucket, pg_id)
            })
            .find(|(_, pg_id)| *pg_id < 2)
            .expect("two-PG topology must place a test bucket");
        let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
        let mut drains = Vec::new();
        for pg_id in [correct_pg_id, wrong_pg_id] {
            let pg = node.get_pg(pg_id).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            drains.push(
                PgMetadataStore::begin_durable_bucket_write_drain(
                    &*pg,
                    &bucket,
                    "wrong-pg-drain",
                    "wrong-pg-drain-owner",
                    config.cluster_epoch,
                    10,
                    80,
                )
                .unwrap(),
            );
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        assert_eq!(
            drains[0], drains[1],
            "wrong-PG drain canary must have equivalent durable state"
        );
        (bucket, correct_pg_id, wrong_pg_id, drains.remove(0))
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..1)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let wrong_pg = bucket_pg_id_for_test(wrong_pg_id);
    let assert_payload_decode = |error: BucketSnapshotLoadError, operation: &str| {
        assert!(
            matches!(
                &error,
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ),
            "wrong-PG {operation} must fail with PayloadDecode, got {error:?}"
        );
    };
    let assert_drains_unchanged = || {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        for pg_id in [correct_pg_id, wrong_pg_id] {
            let pg = node.get_pg(pg_id).unwrap();
            assert_eq!(
                PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket).unwrap(),
                Some(drain.clone()),
                "wrong-PG drain operation must not mutate PG {pg_id}"
            );
        }
    };

    let error = client
        .open_bucket_write_reservation_route(config.cluster_epoch, wrong_pg, &bucket)
        .err()
        .expect("wrong-PG drain route must be rejected before transport");
    assert_payload_decode(error, "route construction");
    assert_drains_unchanged();

    let error = retained_bucket_write_route(&client, wrong_pg, &bucket)
        .clear_durable_bucket_write_drain(&drain)
        .unwrap_err();
    assert_payload_decode(error, "exact drain clear");
    assert_drains_unchanged();

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_delete_finalize_claim_operations_reject_wrong_bucket_pg_before_access() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes = vec![
        StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: config.cluster_epoch,
            state: crate::types::PgState::Active,
            primary_node_id: config.node_id,
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![config.node_id],
        },
        StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: config.cluster_epoch,
            state: crate::types::PgState::Active,
            primary_node_id: config.node_id,
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![config.node_id],
        },
    ];
    let owner = crate::CanonicalUserId::from_principal("owner");
    let (bucket, correct_pg_id, wrong_pg_id, correct_claim, wrong_claim) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let (bucket, correct_pg_id) = (0..100)
            .map(|index| crate::tests::bucket_name(format!("finalize-claim-wrong-pg-{index}")))
            .map(|bucket| {
                let pg_id = node.pg_topology().bucket_pg_for(&bucket);
                (bucket, pg_id)
            })
            .find(|(_, pg_id)| *pg_id < 2)
            .expect("two-PG topology must place a test bucket");
        let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
        let mut generations = Vec::new();
        let mut claims = Vec::new();
        for pg_id in [correct_pg_id, wrong_pg_id] {
            let pg = node.get_pg(pg_id).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            PgMetadataStore::mark_bucket_deleting(&*pg, &bucket).unwrap();
            let generation = PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .bucket_incarnation_generation;
            generations.push(generation);
            claims.push(
                PgMetadataStore::acquire_bucket_delete_finalize_claim(
                    &*pg,
                    &bucket,
                    generation,
                    "wrong-pg-finalize-claim",
                    "wrong-pg-finalize-owner",
                    config.cluster_epoch,
                    10,
                    Some(80),
                    10,
                )
                .unwrap()
                .expect("finalizer claim should be acquired"),
            );
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        assert_eq!(
            generations[0], generations[1],
            "wrong-PG finalizer canaries must use the same bucket generation"
        );
        (
            bucket,
            correct_pg_id,
            wrong_pg_id,
            claims.remove(0),
            claims.remove(0),
        )
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..1)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let wrong_pg = bucket_pg_id_for_test(wrong_pg_id);
    let assert_payload_decode = |error: BucketSnapshotLoadError, operation: &str| {
        assert!(
            matches!(
                &error,
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ),
            "wrong-PG {operation} must fail with PayloadDecode, got {error:?}"
        );
    };
    let assert_claims_unchanged = || {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        for (pg_id, expected) in [(correct_pg_id, &correct_claim), (wrong_pg_id, &wrong_claim)] {
            let pg = node.get_pg(pg_id).unwrap();
            assert_eq!(
                PgMetadataStore::bucket_delete_finalize_claim(&*pg).unwrap(),
                Some(expected.clone()),
                "wrong-PG finalizer-claim operation must not mutate PG {pg_id}"
            );
        }
    };

    let error = client
        .open_bucket_write_reservation_route(config.cluster_epoch, wrong_pg, &bucket)
        .err()
        .expect("wrong-PG finalizer route must be rejected before transport");
    assert_payload_decode(error, "route construction");
    assert_claims_unchanged();

    let error = retained_bucket_write_route(&client, wrong_pg, &bucket)
        .release_bucket_delete_finalize_claim(&wrong_claim)
        .unwrap_err();
    assert_payload_decode(error, "finalizer claim release");
    assert_claims_unchanged();

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_lifecycle_sweep_claim_operations_reject_wrong_bucket_pg_before_access() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes = vec![
        StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: config.cluster_epoch,
            state: crate::types::PgState::Active,
            primary_node_id: config.node_id,
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![config.node_id],
        },
        StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: config.cluster_epoch,
            state: crate::types::PgState::Active,
            primary_node_id: config.node_id,
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: vec![config.node_id],
        },
    ];
    let owner = crate::CanonicalUserId::from_principal("owner");
    let (bucket, correct_pg_id, wrong_pg_id, generation, correct_claim, wrong_claim) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let (bucket, correct_pg_id) = (0..100)
            .map(|index| crate::tests::bucket_name(format!("lifecycle-claim-wrong-pg-{index}")))
            .map(|bucket| {
                let pg_id = node.pg_topology().bucket_pg_for(&bucket);
                (bucket, pg_id)
            })
            .find(|(_, pg_id)| *pg_id < 2)
            .expect("two-PG topology must place a test bucket");
        let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
        let mut generations = Vec::new();
        let mut claims = Vec::new();
        for pg_id in [correct_pg_id, wrong_pg_id] {
            let pg = node.get_pg(pg_id).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            PgMetadataStore::put_bucket_subresource(
                &*pg,
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration/>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
            let generation = PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .bucket_incarnation_generation;
            generations.push(generation);
            claims.push(
                PgMetadataStore::acquire_lifecycle_sweep_claim(
                    &*pg,
                    &bucket,
                    generation,
                    "wrong-pg-lifecycle-claim",
                    "wrong-pg-lifecycle-owner",
                    config.cluster_epoch,
                    10,
                    None,
                    10,
                )
                .unwrap()
                .expect("lifecycle claim should be acquired"),
            );
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        assert_eq!(
            generations[0], generations[1],
            "wrong-PG lifecycle canaries must use the same bucket generation"
        );
        (
            bucket,
            correct_pg_id,
            wrong_pg_id,
            generations[0],
            claims.remove(0),
            claims.remove(0),
        )
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..1)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let wrong_pg = bucket_pg_id_for_test(wrong_pg_id);
    let assert_payload_decode = |error: BucketSnapshotLoadError, operation: &str| {
        assert!(
            matches!(
                &error,
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ),
            "wrong-PG {operation} must fail with PayloadDecode, got {error:?}"
        );
    };
    let assert_claims_unchanged = || {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        for (pg_id, expected) in [(correct_pg_id, &correct_claim), (wrong_pg_id, &wrong_claim)] {
            let pg = node.get_pg(pg_id).unwrap();
            assert_eq!(
                PgMetadataStore::acquire_lifecycle_sweep_claim(
                    &*pg,
                    &bucket,
                    generation,
                    &expected.claim_id,
                    &expected.owner_token,
                    expected.cluster_epoch,
                    expected.claimed_at,
                    expected.lease_deadline,
                    20,
                )
                .unwrap(),
                Some(expected.clone()),
                "wrong-PG lifecycle-claim operation must not mutate PG {pg_id}"
            );
        }
    };

    let error = client
        .open_bucket_write_reservation_route(config.cluster_epoch, wrong_pg, &bucket)
        .err()
        .expect("wrong-PG lifecycle route must be rejected before transport");
    assert_payload_decode(error, "route construction");
    assert_claims_unchanged();

    let error = retained_bucket_write_route(&client, wrong_pg, &bucket)
        .release_lifecycle_sweep_claim(&wrong_claim)
        .unwrap_err();
    assert_payload_decode(error, "lifecycle claim release");
    assert_claims_unchanged();

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_metadata_client_rejects_proof_release_on_non_primary() {
    let tmp = test_util::tempdir();
    let mut primary_config = test_config(&tmp);
    primary_config.node_id = NodeId::new(7);
    primary_config.data_dir = tmp.path().join("primary-node");
    primary_config.pg_routes[0].primary_node_id = NodeId::new(7);
    primary_config.pg_routes[0].acting_set = vec![NodeId::new(8), NodeId::new(7)];
    let mut replica_config = primary_config.clone();
    replica_config.node_id = NodeId::new(8);
    replica_config.data_dir = tmp.path().join("replica-node");
    replica_config.socket_path = tmp.path().join("sock").join("replica-storage.sock");
    let bucket = crate::tests::bucket_name("proof-release-replica-bucket");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let reservation = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &primary_config.data_dir,
            &primary_config.pg_ids,
            primary_config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-1",
                owner_token: "owner-token-1",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key=a"),
            },
        )
        .unwrap()
    };
    private_socket_dir(replica_config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(replica_config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(8),
        ClusterEpoch::new(1).unwrap(),
        replica_config.socket_path.clone(),
    );

    let err = retained_bucket_write_route(&client, bucket_pg_id_for_test(0), &bucket)
        .release_metadata_command_bucket_write_reservation(&BucketWriteReservationProof::from(
            &reservation,
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "proof release",
            ..
        })
    ));
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &primary_config.data_dir,
        &primary_config.pg_ids,
        primary_config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(
        PgMetadataStore::durable_bucket_write_reservation(&*pg, &bucket, "reservation-1")
            .unwrap()
            .is_some()
    );
}

#[test]
fn unix_bucket_delete_replica_head_reads_non_primary_acting_replica() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.node_id = NodeId::new(8);
    config.pg_routes[0].primary_node_id = NodeId::new(7);
    config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
    let bucket = crate::tests::bucket_name("delete-replica-head-bucket");
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..2)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(PgTopology::new(&config.pg_ids).unwrap()));
    let pg_id = bucket_pg_id_for_test(0);

    let ordinary_error = bucket_metadata_route(&client, config.cluster_epoch, pg_id, &bucket)
        .head_bucket_raw()
        .expect_err("ordinary bucket reads must remain primary-only");
    assert!(matches!(
        ordinary_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::NonActingSetAccess,
            ..
        })
    ));

    let replica = client
        .open_bucket_delete_replica_metadata_route(config.cluster_epoch, pg_id, &bucket)
        .unwrap()
        .head_bucket_replica_for_delete()
        .unwrap();
    assert_eq!(replica.name, bucket);

    for server_thread in server_threads {
        server_thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_delete_replica_head_reads_authorized_historical_active_replica() {
    let info = historical_bucket_delete_replica_head(HistoricalReplicaHeadAuthorization::Matching)
        .unwrap();
    assert_eq!(info.name.as_str(), "historical-delete-replica-head-bucket");
}

#[test]
fn unix_bucket_delete_replica_head_rejects_missing_historical_authorization() {
    let error = historical_bucket_delete_replica_head(HistoricalReplicaHeadAuthorization::Missing)
        .unwrap_err();
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::StaleShardLocation,
            ..
        })
    ));
}

#[test]
fn unix_bucket_delete_replica_head_rejects_wrong_historical_authorization() {
    let error =
        historical_bucket_delete_replica_head(HistoricalReplicaHeadAuthorization::WrongEpoch)
            .unwrap_err();
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::StaleShardLocation,
            ..
        })
    ));
}
