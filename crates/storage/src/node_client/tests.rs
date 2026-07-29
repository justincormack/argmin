use super::*;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::thread;
use std::time::{Duration, Instant};

use crate::metadata_command::ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND;
use crate::storage_node_server::{StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeServer};
use crate::storage_rpc::{
    encode_metadata_command_acceptance_response, encode_metadata_command_applied_hashes_response,
    encode_metadata_command_bool_outcome_response, encode_metadata_command_next_id_response,
    encode_metadata_command_pending_slot_insert_response,
    encode_metadata_command_state_outcome_response, encode_read_handle_acquire_response,
    encode_scavenger_observations_response, encode_storage_rpc_success_response,
    read_storage_rpc_frame_from, write_storage_rpc_frame_to,
    StorageRpcMetadataCommandAcceptanceResponse, StorageRpcMetadataCommandAppliedHashesResponse,
    StorageRpcMetadataCommandBoolOutcomeResponse, StorageRpcMetadataCommandNextIdResponse,
    StorageRpcMetadataCommandPendingSlotInsertResponse,
    StorageRpcMetadataCommandStateOutcomeResponse, StorageRpcReadHandleAcquireResponse,
    StorageRpcStreamError, STORAGE_RPC_CLIENT_RESPONSE_TIMEOUT,
};
use crate::types::{
    DeleteMarkerRecord, EtagKind, ObjectEncryption, ObjectLockState, SerializedMetadataBlob,
    SerializedSystemMetadataBlob, SerializedTagSet, StorageClass, StreamUploadPartSnapshot,
};
use crate::ShardScavengerObservationReason;
use crate::{RouteMapValidity, ShardIndex};

fn test_config(tmp: &test_util::TempDir) -> StorageNodeProcessConfig {
    StorageNodeProcessConfig {
        node_id: NodeId::new(7),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        route_map_validity: RouteMapValidity::Forever,
        data_dir: tmp.path().join("node"),
        default_ec_shape: EcShape { k: 4, m: 2 },
        pg_ids: vec![0],
        socket_path: tmp.path().join("sock").join("storage.sock"),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            metadata_transfer_destination_epoch: None,
            acting_set: vec![NodeId::new(7)],
        }],
        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    }
}

fn private_socket_dir(path: &std::path::Path) {
    fs::create_dir_all(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

fn test_shard_scavenger_observation(data_pg_id: u32, seed: u8) -> ShardScavengerObservationRecord {
    let shard_key = ShardKey::new(&[seed; 16], u64::from(seed), 0);
    ShardScavengerObservationRecord {
        key: ShardScavengerObservationKey {
            node_id: 7,
            data_pg_id,
            shard_index: shard_key.shard_index(),
            shard_key,
        },
        data_size: None,
        crc64: None,
        file_exists: false,
        shard_row_exists: false,
        reason: ShardScavengerObservationReason::ScanIncomplete,
        last_error: Some(format!("PG {data_pg_id} canary")),
    }
}

struct TestRetainedBucketWriteSubjects {
    reservation: BucketWriteReservationRecord,
    proof: BucketWriteReservationProof,
    drain: BucketWriteDrainRecord,
    delete_claim: BucketDeleteFinalizeClaimRecord,
    lifecycle_claim: LifecycleSweepClaimRecord,
}

fn test_retained_bucket_write_subjects(
    bucket: BucketName,
    pg_id: u32,
) -> TestRetainedBucketWriteSubjects {
    let reservation = BucketWriteReservationRecord {
        bucket: bucket.clone(),
        reservation_id: "retained-route-reservation".to_string(),
        owner_token: "retained-route-reservation-owner".to_string(),
        cluster_epoch: ClusterEpoch::INITIAL,
        bucket_execution_generation: 2,
        bucket_incarnation_generation: 3,
        operation_kind: "retained-route-test".to_string(),
        created_at: 10,
        lease_deadline: 20,
        target_context: None,
    };
    TestRetainedBucketWriteSubjects {
        proof: BucketWriteReservationProof::from(&reservation),
        reservation,
        drain: BucketWriteDrainRecord {
            bucket: bucket.clone(),
            drain_id: "retained-route-drain".to_string(),
            owner_token: "retained-route-drain-owner".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 2,
            state: crate::types::BucketWriteDrainState::Draining,
            created_at: 10,
            lease_deadline: 20,
        },
        delete_claim: BucketDeleteFinalizeClaimRecord {
            bucket: bucket.clone(),
            bucket_incarnation_generation: 3,
            claim_id: "retained-route-delete-claim".to_string(),
            owner_token: "retained-route-delete-owner".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id,
            claimed_at: 10,
            lease_deadline: Some(20),
            attempt_count: 1,
            last_error: None,
        },
        lifecycle_claim: LifecycleSweepClaimRecord {
            bucket,
            bucket_incarnation_generation: 3,
            claim_id: "retained-route-lifecycle-claim".to_string(),
            owner_token: "retained-route-lifecycle-owner".to_string(),
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id,
            claimed_at: 10,
            heartbeat_at: 10,
            lease_deadline: Some(20),
            attempt_count: 1,
            last_error: None,
        },
    }
}

fn assert_route_subject_mismatch(error: BucketSnapshotLoadError, expected_operation: &'static str) {
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation,
        }) if operation == expected_operation
    ));
}

fn test_object_payload_reclaim_claim(
    bucket: BucketName,
    key: ObjectKey,
    pg_id: u32,
) -> ObjectPayloadReclaimClaimRecord {
    ObjectPayloadReclaimClaimRecord {
        bucket,
        bucket_incarnation_generation: 3,
        key,
        generation_id: GenerationId::new(4).unwrap(),
        reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
        claim_id: "retained-object-route-claim".to_string(),
        owner_token: "retained-object-route-owner".to_string(),
        cluster_epoch: ClusterEpoch::INITIAL,
        pg_id,
        claimed_at: 10,
        lease_deadline: Some(20),
        attempt_count: 1,
        last_error: None,
    }
}

fn test_object_payload_reclaim_proof(
    cluster_epoch: ClusterEpoch,
) -> ObjectPayloadReclaimClaimProof {
    ObjectPayloadReclaimClaimProof {
        bucket_incarnation_generation: 3,
        reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
        claim_id: "retained-payload-route-claim".to_string(),
        owner_token: "retained-payload-route-owner".to_string(),
        cluster_epoch,
    }
}

#[test]
fn local_retained_placed_shard_route_is_bound_to_exact_placement() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let bound_key = ShardKey::new(&[0x41; 16], 11, 0);
    let foreign_key = ShardKey::new(&[0x42; 16], 12, 0);
    let bound_data = b"bound retained shard";
    let foreign_data = b"foreign retained shard";
    let bound_ack = storage_node
        .write_shard_file(0, &bound_key, bound_data)
        .unwrap();
    storage_node
        .write_shard_file(0, &foreign_key, foreign_data)
        .unwrap();
    let location = crate::cluster::ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new_for_test(PgId::new(0)),
        bound_key.shard_index(),
        NodeId::new(7),
    );

    let route = client
        .open_retained_placed_shard_route(location, &bound_key)
        .unwrap();
    assert_eq!(
        route
            .read_placed_shard_for_historical_inspection(bound_ack)
            .unwrap(),
        bound_data
    );
    route.delete_placed_shard_for_historical_cleanup().unwrap();
    assert!(storage_node.read_shard_file(0, &bound_key).is_err());
    assert_eq!(
        storage_node.read_shard_file(0, &foreign_key).unwrap(),
        foreign_data
    );

    let wrong_shard_location = crate::cluster::ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new_for_test(PgId::new(0)),
        ShardIndex::new(1),
        NodeId::new(7),
    );
    assert!(matches!(
        client
            .open_retained_placed_shard_route(wrong_shard_location, &bound_key)
            .err()
            .expect("foreign shard index must be rejected before storage"),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "open retained placed shard route",
        }
    ));
    let foreign_pg_location = crate::cluster::ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new_for_test(PgId::new(1)),
        bound_key.shard_index(),
        NodeId::new(7),
    );
    assert!(matches!(
        client
            .open_retained_placed_shard_route(foreign_pg_location, &bound_key)
            .err()
            .expect("foreign retained data PG must be rejected before storage"),
        StoreError::PgNotFound { pg_id: 1 }
    ));
}

#[test]
fn unix_retained_placed_shard_route_rejects_foreign_subject_before_rpc() {
    let client = test_unix_storage_node_client();
    let key = ShardKey::new(&[0x51; 16], 21, 0);
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let foreign_node_location = crate::cluster::ShardLocation::new(
        ClusterEpoch::INITIAL,
        data_pg_id,
        key.shard_index(),
        NodeId::new(8),
    );
    assert!(matches!(
        client
            .open_retained_placed_shard_route(foreign_node_location, &key)
            .err()
            .expect("foreign retained node must be rejected before RPC"),
        StoreError::NodeNotFound {
            node_id: 8,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
        }
    ));

    let wrong_shard_location = crate::cluster::ShardLocation::new(
        ClusterEpoch::INITIAL,
        data_pg_id,
        ShardIndex::new(1),
        NodeId::new(7),
    );
    assert!(matches!(
        client
            .open_retained_placed_shard_route(wrong_shard_location, &key)
            .err()
            .expect("foreign retained shard index must be rejected before RPC"),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "open retained placed shard route",
        }
    ));

    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get().saturating_add(1)).unwrap();
    let future_location = crate::cluster::ShardLocation::new(
        future_epoch,
        data_pg_id,
        key.shard_index(),
        NodeId::new(7),
    );
    assert!(matches!(
        client
            .open_retained_placed_shard_route(future_location, &key)
            .err()
            .expect("future retained shard epoch must be rejected before RPC"),
        StoreError::StalePayloadOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_retained_shard_ack_route_is_bound_to_exact_pg_and_key() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let bound_key = ShardKey::new(&[0x61; 16], 31, 0);
    let foreign_key = ShardKey::new(&[0x62; 16], 32, 0);
    let bound_ack = WriteAck {
        crc64: 41,
        stored_size: 51,
    };
    let foreign_ack = WriteAck {
        crc64: 42,
        stored_size: 52,
    };
    client
        .register_written_shard_acks(
            data_pg_id,
            &[(&bound_key, bound_ack), (&foreign_key, foreign_ack)],
        )
        .unwrap();

    let route = client
        .open_retained_shard_ack_route(ClusterEpoch::INITIAL, data_pg_id, &bound_key)
        .unwrap();
    assert_eq!(
        route
            .load_written_shard_ack_for_historical_inspection()
            .unwrap(),
        bound_ack
    );
    route.delete_retained_shard_ack().unwrap();
    assert!(matches!(
        client.load_written_shard_ack(data_pg_id, &bound_key),
        Err(StoreError::NotFound)
    ));
    assert_eq!(
        client
            .load_written_shard_ack(data_pg_id, &foreign_key)
            .unwrap(),
        foreign_ack
    );

    let foreign_pg_id = DataPgId::new_for_test(PgId::new(1));
    assert!(matches!(
        client
            .open_retained_shard_ack_route(ClusterEpoch::INITIAL, foreign_pg_id, &bound_key,)
            .err()
            .expect("foreign retained ack PG must be rejected before storage"),
        StoreError::PgNotFound { pg_id: 1 }
    ));
}

#[test]
fn unix_retained_shard_ack_route_rejects_future_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let key = ShardKey::new(&[0x71; 16], 41, 0);
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get().saturating_add(1)).unwrap();

    assert!(matches!(
        client
            .open_retained_shard_ack_route(future_epoch, data_pg_id, &key)
            .err()
            .expect("future retained ack epoch must be rejected before RPC"),
        StoreError::StalePayloadOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_object_payload_lease_route_is_bound_to_exact_subject() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let bucket = crate::tests::bucket_name("payload-lease-route-bucket");
    let bound_key = crate::tests::object_key("payload-lease-route-bound-key");
    let foreign_key = crate::tests::object_key("payload-lease-route-foreign-key");
    let generation_id = GenerationId::new(4).unwrap();
    let authority = test_object_payload_reclaim_proof(ClusterEpoch::INITIAL);
    let route = client
        .open_object_payload_lease_route(ClusterEpoch::INITIAL, &bucket, &bound_key, generation_id)
        .unwrap();

    let mut lease = route
        .acquire_object_payload_lease(ObjectPayloadLeaseKind::BroadSnapshot)
        .unwrap()
        .expect("bound route must acquire its exact payload lease");
    assert_eq!(route.object_payload_lease_count().unwrap(), 1);
    assert_eq!(
        storage_node.object_payload_lease_count(&bucket, &foreign_key, generation_id),
        0
    );
    assert!(!route.try_begin_object_payload_reclaim(&authority).unwrap());
    assert_eq!(lease.release().unwrap(), 0);

    let mut wrong_epoch = authority.clone();
    wrong_epoch.cluster_epoch = ClusterEpoch::new(2).unwrap();
    assert!(matches!(
        route
            .try_begin_object_payload_reclaim(&wrong_epoch)
            .unwrap_err(),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "begin object payload reclaim",
        }
    ));
    assert!(route.try_begin_object_payload_reclaim(&authority).unwrap());
    assert!(storage_node.test_object_payload_reclaim_is_active(&bucket, &bound_key, generation_id,));
    assert!(!storage_node.test_object_payload_reclaim_is_active(
        &bucket,
        &foreign_key,
        generation_id,
    ));
    client
        .open_retained_object_payload_reclaim_route(
            ClusterEpoch::INITIAL,
            &bucket,
            &bound_key,
            generation_id,
            &authority,
        )
        .unwrap()
        .finish_object_payload_reclaim(false)
        .unwrap();
}

#[test]
fn local_retained_object_payload_reclaim_route_is_bound_to_exact_subject() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let bucket = crate::tests::bucket_name("retained-payload-route-bucket");
    let bound_key = crate::tests::object_key("retained-payload-route-bound-key");
    let foreign_key = crate::tests::object_key("retained-payload-route-foreign-key");
    let generation_id = GenerationId::new(4).unwrap();
    let authority = test_object_payload_reclaim_proof(ClusterEpoch::INITIAL);

    assert!(storage_node.try_begin_object_payload_reclaim(
        &bucket,
        &bound_key,
        generation_id,
        &authority,
    ));
    assert!(storage_node.try_begin_object_payload_reclaim(
        &bucket,
        &foreign_key,
        generation_id,
        &authority,
    ));

    let route = client
        .open_retained_object_payload_reclaim_route(
            ClusterEpoch::INITIAL,
            &bucket,
            &bound_key,
            generation_id,
            &authority,
        )
        .unwrap();
    route.finish_object_payload_reclaim(true).unwrap();
    assert!(!storage_node.test_object_payload_reclaim_is_active(
        &bucket,
        &bound_key,
        generation_id,
    ));
    assert!(storage_node.test_object_payload_reclaim_is_active(
        &bucket,
        &foreign_key,
        generation_id,
    ));
    assert!(!storage_node.try_acquire_object_payload_lease(&bucket, &bound_key, generation_id,));
    route.clear_object_payload_reclaim_fence().unwrap();
    assert!(storage_node.try_acquire_object_payload_lease(&bucket, &bound_key, generation_id,));
    assert!(!storage_node.try_acquire_object_payload_lease(&bucket, &foreign_key, generation_id,));
    assert_eq!(
        storage_node.release_object_payload_lease(&bucket, &bound_key, generation_id),
        0
    );

    let mut wrong_epoch = authority;
    wrong_epoch.cluster_epoch = ClusterEpoch::new(2).unwrap();
    assert!(matches!(
        client
            .open_retained_object_payload_reclaim_route(
                ClusterEpoch::INITIAL,
                &bucket,
                &bound_key,
                generation_id,
                &wrong_epoch,
            )
            .err()
            .expect("foreign reclaim authority epoch must be rejected"),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "open retained object payload reclaim route",
        }
    ));
}

#[test]
fn local_retained_object_mutation_route_rejects_foreign_claim_before_storage() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), storage_node);
    let bound_bucket = crate::tests::bucket_name("retained-object-route-bound-bucket");
    let bound_key = crate::tests::object_key("retained-object-route-bound-key");
    let foreign_bucket = crate::tests::bucket_name("retained-object-route-foreign-bucket");
    let foreign_key = crate::tests::object_key("retained-object-route-foreign-key");
    let pg_id = client
        .storage_node
        .object_metadata_pg_for(&bound_bucket, &bound_key);
    assert_route_subject_mismatch(
        client
            .open_retained_object_mutation_route(
                ObjectMetadataPgId::new_for_test(PgId::new(pg_id.get().saturating_add(1))),
                ClusterEpoch::INITIAL,
                &bound_bucket,
                &bound_key,
            )
            .err()
            .expect("foreign object PG must be rejected"),
        "open retained object mutation route",
    );
    let route = client
        .open_retained_object_mutation_route(
            pg_id,
            ClusterEpoch::INITIAL,
            &bound_bucket,
            &bound_key,
        )
        .unwrap();

    assert_route_subject_mismatch(
        route
            .release_object_payload_reclaim_claim(&test_object_payload_reclaim_claim(
                foreign_bucket,
                foreign_key,
                pg_id.get(),
            ))
            .unwrap_err(),
        "release object payload reclaim claim",
    );
    assert_route_subject_mismatch(
        route
            .release_object_payload_reclaim_claim(&test_object_payload_reclaim_claim(
                bound_bucket.clone(),
                bound_key.clone(),
                pg_id.get().saturating_add(1),
            ))
            .unwrap_err(),
        "release object payload reclaim claim",
    );
    let mut wrong_epoch = test_object_payload_reclaim_claim(bound_bucket, bound_key, pg_id.get());
    wrong_epoch.cluster_epoch = ClusterEpoch::new(2).unwrap();
    assert_route_subject_mismatch(
        route
            .release_object_payload_reclaim_claim(&wrong_epoch)
            .unwrap_err(),
        "release object payload reclaim claim",
    );
}

#[test]
fn local_retained_bucket_write_route_rejects_foreign_subject_before_storage() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), storage_node);
    let bound_bucket = crate::tests::bucket_name("retained-route-bound-bucket");
    let foreign_bucket = crate::tests::bucket_name("retained-route-foreign-bucket");
    let pg_id = client.storage_node.bucket_metadata_pg_for(&bound_bucket);
    let route = client
        .open_retained_bucket_write_reservation_route(pg_id, &bound_bucket)
        .unwrap();
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
fn local_shard_scavenger_observation_route_rejects_foreign_subject_without_mutation() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0, 1],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let pg0_observation = test_shard_scavenger_observation(0, 0x41);
    let pg1_observation = test_shard_scavenger_observation(1, 0x42);
    storage_node
        .get_pg(0)
        .unwrap()
        .record_shard_scavenger_observation(&pg0_observation)
        .unwrap();
    storage_node
        .get_pg(1)
        .unwrap()
        .record_shard_scavenger_observation(&pg1_observation)
        .unwrap();
    let before = [0, 1].map(|pg_id| {
        storage_node
            .get_pg(pg_id)
            .unwrap()
            .list_shard_scavenger_observations()
            .unwrap()
    });

    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let route = client
        .open_shard_scavenger_observation_route(DataPgId::new_for_test(PgId::new(0)))
        .unwrap();
    assert_eq!(
        route.list_shard_scavenger_observations().unwrap(),
        before[0]
    );
    let mut redirected = pg1_observation;
    redirected.last_error = Some("must not persist".to_string());
    for error in [
        route
            .record_shard_scavenger_observation(&redirected)
            .unwrap_err(),
        route
            .resolve_shard_scavenger_observation(&redirected.key)
            .unwrap_err(),
    ] {
        assert!(matches!(
            error,
            StoreError::ShardScavengerObservationWrongPg {
                store_pg_id: 0,
                observation_pg_id: 1,
            }
        ));
    }
    drop(route);

    for (pg_id, expected) in [0, 1].into_iter().zip(before) {
        assert_eq!(
            storage_node
                .get_pg(pg_id)
                .unwrap()
                .list_shard_scavenger_observations()
                .unwrap(),
            expected
        );
    }
}

#[test]
fn local_recovery_critical_section_rejects_command_for_another_pg_without_mutation() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0, 1],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let recovery =
        MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
            &client,
            PgId::new(0),
            ClusterEpoch::new(1).unwrap(),
        )
        .unwrap();

    let error = recovery
        .record_metadata_command_abandoned(&test_metadata_command(1, 1))
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::MetadataCommandWrongPg {
            command_pg_id: 1,
            target_pg_id: 0,
            ..
        }
    ));
    drop(recovery);

    for pg_id in [0, 1] {
        assert_eq!(
            storage_node
                .get_pg(pg_id)
                .unwrap()
                .max_metadata_command_log_index(ClusterEpoch::new(1).unwrap())
                .unwrap(),
            0
        );
    }
}

#[test]
fn local_recovery_critical_section_rejects_future_epoch_command_without_mutation() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let captured_epoch = ClusterEpoch::new(1).unwrap();
    let future_epoch = ClusterEpoch::new(2).unwrap();
    let recovery =
        MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
            &client,
            PgId::new(0),
            captured_epoch,
        )
        .unwrap();
    let command = test_metadata_command(0, 1);
    let future_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(future_epoch, PgId::new(0), command.id().log_index()),
        command.payload().clone(),
    );

    let error = recovery
        .record_metadata_command_abandoned(&future_command)
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == future_epoch && current_epoch == captured_epoch
    ));
    drop(recovery);

    let pg = storage_node.get_pg(0).unwrap();
    assert_eq!(
        pg.max_metadata_command_log_index(captured_epoch).unwrap(),
        0
    );
    assert_eq!(pg.max_metadata_command_log_index(future_epoch).unwrap(), 0);
}

#[test]
fn local_active_critical_section_rejects_command_for_another_pg_without_mutation() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0, 1],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let active = MetadataCommandNodeClient::open_metadata_command_critical_section(
        &client,
        PgId::new(0),
        ClusterEpoch::new(1).unwrap(),
    )
    .unwrap();

    let error = active
        .apply_metadata_command_and_record(&test_metadata_command(1, 1))
        .unwrap_err();
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::MetadataCommandWrongPg {
            command_pg_id: 1,
            target_pg_id: 0,
            ..
        })
    ));
    drop(active);

    for pg_id in [0, 1] {
        assert_eq!(
            storage_node
                .get_pg(pg_id)
                .unwrap()
                .max_metadata_command_log_index(ClusterEpoch::new(1).unwrap())
                .unwrap(),
            0
        );
    }
}

#[test]
fn local_active_critical_section_rejects_future_epoch_command_without_mutation() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let captured_epoch = ClusterEpoch::new(1).unwrap();
    let future_epoch = ClusterEpoch::new(2).unwrap();
    let active = MetadataCommandNodeClient::open_metadata_command_critical_section(
        &client,
        PgId::new(0),
        captured_epoch,
    )
    .unwrap();
    let command = test_metadata_command(0, 1);
    let future_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(future_epoch, PgId::new(0), command.id().log_index()),
        command.payload().clone(),
    );

    let error = active
        .apply_metadata_command_and_record(&future_command)
        .unwrap_err();
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        }) if operation_epoch == future_epoch && current_epoch == captured_epoch
    ));
    drop(active);

    let pg = storage_node.get_pg(0).unwrap();
    assert_eq!(
        pg.max_metadata_command_log_index(captured_epoch).unwrap(),
        0
    );
    assert_eq!(pg.max_metadata_command_log_index(future_epoch).unwrap(), 0);
}

#[test]
fn local_peering_route_rejects_command_for_another_pg_without_mutation() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0, 1],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let peering = MetadataCommandPeeringNodeClient::open_metadata_command_peering_route(
        &client,
        PgId::new(0),
        ClusterEpoch::new(1).unwrap(),
    )
    .unwrap();

    let wrong_command = test_metadata_command(1, 1);
    let error = peering
        .replay_metadata_command_for_peering(&wrong_command)
        .unwrap_err();
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::MetadataCommandWrongPg {
            command_pg_id: 1,
            target_pg_id: 0,
            ..
        })
    ));
    let error = peering
        .adopt_metadata_transfer_state_from_rebased_commands(
            &[MetadataTransferCommand {
                command: wrong_command,
                pre_state_digest: 0,
                post_state_digest: 1,
            }],
            1,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::MetadataCommandWrongPg {
            command_pg_id: 1,
            target_pg_id: 0,
            ..
        }
    ));
    let wrong_checkpoint = {
        let pg = storage_node.get_pg(1).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        pg.metadata_command_checkpoint(7, ClusterEpoch::new(1).unwrap())
            .unwrap()
    };
    let error = peering
        .install_metadata_transfer_checkpoint_base(&wrong_checkpoint)
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::MetadataCheckpointInvalid {
            pg_id: 0,
            cluster_epoch,
            ..
        } if cluster_epoch == ClusterEpoch::new(1).unwrap()
    ));
    drop(peering);

    for pg_id in [0, 1] {
        assert_eq!(
            storage_node
                .get_pg(pg_id)
                .unwrap()
                .max_metadata_command_log_index(ClusterEpoch::new(1).unwrap())
                .unwrap(),
            0
        );
    }
}

#[test]
fn local_peering_route_rejects_future_epoch_command_without_mutation() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let captured_epoch = ClusterEpoch::new(1).unwrap();
    let future_epoch = ClusterEpoch::new(2).unwrap();
    let peering = MetadataCommandPeeringNodeClient::open_metadata_command_peering_route(
        &client,
        PgId::new(0),
        captured_epoch,
    )
    .unwrap();
    let command = test_metadata_command(0, 1);
    let future_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(future_epoch, PgId::new(0), command.id().log_index()),
        command.payload().clone(),
    );

    let error = peering
        .replay_metadata_command_for_peering(&future_command)
        .unwrap_err();
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        }) if operation_epoch == future_epoch && current_epoch == captured_epoch
    ));
    let error = peering
        .adopt_metadata_transfer_state_from_rebased_commands(
            &[MetadataTransferCommand {
                command: future_command,
                pre_state_digest: 0,
                post_state_digest: 1,
            }],
            1,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == future_epoch && current_epoch == captured_epoch
    ));
    drop(peering);

    let pg = storage_node.get_pg(0).unwrap();
    assert_eq!(
        pg.max_metadata_command_log_index(captured_epoch).unwrap(),
        0
    );
    assert_eq!(pg.max_metadata_command_log_index(future_epoch).unwrap(), 0);
}

#[test]
fn multipart_completion_barrier_rejects_non_completion_bucket_write_reservation() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("completed-order-wrong-proof-bucket");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap(),
    );
    let pg = storage_node.get_pg(0).unwrap();
    crate::PgMetadataStore::create_bucket(
        &*pg,
        &bucket,
        "owner",
        &owner,
        &crate::AclGrants::default(),
        false,
        false,
    )
    .unwrap();
    let reservation = crate::PgMetadataStore::acquire_durable_bucket_write_reservation(
        &*pg,
        crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
            name: &bucket,
            reservation_id: "put-object-reservation",
            owner_token: "put-object-owner",
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            operation_kind: "put-object",
            created_at: 1_000,
            lease_deadline: crate::clock::current_time_millis() + 60_000,
            target_context: Some("object-key"),
        },
    )
    .unwrap();
    pg.refresh_metadata_command_state_digest().unwrap();

    let client = LocalStorageNodeClient::new(config.node_id, Arc::clone(&storage_node));
    let command_id = MetadataCommandId::new(
        ClusterEpoch::new(1).unwrap(),
        PgId::new(0),
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    let proof = BucketWriteReservationProof::from(&reservation);
    let err = BucketMetadataNodeClient::build_advance_multipart_completion_barrier_command(
        &client,
        BucketPgId::new_for_test(PgId::new(0)),
        &bucket,
        command_id,
        "object-key",
        &proof,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationConflict {
            reservation_id
        }) if reservation_id == "put-object-reservation"
    ));
}

#[test]
fn upload_part_stream_upload_match_accepts_existing_row_without_create_proof() {
    let bucket = crate::tests::bucket_name("upload-part-stream-match-bucket");
    let key = crate::tests::object_key("upload-part-stream-match-key");
    let upload_id = crate::tests::multipart_upload_id("upload-part-stream-match-upload");
    let session_id = SessionId::try_from("b6".repeat(16)).unwrap();
    let request = CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: StreamUploadTarget::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 1,
        },
        encryption: ObjectEncryption::None,
    };
    let proof = crate::metadata_command::BucketWriteReservationProof {
        bucket: bucket.clone(),
        reservation_id: "upload-part-stream-create-proof".to_string(),
        owner_token: "upload-part-stream-create-owner".to_string(),
        cluster_epoch: ClusterEpoch::INITIAL,
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        operation_kind:
            crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
                .to_string(),
        created_at: 10,
        lease_deadline: 20,
        target_context: Some(key.as_str().to_string()),
    };
    let command =
        CreateStreamUploadCommand::from_request_with_bucket_write_reservation(request, 10, proof);
    let existing = StreamUploadRecord {
        session_id,
        bucket,
        key,
        target: StreamUploadTarget::UploadPart {
            upload_id,
            part_number: 1,
        },
        state: StreamUploadState::InProgress,
        created_at: command.session.created_at,
        cleanup_after: command.cleanup_after,
        encryption: ObjectEncryption::None,
        next_segment_vid: command.initial_next_segment_vid,
        bucket_write_reservation: None,
    };

    assert!(
        stream_upload_matches_command(&existing, &command),
        "UploadPart stream-create replay must match the applied row without a stored create proof"
    );
}

fn test_live_stored_object(
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
    layout: ObjectLayout,
) -> StoredObject {
    StoredObject::Live(LiveObjectRecord {
        bucket,
        key,
        version_id: VersionId::Null,
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id,
        size: 0,
        etag: ObjectEtag::single_part(0),
        last_modified: 0,
        became_noncurrent_at: None,
        storage_class: StorageClass::Standard,
        ec: EcShape { k: 4, m: 2 },
        layout,
        tags: None,
        metadata_blob: None,
        system_metadata_blob: None,
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    })
}

fn test_delete_marker_stored_object(bucket: BucketName, key: ObjectKey) -> StoredObject {
    StoredObject::DeleteMarker(DeleteMarkerRecord {
        bucket,
        key,
        version_id: VersionId::Null,
        owner: OwnerIdentity::from_principal("owner"),
        last_modified: 0,
    })
}

fn test_segments_reclaim(
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
) -> ObjectPayloadReclaimCommand {
    ObjectPayloadReclaimCommand::Segments(ObjectSegmentsReclaimRecord {
        bucket,
        key,
        generation_id,
        created_at: 0,
        segments: Vec::new(),
    })
}

fn test_bucket_write_reservation_proof(
    bucket: BucketName,
    key: &ObjectKey,
) -> BucketWriteReservationProof {
    BucketWriteReservationProof {
        bucket,
        reservation_id: "reservation-id".to_string(),
        owner_token: "owner-token".to_string(),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        operation_kind: "object-mutation-test".to_string(),
        created_at: 1,
        lease_deadline: 20,
        target_context: Some(key.as_str().to_string()),
    }
}

fn test_multipart_upload_record(
    bucket: BucketName,
    key: ObjectKey,
    upload_id: UploadId,
    state: UploadState,
) -> MultipartUploadRecord {
    MultipartUploadRecord {
        upload_id,
        bucket,
        key,
        initiated_at: 1,
        state,
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        initiator: OwnerIdentity::from_principal("owner"),
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_generation_id: GenerationId::new(30).unwrap(),
        initiated_object_identity: None,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: ObjectEncryption::None,
    }
}

fn test_multipart_part_record(upload_id: UploadId, part_number: u32) -> MultipartPartRecord {
    MultipartPartRecord {
        upload_id,
        part_number,
        generation: 1,
        size: 12,
        payload_crc64: 0,
        etag: vec![part_number as u8; 8],
        etag_kind: EtagKind::Crc64,
        part_vid: GenerationId::new(40 + u64::from(part_number)).unwrap(),
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
        last_modified: 10 + u64::from(part_number),
        checksum: None,
    }
}

fn test_multipart_layout() -> ObjectLayout {
    ObjectLayout::MultipartManifest {
        parts_count: std::num::NonZeroU32::new(1).unwrap(),
    }
}

fn test_object_read_multipart_part(
    bucket: &BucketName,
    key: &ObjectKey,
    part_number: u32,
) -> ObjectPartRecord {
    ObjectPartRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: VersionId::Null,
        part_number,
        size: 12,
        payload_crc64: 0,
        etag: vec![part_number as u8; 16],
        etag_kind: EtagKind::Crc64,
        part_vid: GenerationId::new(20 + u64::from(part_number)).unwrap(),
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
        data_pg_id: 0,
        checksum: None,
    }
}

fn test_object_read_multipart_segment(
    bucket: &BucketName,
    key: &ObjectKey,
    part_number: u32,
) -> MultipartPartSegmentRecord {
    MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: UploadId::try_from("u".repeat(128)).unwrap(),
        version_id: VersionId::Null.to_u64(),
        part_number,
        segment_index: 0,
        size: 12,
        segment_crc64: 99,
        segment_okh: [7; 16],
        segment_vid: GenerationId::new(30 + u64::from(part_number)).unwrap(),
        data_pg_id: 0,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
    }
}

fn assert_object_read_snapshot_rejected(
    snapshot: &ObjectReadSnapshot,
    snapshot_mode: ObjectReadSnapshotMode,
) {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let identity = ObjectReadAuthSubjectIdentity::for_stored(&snapshot.stored);
    let err = client
        .validate_object_read_snapshot_response(
            snapshot,
            snapshot.stored.bucket(),
            snapshot.stored.key(),
            Some(snapshot.stored.version_id()),
            &identity,
            snapshot_mode,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate object read snapshot response",
            ..
        })
    ));
}

fn assert_direct_put_snapshot_rejected(
    snapshot: &DirectPutCommitStorageSnapshot,
    bucket: &BucketName,
    key: &ObjectKey,
) {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let err = client
        .validate_direct_put_commit_snapshot_response(snapshot, bucket, key)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate direct PUT commit snapshot response",
            ..
        })
    ));
}

fn assert_direct_put_snapshot_accepted(
    snapshot: &DirectPutCommitStorageSnapshot,
    bucket: &BucketName,
    key: &ObjectKey,
) {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    client
        .validate_direct_put_commit_snapshot_response(snapshot, bucket, key)
        .unwrap();
}

fn test_unix_storage_node_client() -> UnixStorageNodeClient {
    let tmp = test_util::tempdir();
    UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    )
}

fn test_unix_storage_node_client_with_rpc_admission_timeout(
    limit: usize,
    wait_timeout: Duration,
) -> UnixStorageNodeClient {
    let tmp = test_util::tempdir();
    UnixStorageNodeClient::with_rpc_admission(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
        Arc::new(UnixStorageNodeRpcAdmission::new_with_settings(
            crate::node_client::UnixStorageNodeRpcAdmissionSettings {
                limit,
                wait_timeout,
                control_wait_timeout: wait_timeout,
            },
        )),
        None,
    )
}

fn test_metadata_command(pg_id: u32, log_index: u64) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(pg_id),
            MetadataCommandLogIndex::new(log_index).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(
            crate::metadata_command::ReserveObjectGenerationCommand::new(
                crate::tests::bucket_name("metadata-rpc-bucket"),
                crate::tests::object_key("object"),
                crate::tests::stream_session_id("metadata-rpc"),
                GenerationId::new(1).unwrap(),
                123,
            ),
        ),
    )
}

#[path = "tests/unix_bucket_rpc.rs"]
mod unix_bucket_rpc;
#[path = "tests/unix_metadata_rpc.rs"]
mod unix_metadata_rpc;
#[path = "tests/unix_object_rpc.rs"]
mod unix_object_rpc;
