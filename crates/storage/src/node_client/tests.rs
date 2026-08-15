// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::thread;
use std::time::{Duration, Instant};

use crate::metadata_command::{
    ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
};
use crate::storage_node_server::{StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeServer};
use crate::storage_rpc::{
    decode_metadata_command_pending_slot_request, encode_metadata_command_acceptance_response,
    encode_metadata_command_applied_hashes_response, encode_metadata_command_bool_outcome_response,
    encode_metadata_command_next_id_response, encode_metadata_command_pending_slot_insert_response,
    encode_metadata_command_state_outcome_response,
    encode_placed_segment_shard_backfill_claim_optional_record_response,
    encode_placed_segment_shard_repair_claim_optional_record_response,
    encode_read_handle_acquire_response, encode_scavenger_observations_response,
    encode_storage_rpc_success_response, read_storage_rpc_frame_from, write_storage_rpc_frame_to,
    StorageRpcMetadataCommandAcceptanceResponse, StorageRpcMetadataCommandAppliedHashesResponse,
    StorageRpcMetadataCommandBoolOutcomeResponse, StorageRpcMetadataCommandNextIdResponse,
    StorageRpcMetadataCommandPendingSlotInsertResponse,
    StorageRpcMetadataCommandStateOutcomeResponse,
    StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse,
    StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse,
    StorageRpcReadHandleAcquireResponse, StorageRpcStreamError,
    STORAGE_RPC_CLIENT_RESPONSE_TIMEOUT,
};
use crate::types::{
    DeleteMarkerRecord, EtagKind, ObjectEncryption, ObjectLockState, SerializedMetadataBlob,
    SerializedSystemMetadataBlob, SerializedTagSet, StorageClass, StreamUploadPartSnapshot,
};
use crate::ShardScavengerObservationReason;
use crate::{RouteMapValidity, SegmentStoredBytesRequest, ShardIndex};

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
            metadata_read_route: None,
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

struct TestStorageNodeServerGuard {
    stop: Arc<std::sync::atomic::AtomicBool>,
    socket_path: std::path::PathBuf,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for TestStorageNodeServerGuard {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        let _ = std::os::unix::net::UnixStream::connect(&self.socket_path);
        if let Some(thread) = self.thread.take() {
            if let Err(panic) = thread.join() {
                if std::thread::panicking() {
                    return;
                }
                std::panic::resume_unwind(panic);
            }
        }
    }
}

fn spawn_test_storage_node_server(server: StorageNodeServer) -> TestStorageNodeServerGuard {
    let socket_path = server.socket_path_for_test();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let thread = thread::spawn(move || loop {
        let result = server.accept_one();
        if thread_stop.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        result.unwrap();
    });
    TestStorageNodeServerGuard {
        stop,
        socket_path,
        thread: Some(thread),
    }
}

fn rpc_requests_started_for_test(client: &UnixStorageNodeClient) -> u64 {
    client
        .next_request_id
        .load(std::sync::atomic::Ordering::Relaxed)
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
fn local_placed_shard_route_is_bound_to_exact_placement() {
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
    let bound_key = ShardKey::new(&[0x31; 16], 11, 0);
    let foreign_key = ShardKey::new(&[0x32; 16], 12, 0);
    let foreign_data = b"foreign active shard";
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
        .open_placed_shard_route(location, &bound_key)
        .unwrap();
    let bound_data = b"bound active shard";
    let ack = route.write_placed_shard(bound_data).unwrap();
    assert_eq!(route.read_placed_shard(ack).unwrap(), bound_data);
    route.delete_placed_shard().unwrap();
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
            .open_placed_shard_route(wrong_shard_location, &bound_key)
            .err()
            .expect("foreign shard index must be rejected before storage"),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "open active placed shard route",
        }
    ));
    let foreign_node_location = crate::cluster::ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new_for_test(PgId::new(0)),
        bound_key.shard_index(),
        NodeId::new(8),
    );
    assert!(matches!(
        client
            .open_placed_shard_route(foreign_node_location, &bound_key)
            .err()
            .expect("foreign active node must be rejected before storage"),
        StoreError::NodeNotFound { node_id: 8, .. }
    ));
    let foreign_pg_location = crate::cluster::ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new_for_test(PgId::new(1)),
        bound_key.shard_index(),
        NodeId::new(7),
    );
    assert!(matches!(
        client
            .open_placed_shard_route(foreign_pg_location, &bound_key)
            .err()
            .expect("foreign active data PG must be rejected before storage"),
        StoreError::PgNotFound { pg_id: 1 }
    ));
}

#[test]
fn unix_placed_shard_route_rejects_foreign_subject_before_rpc() {
    let client = test_unix_storage_node_client();
    let key = ShardKey::new(&[0x33; 16], 13, 0);
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let foreign_node_location = crate::cluster::ShardLocation::new(
        client.cluster_epoch,
        data_pg_id,
        key.shard_index(),
        NodeId::new(client.node_id.as_u32() + 1),
    );
    assert!(matches!(
        client
            .open_placed_shard_route(foreign_node_location, &key)
            .err()
            .expect("foreign active node must be rejected before RPC"),
        StoreError::NodeNotFound { .. }
    ));
    let wrong_shard_location = crate::cluster::ShardLocation::new(
        client.cluster_epoch,
        data_pg_id,
        ShardIndex::new(1),
        client.node_id,
    );
    assert!(matches!(
        client
            .open_placed_shard_route(wrong_shard_location, &key)
            .err()
            .expect("foreign active shard index must be rejected before RPC"),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "open active placed shard route",
        }
    ));
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let future_location = crate::cluster::ShardLocation::new(
        future_epoch,
        data_pg_id,
        key.shard_index(),
        client.node_id,
    );
    assert!(matches!(
        client
            .open_placed_shard_route(future_location, &key)
            .err()
            .expect("future active epoch must be rejected before RPC"),
        StoreError::StalePayloadOperation {
            operation_epoch,
            current_epoch,
            ..
        } if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_shard_read_handle_route_is_bound_to_exact_batch() {
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
    let epoch = ClusterEpoch::INITIAL;
    let key = ShardKey::new(&[0x34; 16], 14, 0);
    let location = crate::cluster::ShardLocation::new(
        epoch,
        DataPgId::new_for_test(PgId::new(0)),
        key.shard_index(),
        NodeId::new(7),
    );
    let mut lease = client
        .open_shard_read_handle_route(epoch, "bound-read", vec![(location, key.clone())])
        .and_then(|route| route.acquire())
        .unwrap();
    lease.release().unwrap();

    let wrong_epoch = ClusterEpoch::new(epoch.get() + 1).unwrap();
    assert!(matches!(
        client
            .open_shard_read_handle_route(
                wrong_epoch,
                "wrong-epoch-read",
                vec![(location, key.clone())],
            )
            .err()
            .expect("crossed route and placement epochs must fail before storage"),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "open shard read-handle route",
        }
    ));
    let foreign_node_location = crate::cluster::ShardLocation::new(
        epoch,
        DataPgId::new_for_test(PgId::new(0)),
        key.shard_index(),
        NodeId::new(8),
    );
    assert!(matches!(
        client
            .open_shard_read_handle_route(
                epoch,
                "foreign-node-read",
                vec![(foreign_node_location, key.clone())],
            )
            .err()
            .expect("foreign read-handle node must fail before storage"),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "open shard read-handle route",
        }
    ));
    let wrong_index_location = crate::cluster::ShardLocation::new(
        epoch,
        DataPgId::new_for_test(PgId::new(0)),
        ShardIndex::new(1),
        NodeId::new(7),
    );
    assert!(matches!(
        client
            .open_shard_read_handle_route(
                epoch,
                "wrong-index-read",
                vec![(wrong_index_location, key.clone())],
            )
            .err()
            .expect("crossed read-handle shard index must fail before storage"),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "open shard read-handle route",
        }
    ));
    let foreign_pg_location = crate::cluster::ShardLocation::new(
        epoch,
        DataPgId::new_for_test(PgId::new(1)),
        key.shard_index(),
        NodeId::new(7),
    );
    assert!(matches!(
        client
            .open_shard_read_handle_route(
                epoch,
                "foreign-pg-read",
                vec![(foreign_pg_location, key)],
            )
            .err()
            .expect("unavailable read-handle PG must fail before storage"),
        StoreError::PgNotFound { pg_id: 1 }
    ));
    assert!(matches!(
        client
            .open_shard_read_handle_route(epoch, "empty-read", Vec::new())
            .err()
            .expect("empty read-handle routes must fail before storage"),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "open shard read-handle route",
        }
    ));
}

#[test]
fn unix_shard_read_handle_route_rejects_foreign_subject_before_rpc() {
    let client = test_unix_storage_node_client();
    let epoch = client.cluster_epoch;
    let key = ShardKey::new(&[0x35; 16], 15, 0);
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let location =
        crate::cluster::ShardLocation::new(epoch, data_pg_id, key.shard_index(), client.node_id);
    let future_epoch = ClusterEpoch::new(epoch.get() + 1).unwrap();
    assert!(matches!(
        client
            .open_shard_read_handle_route(
                future_epoch,
                "future-read",
                vec![(location, key.clone())],
            )
            .err()
            .expect("future read-handle routes must fail before RPC"),
        StoreError::StalePayloadOperation {
            operation_epoch,
            current_epoch,
            ..
        } if operation_epoch == future_epoch && current_epoch == epoch
    ));
    let foreign_node_location = crate::cluster::ShardLocation::new(
        epoch,
        data_pg_id,
        key.shard_index(),
        NodeId::new(client.node_id.as_u32() + 1),
    );
    assert!(matches!(
        client
            .open_shard_read_handle_route(
                epoch,
                "foreign-node-read",
                vec![(foreign_node_location, key.clone())],
            )
            .err()
            .expect("foreign read-handle nodes must fail before RPC"),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "open shard read-handle route",
        }
    ));
    let wrong_index_location =
        crate::cluster::ShardLocation::new(epoch, data_pg_id, ShardIndex::new(1), client.node_id);
    assert!(matches!(
        client
            .open_shard_read_handle_route(
                epoch,
                "wrong-index-read",
                vec![(wrong_index_location, key)],
            )
            .err()
            .expect("crossed read-handle shard index must fail before RPC"),
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "open shard read-handle route",
        }
    ));
}

#[test]
fn local_shard_scavenger_routes_bind_exact_pgs() {
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
    let data_pg = DataPgId::new_for_test(PgId::new(0));
    assert!(client
        .open_shard_scavenger_data_route(ClusterEpoch::INITIAL, data_pg)
        .and_then(|route| route.list_scavenger_shard_files())
        .unwrap()
        .files
        .is_empty());
    let scan_pg = ObjectMetadataScanPgId::new_for_test(PgId::new(0));
    assert!(client
        .open_shard_scavenger_object_scan_route(ClusterEpoch::INITIAL, scan_pg)
        .and_then(|route| route.list_shard_scavenger_payload_references())
        .unwrap()
        .is_empty());

    assert!(matches!(
        client
            .open_shard_scavenger_data_route(
                ClusterEpoch::INITIAL,
                DataPgId::new_for_test(PgId::new(1)),
            )
            .err()
            .expect("unknown scavenger data PG must fail before storage"),
        StoreError::PgNotFound { pg_id: 1 }
    ));
    assert!(matches!(
        client
            .open_shard_scavenger_object_scan_route(
                ClusterEpoch::INITIAL,
                ObjectMetadataScanPgId::new_for_test(PgId::new(1)),
            )
            .err()
            .expect("unknown scavenger object PG must fail before storage"),
        StoreError::PgNotFound { pg_id: 1 }
    ));
}

#[test]
fn unix_shard_scavenger_routes_reject_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    assert!(matches!(
        client
            .open_shard_scavenger_data_route(
                future_epoch,
                DataPgId::new_for_test(PgId::new(0)),
            )
            .err()
            .expect("future scavenger data route must fail before RPC"),
        StoreError::StalePayloadOperation {
            operation_epoch,
            current_epoch,
            ..
        } if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
    assert!(matches!(
        client
            .open_shard_scavenger_object_scan_route(
                future_epoch,
                ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
            )
            .err()
            .expect("future scavenger object route must fail before RPC"),
        StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        } if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
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
        route.read_placed_shard_for_historical_inspection().unwrap(),
        (bound_data.to_vec(), bound_ack)
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
    let active_route = client
        .open_shard_ack_route(ClusterEpoch::INITIAL, data_pg_id)
        .unwrap();
    active_route
        .register_shard_acks(&[(&bound_key, bound_ack), (&foreign_key, foreign_ack)])
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
        active_route.load_shard_ack(&bound_key),
        Err(StoreError::NotFound)
    ));
    assert_eq!(
        active_route.load_shard_ack(&foreign_key).unwrap(),
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

fn test_shard_repair_work_item(data_pg_id: u32, seed: u8) -> PlacedSegmentShardRepairWorkItem {
    PlacedSegmentShardRepairWorkItem {
        request: SegmentStoredBytesRequest {
            data_pg_id,
            segment_okh: [seed; 16],
            segment_vid: GenerationId::new(u64::from(seed) + 1).unwrap(),
            stored_size: 1024,
            segment_crc64: u64::from(seed),
            ec: EcShape { k: 4, m: 2 },
        },
        shard_index: ShardIndex::new(seed % 6),
    }
}

fn test_shard_backfill_work_item(data_pg_id: u32, seed: u8) -> PlacedSegmentShardBackfillWorkItem {
    PlacedSegmentShardBackfillWorkItem {
        request: test_shard_repair_work_item(data_pg_id, seed).request,
        source_cluster_epoch: ClusterEpoch::INITIAL,
        desired_cluster_epoch: ClusterEpoch::new(2).unwrap(),
    }
}

#[test]
fn local_shard_ack_route_is_bound_to_active_epoch_and_data_pg() {
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
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let route = client
        .open_shard_ack_route(ClusterEpoch::INITIAL, data_pg_id)
        .unwrap();
    let repair = test_shard_repair_work_item(0, 11);
    let backfill = test_shard_backfill_work_item(0, 12);
    route
        .record_placed_segment_shard_repair(&repair, None)
        .unwrap();
    route
        .record_placed_segment_shard_backfill(&backfill, backfill.request.ec.m, None)
        .unwrap();
    assert_eq!(route.list_placed_segment_shard_repairs().unwrap().len(), 1);
    assert_eq!(
        route.list_placed_segment_shard_backfills().unwrap().len(),
        1
    );

    let foreign_repair = test_shard_repair_work_item(1, 13);
    let foreign_backfill = test_shard_backfill_work_item(1, 14);
    assert!(matches!(
        route.record_placed_segment_shard_repair(&foreign_repair, None),
        Err(StoreError::PayloadShardSetMismatch { .. })
    ));
    assert!(matches!(
        route.record_placed_segment_shard_backfill(
            &foreign_backfill,
            foreign_backfill.request.ec.m,
            None,
        ),
        Err(StoreError::PayloadShardSetMismatch { .. })
    ));
    assert_eq!(route.list_placed_segment_shard_repairs().unwrap().len(), 1);
    assert_eq!(
        route.list_placed_segment_shard_backfills().unwrap().len(),
        1
    );

    let future = ClusterEpoch::new(2).unwrap();
    let acquire = PlacedSegmentShardRepairClaimAcquire {
        claim_id: "foreign-epoch".to_string(),
        owner_token: "owner".to_string(),
        cluster_epoch: future,
        claimed_at: 10,
        lease_deadline: 20,
        now: 10,
    };
    assert!(matches!(
        route.acquire_placed_segment_shard_repair_claim(&acquire),
        Err(StoreError::StalePayloadOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future && current_epoch == ClusterEpoch::INITIAL
    ));
    assert!(matches!(
        client
            .open_shard_ack_route(ClusterEpoch::INITIAL, DataPgId::new_for_test(PgId::new(1)),)
            .err()
            .expect("unavailable active shard-ack PG must fail before storage"),
        StoreError::PgNotFound { pg_id: 1 }
    ));
}

#[test]
fn unix_shard_ack_route_rejects_foreign_epoch_and_work_before_rpc() {
    let client = test_unix_storage_node_client();
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get().saturating_add(1)).unwrap();
    assert!(matches!(
        client
            .open_shard_ack_route(future_epoch, data_pg_id)
            .err()
            .expect("future active shard-ack route must fail before RPC"),
        StoreError::StalePayloadOperation {
            operation_epoch,
            current_epoch,
            ..
        } if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));

    let route = client
        .open_shard_ack_route(client.cluster_epoch, data_pg_id)
        .unwrap();
    assert!(matches!(
        route.record_placed_segment_shard_repair(&test_shard_repair_work_item(1, 21), None),
        Err(StoreError::PayloadShardSetMismatch { .. })
    ));
    let foreign_backfill = test_shard_backfill_work_item(1, 22);
    assert!(matches!(
        route.record_placed_segment_shard_backfill(
            &foreign_backfill,
            foreign_backfill.request.ec.m,
            None,
        ),
        Err(StoreError::PayloadShardSetMismatch { .. })
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
fn local_recovery_abandonment_operations_honor_absolute_deadline_while_pg_locked() {
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
    let recovery =
        MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
            &client,
            PgId::new(0),
            ClusterEpoch::new(1).unwrap(),
        )
        .unwrap();
    let command = test_metadata_command(0, 1);
    let storage_node_for_holder = Arc::clone(&storage_node);
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = thread::spawn(move || {
        let held_pg = storage_node_for_holder.get_pg(0).unwrap();
        locked_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(held_pg);
    });
    locked_rx.recv().unwrap();

    for operation in ["acceptance", "record"] {
        let deadline = Instant::now() + Duration::from_millis(50);
        let error = match operation {
            "acceptance" => recovery
                .metadata_command_abandon_acceptance_until(&command, deadline)
                .map(|_| ()),
            "record" => recovery
                .record_metadata_command_abandoned_until(&command, deadline)
                .map(|_| ()),
            _ => unreachable!(),
        }
        .unwrap_err();
        assert!(
            matches!(error, StoreError::OperationDeadlineExceeded { .. }),
            "{operation} should stop at the absolute deadline, got {error:?}"
        );
    }

    release_tx.send(()).unwrap();
    holder.join().unwrap();
    drop(recovery);
    assert_eq!(
        storage_node
            .get_pg(0)
            .unwrap()
            .max_metadata_command_log_index(ClusterEpoch::new(1).unwrap())
            .unwrap(),
        0,
        "deadline expiry must not record an abandonment"
    );
}

#[test]
fn local_recovery_replica_route_rejects_crossed_authority_without_mutation() {
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
    let epoch = ClusterEpoch::new(1).unwrap();
    let source = test_metadata_command(0, 1);
    let abandoned = test_metadata_command(0, 2);
    let command = test_metadata_command(0, 3);
    let wrong_pg = test_metadata_command(1, 4);
    let future_epoch = ClusterEpoch::new(2).unwrap();
    let future_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(future_epoch, PgId::new(0), command.id().log_index()),
        command.payload().clone(),
    );
    let (certificate_source, reissued, cleanup, unrelated) =
        test_metadata_command_recovery_chain(0);
    let cleanup_before_abandoned =
        MetadataCommandEnvelope::new(reissued.id(), cleanup.payload().clone());

    for (authorized_source, abandoned_source, command) in [
        (&wrong_pg, Some(&abandoned), &command),
        (&source, Some(&wrong_pg), &command),
        (&source, Some(&abandoned), &wrong_pg),
    ] {
        assert!(matches!(
            client
                .open_metadata_command_recovery_replica_apply_route(
                    PgId::new(0),
                    epoch,
                    authorized_source,
                    abandoned_source,
                    command,
                )
                .err()
                .expect("crossed recovery command subject must be rejected"),
            StoreError::MetadataCommandWrongPg {
                command_pg_id: 1,
                target_pg_id: 0,
                ..
            }
        ));
    }
    assert!(matches!(
        client
            .open_metadata_command_recovery_replica_apply_route(
                PgId::new(0),
                epoch,
                &source,
                Some(&abandoned),
                &future_command,
            )
            .err()
            .expect("crossed recovery command epoch must be rejected"),
        StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        } if operation_epoch == future_epoch && current_epoch == epoch
    ));
    assert!(matches!(
        client
            .open_metadata_command_recovery_replica_abandon_route(
                PgId::new(0),
                epoch,
                &wrong_pg,
                Some(&abandoned),
                &command,
            )
            .err()
            .expect("crossed tombstone recovery subject must be rejected"),
        StoreError::MetadataCommandWrongPg {
            command_pg_id: 1,
            target_pg_id: 0,
            ..
        }
    ));

    for (authorized_source, abandoned_source, command) in [
        (&certificate_source, None, &unrelated),
        (&certificate_source, Some(&reissued), &reissued),
        (&certificate_source, None, &cleanup),
        (&certificate_source, Some(&unrelated), &cleanup),
        (
            &certificate_source,
            Some(&reissued),
            &cleanup_before_abandoned,
        ),
        (&certificate_source, Some(&reissued), &cleanup),
    ] {
        assert!(matches!(
            client
                .open_metadata_command_recovery_replica_apply_route(
                    PgId::new(0),
                    epoch,
                    authorized_source,
                    abandoned_source,
                    command,
                )
                .err()
                .expect("invalid recovery certificate must reject replica apply"),
            StoreError::RouteCapabilitySubjectMismatch {
                operation: "open metadata command recovery replica route"
            }
        ));
        assert!(matches!(
            client
                .open_metadata_command_recovery_replica_abandon_route(
                    PgId::new(0),
                    epoch,
                    authorized_source,
                    abandoned_source,
                    command,
                )
                .err()
                .expect("invalid recovery certificate must reject replica abandonment"),
            StoreError::RouteCapabilitySubjectMismatch {
                operation: "open metadata command recovery replica route"
            }
        ));
    }

    for pg_id in [0, 1] {
        let pg = storage_node.get_pg(pg_id).unwrap();
        assert_eq!(pg.max_metadata_command_log_index(epoch).unwrap(), 0);
        assert_eq!(pg.max_metadata_command_log_index(future_epoch).unwrap(), 0);
    }

    let pg = storage_node.get_pg(0).unwrap();
    pg.record_metadata_command_abandoned(client.node_id.as_u32(), &certificate_source)
        .unwrap();
    pg.record_metadata_command_abandoned(client.node_id.as_u32(), &reissued)
        .unwrap();
    drop(pg);
    drop(
        client
            .open_metadata_command_recovery_replica_apply_route(
                PgId::new(0),
                epoch,
                &certificate_source,
                Some(&reissued),
                &cleanup,
            )
            .expect("durably certified cleanup must open an embedded replica apply route"),
    );
    drop(
        client
            .open_metadata_command_recovery_replica_abandon_route(
                PgId::new(0),
                epoch,
                &certificate_source,
                Some(&reissued),
                &cleanup,
            )
            .expect("durably certified cleanup must open an embedded replica abandonment route"),
    );
    assert_eq!(
        storage_node
            .get_pg(0)
            .unwrap()
            .max_metadata_command_log_index(epoch)
            .unwrap(),
        2,
        "opening a certified route must not itself mutate metadata"
    );
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
                pre_state_digest: crate::control_plane::CanonicalStateDigest::for_test(0),
                post_state_digest: crate::control_plane::CanonicalStateDigest::for_test(1),
            }],
            crate::control_plane::CanonicalStateDigest::for_test(1),
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
                pre_state_digest: crate::control_plane::CanonicalStateDigest::for_test(0),
                post_state_digest: crate::control_plane::CanonicalStateDigest::for_test(1),
            }],
            crate::control_plane::CanonicalStateDigest::for_test(1),
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
fn local_object_listing_metadata_route_binds_scan_pg() {
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
    let bucket = crate::tests::bucket_name("object-listing-route-bucket");

    assert!(matches!(
        client
            .open_object_listing_metadata_route(
                ClusterEpoch::INITIAL,
                ObjectMetadataScanPgId::new_for_test(PgId::new(1)),
                MetadataReadAuthorization::active(PgId::new(1)),
            )
            .err()
            .expect("unavailable object-listing PG must fail before storage"),
        BucketSnapshotLoadError::Store(StoreError::PgNotFound { pg_id: 1 })
    ));

    let route = client
        .open_object_listing_metadata_route(
            ClusterEpoch::INITIAL,
            ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
            MetadataReadAuthorization::active(PgId::new(0)),
        )
        .unwrap();
    assert!(route
        .list_objects_page(&ListObjectsReq {
            bucket: bucket.clone(),
            prefix: None,
            start_after: None,
            start_at: None,
            max_keys: 1,
        })
        .unwrap()
        .objects
        .is_empty());
    assert!(route
        .list_object_versions_page(&ListObjectVersionsReq {
            bucket: bucket.clone(),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            start_at: None,
            max_keys: 1,
        })
        .unwrap()
        .versions
        .is_empty());
    assert!(route
        .list_multipart_uploads_page(&ListMultipartUploadsReq {
            bucket,
            prefix: None,
            page_start: None,
            max_uploads: 1,
        })
        .unwrap()
        .uploads
        .is_empty());
}

#[test]
fn local_object_listing_metadata_route_rejects_foreign_scan_pg_rows() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0, 1],
            EcShape { k: 4, m: 2 },
        )
        .unwrap(),
    );
    let bucket = crate::tests::bucket_name("foreign-local-listing-row-bucket");
    let key = (0..10_000)
        .map(|index| crate::tests::object_key(format!("foreign-local-listing-row-{index}")))
        .find(|key| storage_node.object_metadata_pg_for(&bucket, key).get() == 1)
        .expect("test must find a key placed on the foreign scan PG");
    let upload_id = UploadId::try_from("u".repeat(crate::UPLOAD_ID_LEN)).unwrap();
    let pg = storage_node.get_pg(0).unwrap();
    PgMetadataStore::put_object_with_segments(
        &*pg,
        &PutLiveObjectReq {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::new(10).unwrap(),
            size: 0,
            etag: ObjectEtag::single_part(0),
            ec: EcShape { k: 4, m: 2 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        },
        &[],
    )
    .unwrap();
    PgMetadataStore::create_multipart_upload(
        &*pg,
        &CreateMultipartUploadReq {
            upload_id,
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: OwnerIdentity::from_principal("owner"),
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        },
    )
    .unwrap();
    drop(pg);

    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let route = client
        .open_object_listing_metadata_route(
            ClusterEpoch::INITIAL,
            ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
            MetadataReadAuthorization::active(PgId::new(0)),
        )
        .unwrap();
    let Err(object_error) = route.list_objects_page(&ListObjectsReq {
        bucket: bucket.clone(),
        prefix: None,
        start_after: None,
        start_at: None,
        max_keys: 1,
    }) else {
        panic!("misplaced object listing row must fail closed");
    };
    let Err(version_error) = route.list_object_versions_page(&ListObjectVersionsReq {
        bucket: bucket.clone(),
        prefix: None,
        key_marker: None,
        version_id_marker: None,
        start_at: None,
        max_keys: 1,
    }) else {
        panic!("misplaced object-version listing row must fail closed");
    };
    let Err(upload_error) = route.list_multipart_uploads_page(&ListMultipartUploadsReq {
        bucket,
        prefix: None,
        page_start: None,
        max_uploads: 1,
    }) else {
        panic!("misplaced multipart-upload listing row must fail closed");
    };

    for (error, operation) in [
        (object_error, "validate object listing response scan PG"),
        (
            version_error,
            "validate object version listing response scan PG",
        ),
        (
            upload_error,
            "validate multipart upload listing response scan PG",
        ),
    ] {
        assert!(matches!(
            error,
            BucketSnapshotLoadError::Store(StoreError::RouteCapabilitySubjectMismatch {
                operation: actual,
            }) if actual == operation
        ));
    }
}

#[test]
fn unix_object_listing_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    assert!(matches!(
        client
            .open_object_listing_metadata_route(
                future_epoch,
                ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
                MetadataReadAuthorization::active(PgId::new(0)),
            )
            .err()
            .expect("future object-listing route must fail before RPC"),
        BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_object_read_metadata_route_binds_exact_object_subject() {
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
    let bucket = crate::tests::bucket_name("object-read-route-bucket");
    let key = crate::tests::object_key("object-read-route-key");
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_object_read_metadata_route(
                ClusterEpoch::INITIAL,
                wrong_pg,
                &bucket,
                &key,
                MetadataReadAuthorization::active(wrong_pg.pg_id()),
            )
            .err()
            .expect("crossed object-read PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open object read metadata route",
        })
    ));

    let route = client
        .open_object_read_metadata_route(
            ClusterEpoch::INITIAL,
            correct_pg,
            &bucket,
            &key,
            MetadataReadAuthorization::active(correct_pg.pg_id()),
        )
        .unwrap();
    assert!(matches!(
        route.load_object_read_auth_subject(None).unwrap_err(),
        ObjectPgActionError::Metadata(MetadataError::ObjectNotFound)
    ));
}

#[test]
fn local_object_generation_metadata_route_binds_exact_object_subject() {
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
    let bucket = crate::tests::bucket_name("object-generation-route-bucket");
    let key = crate::tests::object_key("object-generation-route-key");
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_object_generation_metadata_route(ClusterEpoch::INITIAL, wrong_pg, &bucket, &key,)
            .err()
            .expect("crossed object-generation PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open object generation metadata route",
        })
    ));

    let route = client
        .open_object_generation_metadata_route(ClusterEpoch::INITIAL, correct_pg, &bucket, &key)
        .unwrap();
    assert_eq!(
        route.next_object_generation_id().unwrap(),
        GenerationId::MIN
    );
}

#[test]
fn unix_object_generation_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let bucket = crate::tests::bucket_name("unix-object-generation-route-bucket");
    let key = crate::tests::object_key("unix-object-generation-route-key");
    assert!(matches!(
        client
            .open_object_generation_metadata_route(
                future_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
            )
            .err()
            .expect("future object-generation route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_object_version_metadata_route_binds_exact_object_subject() {
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
    let bucket = crate::tests::bucket_name("object-version-route-bucket");
    let key = crate::tests::object_key("object-version-route-key");
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_object_version_metadata_route(ClusterEpoch::INITIAL, wrong_pg, &bucket, &key,)
            .err()
            .expect("crossed object-version PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open object version metadata route",
        })
    ));

    let route = client
        .open_object_version_metadata_route(ClusterEpoch::INITIAL, correct_pg, &bucket, &key)
        .unwrap();
    assert_eq!(
        route.next_object_version_id().unwrap(),
        VersionId::from_u64(1)
    );
    assert_eq!(
        route.next_completion_object_version_id().unwrap(),
        VersionId::from_u64(1)
    );
}

#[test]
fn unix_object_version_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let bucket = crate::tests::bucket_name("unix-object-version-route-bucket");
    let key = crate::tests::object_key("unix-object-version-route-key");
    assert!(matches!(
        client
            .open_object_version_metadata_route(
                future_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
            )
            .err()
            .expect("future object-version route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_put_object_metadata_route_binds_exact_object_subject() {
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
    let bucket = crate::tests::bucket_name("put-object-metadata-route-bucket");
    let key = crate::tests::object_key("put-object-metadata-route-key");
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_put_object_metadata_route(ClusterEpoch::INITIAL, wrong_pg, &bucket, &key)
            .err()
            .expect("crossed PUT-object-metadata PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open PUT object metadata route",
        })
    ));

    let route = client
        .open_put_object_metadata_route(ClusterEpoch::INITIAL, correct_pg, &bucket, &key)
        .unwrap();
    assert!(matches!(
        route.load_put_object_metadata_snapshot(None).unwrap_err(),
        ObjectPgActionError::Metadata(MetadataError::ObjectNotFound)
    ));
}

#[test]
fn unix_put_object_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let bucket = crate::tests::bucket_name("unix-put-object-metadata-route-bucket");
    let key = crate::tests::object_key("unix-put-object-metadata-route-key");
    assert!(matches!(
        client
            .open_put_object_metadata_route(
                future_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
            )
            .err()
            .expect("future PUT-object-metadata route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_multipart_creation_metadata_route_binds_exact_object_subject() {
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
    let bucket = crate::tests::bucket_name("multipart-creation-route-bucket");
    let key = crate::tests::object_key("multipart-creation-route-key");
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_multipart_upload_creation_metadata_route(
                ClusterEpoch::INITIAL,
                wrong_pg,
                &bucket,
                &key,
            )
            .err()
            .expect("crossed multipart-creation PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open multipart upload creation metadata route",
        })
    ));

    let route = client
        .open_multipart_upload_creation_metadata_route(
            ClusterEpoch::INITIAL,
            correct_pg,
            &bucket,
            &key,
        )
        .unwrap();
    let request = CreateMultipartUploadReq {
        upload_id: crate::tests::multipart_upload_id("multipart-creation-route"),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        initiator: OwnerIdentity::from_principal("owner"),
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: ObjectEncryption::None,
    };
    assert_eq!(
        route
            .matching_multipart_upload_initiated_at(&request, None)
            .unwrap(),
        None
    );

    let mut proof = test_bucket_write_reservation_proof(bucket, &key);
    proof.operation_kind = CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string();
    let pg = storage_node.get_pg(correct_pg.get()).unwrap();
    PgMetadataStore::create_multipart_upload(&*pg, &request).unwrap();
    let expected_command = CreateMultipartUploadCommand::from_parts_for_test(
        PgMetadataStore::get_multipart_upload(&*pg, &request.upload_id).unwrap(),
        proof.clone(),
    );
    drop(pg);
    assert_eq!(
        route
            .matching_multipart_upload_initiated_at(&request, Some(&expected_command))
            .unwrap(),
        Some(expected_command.upload().initiated_at)
    );
    let mut crossed_id = request.clone();
    crossed_id.upload_id = crate::tests::multipart_upload_id("multipart-creation-route-crossed-id");
    let mut crossed_metadata = request.clone();
    crossed_metadata.metadata_blob = SerializedMetadataBlob::new(vec![1]);
    for (case, crossed_request) in [("upload ID", crossed_id), ("metadata", crossed_metadata)] {
        assert!(
            matches!(
                route
                    .matching_multipart_upload_initiated_at(
                        &crossed_request,
                        Some(&expected_command),
                    )
                    .unwrap_err(),
                ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "match multipart upload creation",
                })
            ),
            "crossed multipart creation {case} must not match the expected command"
        );
    }

    let mut crossed_operation = proof.clone();
    crossed_operation.operation_kind = PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND.to_string();
    let mut crossed_target = proof.clone();
    crossed_target.target_context = Some("multipart-creation-route-other-key".to_string());
    let mut crossed_epoch = proof;
    crossed_epoch.cluster_epoch = ClusterEpoch::new(2).unwrap();
    for crossed_proof in [crossed_operation, crossed_target, crossed_epoch] {
        assert!(matches!(
            route
                .build_create_multipart_upload_command(BuildCreateMultipartUploadCommandReq {
                    request: &request,
                    expected_current: None,
                    bucket_write_reservation: &crossed_proof,
                },)
                .unwrap_err(),
            ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
                operation: "build create multipart upload command",
            })
        ));
    }
}

#[test]
fn unix_multipart_creation_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let bucket = crate::tests::bucket_name("unix-multipart-creation-route-bucket");
    let key = crate::tests::object_key("unix-multipart-creation-route-key");
    assert!(matches!(
        client
            .open_multipart_upload_creation_metadata_route(
                future_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
            )
            .err()
            .expect("future multipart-creation route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_multipart_upload_lookup_metadata_route_binds_exact_object_subject() {
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
    let bucket = crate::tests::bucket_name("multipart-lookup-route-bucket");
    let key = crate::tests::object_key("multipart-lookup-route-key");
    let upload_id = crate::tests::multipart_upload_id("multipart-lookup-route-upload");
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_multipart_upload_lookup_metadata_route(
                ClusterEpoch::INITIAL,
                wrong_pg,
                &bucket,
                &key,
            )
            .err()
            .expect("crossed multipart lookup PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open multipart upload lookup metadata route",
        })
    ));

    let route = client
        .open_multipart_upload_lookup_metadata_route(
            ClusterEpoch::INITIAL,
            correct_pg,
            &bucket,
            &key,
        )
        .unwrap();
    assert!(matches!(
        route.load_in_progress_multipart_upload(&upload_id),
        Err(ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { upload_id: missing }))
            if missing == upload_id.as_str()
    ));
}

#[test]
fn unix_multipart_upload_lookup_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let bucket = crate::tests::bucket_name("unix-multipart-lookup-route-bucket");
    let key = crate::tests::object_key("unix-multipart-lookup-route-key");
    assert!(matches!(
        client
            .open_multipart_upload_lookup_metadata_route(
                future_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
            )
            .err()
            .expect("future multipart lookup route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_authorized_multipart_upload_metadata_route_binds_exact_upload_subject() {
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
    let bucket = crate::tests::bucket_name("authorized-multipart-route-bucket");
    let key = crate::tests::object_key("authorized-multipart-route-key");
    let upload_id = crate::tests::multipart_upload_id("authorized-multipart-route-upload");
    let authorized_upload =
        AuthorizedMultipartUploadRecord::assume_authorized(test_multipart_upload_record(
            bucket.clone(),
            key.clone(),
            upload_id.clone(),
            UploadState::InProgress,
        ));
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_authorized_multipart_upload_metadata_route(
                ClusterEpoch::INITIAL,
                wrong_pg,
                &authorized_upload,
            )
            .err()
            .expect("crossed authorized multipart PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open authorized multipart upload metadata route",
        })
    ));

    let route = client
        .open_authorized_multipart_upload_metadata_route(
            ClusterEpoch::INITIAL,
            correct_pg,
            &authorized_upload,
        )
        .unwrap();
    assert!(matches!(
        route.load_multipart_completion_preflight(),
        Err(ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { upload_id: missing }))
            if missing == upload_id.as_str()
    ));
}

#[test]
fn unix_authorized_multipart_upload_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let authorized_upload =
        AuthorizedMultipartUploadRecord::assume_authorized(test_multipart_upload_record(
            crate::tests::bucket_name("unix-authorized-multipart-route-bucket"),
            crate::tests::object_key("unix-authorized-multipart-route-key"),
            crate::tests::multipart_upload_id("unix-authorized-multipart-route-upload"),
            UploadState::InProgress,
        ));
    assert!(matches!(
        client
            .open_authorized_multipart_upload_metadata_route(
                future_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &authorized_upload,
            )
            .err()
            .expect("future authorized multipart route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_multipart_completion_mutation_metadata_route_binds_exact_object_subject() {
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
    let bucket = crate::tests::bucket_name("multipart-completion-route-bucket");
    let key = crate::tests::object_key("multipart-completion-route-key");
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_multipart_completion_mutation_metadata_route(
                ClusterEpoch::INITIAL,
                wrong_pg,
                &bucket,
                &key,
            )
            .err()
            .expect("crossed multipart completion PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open multipart completion mutation metadata route",
        })
    ));

    let route = client
        .open_multipart_completion_mutation_metadata_route(
            ClusterEpoch::INITIAL,
            correct_pg,
            &bucket,
            &key,
        )
        .unwrap();
    assert!(route.load_stale_payload_source().unwrap().is_none());
}

#[test]
fn unix_multipart_completion_mutation_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let bucket = crate::tests::bucket_name("unix-multipart-completion-route-bucket");
    let key = crate::tests::object_key("unix-multipart-completion-route-key");
    assert!(matches!(
        client
            .open_multipart_completion_mutation_metadata_route(
                future_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
            )
            .err()
            .expect("future multipart completion route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_multipart_abort_mutation_metadata_route_binds_exact_upload_subject() {
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
    let bucket = crate::tests::bucket_name("multipart-abort-route-bucket");
    let key = crate::tests::object_key("multipart-abort-route-key");
    let upload_id = crate::tests::multipart_upload_id("multipart-abort-route-upload");
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_multipart_abort_mutation_metadata_route(
                ClusterEpoch::INITIAL,
                wrong_pg,
                &bucket,
                &key,
                &upload_id,
            )
            .err()
            .expect("crossed multipart abort PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open multipart abort mutation metadata route",
        })
    ));

    let route = client
        .open_multipart_abort_mutation_metadata_route(
            ClusterEpoch::INITIAL,
            correct_pg,
            &bucket,
            &key,
            &upload_id,
        )
        .unwrap();
    assert!(route.load_cleanup().unwrap().is_none());
}

#[test]
fn unix_multipart_abort_mutation_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let bucket = crate::tests::bucket_name("unix-multipart-abort-route-bucket");
    let key = crate::tests::object_key("unix-multipart-abort-route-key");
    let upload_id = crate::tests::multipart_upload_id("unix-multipart-abort-route-upload");
    assert!(matches!(
        client
            .open_multipart_abort_mutation_metadata_route(
                future_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
                &upload_id,
            )
            .err()
            .expect("future multipart abort route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_stream_upload_creation_metadata_route_binds_exact_object_and_authority() {
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
    let bucket = crate::tests::bucket_name("stream-creation-route-bucket");
    let key = crate::tests::object_key("stream-creation-route-key");
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_stream_upload_creation_metadata_route(
                ClusterEpoch::INITIAL,
                wrong_pg,
                &bucket,
                &key,
            )
            .err()
            .expect("crossed stream-creation PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open stream upload creation metadata route",
        })
    ));

    let route = client
        .open_stream_upload_creation_metadata_route(
            ClusterEpoch::INITIAL,
            correct_pg,
            &bucket,
            &key,
        )
        .unwrap();
    let request = CreateStreamUploadReq {
        session_id: crate::tests::stream_session_id("create-route"),
        bucket: bucket.clone(),
        key: key.clone(),
        target: StreamUploadTarget::PutObject,
        encryption: ObjectEncryption::None,
    };
    assert!(!route.matching_stream_upload_exists(&request, None).unwrap());

    let mut proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    proof.operation_kind = PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
    let command = route
        .build_create_stream_upload_command(BuildCreateStreamUploadCommandReq {
            request: &request,
            cleanup_after: Some(10_000),
            precondition: CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                require_generation_reservation: false,
            },
            bucket_write_reservation: &proof,
        })
        .unwrap();
    assert!(matches!(
        command.payload(),
        MetadataCommandPayload::CreateStreamUpload(create)
            if create.matches_request(&request)
                && create.bucket_write_reservation == proof
    ));
    let MetadataCommandPayload::CreateStreamUpload(create_command) = command.payload() else {
        panic!("expected create stream upload command");
    };
    assert!(
        !route
            .matching_stream_upload_exists(&request, Some(create_command))
            .unwrap(),
        "an authenticated matching command must still report an absent durable session"
    );
    let mut crossed_command = create_command.as_ref().clone();
    crossed_command.session.key = crate::tests::object_key("crossed-stream-command-key");
    assert!(matches!(
        route
            .matching_stream_upload_exists(&request, Some(&crossed_command))
            .unwrap_err(),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "match stream upload creation",
        })
    ));

    let mut crossed_request = request.clone();
    crossed_request.key = crate::tests::object_key("crossed-stream-creation-route-key");
    assert!(matches!(
        route
            .matching_stream_upload_exists(&crossed_request, None)
            .unwrap_err(),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "match stream upload creation",
        })
    ));

    let mut crossed_proof = proof;
    crossed_proof.operation_kind = PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND.to_string();
    assert!(matches!(
        route
            .build_create_stream_upload_command(BuildCreateStreamUploadCommandReq {
                request: &request,
                cleanup_after: None,
                precondition: CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {
                    require_generation_reservation: false,
                },
                bucket_write_reservation: &crossed_proof,
            })
            .unwrap_err(),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "build create stream upload command",
        })
    ));
}

#[test]
fn unix_stream_upload_creation_metadata_route_rejects_foreign_subject_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let bucket = crate::tests::bucket_name("unix-stream-creation-route-bucket");
    let key = crate::tests::object_key("unix-stream-creation-route-key");
    let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
    assert!(matches!(
        client
            .open_stream_upload_creation_metadata_route(future_epoch, pg_id, &bucket, &key)
            .err()
            .expect("future stream-creation route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));

    let route = client
        .open_stream_upload_creation_metadata_route(client.cluster_epoch, pg_id, &bucket, &key)
        .unwrap();
    let crossed_request = CreateStreamUploadReq {
        session_id: crate::tests::stream_session_id("unix-route"),
        bucket,
        key: crate::tests::object_key("unix-stream-creation-route-crossed-key"),
        target: StreamUploadTarget::PutObject,
        encryption: ObjectEncryption::None,
    };
    assert!(matches!(
        route
            .matching_stream_upload_exists(&crossed_request, None)
            .unwrap_err(),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "match stream upload creation",
        })
    ));
}

#[test]
fn local_object_delete_metadata_route_binds_exact_object_subject() {
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
    let bucket = crate::tests::bucket_name("object-delete-route-bucket");
    let key = crate::tests::object_key("object-delete-route-key");
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_object_delete_metadata_route(ClusterEpoch::INITIAL, wrong_pg, &bucket, &key)
            .err()
            .expect("crossed object-delete PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open object delete metadata route",
        })
    ));

    let route = client
        .open_object_delete_metadata_route(ClusterEpoch::INITIAL, correct_pg, &bucket, &key)
        .unwrap();
    assert_eq!(
        route.load_current_object_delete_snapshot().unwrap(),
        ObjectDeleteStorageSnapshot {
            stored: None,
            target: None,
        }
    );

    let mut crossed_proof = test_bucket_write_reservation_proof(bucket, &key);
    crossed_proof.operation_kind = PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND.to_string();
    assert!(matches!(
        route
            .build_delete_current_object_command(BuildDeleteCurrentObjectCommandReq {
                expected_current: None,
                expected_target: None,
                bucket_write_reservation: &crossed_proof,
            })
            .unwrap_err(),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "build delete-current object command",
        })
    ));
}

#[test]
fn unix_object_delete_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let bucket = crate::tests::bucket_name("unix-object-delete-route-bucket");
    let key = crate::tests::object_key("unix-object-delete-route-key");
    assert!(matches!(
        client
            .open_object_delete_metadata_route(
                future_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
            )
            .err()
            .expect("future object-delete route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn local_direct_put_metadata_route_binds_exact_object_subject() {
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
    let bucket = crate::tests::bucket_name("direct-put-route-bucket");
    let key = crate::tests::object_key("direct-put-route-key");
    let correct_pg = storage_node.object_metadata_pg_for(&bucket, &key);
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(1 - correct_pg.get()));

    assert!(matches!(
        client
            .open_direct_put_metadata_route(ClusterEpoch::INITIAL, wrong_pg, &bucket, &key)
            .err()
            .expect("crossed direct-PUT PG must fail before storage"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open direct PUT metadata route",
        })
    ));

    let route = client
        .open_direct_put_metadata_route(ClusterEpoch::INITIAL, correct_pg, &bucket, &key)
        .unwrap();
    let reservation_id = crate::tests::stream_session_id("dp-route");
    assert!(matches!(
        route
            .load_direct_put_commit_snapshot(&reservation_id, GenerationId::MIN)
            .unwrap_err(),
        ObjectPgActionError::Metadata(MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
}

#[test]
fn unix_direct_put_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let bucket = crate::tests::bucket_name("unix-direct-put-route-bucket");
    let key = crate::tests::object_key("unix-direct-put-route-key");
    assert!(matches!(
        client
            .open_direct_put_metadata_route(
                future_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
            )
            .err()
            .expect("future direct-PUT route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
}

#[test]
fn unix_object_read_metadata_route_rejects_foreign_epoch_before_rpc() {
    let client = test_unix_storage_node_client();
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let bucket = crate::tests::bucket_name("unix-object-read-route-bucket");
    let key = crate::tests::object_key("unix-object-read-route-key");
    assert!(matches!(
        client
            .open_object_read_metadata_route(
                future_epoch,
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
                MetadataReadAuthorization::active(PgId::new(0)),
            )
            .err()
            .expect("future object-read route must fail before RPC"),
        ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        }) if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
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
    let route = client
        .open_bucket_metadata_route(
            ClusterEpoch::new(1).unwrap(),
            BucketPgId::new_for_test(PgId::new(0)),
            &bucket,
        )
        .unwrap();
    let err = route
        .build_advance_multipart_completion_barrier_command(command_id, "object-key", &proof)
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

#[test]
fn stream_upload_match_binds_cleanup_deadline_for_put_and_upload_part() {
    let bucket = crate::tests::bucket_name("stream-cleanup-match-bucket");
    let key = crate::tests::object_key("stream-cleanup-match-key");
    let upload_id = crate::tests::multipart_upload_id("stream-cleanup-match-upload");
    for (label, target, operation_kind) in [
        (
            "c7",
            StreamUploadTarget::PutObject,
            PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        ),
        (
            "c8",
            StreamUploadTarget::UploadPart {
                upload_id,
                part_number: 1,
            },
            crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
        ),
    ] {
        let session_id = SessionId::try_from(label.repeat(16)).unwrap();
        let request = CreateStreamUploadReq {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: target.clone(),
            encryption: ObjectEncryption::None,
        };
        let proof = crate::metadata_command::BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: format!("{label}-reservation"),
            owner_token: format!("{label}-owner"),
            cluster_epoch: ClusterEpoch::INITIAL,
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: operation_kind.to_string(),
            created_at: 10,
            lease_deadline: 20,
            target_context: Some(key.as_str().to_string()),
        };
        let command =
            CreateStreamUploadCommand::from_request_with_bucket_write_reservation_and_cleanup_deadline(
                request,
                10,
                Some(100),
                proof.clone(),
            );
        let mut existing = StreamUploadRecord {
            session_id,
            bucket: bucket.clone(),
            key: key.clone(),
            target,
            state: StreamUploadState::InProgress,
            created_at: command.session.created_at,
            cleanup_after: command.cleanup_after,
            encryption: ObjectEncryption::None,
            next_segment_vid: command.initial_next_segment_vid,
            bucket_write_reservation: matches!(
                command.session.target,
                StreamUploadTarget::PutObject
            )
            .then_some(proof),
        };

        assert!(stream_upload_matches_command(&existing, &command));
        existing.cleanup_after = Some(101);
        assert!(
            !stream_upload_matches_command(&existing, &command),
            "{label} must not treat a different immutable cleanup deadline as idempotent"
        );
    }
}

#[test]
fn local_stream_session_route_binds_pg_object_and_session_before_effects() {
    let tmp = test_util::tempdir();
    let storage_node = Arc::new(
        crate::node::SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0],
            EcShape { k: 1, m: 0 },
        )
        .unwrap(),
    );
    let client = LocalStorageNodeClient::new(NodeId::new(7), Arc::clone(&storage_node));
    let bucket = crate::tests::bucket_name("scoped-stream-session-bucket");
    let key = crate::tests::object_key("scoped-stream-session-key");
    let first_session = crate::tests::stream_session_id("route-first");
    let second_session = crate::tests::stream_session_id("route-second");
    let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
    let pg = storage_node.get_pg(0).unwrap();
    for session_id in [&first_session, &second_session] {
        PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, session_id).unwrap();
        PgMetadataStore::create_stream_upload(
            &*pg,
            &CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::PutObject,
                encryption: ObjectEncryption::None,
            },
        )
        .unwrap();
    }
    drop(pg);

    assert!(matches!(
        client
            .open_stream_upload_session_metadata_route(
                ClusterEpoch::INITIAL,
                ObjectMetadataPgId::new_for_test(PgId::new(1)),
                &bucket,
                &key,
                &first_session,
            )
            .err()
            .expect("wrong stream-session PG must be rejected at route construction"),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "open stream upload session metadata route",
        })
    ));

    let route = client
        .open_stream_upload_session_metadata_route(
            ClusterEpoch::INITIAL,
            pg_id,
            &bucket,
            &key,
            &first_session,
        )
        .unwrap();
    assert_eq!(route.load_session().unwrap().session_id, first_session);
    assert!(route.load_segments().unwrap().is_empty());

    let append = PrepareStreamUploadSegmentAppendReq {
        session_id: first_session.clone(),
        segment_index: 0,
        size: 12,
        segment_crc64: 41,
        payload_crc64: 41,
        segment_okh: [0x41; 16],
    };
    let (_, prepared) = route
        .prepare_segment_append(
            &append,
            AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL),
        )
        .unwrap();
    assert_eq!(prepared.session_id, first_session);

    let second_before = storage_node
        .get_pg(0)
        .unwrap()
        .get_stream_upload(&second_session)
        .unwrap();
    let mut crossed_append = append;
    crossed_append.session_id = second_session.clone();
    assert!(matches!(
        route
            .prepare_segment_append(
                &crossed_append,
                AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL),
            )
            .unwrap_err(),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "prepare stream segment append",
        })
    ));
    assert_eq!(
        storage_node
            .get_pg(0)
            .unwrap()
            .get_stream_upload(&second_session)
            .unwrap(),
        second_before,
        "crossed-session append preparation must not advance the foreign allocator"
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
    .with_pg_topology(Arc::new(PgTopology::new(&[0]).unwrap()))
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

#[test]
fn local_metadata_command_hash_inspection_bounds_pg_lock_wait_by_deadline() {
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
    let pg_guard = storage_node.get_pg(0).unwrap();
    let started = Instant::now();
    let error =
        MetadataCommandInspectionNodeClient::applied_metadata_command_log_entry_hashes_until(
            &client,
            PgId::new(0),
            &test_metadata_command(0, 1),
            Instant::now() + Duration::from_millis(20),
        )
        .unwrap_err();
    drop(pg_guard);

    assert!(matches!(
        error,
        StoreError::OperationDeadlineExceeded { .. }
    ));
    assert_eq!(
        error.operation_failure_class(),
        crate::StoreOperationFailureClass::RetryableConvergence
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "local PG lock wait ignored the confirmation deadline"
    );
}

#[test]
fn local_partial_conflict_state_and_reservation_checks_bound_pg_lock_wait() {
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
    let bucket = crate::tests::bucket_name("deadline-lock-bucket");
    let key = crate::tests::object_key("deadline-lock-key");
    let route = client
        .open_bucket_write_reservation_route(
            ClusterEpoch::new(1).unwrap(),
            BucketPgId::new_for_test(PgId::new(0)),
            &bucket,
        )
        .unwrap();
    let proof = test_bucket_write_reservation_proof(bucket, &key);
    let pg_guard = storage_node.get_pg(0).unwrap();

    for operation in ["replica-state", "reservation"] {
        let started = Instant::now();
        let deadline = started + Duration::from_millis(20);
        let error = match operation {
            "replica-state" => {
                MetadataCommandInspectionNodeClient::metadata_command_replica_state_until(
                    &client,
                    PgId::new(0),
                    deadline,
                )
                .map(|_| ())
                .map_err(BucketSnapshotLoadError::Store)
            }
            "reservation" => route.validate_bucket_write_reservation_proof_until(&proof, deadline),
            _ => unreachable!(),
        }
        .unwrap_err();
        assert!(
            matches!(
                error,
                BucketSnapshotLoadError::Store(StoreError::OperationDeadlineExceeded { .. })
            ),
            "{operation} should stop at the absolute deadline, got {error:?}"
        );
        let BucketSnapshotLoadError::Store(error) = &error else {
            unreachable!("matched store error")
        };
        assert_eq!(
            error.operation_failure_class(),
            crate::StoreOperationFailureClass::RetryableConvergence,
            "{operation} timeout must map to retryable S3 convergence"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{operation} local PG lock wait ignored the deadline"
        );
    }
    drop(pg_guard);
}

#[test]
fn unix_endpoint_absence_is_retryable_convergence() {
    let client = test_unix_storage_node_client();
    let error = client
        .metadata_command_replica_state(PgId::new(0))
        .expect_err("missing Unix endpoint must fail before dispatch");

    assert!(matches!(
        error,
        StoreError::StorageRpc {
            failure: StorageRpcErrorCode::TransportClosed,
            ..
        }
    ));
    assert_eq!(
        error.operation_failure_class(),
        crate::StoreOperationFailureClass::RetryableConvergence
    );
}

fn test_metadata_command_recovery_chain(
    pg_id: u32,
) -> (
    MetadataCommandEnvelope,
    MetadataCommandEnvelope,
    MetadataCommandEnvelope,
    MetadataCommandEnvelope,
) {
    let epoch = ClusterEpoch::new(1).unwrap();
    let bucket = crate::tests::bucket_name("metadata-recovery-certificate-bucket");
    let key = crate::tests::object_key("metadata-recovery-certificate-object");
    let session_id = crate::tests::stream_session_id("recovery-cert");
    let mut proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    proof.operation_kind = PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
    let source = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            epoch,
            PgId::new(pg_id),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::CreateStreamUpload(Box::new(
            CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                CreateStreamUploadReq {
                    session_id,
                    bucket,
                    key,
                    target: StreamUploadTarget::PutObject,
                    encryption: ObjectEncryption::None,
                },
                123,
                proof,
            ),
        )),
    );
    let reissued = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            epoch,
            PgId::new(pg_id),
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        source.payload().clone(),
    );
    let cleanup = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            epoch,
            PgId::new(pg_id),
            MetadataCommandLogIndex::new(3).unwrap(),
        ),
        source
            .payload()
            .abandoned_recovery_follow_up()
            .expect("PutObject stream creation requires generation cleanup"),
    );
    let unrelated = test_metadata_command(pg_id, 4);
    (source, reissued, cleanup, unrelated)
}

#[path = "tests/unix_bucket_rpc.rs"]
mod unix_bucket_rpc;
#[path = "tests/unix_metadata_rpc.rs"]
mod unix_metadata_rpc;
#[path = "tests/unix_object_rpc.rs"]
mod unix_object_rpc;
