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
    encode_storage_rpc_success_response, read_storage_rpc_frame_from, write_storage_rpc_frame_to,
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
use crate::RouteMapValidity;

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
