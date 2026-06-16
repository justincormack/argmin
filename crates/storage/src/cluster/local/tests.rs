use super::*;
use crate::cluster::{MetadataCommandRecoveryWaiterOutcome, PendingMetadataCommandOutcome};
use crate::metadata_command::{
    metadata_command_log_hash, AbortMultipartUploadCommand, AbortStreamUploadCommand,
    AdvanceCompletedMultipartUploadSequenceCommand, AppendStreamSegmentCommand,
    BucketPropertyMutation, BucketSubresourceMutation, CommitDirectPutObjectCommand,
    CommitMultipartObjectCommand, CommitStreamPartCommand, CreateBucketCommand,
    DeleteObjectVersionCommand, DeleteObjectVersionTarget, InsertDeleteMarkerCommand,
    MarkBucketDeletingCommand, MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex,
    MetadataCommandPayload, PutBucketAclCommand, PutBucketSubresourceCommand,
    PutBucketVersioningCommand, PutObjectMetadataCommand, PutObjectMetadataMutation,
    ReserveObjectGenerationCommand, ReserveObjectVersionCommand,
};
use crate::storage_node_server::{StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeServer};
use crate::StorageCluster;
use proptest::prelude::*;
use proptest::test_runner::{TestCaseError, TestCaseResult};
use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

static METADATA_COMMAND_APPLY_HOOK_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
static PAYLOAD_CLEANUP_HOOK_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
static BUCKET_SCOPED_HOOK_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

fn lock_metadata_command_apply_hook_test() -> std::sync::MutexGuard<'static, ()> {
    METADATA_COMMAND_APPLY_HOOK_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn lock_payload_cleanup_hook_test() -> std::sync::MutexGuard<'static, ()> {
    PAYLOAD_CLEANUP_HOOK_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn lock_bucket_scoped_hook_test() -> std::sync::MutexGuard<'static, ()> {
    BUCKET_SCOPED_HOOK_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

#[test]
fn object_payload_reclaim_capacity_counts_dequeued_work_until_finished() {
    let runtime_state = LocalClusterRuntimeState::new();
    let bucket = BucketName::try_from("bucket").unwrap();
    let key_a = ObjectKey::try_from("a".to_string()).unwrap();
    let key_b = ObjectKey::try_from("b".to_string()).unwrap();
    let key_c = ObjectKey::try_from("c".to_string()).unwrap();
    let generation_a = GenerationId::new(1).unwrap();
    let generation_b = GenerationId::new(2).unwrap();
    let generation_c = GenerationId::new(3).unwrap();

    assert_eq!(
        runtime_state.enqueue_object_payload_reclaim(&bucket, &key_a, generation_a, 0),
        ReclaimQueueInsert::Queued
    );
    assert_eq!(
        runtime_state.enqueue_object_payload_reclaim(&bucket, &key_b, generation_b, 0),
        ReclaimQueueInsert::Queued
    );
    assert_eq!(
        runtime_state.enqueue_object_payload_reclaim(&bucket, &key_c, generation_c, 0),
        ReclaimQueueInsert::PgCapacityDeferred
    );

    assert_eq!(
        runtime_state.try_take_reclaim_work(),
        Some(ReclaimWorkItem::ObjectPayload((
            bucket.clone(),
            key_a.clone(),
            generation_a
        )))
    );
    assert_eq!(
        runtime_state.try_take_reclaim_work(),
        Some(ReclaimWorkItem::ObjectPayload((
            bucket.clone(),
            key_b.clone(),
            generation_b
        )))
    );
    assert_eq!(
        runtime_state.enqueue_object_payload_reclaim(&bucket, &key_c, generation_c, 0),
        ReclaimQueueInsert::PgCapacityDeferred
    );

    runtime_state.finish_object_payload_reclaim_work(&bucket, &key_a, generation_a);
    assert_eq!(
        runtime_state.enqueue_object_payload_reclaim(&bucket, &key_c, generation_c, 0),
        ReclaimQueueInsert::Queued
    );
}

#[test]
fn metadata_command_recovery_single_flight_waits_for_matching_command() {
    let runtime_state = Arc::new(LocalClusterRuntimeState::new());
    let pg_id = PgId::new(1);
    let bucket = BucketName::new("single-flight-pending-command").unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    let recovery = runtime_state.join_metadata_command_recovery(pg_id, &command);
    let MetadataCommandRecoveryAdmission::Leader(leader_guard) = recovery else {
        panic!("first recovery caller should lead the single-flight");
    };

    let waiter_state = Arc::clone(&runtime_state);
    let waiter_command = command.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let waiter = thread::spawn(move || {
        let admission = waiter_state.join_metadata_command_recovery(pg_id, &waiter_command);
        tx.send(matches!(
            admission,
            MetadataCommandRecoveryAdmission::Waited { .. }
        ))
        .unwrap();
    });

    assert!(
        rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "second recovery caller should wait while the leader is active"
    );
    drop(leader_guard);
    assert!(
        rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        "second recovery caller should return as a waiter after the leader finishes"
    );
    waiter.join().unwrap();
}

#[test]
fn metadata_command_recovery_single_flight_wait_is_bounded() {
    let runtime_state = Arc::new(LocalClusterRuntimeState::new());
    let pg_id = PgId::new(1);
    let bucket = BucketName::new("single-flight-timeout-pending-command").unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    let recovery = runtime_state.join_metadata_command_recovery(pg_id, &command);
    let MetadataCommandRecoveryAdmission::Leader(_leader_guard) = recovery else {
        panic!("first recovery caller should lead the single-flight");
    };

    let timed_out = runtime_state.join_metadata_command_recovery(pg_id, &command);
    assert!(
        matches!(timed_out, MetadataCommandRecoveryAdmission::TimedOut { .. }),
        "waiter should return a bounded timeout while the leader remains active"
    );
}

fn acquire_test_bucket_write_proof(
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    operation_kind: &'static str,
    target_context: Option<&str>,
) -> crate::metadata_command::BucketWriteReservationProof {
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(bucket, operation_kind, target_context)
        .unwrap();
    crate::metadata_command::BucketWriteReservationProof::from(&reservation.record)
}

fn assert_bucket_write_reservations_released(map: &LocalClusterMap, bucket: &crate::BucketName) {
    let pg_id = PgId::new(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology()
            .bucket_pg_for(bucket),
    );
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let bucket_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, bucket)
            .unwrap()
            .is_empty(),
        "metadata command convergence must release the durable bucket write reservation"
    );
}

struct CommittedDirectSegment {
    version_id: crate::VersionId,
    generation_id: crate::GenerationId,
    segment_okh: [u8; 16],
    payload: Vec<u8>,
    written: crate::DirectPutWrittenSegment,
    locations: Vec<ShardLocation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplayStateSnapshot {
    state: crate::metadata_command::MetadataCommandReplicaState,
    max_log_index: u64,
}

fn collect_metadata_replay_snapshot(
    map: &LocalClusterMap,
    node_ids: &[NodeId],
    pg_ids: &[u32],
) -> std::collections::BTreeMap<(u32, u32), ReplayStateSnapshot> {
    let mut snapshot = std::collections::BTreeMap::new();
    for &node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        for &pg_id in pg_ids {
            let pg = node.get_pg(pg_id).unwrap();
            snapshot.insert(
                (node_id.as_u32(), pg_id),
                ReplayStateSnapshot {
                    state: pg.metadata_command_replica_state().unwrap(),
                    max_log_index: pg
                        .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                        .unwrap(),
                },
            );
        }
    }
    snapshot
}

fn assert_clean_metadata_command_stream_at_epoch(
    map: &LocalClusterMap,
    pg_ids: &[u32],
    cluster_epoch: ClusterEpoch,
) {
    let pg_ids: Vec<PgId> = pg_ids.iter().copied().map(PgId::new).collect();
    assert_no_pending_metadata_command_slots_at_epoch(map, &pg_ids, cluster_epoch);
    for &pg_id in &pg_ids {
        for node_id in map.node_ids() {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(pg_id.get())
                .unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            let max_log_index = pg.max_metadata_command_log_index(cluster_epoch).unwrap();
            assert_eq!(
                state.applied_log_index,
                max_log_index,
                "node {} PG {} has unapplied metadata command log tail",
                node_id.as_u32(),
                pg_id.get()
            );
        }
    }
    validate_metadata_command_replay_state(&map.nodes, &map.pg_routes, &pg_ids, cluster_epoch)
        .expect("metadata command stream should validate");
}

fn assert_no_pending_metadata_command_slots_at_epoch(
    map: &LocalClusterMap,
    pg_ids: &[PgId],
    cluster_epoch: ClusterEpoch,
) {
    for &pg_id in pg_ids {
        for node_id in map.node_ids() {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(pg_id.get())
                .unwrap();
            assert!(
                pg.pending_metadata_command_slot(node_id.as_u32(), cluster_epoch)
                    .unwrap()
                    .is_none(),
                "node {} PG {} should not have an unresolved pending command slot",
                node_id.as_u32(),
                pg_id.get()
            );
        }
    }
}

fn assert_clean_metadata_command_stream(map: &LocalClusterMap, pg_ids: &[u32]) {
    assert_clean_metadata_command_stream_at_epoch(map, pg_ids, ClusterEpoch::INITIAL);
}

#[derive(Clone, Copy, Debug)]
enum TerminalMultipartOutcome {
    Aborted,
    Completed,
}

fn assert_terminal_multipart_upload_invariants(
    map: &LocalClusterMap,
    node_ids: &[NodeId],
    object_pg: u32,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    upload_id: &crate::UploadId,
    expected_outcome: TerminalMultipartOutcome,
) {
    for &node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(
                matches!(
                    crate::PgMetadataStore::get_multipart_upload(&*pg, upload_id),
                    Err(crate::MetadataError::NoSuchUpload { .. })
                ),
                "terminal upload {upload_id:?} should not leave a multipart_uploads row on node {node_id:?}"
            );

        let mut active_sessions = Vec::new();
        for session in crate::PgMetadataStore::list_all_stream_uploads(&*pg).unwrap() {
            if session.bucket != *bucket || session.key != *key {
                continue;
            }
            if matches!(
                &session.target,
                crate::StreamUploadTarget::UploadPart {
                    upload_id: session_upload_id,
                    ..
                } if session_upload_id == upload_id
            ) {
                let segments =
                    crate::PgMetadataStore::list_stream_segments(&*pg, &session.session_id)
                        .unwrap();
                active_sessions.push((session, segments));
            }
        }
        assert!(
                active_sessions.is_empty(),
                "terminal upload {upload_id:?} should not leave active UploadPart stream sessions on node {node_id:?}: {active_sessions:?}"
            );

        let completed =
            crate::PgMetadataStore::get_completed_multipart_upload(&*pg, upload_id).unwrap();
        match expected_outcome {
            TerminalMultipartOutcome::Aborted => {
                assert!(
                        completed.is_none(),
                        "aborted upload {upload_id:?} should not leave a completed-upload idempotence row on node {node_id:?}: {completed:?}"
                    );
                let segments = crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(
                    &*pg, upload_id,
                )
                .unwrap();
                assert!(
                        segments.is_empty(),
                        "aborted upload {upload_id:?} should not leave multipart part segment rows on node {node_id:?}: {segments:?}"
                    );
            }
            TerminalMultipartOutcome::Completed => {
                let completed = completed.unwrap_or_else(|| {
                        panic!(
                            "completed upload {upload_id:?} should leave a completed-upload idempotence row on node {node_id:?}"
                        )
                    });
                assert_eq!(completed.bucket, *bucket);
                assert_eq!(completed.key, *key);

                let segments = crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(
                    &*pg, upload_id,
                )
                .unwrap();
                let mut object_parts_by_version = BTreeMap::<u64, BTreeSet<u32>>::new();
                for segment in &segments {
                    assert_ne!(
                            segment.version_id,
                            crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
                            "completed upload {upload_id:?} should not leave staging multipart segment rows on node {node_id:?}: {segment:?}"
                        );
                    let version_id = crate::VersionId::from_u64(segment.version_id);
                    let part_numbers =
                            object_parts_by_version
                                .entry(segment.version_id)
                                .or_insert_with(|| {
                            let stored = crate::PgMetadataStore::get_object_version(
                                &*pg, bucket, key, version_id,
                            )
                            .unwrap_or_else(|err| {
                                panic!(
                                    "completed upload {upload_id:?} segment {segment:?} should reference an existing object version on node {node_id:?}: {err:?}"
                                )
                            });
                            let live = stored.as_live().unwrap_or_else(|| {
                                panic!(
                                    "completed upload {upload_id:?} segment {segment:?} should reference a live object version on node {node_id:?}: {stored:?}"
                                )
                            });
                            assert!(
                                matches!(live.layout, crate::ObjectLayout::MultipartManifest { .. }),
                                "completed upload {upload_id:?} segment {segment:?} should reference a multipart object version on node {node_id:?}: {live:?}"
                            );
                            crate::PgMetadataStore::get_object_parts(
                                &*pg, bucket, key, version_id,
                            )
                            .unwrap()
                            .into_iter()
                            .map(|part| part.part_number)
                            .collect()
                        });
                    assert!(
                            part_numbers.contains(&segment.part_number),
                            "completed upload {upload_id:?} segment {segment:?} should be referenced by an object part row on node {node_id:?}"
                        );
                }
            }
        }
    }
}

fn write_committed_direct_segment(
    cluster: &crate::StorageCluster,
    payload: &[u8],
) -> CommittedDirectSegment {
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    write_committed_direct_segment_for(cluster, &bucket, &key, payload)
}

fn write_committed_direct_segment_for(
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    payload: &[u8],
) -> CommittedDirectSegment {
    write_committed_direct_segment_for_with_okh(cluster, bucket, key, [41; 16], payload)
}

fn write_committed_direct_segment_for_with_okh(
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    segment_okh: [u8; 16],
    payload: &[u8],
) -> CommittedDirectSegment {
    write_committed_direct_segment_for_with_versioning(
        cluster,
        bucket,
        key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        segment_okh,
        payload,
    )
}

fn write_committed_direct_segment_for_with_versioning(
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    versioning: crate::BucketVersioningState,
    reservation_bytes: [u8; 16],
    segment_okh: [u8; 16],
    payload: &[u8],
) -> CommittedDirectSegment {
    ensure_test_bucket(cluster, bucket);
    let reservation_id = crate::SessionId::try_from(
        reservation_bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    )
    .unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(bucket, key, &reservation_id)
        .unwrap();
    let written = cluster
        .write_direct_put_segment_payload_shards(
            bucket,
            key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = crate::CommitDirectPutObjectReq {
        bucket: bucket.clone(),
        key: key.clone(),
        generation_reservation_id: reservation_id,
        versioning,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        generation_id,
        size: payload.len() as u64,
        etag_crc64: checksum::crc64::checksum(payload),
        ec: written.ec,
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        segment_index: 0,
        segment_crc64: Some(checksum::crc64::checksum(payload)),
        segment_okh,
        segment_vid: generation_id,
        data_pg_id: written.data_pg_id,
        bucket_write_reservation: acquire_test_bucket_write_proof(
            cluster,
            bucket,
            "direct-put-commit-test",
            Some(key.as_str()),
        ),
    };
    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    let data_pg_id = DataPgId::new(PgId::new(written.data_pg_id));
    let placement_key = super::super::segment_payload_placement_key(&segment_okh, generation_id);
    let locations = cluster
        .place_payload_shards(data_pg_id, written.ec, &placement_key)
        .unwrap();

    CommittedDirectSegment {
        version_id: outcome.version_id,
        generation_id,
        segment_okh,
        payload: payload.to_vec(),
        written,
        locations,
    }
}

fn bucket_key_with_distinct_object_and_data_pg(
    topology: &crate::PgTopology,
) -> (crate::BucketName, crate::ObjectKey, u32, u32) {
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    for index in 0..1000 {
        let key = crate::ObjectKey::try_from(format!("key-{index}")).unwrap();
        let object_pg = topology.object_pg_for(&bucket, &key);
        let data_pg = topology
            .object_generation_segment_data_pg(&bucket, &key, crate::GenerationId::MIN, 0)
            .get();
        if object_pg != data_pg {
            return (bucket, key, object_pg, data_pg);
        }
    }
    panic!("test topology did not produce distinct object/data PGs");
}

fn key_for_object_pg(
    topology: &crate::PgTopology,
    bucket: &crate::BucketName,
    target_pg_id: u32,
    prefix: &str,
) -> crate::ObjectKey {
    for index in 0..10_000 {
        let key = crate::ObjectKey::try_from(format!("{prefix}{index}")).unwrap();
        if topology.object_pg_for(bucket, &key) == target_pg_id {
            return key;
        }
    }
    panic!("test topology did not produce a key for PG {target_pg_id}");
}

fn bucket_for_pg(
    topology: &crate::PgTopology,
    target_pg_id: u32,
    prefix: &str,
) -> crate::BucketName {
    for index in 0..10_000 {
        let bucket = crate::BucketName::try_from(format!("{prefix}{index}")).unwrap();
        if topology.bucket_pg_for(&bucket) == target_pg_id {
            return bucket;
        }
    }
    panic!("test topology did not produce a bucket for PG {target_pg_id}");
}

#[test]
fn bucket_payload_reclaim_root_validation_rejects_wrong_object_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 0, "payload-root-validation-");
    let key_pg0 = key_for_object_pg(topology, &bucket, 0, "object-pg0-");
    let key_pg1 = key_for_object_pg(topology, &bucket, 1, "object-pg1-");
    let cluster = crate::StorageCluster::from_local_map(Arc::new(map)).unwrap();

    cluster
        .validate_bucket_payload_reclaim_root_for_pg(
            PgId::new(0),
            &crate::PayloadReclaimRoot {
                bucket: bucket.clone(),
                key: key_pg0,
                generation_id: crate::GenerationId::MIN,
            },
            NodeId::new(7),
        )
        .unwrap();
    let err = cluster
        .validate_bucket_payload_reclaim_root_for_pg(
            PgId::new(0),
            &crate::PayloadReclaimRoot {
                bucket,
                key: key_pg1,
                generation_id: crate::GenerationId::MIN,
            },
            NodeId::new(7),
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::BucketWriteDrainError::Store(StoreError::StorageRpc {
            node_id: 7,
            operation: "object bucket payload reclaim root",
            ..
        })
    ));
}

fn create_test_bucket(cluster: &crate::StorageCluster, bucket: &crate::BucketName) {
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
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
        })
        .unwrap();
}

fn ensure_test_bucket(cluster: &crate::StorageCluster, bucket: &crate::BucketName) {
    if cluster
        .load_bucket_snapshot(bucket, crate::BucketSnapshotRequest::default())
        .is_err()
    {
        create_test_bucket(cluster, bucket);
    }
}

fn assert_stream_next_segment_vid(
    map: &LocalClusterMap,
    node_id: NodeId,
    object_pg: u32,
    session_id: &crate::SessionId,
    expected: u64,
) {
    let node = map.node(node_id).unwrap().storage_node();
    let pg = node.get_pg(object_pg).unwrap();
    assert_eq!(
        crate::PgMetadataStore::get_stream_upload(&*pg, session_id)
            .unwrap()
            .next_segment_vid
            .get(),
        expected
    );
}

fn pending_metadata_command_for_test(
    map: &LocalClusterMap,
    pg_id: PgId,
    bucket: &BucketName,
) -> Option<MetadataCommandEnvelope> {
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let command = pg
        .pending_metadata_command_envelope(primary.node_id().as_u32(), ClusterEpoch::INITIAL)
        .unwrap();
    if let Some(command) = &command {
        assert_eq!(command.bucket_name(), bucket);
    }
    command
}

fn object_payload_reclaim_claim_count_for_test(map: &LocalClusterMap, pg_id: PgId) -> usize {
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    pg.connection()
        .query_row(
            "SELECT COUNT(*) FROM object_payload_reclaim_claims",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap() as usize
}

fn insert_pending_metadata_command_for_test(
    map: &LocalClusterMap,
    pg_id: PgId,
    bucket: &BucketName,
    command: &MetadataCommandEnvelope,
) {
    if force_insert_terminal_pending_metadata_command_for_test(map, pg_id, bucket, command) {
        return;
    }
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    pg.try_insert_pending_metadata_command_slot(primary.node_id().as_u32(), command, Some(bucket))
        .unwrap();
}

fn force_insert_pending_metadata_command_for_test(
    map: &LocalClusterMap,
    pg_id: PgId,
    bucket: &BucketName,
    command: &MetadataCommandEnvelope,
) {
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let command_bytes = command.command_bytes();
    pg.connection()
            .execute(
                "INSERT INTO metadata_command_pending_slot \
                 (singleton, cluster_epoch, pg_id, log_index, command_checksum, command_bytes, scope_bucket) \
                 VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT(singleton) DO UPDATE SET \
                   cluster_epoch = excluded.cluster_epoch, \
                   pg_id = excluded.pg_id, \
                   log_index = excluded.log_index, \
                   command_checksum = excluded.command_checksum, \
                   command_bytes = excluded.command_bytes, \
                   scope_bucket = excluded.scope_bucket",
                rusqlite::params![
                    command.id().cluster_epoch().get() as i64,
                    command.id().pg_id().get() as i64,
                    command.id().log_index().get() as i64,
                    command.checksum_crc64() as i64,
                    command_bytes,
                    Some(bucket.as_str()),
                ],
            )
            .unwrap();
}

fn force_insert_terminal_pending_metadata_command_for_test(
    map: &LocalClusterMap,
    pg_id: PgId,
    bucket: &BucketName,
    command: &MetadataCommandEnvelope,
) -> bool {
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let terminal_exists = pg
        .has_matching_applied_metadata_command_log_entry(
            primary.node_id().as_u32(),
            command,
            pg.metadata_command_replica_state()
                .unwrap()
                .applied_log_hash,
        )
        .unwrap_or(false)
        || matches!(
            pg.applied_metadata_command_log_entry_hashes(primary.node_id().as_u32(), command),
            Ok(Some(_))
        );
    drop(pg);
    if terminal_exists {
        force_insert_pending_metadata_command_for_test(map, pg_id, bucket, command);
    }
    terminal_exists
}

fn create_bucket_metadata_command(
    pg_id: PgId,
    log_index: u64,
    bucket: crate::BucketName,
) -> MetadataCommandEnvelope {
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
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(log_index).unwrap(),
        ),
        MetadataCommandPayload::CreateBucket(
            CreateBucketCommand::from_config(&config, 1_234, log_index).unwrap(),
        ),
    )
}

fn create_test_bucket_with_versioning(
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    versioning: crate::BucketVersioningState,
) {
    create_test_bucket(cluster, bucket);
    if versioning != crate::BucketVersioningState::Disabled {
        cluster
            .put_bucket_versioning_and_load_info(bucket, versioning)
            .unwrap();
    }
}

fn put_test_lifecycle(cluster: &crate::StorageCluster, bucket: &crate::BucketName) {
    cluster
        .put_bucket_subresource_and_load_info(
            bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
}

fn current_bucket_incarnation(cluster: &crate::StorageCluster, bucket: &crate::BucketName) -> u64 {
    cluster
        .head_bucket_info(bucket)
        .unwrap()
        .bucket_incarnation_generation
}

struct DirectPutCommitReqFixture<'a> {
    bucket: &'a crate::BucketName,
    key: &'a crate::ObjectKey,
    reservation_id: crate::SessionId,
    generation_id: crate::GenerationId,
    payload: &'a [u8],
    segment_okh: [u8; 16],
    written: &'a crate::DirectPutWrittenSegment,
}

fn direct_put_commit_req(
    cluster: &crate::StorageCluster,
    fixture: DirectPutCommitReqFixture<'_>,
) -> crate::CommitDirectPutObjectReq {
    let bucket_write_reservation = acquire_test_bucket_write_proof(
        cluster,
        fixture.bucket,
        "direct-put-commit-test",
        Some(fixture.key.as_str()),
    );
    direct_put_commit_req_with_bucket_write_proof(fixture, bucket_write_reservation)
}

fn direct_put_commit_req_with_bucket_write_proof(
    fixture: DirectPutCommitReqFixture<'_>,
    bucket_write_reservation: crate::metadata_command::BucketWriteReservationProof,
) -> crate::CommitDirectPutObjectReq {
    crate::CommitDirectPutObjectReq {
        bucket: fixture.bucket.clone(),
        key: fixture.key.clone(),
        generation_reservation_id: fixture.reservation_id,
        versioning: crate::BucketVersioningState::Disabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        generation_id: fixture.generation_id,
        size: fixture.payload.len() as u64,
        etag_crc64: checksum::crc64::checksum(fixture.payload),
        ec: fixture.written.ec,
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        segment_index: 0,
        segment_crc64: Some(checksum::crc64::checksum(fixture.payload)),
        segment_okh: fixture.segment_okh,
        segment_vid: fixture.generation_id,
        data_pg_id: fixture.written.data_pg_id,
        bucket_write_reservation,
    }
}

fn assert_direct_put_metadata_on_acting_nodes(
    map: &LocalClusterMap,
    node_ids: &[NodeId],
    object_pg: u32,
    commit_req: &crate::CommitDirectPutObjectReq,
    outcome: &crate::FinalizeDirectPutObjectOutcome,
) {
    let expected_segment = crate::ObjectSegmentRecord {
        bucket: commit_req.bucket.clone(),
        key: commit_req.key.clone(),
        version_id: outcome.version_id,
        segment_index: commit_req.segment_index,
        size: commit_req.size,
        segment_crc64: commit_req.segment_crc64,
        segment_okh: commit_req.segment_okh,
        segment_vid: commit_req.segment_vid,
        data_pg_id: commit_req.data_pg_id,
        ec_k: commit_req.ec.k,
        ec_m: commit_req.ec.m,
    };

    for node_id in node_ids {
        let node = map.node(*node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored =
            crate::PgMetadataStore::get_object_meta(&*pg, &commit_req.bucket, &commit_req.key)
                .unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(live.version_id, outcome.version_id);
        assert_eq!(live.generation_id, commit_req.generation_id);
        assert_eq!(live.size, commit_req.size);
        assert_eq!(
            live.etag,
            crate::ObjectEtag::single_part(commit_req.etag_crc64)
        );
        assert_eq!(live.last_modified, outcome.live_last_modified);
        assert_eq!(live.metadata_blob, Some(commit_req.metadata_blob.clone()));
        assert_eq!(
            live.system_metadata_blob,
            Some(commit_req.system_metadata_blob.clone())
        );
        assert_eq!(live.object_lock, commit_req.object_lock);
        assert_eq!(live.encryption, commit_req.encryption);
        assert_eq!(
            crate::PgMetadataStore::get_object_segments(
                &*pg,
                &commit_req.bucket,
                &commit_req.key,
                outcome.version_id,
            )
            .unwrap(),
            vec![expected_segment.clone()]
        );
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &commit_req.bucket,
                &commit_req.key,
                &commit_req.generation_reservation_id,
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

fn assert_object_version_counter_on_acting_nodes(
    map: &LocalClusterMap,
    node_ids: &[NodeId],
    object_pg: u32,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    expected_next_version_id: u64,
) {
    for node_id in node_ids {
        let node = map.node(*node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let next_version_id: i64 = pg
            .connection()
            .query_row(
                "SELECT COALESCE(MAX(next_version_id), 0) \
                     FROM object_version_counters WHERE bucket = ?1 AND key = ?2",
                rusqlite::params![bucket.as_str(), key.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            next_version_id as u64, expected_next_version_id,
            "unexpected object version counter on node {node_id:?}"
        );
    }
}

fn assert_bucket_execution_counter_on_acting_nodes(
    map: &LocalClusterMap,
    node_ids: &[NodeId],
    bucket_pg: u32,
    expected_current_generation: u64,
) {
    for node_id in node_ids {
        let node = map.node(*node_id).unwrap().storage_node();
        let pg = node.get_pg(bucket_pg).unwrap();
        let next_generation: i64 = pg
            .connection()
            .query_row(
                "SELECT next_bucket_execution_generation \
                     FROM pg_counters WHERE singleton = 0",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            next_generation as u64, expected_current_generation,
            "unexpected bucket execution counter on node {node_id:?}"
        );
    }
}

fn seed_streamed_multipart_completion(
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    upload_label: &str,
) -> (
    crate::CompleteMultipartCommitRequest,
    crate::MultipartPartSegmentRecord,
) {
    seed_streamed_multipart_completion_with_existing(cluster, bucket, key, upload_label, false)
}

fn seed_streamed_multipart_completion_with_existing(
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    upload_label: &str,
    expect_existing_object: bool,
) -> (
    crate::CompleteMultipartCommitRequest,
    crate::MultipartPartSegmentRecord,
) {
    let upload_id = upload_id_from_label(upload_label);
    cluster
        .create_multipart_upload(
            bucket,
            key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert_eq!(existing_object.is_some(), expect_existing_object);
                Ok::<_, ()>((
                    (),
                    crate::CreateMultipartUploadReq {
                        upload_id: upload_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        tags: None,
                        metadata_blob: crate::SerializedMetadataBlob::default(),
                        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                        initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
                        owner: crate::OwnerIdentity::from_principal("owner"),
                        acl_grants: crate::AclGrants::default(),
                        public_read: false,
                        object_lock: crate::ObjectLockState::default(),
                        checksum: None,
                        encryption: crate::ObjectEncryption::None,
                    },
                ))
            },
        )
        .unwrap()
        .unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(bucket, key, &upload_id)
        .unwrap();
    let (_shard_keys, part, mut expected_segment) = upload_streamed_test_multipart_part(
        cluster,
        bucket,
        key,
        &upload_id,
        1,
        [0xCD; 16],
        b"streamed completion",
    );

    let selected_streaming_segment = expected_segment.clone();
    expected_segment.version_id = crate::VersionId::Null.to_u64();
    (
        crate::CompleteMultipartCommitRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id,
            versioning: crate::BucketVersioningState::Disabled,
            owner: upload.owner,
            acl_grants: upload.acl_grants,
            public_read: upload.public_read,
            generation_id: upload.object_generation_id,
            size: part.size,
            etag_crc64: [0x44; 8],
            tags: upload.tags,
            metadata_blob: Some(upload.metadata_blob),
            system_metadata_blob: Some(upload.system_metadata_blob),
            object_lock: upload.object_lock,
            encryption: upload.encryption,
            expected_stale_payload_source: None,
            part_records: vec![part],
            selected_streaming_segments: vec![selected_streaming_segment],
            expected_cleanup: crate::CompleteMultipartCommitCleanup::default(),
        },
        expected_segment,
    )
}

fn pending_multipart_completion_command_for_test(
    map: &LocalClusterMap,
    cluster: &crate::StorageCluster,
    pg_id: PgId,
    req: &crate::CompleteMultipartCommitRequest,
    last_modified_millis: u64,
) -> (MetadataCommandEnvelope, u64) {
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &req.upload_id)
        .expect("seeded upload is still in progress");
    let parts_count =
        std::num::NonZeroU32::new(u32::try_from(req.part_records.len()).unwrap()).unwrap();
    let topology = primary.storage_node().pg_topology();
    let object_parts = req
        .part_records
        .iter()
        .map(|part| {
            let data_pg_id = topology
                .object_generation_multipart_part_data_pg(
                    &req.bucket,
                    &req.key,
                    req.generation_id,
                    part.part_number,
                )
                .get();
            crate::ObjectPartRecord {
                bucket: req.bucket.clone(),
                key: req.key.clone(),
                version_id: crate::VersionId::Null,
                part_number: part.part_number,
                size: part.size,
                etag: part.etag.clone(),
                etag_kind: part.etag_kind,
                part_okh: part.part_okh,
                part_vid: part.part_vid,
                ec_k: part.ec_k,
                ec_m: part.ec_m,
                data_pg_id,
                checksum: part.checksum.clone(),
            }
        })
        .collect::<Vec<_>>();
    let mut selected_streaming_segments =
        crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(&*pg, &req.upload_id)
            .unwrap();
    for segment in &mut selected_streaming_segments {
        segment.version_id = crate::VersionId::Null.to_u64();
    }
    let write_sequence = pg
        .next_object_write_sequence(req.bucket.as_str(), req.key.as_str())
        .unwrap();
    drop(pg);
    let completion_order = cluster
        .test_reserve_completed_multipart_upload_order(&req.bucket)
        .unwrap();
    let bucket_write_reservation = acquire_test_bucket_write_proof(
        cluster,
        &req.bucket,
        "test-complete-multipart",
        Some(req.key.as_str()),
    );
    (
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CommitMultipartObject(Box::new(CommitMultipartObjectCommand {
                upload_id: req.upload_id.clone(),
                bucket_write_reservation,
                object: crate::PutLiveObjectReq {
                    bucket: req.bucket.clone(),
                    key: req.key.clone(),
                    version_id: crate::VersionId::Null,
                    owner: req.owner.clone(),
                    acl_grants: req.acl_grants.clone(),
                    public_read: req.public_read,
                    generation_id: req.generation_id,
                    size: req.size,
                    etag: crate::ObjectEtag::MultipartComposite {
                        crc64: req.etag_crc64,
                        parts: parts_count,
                    },
                    ec: EcShape { k: 0, m: 0 },
                    layout: crate::ObjectLayout::MultipartManifest { parts_count },
                    tags: req.tags.clone(),
                    metadata_blob: req.metadata_blob.clone(),
                    system_metadata_blob: req.system_metadata_blob.clone(),
                    object_lock: req.object_lock,
                    encryption: req.encryption.clone(),
                },
                parts: object_parts,
                selected_streaming_segments,
                omitted_parts: Vec::new(),
                omitted_streaming_segments: Vec::new(),
                stream_uploads: Vec::new(),
                stream_upload_segments: Vec::new(),
                write_sequence,
                completion_order,
                completed_at_millis: last_modified_millis,
                initiator: upload.initiator.clone(),
                last_modified_millis,
                stale_payload: None,
            })),
        ),
        write_sequence,
    )
}

fn assert_streamed_multipart_completion_on_acting_nodes(
    map: &LocalClusterMap,
    node_ids: &[NodeId],
    object_pg: u32,
    req: &crate::CompleteMultipartCommitRequest,
    expected_segment: &crate::MultipartPartSegmentRecord,
    outcome: &crate::CompleteMultipartCommitOutcome,
) {
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        map,
        node_ids,
        object_pg,
        req,
        expected_segment,
        outcome,
        1,
    );
}

fn assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
    map: &LocalClusterMap,
    node_ids: &[NodeId],
    object_pg: u32,
    req: &crate::CompleteMultipartCommitRequest,
    expected_segment: &crate::MultipartPartSegmentRecord,
    outcome: &crate::CompleteMultipartCommitOutcome,
    expected_write_sequence: u64,
) {
    let mut expected_completion_order = None;
    for node_id in node_ids {
        let node = map.node(*node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &req.bucket, &req.key)
            .unwrap()
            .as_live()
            .unwrap()
            .clone();
        assert_eq!(stored.version_id, outcome.version_id);
        assert_eq!(stored.generation_id, req.generation_id);
        assert_eq!(stored.size, req.size);
        assert_eq!(stored.last_modified, outcome.live_last_modified);
        assert_eq!(stored.layout.parts_count(), Some(1));
        assert_eq!(
            pg.object_write_sequence(req.bucket.as_str(), req.key.as_str(), outcome.version_id)
                .unwrap(),
            Some(expected_write_sequence)
        );

        let parts = crate::PgMetadataStore::get_object_parts(
            &*pg,
            &req.bucket,
            &req.key,
            outcome.version_id,
        )
        .unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_okh, [0u8; 16]);
        assert_eq!(parts[0].part_vid, req.part_records[0].part_vid);

        let segments = crate::PgMetadataStore::get_multipart_part_segments(
            &*pg,
            &req.bucket,
            &req.key,
            outcome.version_id,
            1,
        )
        .unwrap();
        assert_eq!(segments, vec![expected_segment.clone()]);
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &req.upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
        let completed_uploads = pg
            .list_completed_multipart_uploads_for_bucket(req.bucket.as_str())
            .unwrap();
        assert_eq!(completed_uploads.len(), 1);
        assert_eq!(completed_uploads[0].0, req.upload_id);
        if let Some(expected) = expected_completion_order {
            assert_eq!(completed_uploads[0].1, expected);
        } else {
            expected_completion_order = Some(completed_uploads[0].1);
        }
        let bucket_pg = node
            .get_pg(node.pg_topology().bucket_pg_for(&req.bucket))
            .unwrap();
        assert_eq!(
            bucket_pg
                .completed_multipart_upload_sequence_for_bucket(&req.bucket)
                .unwrap(),
            completed_uploads[0].1
        );
    }
}

fn completed_multipart_order_on_node(
    map: &LocalClusterMap,
    node_id: NodeId,
    object_pg: u32,
    bucket: &crate::BucketName,
    upload_id: &crate::UploadId,
) -> u64 {
    let node = map.node(node_id).unwrap().storage_node();
    let pg = node.get_pg(object_pg).unwrap();
    pg.list_completed_multipart_uploads_for_bucket(bucket.as_str())
        .unwrap()
        .into_iter()
        .find_map(|(stored_upload_id, completion_order)| {
            (stored_upload_id == *upload_id).then_some(completion_order)
        })
        .unwrap_or_else(|| panic!("completed upload {upload_id:?} not found on PG {object_pg}"))
}

fn seed_bucket_record(
    map: &LocalClusterMap,
    node_id: NodeId,
    pg_id: u32,
    bucket: &crate::BucketName,
    owner: &crate::CanonicalUserId,
) {
    let node = map.node(node_id).unwrap().storage_node();
    let pg = node.get_pg(pg_id).unwrap();
    crate::PgMetadataStore::create_bucket(
        &*pg,
        bucket,
        "owner",
        owner,
        &crate::AclGrants::default(),
        false,
        false,
    )
    .unwrap();
}

fn upload_id_from_label(label: &str) -> crate::UploadId {
    let mut upload_id = String::from(label);
    upload_id.extend(std::iter::repeat_n(
        '.',
        crate::UPLOAD_ID_LEN - upload_id.len(),
    ));
    crate::UploadId::try_from(upload_id).unwrap()
}

fn seed_multipart_upload_record(
    map: &LocalClusterMap,
    node_id: NodeId,
    pg_id: u32,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    upload_id: &crate::UploadId,
    state: crate::UploadState,
) {
    let node = map.node(node_id).unwrap().storage_node();
    let pg = node.get_pg(pg_id).unwrap();
    crate::PgMetadataStore::create_multipart_upload(
        &*pg,
        &crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: None,
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        },
    )
    .unwrap();
    if state != crate::UploadState::InProgress {
        crate::PgMetadataStore::set_upload_state(&*pg, upload_id, state).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
}

fn upload_streamed_test_multipart_part(
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    upload_id: &crate::UploadId,
    part_number: u32,
    segment_okh: [u8; 16],
    payload: &[u8],
) -> (
    Vec<ShardKey>,
    crate::MultipartPartRecord,
    crate::MultipartPartSegmentRecord,
) {
    let session_seed = segment_okh[0];
    let session_id = crate::SessionId::try_from(format!("{session_seed:02x}").repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(bucket, key, upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            part_number,
            &session_id,
        )
        .unwrap();

    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            bucket,
            key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh,
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
            bucket,
            key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let part = cluster
        .finalize_upload_part_stream(
            bucket,
            key,
            upload_id,
            &session_id,
            part_number,
            |snapshot| {
                let generation = snapshot
                    .existing_part_generation
                    .map_or(0, |generation| generation + 1);
                let part = crate::MultipartPartRecord {
                    upload_id: upload_id.clone(),
                    part_number,
                    generation,
                    size: payload.len() as u64,
                    etag: vec![session_seed; 8],
                    etag_kind: crate::EtagKind::Crc64,
                    part_okh: [0u8; 16],
                    part_vid: crate::GenerationId::new(u64::from(generation) + 1).unwrap(),
                    ec_k: segment.ec_k,
                    ec_m: segment.ec_m,
                    last_modified: 123,
                    checksum: None,
                };
                let segments = snapshot
                    .staging_segments
                    .iter()
                    .map(|staged| crate::MultipartPartSegmentRecord {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        upload_id: upload_id.clone(),
                        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
                        part_number,
                        segment_index: staged.segment_index,
                        size: staged.size,
                        segment_crc64: staged.segment_crc64,
                        segment_okh: staged.segment_okh,
                        segment_vid: staged.segment_vid,
                        data_pg_id: staged.data_pg_id,
                        ec_k: staged.ec_k,
                        ec_m: staged.ec_m,
                    })
                    .collect::<Vec<_>>();
                Ok::<_, ()>(crate::PreparedStreamPartCommit {
                    value: part.clone(),
                    part,
                    segments,
                })
            },
        )
        .unwrap()
        .unwrap()
        .value;

    let uploaded_segment = crate::MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    };
    (
        written_shards
            .into_iter()
            .map(|written| written.key)
            .collect(),
        part,
        uploaded_segment,
    )
}

fn seed_completed_multipart_upload_record(
    map: &LocalClusterMap,
    node_id: NodeId,
    pg_id: u32,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    upload_id: &crate::UploadId,
    completion_order: u64,
) {
    seed_multipart_upload_record(
        map,
        node_id,
        pg_id,
        bucket,
        key,
        upload_id,
        crate::UploadState::InProgress,
    );
    let node = map.node(node_id).unwrap().storage_node();
    let pg = node.get_pg(pg_id).unwrap();
    let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, upload_id).unwrap();
    let version_id = crate::VersionId::Null;
    let part = crate::ObjectPartRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id,
        part_number: 1,
        size: 1,
        etag: vec![completion_order as u8; 8],
        etag_kind: crate::EtagKind::Crc64,
        part_okh: [completion_order as u8; 16],
        part_vid: upload.object_generation_id,
        ec_k: 2,
        ec_m: 1,
        data_pg_id: pg_id,
        checksum: None,
    };
    crate::PgMetadataStore::complete_multipart_commit(
        &*pg,
        upload_id,
        completion_order,
        &crate::CommitMultipartReq {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            generation_id: upload.object_generation_id,
            size: part.size,
            etag_crc64: [completion_order as u8; 8],
            ec: EcShape { k: 2, m: 1 },
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: crate::ObjectLockState::default(),
            encryption: crate::ObjectEncryption::None,
        },
        &[part],
    )
    .unwrap();
    pg.connection()
        .execute(
            "UPDATE completed_multipart_uploads SET completed_at = ?1 WHERE upload_id = ?2",
            rusqlite::params![completion_order as i64, upload_id.as_str()],
        )
        .unwrap();
    pg.refresh_metadata_command_state_digest().unwrap();
}

fn set_route_primary(map: &mut LocalClusterMap, pg_id: u32, primary_node_id: NodeId) {
    let route = map.pg_routes.get_mut(&PgId::new(pg_id)).unwrap();
    route.acting_set = Arc::from([NodeId::new(0), NodeId::new(1), NodeId::new(2)]);
    route.primary_node_id = primary_node_id;
}

mod command_fanout;
mod command_recovery;
mod direct_put;
mod metadata_replay;
mod multipart;
mod multipart_trace;
mod object_metadata;
mod object_read;
mod shard_scavenger;
mod stream_commands;
mod stream_put;
mod trace;
mod unix_clients;

use trace::{current_cluster, trace_node_ids};

#[test]
fn direct_put_metadata_command_retry_reuses_pending_partial_replica_command() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let reservation_id = crate::SessionId::try_from("14".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put metadata command partial apply retry";
    let segment_okh = [64; 16];
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
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected direct put metadata command apply failure",
                        source: std::io::Error::other(
                            "injected direct put metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected direct put metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial direct PUT metadata command must remain pending"
    );
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let slot = pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .expect("partial direct PUT should leave durable primary pending slot");
        assert_eq!(slot.scope_bucket.as_ref(), Some(&bucket));
    }
    for node_id in [NodeId::new(0), NodeId::new(2)] {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
    {
        let bucket_primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg_id))
            .unwrap()
            .storage_node();
        let bucket_pg = bucket_primary.get_pg(bucket_pg_id).unwrap();
        assert_eq!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
                .unwrap()
                .len(),
            1,
            "partial direct PUT command must keep its command-owned bucket write proof live"
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().generation_id, generation_id);
    }

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            |_| -> Result<(), ()> { panic!("retry must reuse the pending direct PUT command") },
        )
        .unwrap()
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    {
        let bucket_primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg_id))
            .unwrap()
            .storage_node();
        let bucket_pg = bucket_primary.get_pg(bucket_pg_id).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
                .unwrap()
                .is_empty(),
            "direct PUT retry convergence must release the command-owned bucket write proof"
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        assert!(pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
    assert_direct_put_metadata_on_acting_nodes(&map, &node_ids, object_pg, &commit_req, &outcome);
    assert_object_version_counter_on_acting_nodes(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        outcome.version_id.to_u64() + 1,
    );
}

#[test]
fn direct_put_open_time_convergence_releases_bucket_write_reservation() {
    let _serial = lock_metadata_command_apply_hook_test();
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let reservation_id = crate::SessionId::try_from("51".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put metadata command open-time convergence";
    let segment_okh = [81; 16];
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
    let failed = Arc::new(AtomicBool::new(false));
    let hook_failed = Arc::clone(&failed);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(1)
                        && !hook_failed.swap(true, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected direct put reopen apply failure",
                        source: std::io::Error::other("injected direct put reopen apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::Io { .. })
    ));
    drop(hook_guard);
    assert!(failed.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial direct PUT should leave a pending command for open-time convergence"
    );
    {
        let bucket_primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg_id))
            .unwrap()
            .storage_node();
        let bucket_pg = bucket_primary.get_pg(bucket_pg_id).unwrap();
        assert_eq!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
                .unwrap()
                .len(),
            1
        );
    }
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap());
    assert!(
        pending_metadata_command_for_test(&reopened, PgId::new(object_pg), &bucket).is_none(),
        "open-time convergence should clear the direct PUT pending command"
    );
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().generation_id, generation_id);
    }
    {
        let bucket_primary = reopened
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg_id))
            .unwrap()
            .storage_node();
        let bucket_pg = bucket_primary.get_pg(bucket_pg_id).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
                .unwrap()
                .is_empty(),
            "open-time convergence must release the command-owned bucket write proof"
        );
    }
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
}

#[test]
fn object_generation_reservation_entry_drains_pending_direct_put_commit() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation_id = crate::SessionId::try_from("22".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put metadata command production retry";
    let segment_okh = [72; 16];
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
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected direct put metadata command apply failure",
                        source: std::io::Error::other(
                            "injected direct put metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected direct put metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial direct PUT metadata command must remain pending"
    );

    let next_reservation_id = crate::SessionId::try_from("23".repeat(16)).unwrap();
    let next_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
        .unwrap();
    assert!(next_generation_id.get() > generation_id.get());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().generation_id, generation_id);
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &next_reservation_id,
            )
            .unwrap(),
            next_generation_id
        );
    }
}

#[test]
fn object_delete_drains_pending_direct_put_commit_before_delete() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let old_committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"old direct object");

    let reservation_id = crate::SessionId::try_from("22".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    assert!(generation_id.get() > old_committed.generation_id.get());
    let payload = b"new direct object with pending command";
    let segment_okh = [73; 16];
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
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected direct put metadata command apply failure",
                        source: std::io::Error::other(
                            "injected direct put metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected direct put metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial direct PUT metadata command must remain pending"
    );
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().generation_id, generation_id);
    }

    let outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            let stored = stored.expect("pending direct PUT should be applied before delete");
            let live = stored
                .as_live()
                .expect("pending direct PUT should publish a live object");
            assert_eq!(live.generation_id, generation_id);
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(matches!(
        outcome.deleted,
        crate::DeletedCurrentObject::Live {
            generation_id: deleted_generation_id,
            ..
        } if deleted_generation_id == generation_id
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }

    let next_reservation_id = crate::SessionId::try_from("23".repeat(16)).unwrap();
    let next_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
        .unwrap();
    assert!(next_generation_id.get() > generation_id.get());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(
                matches!(
                    crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                    Err(crate::MetadataError::ObjectNotFound)
                ),
                "later reservation must not resurrect the deleted pending direct PUT on node {node_id:?}"
            );
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &next_reservation_id,
            )
            .unwrap(),
            next_generation_id
        );
    }
}

#[test]
fn multipart_completion_command_publishes_streamed_part_segments_to_all_acting_nodes() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (mut req, mut expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "streamedcomplete");
    req.versioning = crate::BucketVersioningState::Enabled;

    let replacement_session_id = crate::SessionId::try_from("52".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &req.upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            2,
            &replacement_session_id,
        )
        .unwrap();
    let replacement_payload = b"replacement stream session";
    let replacement_okh = [0x52; 16];
    let (_target, replacement_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: replacement_session_id.clone(),
                segment_index: 0,
                size: replacement_payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(replacement_payload)),
                segment_okh: replacement_okh,
            },
        )
        .unwrap();
    let replacement_shards = cluster
        .write_stream_segment_payload_shards(&replacement_segment, replacement_payload)
        .unwrap();
    let replacement_shard_batch = replacement_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &replacement_session_id,
            replacement_segment.segment_index,
            &replacement_segment,
            &replacement_shard_batch,
        )
        .unwrap();
    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(cluster
            .test_payload_shard_file_exists(
                replacement_segment.data_pg_id,
                ec_shape,
                &replacement_segment.segment_okh,
                replacement_segment.segment_vid,
                shard_index
            )
            .unwrap());
    }
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &req.upload_id)
        .unwrap();
    let requested_parts = req
        .part_records
        .iter()
        .map(|part| part.part_number)
        .collect::<Vec<_>>();
    let completion_snapshot = cluster
        .load_multipart_completion_snapshot(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            &requested_parts,
        )
        .unwrap();
    req.expected_stale_payload_source = completion_snapshot.stale_payload_source;
    req.selected_streaming_segments = completion_snapshot.selected_streaming_segments;
    req.expected_cleanup = completion_snapshot.cleanup;

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();
    expected_segment.version_id = outcome.version_id.to_u64();

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_streamed_multipart_completion_on_acting_nodes(
        &map,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &replacement_session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &replacement_session_id)
                .unwrap()
                .is_empty(),
            "completion must delete staged replacement stream segments on node {node_id:?}"
        );
    }
    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    replacement_segment.data_pg_id,
                    ec_shape,
                    &replacement_segment.segment_okh,
                    replacement_segment.segment_vid,
                    shard_index
                )
                .unwrap(),
            "completion must delete staged replacement stream shard {shard_index}"
        );
    }
    assert_object_version_counter_on_acting_nodes(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        outcome.version_id.to_u64() + 1,
    );
}

#[test]
fn already_recorded_multipart_completion_fanout_cleans_terminal_stream_uploads() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, _) = seed_streamed_multipart_completion(&cluster, &bucket, &key, "mpufanout");
    let stale_session_id = crate::SessionId::try_from("69".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &req.upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            2,
            &stale_session_id,
        )
        .unwrap();
    let (mut command, _) = pending_multipart_completion_command_for_test(
        &map,
        &cluster,
        PgId::new(object_pg),
        &req,
        1234,
    );
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let active_session = crate::PgMetadataStore::get_stream_upload(&*pg, &stale_session_id)
            .expect("active UploadPart stream session");
        let stream_upload_segments =
            crate::PgMetadataStore::list_stream_segments(&*pg, &stale_session_id)
                .expect("active UploadPart stream segments");
        let mut payload = command.payload().clone();
        let MetadataCommandPayload::CommitMultipartObject(commit) = &mut payload else {
            unreachable!("test helper must build a multipart completion command");
        };
        commit.stream_uploads = vec![crate::TerminalStreamCleanupRecord::from(&active_session)];
        commit.stream_upload_segments = stream_upload_segments;
        command = MetadataCommandEnvelope::new(command.id(), payload);
    }

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        crate::PgMetadataStore::create_stream_upload(
            &*pg,
            &crate::CreateStreamUploadReq {
                session_id: stale_session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: crate::StreamUploadTarget::UploadPart {
                    upload_id: req.upload_id.clone(),
                    part_number: 2,
                },
                encryption: crate::ObjectEncryption::None,
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &stale_session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn multipart_completion_over_standard_object_reopens_with_valid_digest() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap());
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let old =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"old standard object payload");
    let (req, expected_segment) = seed_streamed_multipart_completion_with_existing(
        &cluster,
        &bucket,
        &key,
        "stdoverwrite",
        true,
    );

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();
    assert!(
        matches!(
            outcome.stale_payload,
            Some(crate::CompletedMultipartStalePayload::Segments { generation_id, .. })
                if generation_id == old.generation_id
        ),
        "multipart overwrite should record stale standard payload"
    );
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
        2,
    );
    for node_id in &node_ids {
        let pg = map
            .nodes
            .get(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert_eq!(
            pg.test_metadata_digest_table_mismatches().unwrap(),
            Vec::<(String, u64, u64)>::new(),
            "node {node_id:?} should keep digest cache in sync before reopen"
        );
    }

    drop(cluster);
    drop(map);
    LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("multipart overwrite should leave restart digest valid");
}

#[test]
fn multipart_completion_rejects_stale_selected_part_row() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, _) = seed_streamed_multipart_completion(&cluster, &bucket, &key, "stalepart");

    let (_replacement_shards, replacement_part, _replacement_segment) =
        upload_streamed_test_multipart_part(
            &cluster,
            &bucket,
            &key,
            &req.upload_id,
            1,
            [0x5A; 16],
            b"replacement part payload",
        );
    assert_ne!(
        replacement_part, req.part_records[0],
        "test must replace the selected part row after completion snapshot"
    );

    let err = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::StaleMultipartCompletionSnapshot
        ),
        "stale selected part row must fail closed, got {err:?}"
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none(),
        "stale multipart completion must not publish a pending command"
    );
}

#[test]
fn versioned_direct_put_and_multipart_completion_allocate_versions_via_command_stream() {
    #[derive(Default)]
    struct VersionRaceState {
        direct_at_apply: bool,
        multipart_at_apply: bool,
        release_direct: bool,
    }

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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    let (mut multipart_req, mut expected_multipart_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "versionraceupload");
    multipart_req.versioning = crate::BucketVersioningState::Enabled;

    let reservation_id = crate::SessionId::try_from("73".repeat(16)).unwrap();
    let direct_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let direct_payload = b"versioned direct put races multipart completion";
    let direct_okh = [73; 16];
    let direct_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            direct_generation_id,
            0,
            &direct_okh,
            direct_payload,
        )
        .unwrap();
    let mut direct_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id: direct_generation_id,
            payload: direct_payload,
            segment_okh: direct_okh,
            written: &direct_written,
        },
    );
    direct_req.versioning = crate::BucketVersioningState::Enabled;

    let _serial = lock_metadata_command_apply_hook_test();
    let race_state = Arc::new((Mutex::new(VersionRaceState::default()), Condvar::new()));
    let direct_seen = Arc::new(AtomicBool::new(false));
    let multipart_seen = Arc::new(AtomicBool::new(false));
    let hook_key = key.clone();
    let hook_reservation_id = reservation_id.clone();
    let hook_upload_id = multipart_req.upload_id.clone();
    let expected_multipart_req = multipart_req.clone();
    let hook_race_state = Arc::clone(&race_state);
    let hook_direct_seen = Arc::clone(&direct_seen);
    let hook_multipart_seen = Arc::clone(&multipart_seen);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            let (lock, cvar) = &*hook_race_state;
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.key == hook_key
                        && commit.generation_reservation_id == hook_reservation_id
                        && !hook_direct_seen.swap(true, Ordering::SeqCst) =>
                {
                    let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                    state.direct_at_apply = true;
                    cvar.notify_all();
                    while !state.release_direct {
                        state = cvar.wait(state).unwrap_or_else(|e| e.into_inner());
                    }
                }
                MetadataCommandPayload::CommitMultipartObject(commit)
                    if commit.upload_id == hook_upload_id
                        && !hook_multipart_seen.swap(true, Ordering::SeqCst) =>
                {
                    let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                    state.multipart_at_apply = true;
                    cvar.notify_all();
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let direct_cluster = Arc::clone(&cluster);
    let direct_written_shards = direct_written.written_shards.clone();
    let direct_thread = std::thread::spawn(move || {
        direct_cluster.commit_direct_put_object_from_payload_shards(
            &direct_req,
            &direct_written_shards,
            |_| Ok::<_, ()>(()),
        )
    });

    {
        let (lock, cvar) = &*race_state;
        let state = lock.lock().unwrap_or_else(|e| e.into_inner());
        let (state, _) = cvar
            .wait_timeout_while(state, Duration::from_secs(5), |state| {
                !state.direct_at_apply
            })
            .unwrap();
        assert!(
            state.direct_at_apply,
            "direct PUT did not reach command apply"
        );
    }

    let multipart_cluster = Arc::clone(&cluster);
    let multipart_thread = std::thread::spawn(move || {
        multipart_cluster.complete_multipart_upload_commit_serialized(multipart_req, 16)
    });

    {
        let (lock, cvar) = &*race_state;
        let state = lock.lock().unwrap_or_else(|e| e.into_inner());
        let (mut state, _) = cvar
            .wait_timeout_while(state, Duration::from_millis(100), |state| {
                !state.multipart_at_apply
            })
            .unwrap();
        assert!(
            !state.multipart_at_apply,
            "multipart completion applied while direct PUT held the bucket command stream"
        );
        state.release_direct = true;
        cvar.notify_all();
    }

    let direct_outcome = direct_thread.join().unwrap().unwrap().unwrap();
    let multipart_outcome = multipart_thread.join().unwrap().unwrap();
    drop(hook_guard);

    assert_eq!(direct_outcome.version_id, crate::VersionId::from_u64(1));
    assert_eq!(multipart_outcome.version_id, crate::VersionId::from_u64(2));
    expected_multipart_segment.version_id = multipart_outcome.version_id.to_u64();

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let direct_version = crate::PgMetadataStore::get_object_version(
            &*pg,
            &bucket,
            &key,
            direct_outcome.version_id,
        )
        .unwrap();
        let direct_live = direct_version.as_live().unwrap();
        assert_eq!(direct_live.generation_id, direct_generation_id);
        assert!(direct_live.became_noncurrent_at.is_some());
    }
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        object_pg,
        &expected_multipart_req,
        &expected_multipart_segment,
        &multipart_outcome,
        2,
    );
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
}

#[test]
fn stream_upload_part_staging_and_finalize_use_object_metadata_commands() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("streampartcmd");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
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

    let session_id = crate::SessionId::try_from("43".repeat(16)).unwrap();
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

    let payload = b"streamed multipart command part";
    let segment_okh = [0x43; 16];
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh,
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

    let expected_part = cluster
        .finalize_upload_part_stream(&bucket, &key, &upload_id, &session_id, 1, |snapshot| {
            let generation = snapshot
                .existing_part_generation
                .map_or(0, |generation| generation + 1);
            let part = crate::MultipartPartRecord {
                upload_id: upload_id.clone(),
                part_number: 1,
                generation,
                size: payload.len() as u64,
                etag: vec![0x55; 8],
                etag_kind: crate::EtagKind::Crc64,
                part_okh: [0u8; 16],
                part_vid: crate::GenerationId::new(u64::from(generation) + 1).unwrap(),
                ec_k: segment.ec_k,
                ec_m: segment.ec_m,
                last_modified: 123_456,
                checksum: None,
            };
            let segments = snapshot
                .staging_segments
                .iter()
                .map(|staged| crate::MultipartPartSegmentRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload_id: upload_id.clone(),
                    version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
                    part_number: 1,
                    segment_index: staged.segment_index,
                    size: staged.size,
                    segment_crc64: staged.segment_crc64,
                    segment_okh: staged.segment_okh,
                    segment_vid: staged.segment_vid,
                    data_pg_id: staged.data_pg_id,
                    ec_k: staged.ec_k,
                    ec_m: staged.ec_m,
                })
                .collect::<Vec<_>>();
            Ok::<_, ()>(crate::PreparedStreamPartCommit {
                value: part.clone(),
                part,
                segments,
            })
        })
        .unwrap()
        .unwrap()
        .value;

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
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
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];
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

fn stream_upload_part_create_rejects_raced_upload_state(target_state: crate::UploadState) {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("streamraced");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
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

    let _serial = lock_metadata_command_apply_hook_test();
    let did_flip = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_upload_id = upload_id.clone();
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_node_ids = node_ids;
    let hook_did_flip = Arc::clone(&did_flip);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            if let MetadataCommandPayload::CreateStreamUpload(create) = command.payload() {
                let is_target_upload = matches!(
                    &create.session.target,
                    crate::StreamUploadTarget::UploadPart { upload_id, .. }
                        if upload_id == &hook_upload_id
                );
                if create.session.bucket == hook_bucket
                    && create.session.key == hook_key
                    && is_target_upload
                    && !hook_did_flip.swap(true, Ordering::SeqCst)
                {
                    for node_id in hook_node_ids {
                        let node = hook_map.node(node_id).unwrap().storage_node();
                        let pg = node.get_pg(object_pg).unwrap();
                        crate::PgMetadataStore::set_upload_state(
                            &*pg,
                            &hook_upload_id,
                            target_state,
                        )
                        .unwrap();
                        pg.refresh_metadata_command_state_digest().unwrap();
                    }
                }
            }
            Ok(())
        },
    ));

    let session_id = crate::SessionId::try_from("44".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    let err = cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap_err();
    drop(hook_guard);

    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(crate::MetadataError::NoSuchUpload { .. })
        ),
        "expected raced upload state to reject stream session creation, got {err:?}"
    );
    assert!(did_flip.load(Ordering::SeqCst));
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
fn stream_upload_part_create_rejects_raced_multipart_abort() {
    stream_upload_part_create_rejects_raced_upload_state(crate::UploadState::Aborting);
}

#[test]
fn stream_upload_part_create_rejects_raced_multipart_completion() {
    stream_upload_part_create_rejects_raced_upload_state(crate::UploadState::Completing);
}

#[test]
fn multipart_completion_order_is_bucket_primary_serialized_across_object_pgs() {
    #[derive(Default)]
    struct CompletionRaceState {
        first_at_apply: bool,
        second_at_apply: bool,
        release_first: bool,
    }

    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key_a, key_b, object_pg_a, object_pg_b) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 0, "mpu-order-bucket-");
        let key_a = key_for_object_pg(topology, &bucket, 1, "mpu-order-a-");
        let key_b = key_for_object_pg(topology, &bucket, 2, "mpu-order-b-");
        (bucket, key_a, key_b, 1, 2)
    };
    set_route_primary(&mut map, object_pg_a, NodeId::new(1));
    set_route_primary(&mut map, object_pg_b, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req_a, _) = seed_streamed_multipart_completion(&cluster, &bucket, &key_a, "bucketordera");
    let (req_b, _) = seed_streamed_multipart_completion(&cluster, &bucket, &key_b, "bucketorderb");

    let _serial = lock_metadata_command_apply_hook_test();
    let race_state = Arc::new((Mutex::new(CompletionRaceState::default()), Condvar::new()));
    let first_seen = Arc::new(AtomicBool::new(false));
    let second_seen = Arc::new(AtomicBool::new(false));
    let hook_key_a = key_a.clone();
    let hook_key_b = key_b.clone();
    let hook_race_state = Arc::clone(&race_state);
    let hook_first_seen = Arc::clone(&first_seen);
    let hook_second_seen = Arc::clone(&second_seen);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() else {
                return Ok(());
            };
            let (lock, cvar) = &*hook_race_state;
            if commit.object.key == hook_key_a && !hook_first_seen.swap(true, Ordering::SeqCst) {
                let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                state.first_at_apply = true;
                cvar.notify_all();
                while !state.release_first {
                    state = cvar.wait(state).unwrap_or_else(|e| e.into_inner());
                }
            } else if commit.object.key == hook_key_b
                && !hook_second_seen.swap(true, Ordering::SeqCst)
            {
                let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                state.second_at_apply = true;
                cvar.notify_all();
            }
            Ok(())
        },
    ));

    let cluster_a = Arc::clone(&cluster);
    let first = std::thread::spawn(move || {
        cluster_a.complete_multipart_upload_commit_serialized(req_a, 16)
    });

    {
        let (lock, cvar) = &*race_state;
        let state = lock.lock().unwrap_or_else(|e| e.into_inner());
        let (state, _) = cvar
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.first_at_apply)
            .unwrap();
        assert!(
            state.first_at_apply,
            "first completion did not reach command apply"
        );
    }

    let cluster_b = Arc::clone(&cluster);
    let second_upload_id = req_b.upload_id.clone();
    let second = std::thread::spawn(move || {
        cluster_b.complete_multipart_upload_commit_serialized(req_b, 16)
    });

    {
        let (lock, cvar) = &*race_state;
        let state = lock.lock().unwrap_or_else(|e| e.into_inner());
        let (mut state, _) = cvar
            .wait_timeout_while(state, Duration::from_millis(100), |state| {
                !state.second_at_apply
            })
            .unwrap();
        state.release_first = true;
        cvar.notify_all();
    }

    let first_outcome = first.join().unwrap().unwrap();
    let second_outcome = second.join().unwrap().unwrap();
    drop(hook_guard);

    assert_eq!(first_outcome.version_id, crate::VersionId::Null);
    assert_eq!(second_outcome.version_id, crate::VersionId::Null);
    let mut orders = vec![
        completed_multipart_order_on_node(
            &map,
            NodeId::new(0),
            object_pg_a,
            &bucket,
            &upload_id_from_label("bucketordera"),
        ),
        completed_multipart_order_on_node(
            &map,
            NodeId::new(0),
            object_pg_b,
            &bucket,
            &second_upload_id,
        ),
    ];
    orders.sort_unstable();
    assert_eq!(orders, vec![1, 2]);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let bucket_pg = node
            .get_pg(node.pg_topology().bucket_pg_for(&bucket))
            .unwrap();
        assert_eq!(
            bucket_pg
                .completed_multipart_upload_sequence_for_bucket(&bucket)
                .unwrap(),
            2,
            "node {node_id:?} did not catch up bucket completed MPU order"
        );
    }
}

#[test]
fn multipart_completion_zero_apply_failure_retains_pending_command_for_retry() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "zerofailcomplete");

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitMultipartObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected multipart completion command apply failure",
                        source: std::io::Error::other(
                            "injected multipart completion command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected multipart completion command apply failure",
                ..
            })
        ),
        "expected injected zero-apply failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "zero-apply multipart completion failure must keep its pending command"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_streamed_multipart_completion_on_acting_nodes(
        &map,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
    );
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn multipart_completion_partial_apply_reopens_and_converges() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, mut expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "completereopen");

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_upload_id = req.upload_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitMultipartObject(commit)
                    if commit.upload_id == hook_upload_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected multipart completion reopen failure",
                        source: std::io::Error::other(
                            "injected multipart completion reopen failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected multipart completion reopen failure",
                ..
            })
        ),
        "expected injected partial completion failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial multipart completion command must remain durable before reopen"
    );
    drop(cluster);
    drop(map);

    let reopened = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("reopen local map with in-flight multipart completion");
    let reopened = Arc::new(reopened);
    assert!(
        pending_metadata_command_for_test(&reopened, PgId::new(object_pg), &bucket).is_none(),
        "open-time recovery should converge and clear the partial completion command"
    );

    let first_pg = reopened
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    let stored = crate::PgMetadataStore::get_object_meta(&*first_pg, &bucket, &key).unwrap();
    let live = stored.as_live().unwrap();
    let outcome = crate::CompleteMultipartCommitOutcome {
        version_id: live.version_id,
        stale_payload: None,
        live_tags: live.tags.clone(),
        live_size: live.size,
        live_last_modified: live.last_modified,
    };
    expected_segment.version_id = outcome.version_id.to_u64();
    drop(first_pg);

    assert_streamed_multipart_completion_on_acting_nodes(
        &reopened,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
    );
    assert_terminal_multipart_upload_invariants(
        &reopened,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
    assert_bucket_write_reservations_released(&reopened, &bucket);
}

#[test]
fn multipart_completion_command_id_race_drains_winner_and_resnapshots_stale_payload() {
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
    let bucket = bucket_for_pg(topology, 1, "mpu-complete-id-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);
    let (req, expected_segment) =
        seed_streamed_multipart_completion(&first_cluster, &bucket, &key, "completeidrace");

    let winner_payload = b"winner before multipart completion command id";
    let winner_reservation_id =
        crate::SessionId::try_from("8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            winner_generation_id,
            0,
            &[0x8a; 16],
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
            segment_okh: [0x8a; 16],
            written: &winner_written,
        },
    );
    let winner_shard_batch: Vec<(&ShardKey, WriteAck)> = winner_written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    second_cluster
        .register_payload_shard_acks(winner_req.data_pg_id, &winner_shard_batch)
        .unwrap();
    let winner_command = {
        let primary = second_map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
            .unwrap();
        let pg = primary.storage_node().get_pg(2).unwrap();
        second_cluster
            .prepare_commit_direct_put_object_command(
                PgId::new(2),
                &pg,
                &winner_req,
                crate::VersionId::Null,
                winner_req.bucket_write_reservation.clone(),
            )
            .unwrap()
    };

    let slot_installed = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_command = winner_command.clone();
    let hook_slot_installed = Arc::clone(&slot_installed);
    let hook_guard = first_cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            let MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(advance) =
                command.payload()
            else {
                return Ok(());
            };
            if advance.bucket == hook_bucket && !hook_slot_installed.swap(true, Ordering::SeqCst) {
                let pg_id = PgId::new(2);
                let primary = hook_map
                    .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                    .unwrap();
                let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
                pg.try_insert_pending_metadata_command_slot(
                    primary.node_id().as_u32(),
                    &hook_command,
                    Some(&hook_bucket),
                )
                .unwrap();
            }
            Ok(())
        },
    ));

    let outcome = first_cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();
    drop(hook_guard);

    assert!(slot_installed.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());
    assert!(
        matches!(
            outcome.stale_payload,
            Some(crate::CompletedMultipartStalePayload::Segments { generation_id, .. })
                if generation_id == winner_generation_id
        ),
        "completion must resnapshot stale payload after draining the winning object command"
    );
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &first_map,
        &node_ids,
        2,
        &req,
        &expected_segment,
        &outcome,
        2,
    );
    assert_clean_metadata_command_stream(&first_map, &[1, 2]);
}

#[test]
fn multipart_completion_retries_partial_bucket_order_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "mpu-order-retry-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "orderretrycomplete");

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(advance)
                    if advance.bucket == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected completed MPU order apply failure",
                        source: std::io::Error::other("injected completed MPU order apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected completed MPU order apply failure",
                ..
            })
        ),
        "expected injected bucket-PG order failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_some(),
        "partial bucket-PG order command must remain pending"
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none(),
        "object-PG completion must not publish before order command converges"
    );

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());
    assert!(pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none());
    assert_eq!(
        completed_multipart_order_on_node(&map, NodeId::new(0), 2, &bucket, &req.upload_id),
        1
    );
    assert_streamed_multipart_completion_on_acting_nodes(
        &map,
        &node_ids,
        2,
        &req,
        &expected_segment,
        &outcome,
    );
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.completed_multipart_upload_sequence, 1);
    }
    assert_clean_metadata_command_stream(&map, &[1, 2]);
}

#[test]
fn completed_multipart_order_command_id_race_drains_winner_and_retries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "mpu-order-command-id-race-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let contender = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_once = Arc::new(AtomicBool::new(true));
    let hook_once_for_closure = Arc::clone(&hook_once);
    let _hook_guard = cluster.test_install_before_completed_multipart_order_command_id_hook(
        Arc::new(move || {
            if !hook_once_for_closure.swap(false, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(1);
            let command = MetadataCommandEnvelope::new(
                contender.next_metadata_command_id(pg_id).unwrap(),
                MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
                    AdvanceCompletedMultipartUploadSequenceCommand {
                        bucket: hook_bucket.clone(),
                        completion_order: 1,
                    },
                ),
            );
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
        }),
    );

    let completion_order = cluster
        .test_reserve_completed_multipart_upload_order(&bucket)
        .unwrap();

    assert!(!hook_once.load(Ordering::SeqCst));
    assert_eq!(completion_order, 2);
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.completed_multipart_upload_sequence, 2);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn multipart_completion_drains_matching_pending_completion() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, mut expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "samecomplete");
    let pg_id = PgId::new(object_pg);
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let last_modified_millis = 987_657;
    let (pending_completion, write_sequence) = pending_multipart_completion_command_for_test(
        &map,
        &cluster,
        pg_id,
        &req,
        last_modified_millis,
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending_completion);

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_tags, req.tags);
    assert_eq!(outcome.live_size, req.size);
    assert_eq!(outcome.live_last_modified, last_modified_millis);
    assert!(outcome.stale_payload.is_none());
    expected_segment.version_id = crate::VersionId::Null.to_u64();
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
        write_sequence,
    );
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    for node_id in node_ids {
        let bucket_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(bucket_pg_id)
            .unwrap();
        let info =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*bucket_pg, &bucket).unwrap();
        assert_eq!(
            info.completed_multipart_upload_sequence, 1,
            "same-upload completion retry must not allocate a second order on node {node_id:?}"
        );
    }
    assert_clean_metadata_command_stream(&map, &[bucket_pg_id, object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn multipart_completion_pending_install_conflict_with_matching_completion_returns_success() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, mut expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "racecomplete");
    let pg_id = PgId::new(object_pg);
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let last_modified_millis = 987_659;
    let (pending_completion, write_sequence) = pending_multipart_completion_command_for_test(
        &map,
        &cluster,
        pg_id,
        &req,
        last_modified_millis,
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_command_template = pending_completion.clone();
    let hook_bucket_pg_id = bucket_pg_id;
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let bucket_primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(hook_bucket_pg_id))
                .unwrap();
            let bucket_pg = bucket_primary
                .storage_node()
                .get_pg(hook_bucket_pg_id)
                .unwrap();
            let completion_order = bucket_pg
                .completed_multipart_upload_sequence_for_bucket(&hook_bucket)
                .unwrap();
            drop(bucket_pg);
            let mut payload = hook_command_template.payload().clone();
            let MetadataCommandPayload::CommitMultipartObject(commit) = &mut payload else {
                panic!("test command must be a multipart completion");
            };
            commit.completion_order = completion_order;
            let hook_command = MetadataCommandEnvelope::new(hook_command_template.id(), payload);
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &hook_command);
        }));

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();

    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_tags, req.tags);
    assert_eq!(outcome.live_size, req.size);
    assert_eq!(outcome.live_last_modified, last_modified_millis);
    assert!(outcome.stale_payload.is_none());
    expected_segment.version_id = crate::VersionId::Null.to_u64();
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
        write_sequence,
    );
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_clean_metadata_command_stream(&map, &[bucket_pg_id, object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn multipart_completion_drains_other_upload_same_key_and_resnapshots_stale_payload() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (first_req, first_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "firstsamekey");
    let (second_req, mut second_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "secondsamekey");
    assert_ne!(first_req.upload_id, second_req.upload_id);
    assert_ne!(first_req.generation_id, second_req.generation_id);
    let pg_id = PgId::new(object_pg);
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let first_last_modified_millis = 987_658;
    let (pending_first_completion, _first_write_sequence) =
        pending_multipart_completion_command_for_test(
            &map,
            &cluster,
            pg_id,
            &first_req,
            first_last_modified_millis,
        );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending_first_completion);

    let second_outcome = cluster
        .complete_multipart_upload_commit_serialized(second_req.clone(), 16)
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert!(
        matches!(
            second_outcome.stale_payload,
            Some(crate::CompletedMultipartStalePayload::Multipart { generation_id, .. })
                if generation_id == first_req.generation_id
        ),
        "second completion should reclaim the first completed upload payload, got {:?}",
        second_outcome.stale_payload
    );
    second_segment.version_id = crate::VersionId::Null.to_u64();
    assert_eq!(
        completed_multipart_order_on_node(
            &map,
            NodeId::new(0),
            object_pg,
            &bucket,
            &first_req.upload_id
        ),
        1
    );
    assert_eq!(
        completed_multipart_order_on_node(
            &map,
            NodeId::new(0),
            object_pg,
            &bucket,
            &second_req.upload_id
        ),
        2
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let object_pg_store = node.get_pg(object_pg).unwrap();
        let stored =
            crate::PgMetadataStore::get_object_meta(&*object_pg_store, &bucket, &key).unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(live.generation_id, second_req.generation_id);
        assert_eq!(live.version_id, second_outcome.version_id);
        assert_eq!(live.size, second_req.size);
        assert_eq!(live.last_modified, second_outcome.live_last_modified);
        assert_eq!(
            crate::PgMetadataStore::get_object_parts(
                &*object_pg_store,
                &bucket,
                &key,
                second_outcome.version_id
            )
            .unwrap(),
            vec![crate::ObjectPartRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: second_outcome.version_id,
                part_number: second_req.part_records[0].part_number,
                size: second_req.part_records[0].size,
                etag: second_req.part_records[0].etag.clone(),
                etag_kind: second_req.part_records[0].etag_kind,
                part_okh: second_req.part_records[0].part_okh,
                part_vid: second_req.part_records[0].part_vid,
                ec_k: second_req.part_records[0].ec_k,
                ec_m: second_req.part_records[0].ec_m,
                data_pg_id: node
                    .pg_topology()
                    .object_generation_multipart_part_data_pg(
                        &bucket,
                        &key,
                        second_req.generation_id,
                        second_req.part_records[0].part_number,
                    )
                    .get(),
                checksum: second_req.part_records[0].checksum.clone(),
            }]
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part_segments(
                &*object_pg_store,
                &bucket,
                &key,
                second_outcome.version_id,
                second_segment.part_number,
            )
            .unwrap(),
            vec![second_segment.clone()]
        );
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*object_pg_store,
            &bucket,
            &key,
            first_req.generation_id
        )
        .unwrap());
        assert_eq!(
            crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(
                &*object_pg_store,
                &first_req.upload_id
            )
            .unwrap(),
            Vec::<crate::MultipartPartSegmentRecord>::new(),
            "first completed upload staging rows should be removed on node {node_id:?}"
        );
        let bucket_pg_store = node.get_pg(bucket_pg_id).unwrap();
        let info =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*bucket_pg_store, &bucket)
                .unwrap();
        assert_eq!(info.completed_multipart_upload_sequence, 2);
    }
    let mut first_readback = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: first_segment.data_pg_id,
                segment_okh: first_segment.segment_okh,
                segment_vid: first_segment.segment_vid,
                stored_size: first_segment.size as usize,
                segment_crc64: first_segment.segment_crc64,
                ec: EcShape {
                    k: first_segment.ec_k,
                    m: first_segment.ec_m,
                },
            },
            &mut first_readback,
        )
        .unwrap();
    assert_eq!(first_readback, b"streamed completion");
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &first_req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &second_req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_clean_metadata_command_stream(&map, &[bucket_pg_id, object_pg]);
}

#[test]
fn multipart_completion_drains_pending_abort_before_completing() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, uploaded_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "abortwinscomplete");

    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(cluster
            .test_payload_shard_file_exists(
                uploaded_segment.data_pg_id,
                ec_shape,
                &uploaded_segment.segment_okh,
                uploaded_segment.segment_vid,
                shard_index,
            )
            .unwrap());
    }

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_upload_id = req.upload_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AbortMultipartUpload(abort)
                    if abort.upload_id == hook_upload_id
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected multipart abort before completion failure",
                        source: std::io::Error::other(
                            "injected multipart abort before completion failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .abort_multipart_upload(&bucket, &key, &req.upload_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected multipart abort before completion failure",
                ..
            })
        ),
        "expected injected abort failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial abort must remain pending for completion to drain"
    );

    let err = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(crate::MetadataError::NoSuchUpload { .. })
        ),
        "completion should observe the pending abort as the terminal upload outcome, got {err:?}"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &req.upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(pg
            .list_completed_multipart_uploads_for_bucket(bucket.as_str())
            .unwrap()
            .is_empty());
    }
    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    uploaded_segment.data_pg_id,
                    ec_shape,
                    &uploaded_segment.segment_okh,
                    uploaded_segment.segment_vid,
                    shard_index,
                )
                .unwrap(),
            "completion draining abort should delete uploaded part shard {shard_index}"
        );
    }
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Aborted,
    );
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn object_delete_metadata_command_applies_to_all_acting_object_pg_nodes() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"delete me");

    let outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(matches!(
        outcome.deleted,
        crate::DeletedCurrentObject::Live {
            generation_id,
            ..
        } if generation_id == committed.generation_id
    ));

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(
            crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap(),
            "delete command should publish reclaim metadata on node {node_id:?}"
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn object_delete_metadata_command_retry_reuses_pending_partial_replica_command() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"partial delete");

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected object delete metadata command apply failure",
                        source: std::io::Error::other(
                            "injected object delete metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected object delete metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial object delete command must remain pending"
    );
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
    {
        let failed_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = failed_replica.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(
            stored.as_live().unwrap().generation_id,
            committed.generation_id
        );
    }

    let outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(stored.is_none());
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(
        matches!(outcome.deleted, crate::DeletedCurrentObject::Missing)
            || matches!(
                outcome.deleted,
                crate::DeletedCurrentObject::Live {
                    generation_id,
                    ..
                } if generation_id == committed.generation_id
            )
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap());
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn object_delete_exact_pending_retry_converges_partial_exact_conflict() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let _committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"exact delete retry");

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let fail_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected exact pending delete apply failure",
                        source: std::io::Error::other(
                            "injected exact pending delete apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected exact pending delete apply failure",
                ..
            })
        ),
        "expected injected node-2 failure, got {err:?}"
    );
    drop(fail_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some());

    let applied_by_hook = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let applied_by_hook_guard = Arc::clone(&applied_by_hook);
    let apply_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && node_id == NodeId::new(2)
                        && !applied_by_hook_guard.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(NodeId::new(2)).unwrap().storage_node();
                    let pg = node.get_pg(command.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(NodeId::new(2).as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual exact pending delete command apply failed: {error}")
                            }
                        })?;
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let outcome = cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                assert!(
                    stored.is_none(),
                    "primary-first retry should drain the pending delete before observing a fresh missing object"
                );
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
    drop(apply_guard);
    assert!(applied_by_hook.load(Ordering::SeqCst));
    assert!(matches!(
        outcome.deleted,
        crate::DeletedCurrentObject::Missing
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
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
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn object_delete_metadata_command_partial_apply_reopens_and_releases_bucket_write_reservation() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"delete reopen");

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected object delete reopen apply failure",
                        source: std::io::Error::other(
                            "injected object delete reopen apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected object delete reopen apply failure",
                ..
            })
        ),
        "expected injected primary failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial object delete command must remain durable before reopen"
    );
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(
            stored.as_live().unwrap().generation_id,
            committed.generation_id
        );
    }
    {
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*primary_pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
    drop(cluster);
    drop(map);

    let reopened = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
            .expect("reopen local map with in-flight object delete command"),
    );
    assert!(
        pending_metadata_command_for_test(&reopened, PgId::new(object_pg), &bucket).is_none(),
        "open-time recovery should converge and clear the partial object delete command"
    );
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(
            crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap(),
            "open-time delete convergence should publish reclaim metadata on node {node_id:?}"
        );
    }
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
    assert_bucket_write_reservations_released(&reopened, &bucket);
}

#[test]
fn completed_multipart_order_drains_same_pg_object_command_with_cleanup_hooks() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, pg_id) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "mpu-order-drains-object-");
        let key = key_for_object_pg(topology, &bucket, 1, "same-pg-key-");
        (bucket, key, 1)
    };
    set_route_primary(&mut map, pg_id, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let committed = write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &key,
        b"same pg object command cleanup",
    );
    assert!(cluster.try_take_reclaim_work().is_none());

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected same-pg object delete apply failure",
                        source: std::io::Error::other(
                            "injected same-pg object delete apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected same-pg object delete apply failure",
                ..
            })
        ),
        "expected injected delete failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, PgId::new(pg_id), &bucket).is_some());
    assert!(cluster.try_take_reclaim_work().is_none());

    let completion_order = cluster
        .test_reserve_completed_multipart_upload_order(&bucket)
        .unwrap();
    assert_eq!(completion_order, 1);
    assert!(pending_metadata_command_for_test(&map, PgId::new(pg_id), &bucket).is_none());
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == committed.generation_id
    ));
    assert!(cluster.try_take_reclaim_work().is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap());
        let info = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.completed_multipart_upload_sequence, completion_order);
    }
}

#[test]
fn completed_multipart_order_drains_other_bucket_sequence_without_stealing_order() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket_a = bucket_for_pg(topology, 1, "mpu-order-other-a-");
    let bucket_b = bucket_for_pg(topology, 1, "mpu-order-other-b-");
    assert_ne!(bucket_a, bucket_b);
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket_a);
    create_test_bucket(&cluster, &bucket_b);

    assert_eq!(
        cluster
            .test_reserve_completed_multipart_upload_order(&bucket_b)
            .unwrap(),
        1
    );

    let pg_id = PgId::new(1);
    let command = MetadataCommandEnvelope::new(
        cluster.next_metadata_command_id(pg_id).unwrap(),
        MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
            AdvanceCompletedMultipartUploadSequenceCommand {
                bucket: bucket_a.clone(),
                completion_order: 1,
            },
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket_a, &command);

    assert_eq!(
        cluster
            .test_reserve_completed_multipart_upload_order(&bucket_b)
            .unwrap(),
        2
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket_b).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let bucket_a_info =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket_a).unwrap();
        let bucket_b_info =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket_b).unwrap();
        assert_eq!(bucket_a_info.completed_multipart_upload_sequence, 1);
        assert_eq!(bucket_b_info.completed_multipart_upload_sequence, 2);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn object_metadata_update_commands_apply_to_all_acting_object_pg_nodes() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"object metadata");
    let tags = "<Tagging><TagSet><Tag><Key>tier</Key><Value>hot</Value></Tag></TagSet></Tagging>";
    let retention = crate::ObjectRetention {
        mode: crate::ObjectLockMode::Governance,
        retain_until_unix_seconds: 123_456,
    };
    let acl_grants = crate::AclGrants::default();

    let tagged_version = cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap()
        .unwrap();
    assert_eq!(tagged_version, crate::VersionId::Null);
    cluster
        .put_object_retention_if(&bucket, &key, None, retention, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap()
        .unwrap();
    cluster
        .put_object_legal_hold_if(
            &bucket,
            &key,
            None,
            crate::StoredLegalHoldStatus::On,
            |stored| Ok::<_, ()>(stored.version_id()),
        )
        .unwrap()
        .unwrap();
    let acl_version = cluster
        .put_object_acl_if(&bucket, &key, None, |stored| {
            Ok::<_, ()>((stored.version_id(), acl_grants.clone(), true))
        })
        .unwrap()
        .unwrap();
    assert_eq!(acl_version, crate::VersionId::Null);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, crate::VersionId::Null)
                .unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(live.tags.as_ref().map(|tags| tags.as_str()), Some(tags));
        assert_eq!(live.object_lock.retention, Some(retention));
        assert_eq!(
            live.object_lock.legal_hold,
            crate::StoredLegalHoldStatus::On
        );
        assert_eq!(live.acl_grants, acl_grants);
        assert!(live.public_read);
    }

    cluster
        .delete_object_tags_if(&bucket, &key, None, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap()
        .unwrap();
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap(),
            None
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn object_metadata_update_retry_converges_pending_partial_replica_command() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"partial object metadata");
    let tags = "<Tagging><TagSet><Tag><Key>retry</Key><Value>yes</Value></Tag></TagSet></Tagging>";
    fn require_tags_absent(stored: &crate::StoredObject) -> Result<crate::VersionId, &'static str> {
        if stored.as_live().unwrap().tags.is_some() {
            Err("tags already visible before pending command convergence")
        } else {
            Ok(stored.version_id())
        }
    }

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutObjectMetadata(update)
                    if update.object.bucket == hook_bucket
                        && update.object.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected object metadata command apply failure",
                        source: std::io::Error::other(
                            "injected object metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_object_tags_if(&bucket, &key, None, tags, require_tags_absent)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected object metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial object metadata command must remain pending"
    );
    for node_id in [NodeId::new(0), NodeId::new(2)] {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap(),
            None
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap()
                .as_deref(),
            Some(tags)
        );
    }

    cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap()
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap()
                .as_deref(),
            Some(tags)
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn object_metadata_partial_apply_reopens_and_converges() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"object metadata reopen");
    let tags =
        "<Tagging><TagSet><Tag><Key>retry</Key><Value>reopen</Value></Tag></TagSet></Tagging>";

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutObjectMetadata(update)
                    if update.object.bucket == hook_bucket
                        && update.object.key == hook_key
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected object metadata reopen apply failure",
                        source: std::io::Error::other(
                            "injected object metadata reopen apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));
    let err = cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected object metadata reopen apply failure",
                ..
            })
        ),
        "expected injected primary failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial object metadata command must remain durable before reopen"
    );
    drop(cluster);
    drop(map);

    let reopened_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let reopened_map = Arc::new(reopened_map);
    assert!(
        pending_metadata_command_for_test(&reopened_map, PgId::new(object_pg), &bucket).is_none(),
        "open-time recovery should converge and clear the partial object metadata command"
    );
    for node_id in node_ids {
        let node = reopened_map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(&*pg, &bucket, &key, crate::VersionId::Null,)
                .unwrap()
                .as_deref(),
            Some(tags)
        );
    }
    assert_clean_metadata_command_stream(&reopened_map, &[object_pg]);
    assert_bucket_write_reservations_released(&reopened_map, &bucket);
}

#[test]
fn object_metadata_retry_rejects_same_mutation_with_mismatched_post_image() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"metadata mismatch");
    let tags = "<Tagging><TagSet><Tag><Key>retry</Key><Value>no</Value></Tag></TagSet></Tagging>";

    let primary = map.node(NodeId::new(1)).unwrap().storage_node();
    let pg = primary.get_pg(object_pg).unwrap();
    let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
        .unwrap()
        .into_live()
        .unwrap();
    drop(pg);
    let mut mismatched_post_image = stored.clone();
    mismatched_post_image.tags = Some(crate::SerializedTagSet::new(tags.to_string()));
    mismatched_post_image.public_read = !stored.public_read;
    let pg_id = PgId::new(object_pg);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-put-object-metadata",
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
            bucket_write_reservation: proof,
            object: mismatched_post_image,
        })),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let err = cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                context: "conflicting pending command for object metadata update",
            })
        ),
        "expected conflicting post-image error, got {err:?}"
    );
    let pg = primary.get_pg(object_pg).unwrap();
    let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
    assert_eq!(stored.as_live().unwrap().tags, None);
    drop(pg);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_some());
}

#[test]
fn object_metadata_command_rejects_non_metadata_post_image_mismatch() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"metadata apply mismatch");
    let tags = "<Tagging><TagSet><Tag><Key>apply</Key><Value>no</Value></Tag></TagSet></Tagging>";
    let primary = map.node(NodeId::new(1)).unwrap().storage_node();
    let pg = primary.get_pg(object_pg).unwrap();
    let mut post_image = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
        .unwrap()
        .into_live()
        .unwrap();
    drop(pg);
    post_image.tags = Some(crate::SerializedTagSet::new(tags.to_string()));
    post_image.size += 1;

    let pg_id = PgId::new(object_pg);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-put-object-metadata",
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
            bucket_write_reservation: proof,
            object: post_image,
        })),
    );
    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
                context: "put object metadata command preimage mismatch",
                ..
            })
        ),
        "expected preimage mismatch, got {err:?}"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().tags, None);
    }
}

#[test]
fn lifecycle_current_expiration_delete_command_applies_to_all_acting_object_pg_nodes() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    put_test_lifecycle(&cluster, &bucket);
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"expired current");

    let outcome = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            committed.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, record| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert_eq!(record.generation_id, committed.generation_id);
                Ok::<_, ()>(true)
            },
        )
        .unwrap()
        .unwrap()
        .expect("current object should expire");
    assert_eq!(outcome.reclaim_generation_id, Some(committed.generation_id));
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == committed.generation_id
    ));
    assert!(cluster.try_take_reclaim_work().is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap());
    }
}

#[test]
fn lifecycle_current_expiration_stops_after_bucket_recreate_before_proof() {
    let _serial = lock_bucket_scoped_hook_test();
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
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    put_test_lifecycle(&cluster, &bucket);
    let old_bucket_incarnation = cluster
        .head_bucket_info(&bucket)
        .unwrap()
        .bucket_incarnation_generation;
    let old_committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"old lifecycle object");

    let hook_ran = Arc::new(AtomicBool::new(false));
    let fresh_generation_id = Arc::new(Mutex::new(None));
    let hook_cluster = Arc::clone(&cluster);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let old_generation_id = old_committed.generation_id;
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let fresh_generation_id_for_hook = Arc::clone(&fresh_generation_id);
    let _hook_guard =
        crate::node::install_bucket_scoped_test_hooks(crate::node::BucketScopedTestHooks {
            target: Some(bucket.clone()),
            before_lifecycle_bucket_write_proof_acquire: Some(Arc::new(move || {
                if hook_ran_for_hook.swap(true, Ordering::SeqCst) {
                    return;
                }
                hook_cluster
                    .delete_current_object_if(&hook_bucket, &hook_key, |_| Ok::<(), ()>(()))
                    .unwrap()
                    .expect("old live object should be deleted before bucket recreate");
                hook_cluster
                    .reclaim_object_payload_if_unleased(&hook_bucket, &hook_key, old_generation_id)
                    .expect("test should reclaim the old payload");
                hook_cluster
                    .begin_bucket_delete(&hook_bucket)
                    .expect("test should begin old bucket delete");
                assert_eq!(
                    hook_cluster
                        .try_finalize_bucket_delete(&hook_bucket)
                        .expect("test should finalize old bucket delete"),
                    crate::BucketDeleteFinalizeOutcome::Finalized
                );
                create_test_bucket(&hook_cluster, &hook_bucket);
                let fresh = write_committed_direct_segment_for_with_versioning(
                    &hook_cluster,
                    &hook_bucket,
                    &hook_key,
                    crate::BucketVersioningState::Disabled,
                    [0xc1; 16],
                    [0xc2; 16],
                    b"fresh recreated object",
                );
                *fresh_generation_id_for_hook.lock().unwrap() = Some(fresh.generation_id);
            })),
            ..crate::node::BucketScopedTestHooks::default()
        });

    let outcome = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            old_committed.version_id,
            old_bucket_incarnation,
            |_, _| Ok::<_, ()>(true),
        )
        .unwrap()
        .unwrap();

    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(
        outcome.is_none(),
        "stale lifecycle context must not delete the recreated bucket's object"
    );
    let new_bucket = cluster.head_bucket_info(&bucket).unwrap();
    assert!(
        new_bucket.bucket_incarnation_generation > old_bucket_incarnation,
        "test setup should recreate the bucket incarnation"
    );
    assert!(
        !new_bucket.bucket_lifecycle_present,
        "recreated bucket should not inherit the old lifecycle config"
    );
    let current = cluster.test_get_object_meta(&bucket, &key).unwrap();
    let live = current.as_live().expect("fresh object should remain live");
    let fresh_generation_id =
        (*fresh_generation_id.lock().unwrap()).expect("hook should write a fresh recreated object");
    assert_eq!(live.version_id, crate::VersionId::Null);
    assert_eq!(live.generation_id, fresh_generation_id);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn lifecycle_current_expiration_stops_after_bucket_recreate_before_context_load() {
    let _serial = lock_bucket_scoped_hook_test();
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
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    put_test_lifecycle(&cluster, &bucket);
    let old_bucket_incarnation = current_bucket_incarnation(&cluster, &bucket);
    let old_committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"old lifecycle object");

    let hook_ran = Arc::new(AtomicBool::new(false));
    let selector_ran = Arc::new(AtomicBool::new(false));
    let fresh_generation_id = Arc::new(Mutex::new(None));
    let hook_cluster = Arc::clone(&cluster);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let old_generation_id = old_committed.generation_id;
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let fresh_generation_id_for_hook = Arc::clone(&fresh_generation_id);
    let _hook_guard =
        crate::node::install_bucket_scoped_test_hooks(crate::node::BucketScopedTestHooks {
            target: Some(bucket.clone()),
            before_lifecycle_context_load: Some(Arc::new(move || {
                if hook_ran_for_hook.swap(true, Ordering::SeqCst) {
                    return;
                }
                hook_cluster
                    .delete_current_object_if(&hook_bucket, &hook_key, |_| Ok::<(), ()>(()))
                    .unwrap()
                    .expect("old live object should be deleted before bucket recreate");
                hook_cluster
                    .reclaim_object_payload_if_unleased(&hook_bucket, &hook_key, old_generation_id)
                    .expect("test should reclaim the old payload");
                hook_cluster
                    .begin_bucket_delete(&hook_bucket)
                    .expect("test should begin old bucket delete");
                assert_eq!(
                    hook_cluster
                        .try_finalize_bucket_delete(&hook_bucket)
                        .expect("test should finalize old bucket delete"),
                    crate::BucketDeleteFinalizeOutcome::Finalized
                );
                create_test_bucket(&hook_cluster, &hook_bucket);
                put_test_lifecycle(&hook_cluster, &hook_bucket);
                let fresh = write_committed_direct_segment_for_with_versioning(
                    &hook_cluster,
                    &hook_bucket,
                    &hook_key,
                    crate::BucketVersioningState::Disabled,
                    [0xd1; 16],
                    [0xd2; 16],
                    b"fresh recreated object",
                );
                *fresh_generation_id_for_hook.lock().unwrap() = Some(fresh.generation_id);
            })),
            ..crate::node::BucketScopedTestHooks::default()
        });

    let selector_ran_for_closure = Arc::clone(&selector_ran);
    let outcome = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            old_committed.version_id,
            old_bucket_incarnation,
            move |_, _| {
                selector_ran_for_closure.store(true, Ordering::SeqCst);
                Ok::<_, ()>(true)
            },
        )
        .unwrap()
        .unwrap();

    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(
        !selector_ran.load(Ordering::SeqCst),
        "old lifecycle claim must not evaluate the recreated bucket's lifecycle"
    );
    assert!(
        outcome.is_none(),
        "old lifecycle claim must not delete the recreated bucket's object"
    );
    let new_bucket = cluster.head_bucket_info(&bucket).unwrap();
    assert!(
        new_bucket.bucket_incarnation_generation > old_bucket_incarnation,
        "test setup should recreate the bucket incarnation"
    );
    assert!(
        new_bucket.bucket_lifecycle_present,
        "recreated bucket intentionally has lifecycle to prove the incarnation fence"
    );
    let current = cluster.test_get_object_meta(&bucket, &key).unwrap();
    let live = current.as_live().expect("fresh object should remain live");
    let fresh_generation_id =
        (*fresh_generation_id.lock().unwrap()).expect("hook should write a fresh recreated object");
    assert_eq!(live.version_id, crate::VersionId::Null);
    assert_eq!(live.generation_id, fresh_generation_id);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn lifecycle_suspended_current_expiration_replaces_null_live_on_all_acting_nodes() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Suspended);
    put_test_lifecycle(&cluster, &bucket);
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"suspended current");
    assert_eq!(committed.version_id, crate::VersionId::Null);

    let outcome = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            committed.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, record| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert_eq!(record.generation_id, committed.generation_id);
                Ok::<_, ()>(true)
            },
        )
        .unwrap()
        .unwrap()
        .expect("suspended null live object should expire");
    assert_eq!(outcome.reclaim_generation_id, Some(committed.generation_id));
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == committed.generation_id
    ));
    assert!(cluster.try_take_reclaim_work().is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, crate::VersionId::Null)
                .unwrap();
        assert!(matches!(stored, crate::StoredObject::DeleteMarker(_)));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap());
        assert!(
            crate::PgMetadataStore::get_object_segments(
                &*pg,
                &bucket,
                &key,
                crate::VersionId::Null,
            )
            .unwrap()
            .is_empty(),
            "null live segment rows should be removed on node {node_id:?}"
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn lifecycle_enabled_current_expiration_reserves_delete_marker_version() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let committed = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0x77; 16],
        [0x78; 16],
        b"enabled lifecycle current",
    );
    assert_eq!(committed.version_id, crate::VersionId::from_u64(1));

    let outcome = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            committed.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, record| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert_eq!(record.generation_id, committed.generation_id);
                Ok::<_, ()>(true)
            },
        )
        .unwrap()
        .unwrap()
        .expect("enabled current live object should expire");
    assert_eq!(outcome.reclaim_generation_id, None);
    assert!(cluster.try_take_reclaim_work().is_none());

    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let current = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let crate::StoredObject::DeleteMarker(marker) = current else {
            panic!("expected current delete marker on node {node_id:?}, got {current:?}");
        };
        assert_eq!(marker.version_id, crate::VersionId::from_u64(2));

        let stored_live =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, committed.version_id)
                .unwrap();
        let live = stored_live.as_live().unwrap();
        assert_eq!(live.generation_id, committed.generation_id);
        assert!(live.became_noncurrent_at.is_some());
        assert!(
            !crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id,
            )
            .unwrap(),
            "enabled current expiration should not reclaim the preserved live version"
        );
    }
}

#[test]
fn lifecycle_noncurrent_and_delete_marker_expiration_use_object_commands() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let older = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [11; 16],
        [51; 16],
        b"older version",
    );
    let middle = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [13; 16],
        [53; 16],
        b"middle version",
    );
    let current = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [12; 16],
        [52; 16],
        b"current version",
    );

    let mut reclaimed = cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                Ok::<_, ()>(
                    [older.version_id, middle.version_id]
                        .into_iter()
                        .filter(|version_id| {
                            versions
                                .iter()
                                .any(|stored| stored.version_id() == *version_id)
                        })
                        .collect(),
                )
            },
        )
        .unwrap()
        .unwrap();
    let next_reclaimed = cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                Ok::<_, ()>(
                    [older.version_id, middle.version_id]
                        .into_iter()
                        .filter(|version_id| {
                            versions
                                .iter()
                                .any(|stored| stored.version_id() == *version_id)
                        })
                        .collect(),
                )
            },
        )
        .unwrap()
        .unwrap();
    reclaimed.extend(next_reclaimed);
    assert_eq!(reclaimed.len(), 2);
    assert!(reclaimed.contains(&older.generation_id));
    assert!(reclaimed.contains(&middle.generation_id));

    let marker = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::OwnerIdentity::from_principal("owner"),
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap();

    let deleted_marker = cluster
        .delete_expired_delete_marker_if_due(
            &bucket,
            &key,
            marker.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert!(versions.iter().any(|stored| {
                    stored.version_id() == marker.version_id
                        && matches!(stored, crate::StoredObject::DeleteMarker(_))
                }));
                Ok::<_, ()>(true)
            },
        )
        .unwrap()
        .unwrap();
    assert!(deleted_marker);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, older.version_id,),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, middle.version_id,),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            older.generation_id
        )
        .unwrap());
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            middle.generation_id
        )
        .unwrap());
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, marker.version_id,),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.version_id(), current.version_id);
    }
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn lifecycle_noncurrent_pending_install_race_reruns_selector() {
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
    let bucket = bucket_for_pg(topology, 1, "lifecycle-pending-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    create_test_bucket_with_versioning(
        &first_cluster,
        &bucket,
        crate::BucketVersioningState::Enabled,
    );
    put_test_lifecycle(&first_cluster, &bucket);
    let older = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xa1; 16],
        [0xb1; 16],
        b"older",
    );
    let _current = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xa2; 16],
        [0xb2; 16],
        b"current",
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let selector_calls = Arc::new(AtomicUsize::new(0));
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_version_id = older.version_id;
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_proof = acquire_test_bucket_write_proof(
        &first_cluster,
        &bucket,
        "test-put-object-metadata-race",
        Some(key.as_str()),
    );
    let _hook_guard = first_cluster.test_install_before_metadata_command_pending_install_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(2);
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let stored = crate::PgMetadataStore::get_object_version(
                &*pg,
                &hook_bucket,
                &hook_key,
                hook_version_id,
            )
            .unwrap();
            let live = stored.as_live().expect("older object is live").clone();
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
                MetadataCommandPayload::PutObjectMetadata(Box::new(
                    PutObjectMetadataCommand::from_live_object_and_mutation(
                        live,
                        PutObjectMetadataMutation::PutLegalHold(crate::StoredLegalHoldStatus::On),
                        hook_proof.clone(),
                    ),
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

    let calls_for_selector = Arc::clone(&selector_calls);
    let reclaimed = first_cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&first_cluster, &bucket),
            move |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                calls_for_selector.fetch_add(1, Ordering::SeqCst);
                let older_live = versions
                    .iter()
                    .find(|stored| stored.version_id() == older.version_id)
                    .and_then(crate::StoredObject::as_live)
                    .expect("older version should be listed");
                if older_live.object_lock.legal_hold == crate::StoredLegalHoldStatus::On {
                    Ok::<_, ()>(HashSet::new())
                } else {
                    Ok(HashSet::from([older.version_id]))
                }
            },
        )
        .unwrap()
        .unwrap();
    assert!(reclaimed.is_empty());
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        selector_calls.load(Ordering::SeqCst),
        2,
        "lifecycle selector must be rerun after slot contention changes object lock state"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, older.version_id)
                .unwrap();
        let live = stored.as_live().expect("older object remains live");
        assert_eq!(
            live.object_lock.legal_hold,
            crate::StoredLegalHoldStatus::On
        );
        assert!(!crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            older.generation_id
        )
        .unwrap());
    }
}

#[test]
fn lifecycle_noncurrent_command_id_race_drains_winner_and_reruns_selector() {
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
    let bucket = bucket_for_pg(topology, 1, "lifecycle-id-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    create_test_bucket_with_versioning(
        &first_cluster,
        &bucket,
        crate::BucketVersioningState::Enabled,
    );
    put_test_lifecycle(&first_cluster, &bucket);
    let older = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xc1; 16],
        [0xd1; 16],
        b"older",
    );
    let _current = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xc2; 16],
        [0xd2; 16],
        b"current",
    );

    let slot_installed = Arc::new(AtomicBool::new(false));
    let selector_calls = Arc::new(AtomicUsize::new(0));
    let install_map = Arc::clone(&second_map);
    let install_bucket = bucket.clone();
    let install_key = key.clone();
    let install_version_id = older.version_id;
    let install_once = Arc::clone(&slot_installed);
    let calls_for_selector = Arc::clone(&selector_calls);
    let install_proof = acquire_test_bucket_write_proof(
        &first_cluster,
        &bucket,
        "test-put-object-metadata-race",
        Some(key.as_str()),
    );
    let reclaimed = first_cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&first_cluster, &bucket),
            move |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                calls_for_selector.fetch_add(1, Ordering::SeqCst);
                let older_live = versions
                    .iter()
                    .find(|stored| stored.version_id() == install_version_id)
                    .and_then(crate::StoredObject::as_live)
                    .expect("older version should be listed");
                if older_live.object_lock.legal_hold == crate::StoredLegalHoldStatus::On {
                    return Ok::<_, ()>(HashSet::new());
                }
                if !install_once.swap(true, Ordering::SeqCst) {
                    let pg_id = PgId::new(2);
                    let primary = install_map
                        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                        .unwrap();
                    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
                    let stored = crate::PgMetadataStore::get_object_version(
                        &*pg,
                        &install_bucket,
                        &install_key,
                        install_version_id,
                    )
                    .unwrap();
                    let live = stored.as_live().expect("older object is live").clone();
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
                        MetadataCommandPayload::PutObjectMetadata(Box::new(
                            PutObjectMetadataCommand::from_live_object_and_mutation(
                                live,
                                PutObjectMetadataMutation::PutLegalHold(
                                    crate::StoredLegalHoldStatus::On,
                                ),
                                install_proof.clone(),
                            ),
                        )),
                    );
                    pg.try_insert_pending_metadata_command_slot(
                        primary.node_id().as_u32(),
                        &command,
                        Some(&install_bucket),
                    )
                    .unwrap();
                }
                Ok(HashSet::from([install_version_id]))
            },
        )
        .unwrap()
        .unwrap();
    assert!(reclaimed.is_empty());
    assert!(slot_installed.load(Ordering::SeqCst));
    assert_eq!(
        selector_calls.load(Ordering::SeqCst),
        2,
        "lifecycle selector must rerun after command-id contention"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, older.version_id)
                .unwrap();
        let live = stored.as_live().expect("older object remains live");
        assert_eq!(
            live.object_lock.legal_hold,
            crate::StoredLegalHoldStatus::On
        );
        assert!(!crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            older.generation_id
        )
        .unwrap());
    }
    assert_clean_metadata_command_stream(&first_map, &[2]);
}

#[test]
fn lifecycle_noncurrent_version_list_change_defers_delete() {
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
    let bucket = bucket_for_pg(topology, 1, "lifecycle-version-list-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket_with_versioning(
        &first_cluster,
        &bucket,
        crate::BucketVersioningState::Enabled,
    );
    put_test_lifecycle(&first_cluster, &bucket);
    let older = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xe1; 16],
        [0xf1; 16],
        b"older",
    );
    let _current = write_committed_direct_segment_for_with_versioning(
        &first_cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xe2; 16],
        [0xf2; 16],
        b"current",
    );

    let selector_calls = Arc::new(AtomicUsize::new(0));
    let calls_for_selector = Arc::clone(&selector_calls);
    let race_bucket = bucket.clone();
    let race_key = key.clone();
    let reclaimed = first_cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&first_cluster, &bucket),
            move |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                let call = calls_for_selector.fetch_add(1, Ordering::SeqCst);
                assert!(versions
                    .iter()
                    .any(|stored| stored.version_id() == older.version_id));
                if call == 0 {
                    write_committed_direct_segment_for_with_versioning(
                        &second_cluster,
                        &race_bucket,
                        &race_key,
                        crate::BucketVersioningState::Enabled,
                        [0xe3; 16],
                        [0xf3; 16],
                        b"racing current",
                    );
                    Ok::<_, ()>(HashSet::from([older.version_id]))
                } else {
                    Ok(HashSet::new())
                }
            },
        )
        .unwrap()
        .unwrap();
    assert!(reclaimed.is_empty());
    assert_eq!(
        selector_calls.load(Ordering::SeqCst),
        1,
        "version-list drift must defer lifecycle work to a later sweep"
    );

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, older.version_id)
                .unwrap();
        assert!(stored.as_live().is_some());
        assert!(!crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            older.generation_id
        )
        .unwrap());
    }
    assert_clean_metadata_command_stream(&first_map, &[2]);
    assert_bucket_write_reservations_released(&first_map, &bucket);
}

#[test]
fn lifecycle_expired_marker_version_list_change_defers_delete() {
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
    let bucket = bucket_for_pg(topology, 1, "lifecycle-marker-list-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket_with_versioning(
        &first_cluster,
        &bucket,
        crate::BucketVersioningState::Enabled,
    );
    put_test_lifecycle(&first_cluster, &bucket);
    let marker = first_cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::OwnerIdentity::from_principal("owner"),
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap();

    let selector_calls = Arc::new(AtomicUsize::new(0));
    let calls_for_selector = Arc::clone(&selector_calls);
    let race_bucket = bucket.clone();
    let race_key = key.clone();
    let deleted = first_cluster
        .delete_expired_delete_marker_if_due(
            &bucket,
            &key,
            marker.version_id,
            current_bucket_incarnation(&first_cluster, &bucket),
            move |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                let call = calls_for_selector.fetch_add(1, Ordering::SeqCst);
                assert!(versions.iter().any(|stored| {
                    stored.version_id() == marker.version_id
                        && matches!(stored, crate::StoredObject::DeleteMarker(_))
                }));
                if call == 0 {
                    assert_eq!(versions.len(), 1);
                    write_committed_direct_segment_for_with_versioning(
                        &second_cluster,
                        &race_bucket,
                        &race_key,
                        crate::BucketVersioningState::Enabled,
                        [0xe4; 16],
                        [0xf4; 16],
                        b"racing live",
                    );
                    Ok::<_, ()>(true)
                } else {
                    panic!("version-list drift should defer lifecycle work without retrying")
                }
            },
        )
        .unwrap()
        .unwrap();
    assert!(!deleted);
    assert_eq!(
        selector_calls.load(Ordering::SeqCst),
        1,
        "delete-marker version-list drift must defer lifecycle work to a later sweep"
    );

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, marker.version_id)
                .unwrap();
        assert!(matches!(stored, crate::StoredObject::DeleteMarker(_)));
    }
    assert_clean_metadata_command_stream(&first_map, &[2]);
    assert_bucket_write_reservations_released(&first_map, &bucket);
}

#[test]
fn insert_delete_marker_metadata_command_applies_to_all_acting_object_pg_nodes() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let owner = crate::OwnerIdentity::from_principal("owner");

    let marker = cluster
        .insert_current_delete_marker_if(&bucket, &key, owner.clone(), |stored| {
            assert!(stored.is_none());
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert_eq!(marker.version_id, crate::VersionId::from_u64(1));

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        match stored {
            crate::StoredObject::DeleteMarker(record) => {
                assert_eq!(record.version_id, marker.version_id);
                assert_eq!(record.owner, owner);
            }
            other => panic!("expected delete marker on node {node_id:?}, got {other:?}"),
        }
    }
    assert_object_version_counter_on_acting_nodes(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        marker.version_id.to_u64() + 1,
    );
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn insert_delete_marker_partial_apply_reopens_and_releases_bucket_write_reservation() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let owner = crate::OwnerIdentity::from_principal("owner");

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::InsertDeleteMarker(marker)
                    if marker.bucket == hook_bucket
                        && marker.key == hook_key
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected delete marker reopen apply failure",
                        source: std::io::Error::other(
                            "injected delete marker reopen apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .insert_current_delete_marker_if(&bucket, &key, owner.clone(), |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected delete marker reopen apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial delete-marker command must remain durable before reopen"
    );
    {
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*primary_pg, &bucket, &key).unwrap();
        assert!(matches!(stored, crate::StoredObject::DeleteMarker(_)));
    }
    for node_id in [NodeId::new(1), NodeId::new(2)] {
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
    drop(cluster);
    drop(map);

    let reopened = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
            .expect("reopen local map with in-flight delete-marker command"),
    );
    assert!(
        pending_metadata_command_for_test(&reopened, PgId::new(object_pg), &bucket).is_none(),
        "open-time recovery should converge and clear the partial delete-marker command"
    );
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        match stored {
            crate::StoredObject::DeleteMarker(record) => {
                assert_eq!(record.version_id, crate::VersionId::from_u64(1));
                assert_eq!(record.owner, owner);
            }
            other => panic!("expected delete marker on node {node_id:?}, got {other:?}"),
        }
    }
    assert_object_version_counter_on_acting_nodes(
        &reopened, &node_ids, object_pg, &bucket, &key, 2,
    );
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
    assert_bucket_write_reservations_released(&reopened, &bucket);
}

#[test]
fn stream_append_registers_payload_acks_on_routed_data_pg_primary() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("03".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let payload = b"stream append payload with routed data acks";
    let segment_okh = crate::stream_segment_key_hash(&session_id, 0);
    let (_, segment_record) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh,
            },
        )
        .unwrap();
    assert_eq!(segment_record.data_pg_id, data_pg);

    let written = cluster
        .write_stream_segment_payload_shards(&segment_record, payload)
        .unwrap();
    let shard_batch: Vec<(&ShardKey, crate::WriteAck)> = written
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    cluster
        .commit_stream_segment_append(&bucket, &key, &session_id, 0, &segment_record, &shard_batch)
        .unwrap();

    let first_shard_key = &written[0].key;
    assert!(map
        .nodes
        .get(&NodeId::new(2))
        .unwrap()
        .storage_node()
        .test_shard_exists(data_pg, first_shard_key)
        .unwrap());
    assert!(!map
        .nodes
        .get(&NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_shard_exists(data_pg, first_shard_key)
        .unwrap());

    let mut readback = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: data_pg,
                segment_okh: segment_record.segment_okh,
                segment_vid: segment_record.segment_vid,
                stored_size: payload.len(),
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                ec: EcShape {
                    k: segment_record.ec_k,
                    m: segment_record.ec_m,
                },
            },
            &mut readback,
        )
        .unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn lease_release_requeues_routed_reclaim_after_worker_defers_for_active_lease() {
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

    let cluster = crate::StorageCluster::from_local_map(Arc::new(map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"leased payload");
    assert_eq!(committed.written.data_pg_id, data_pg);

    let lease = cluster
        .acquire_object_payload_lease(&bucket, &key, committed.generation_id)
        .unwrap();
    let delete_outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(matches!(
        delete_outcome.deleted,
        crate::DeletedCurrentObject::Live {
            generation_id,
            ..
        } if generation_id == committed.generation_id
    ));
    assert!(
        cluster
            .payload_reclaim_exists(&bucket, &key, committed.generation_id)
            .unwrap(),
        "delete should create reclaim metadata on the routed object PG primary"
    );

    cluster.enqueue_object_payload_reclaim(&bucket, &key, committed.generation_id);
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == committed.generation_id
    ));
    assert!(
        !cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "active lease should make the worker defer reclaim"
    );
    assert!(cluster.try_take_reclaim_work().is_none());

    let released = lease.release();
    assert_eq!(released.remaining(), 0);
    assert!(
        released.payload_reclaim_exists().unwrap(),
        "released lease must find reclaim metadata through the routed object PG primary"
    );
    let object_payload_reclaim_event_count = |event: &'static str| {
        observability::object_payload_reclaim_event_dimension_snapshot()
            .iter()
            .find(|sample| sample.pg_id == object_pg && sample.event == event)
            .map_or(0, |sample| sample.count)
    };
    let deduplicated_events_before_release_requeue =
        object_payload_reclaim_event_count("deduplicated");
    released.enqueue_object_payload_reclaim();
    assert!(
        object_payload_reclaim_event_count("deduplicated")
            > deduplicated_events_before_release_requeue,
        "lease-release requeue should deduplicate against the worker's deferred reclaim root"
    );
    assert!(
        cluster.try_take_reclaim_work().is_none(),
        "the deferred root remains owned by the worker until terminal completion"
    );

    assert!(
        cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "lease release should make the worker's deferred reclaim retryable"
    );
    cluster.finish_object_payload_reclaim_work(&bucket, &key, committed.generation_id);
    assert!(!cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
    for shard_index in 0..committed.written.ec.k + committed.written.ec.m {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    committed.written.data_pg_id,
                    committed.written.ec,
                    &committed.segment_okh,
                    committed.generation_id,
                    shard_index,
                )
                .unwrap(),
            "retried reclaim should delete placed shard {shard_index}"
        );
    }
}

#[test]
fn object_payload_reclaim_defers_behind_unrelated_pending_object_command() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let old = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [31; 16],
        [32; 16],
        b"old payload",
    );
    let _new = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [33; 16],
        [34; 16],
        b"new payload",
    );
    assert!(
        cluster
            .payload_reclaim_exists(&bucket, &key, old.generation_id)
            .unwrap(),
        "overwrite should create stale-payload reclaim metadata"
    );

    let pg_id = PgId::new(object_pg);
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let pending = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            crate::SessionId::try_from("ab".repeat(16)).unwrap(),
            GenerationId::new(100).unwrap(),
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending);

    assert!(
        !cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, old.generation_id)
            .unwrap(),
        "background reclaim should defer instead of draining foreground object metadata"
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(pending),
        "deferred reclaim must leave the existing pending command for foreground drain"
    );
    assert_eq!(
        object_payload_reclaim_claim_count_for_test(&map, pg_id),
        0,
        "deferred reclaim must not acquire a durable claim before it owns cleanup"
    );
    assert!(
        cluster
            .payload_reclaim_exists(&bucket, &key, old.generation_id)
            .unwrap(),
        "deferred reclaim root must remain retryable"
    );
}

#[test]
fn object_payload_reclaim_acquires_and_releases_durable_claim() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"claim payload");
    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let saw_claim = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let saw_claim_hook = Arc::clone(&saw_claim);
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            assert_eq!(
                object_payload_reclaim_claim_count_for_test(&hook_map, PgId::new(object_pg)),
                1,
                "reclaim worker must hold a durable claim before publishing terminal cleanup"
            );
            saw_claim_hook.store(true, Ordering::SeqCst);
        }));

    assert!(
        cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "unleased reclaim should complete"
    );
    assert!(saw_claim.load(Ordering::SeqCst));
    assert_eq!(
        object_payload_reclaim_claim_count_for_test(&map, PgId::new(object_pg)),
        0,
        "terminal reclaim command should release the durable claim"
    );
    assert!(!cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
}

#[test]
fn object_payload_reclaim_retry_releases_surviving_terminal_claim() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"retry claim payload");
    let generation_id = committed.generation_id;
    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectPayloadReclaim(reclaim)
                    if reclaim.matches_request(&hook_bucket, &hook_key, generation_id)
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected reclaim apply failure",
                        source: std::io::Error::other("injected reclaim apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, generation_id)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::Io {
            context: "injected reclaim apply failure",
            ..
        })
    ));
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "failed terminal cleanup must keep the pending command"
    );
    assert_eq!(
        object_payload_reclaim_claim_count_for_test(&map, PgId::new(object_pg)),
        0,
        "primary-first terminal cleanup releases the durable claim before replica failure"
    );

    assert!(
        cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, generation_id)
            .unwrap(),
        "retry should finish the exact pending reclaim command"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_eq!(
        object_payload_reclaim_claim_count_for_test(&map, PgId::new(object_pg)),
        0,
        "retrying the terminal command must release the surviving durable claim"
    );
    assert!(!cluster
        .payload_reclaim_exists(&bucket, &key, generation_id)
        .unwrap());
}

#[test]
fn durable_reclaim_scan_recovers_lost_local_queue_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let pg_ids = [0, 1, 2, 3];

    let (bucket, key, generation_id) = {
        let map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap());
        let (bucket, key, _, _) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let committed =
            write_committed_direct_segment_for(&cluster, &bucket, &key, b"lost hint payload");
        cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
        assert!(
            cluster
                .payload_reclaim_exists(&bucket, &key, committed.generation_id)
                .unwrap(),
            "delete should leave a durable reclaim root"
        );
        (bucket, key, committed.generation_id)
    };

    let reopened_map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap());
    let reopened_cluster =
        crate::StorageCluster::from_local_map(Arc::clone(&reopened_map)).unwrap();
    assert_eq!(
        reopened_cluster
            .enqueue_durable_object_payload_reclaim_roots_excluding(&HashSet::new())
            .queued,
        1,
        "startup scan should rediscover the durable root without an in-memory hint"
    );
    assert!(matches!(
        reopened_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == generation_id
    ));
    assert!(
        reopened_cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, generation_id)
            .unwrap(),
        "reopened worker should complete durable reclaim"
    );
    assert!(!reopened_cluster
        .payload_reclaim_exists(&bucket, &key, generation_id)
        .unwrap());
}

#[test]
fn durable_reclaim_scan_continues_after_unavailable_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let pg_ids = [0, 1, 2, 3];
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        key_for_object_pg(topology, &bucket, 1, "scan-key-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"later pg reclaim");
    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();
    drop(cluster);

    let mut map = Arc::try_unwrap(map).expect("test should hold the only map reference");
    map.pg_routes.get_mut(&PgId::new(0)).unwrap().state = PgState::Peering;
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let scan = cluster.enqueue_durable_object_payload_reclaim_roots_excluding(&HashSet::new());
    assert_eq!(
        scan.errors, 1,
        "unavailable PG should be reported in scan stats"
    );
    assert_eq!(
        scan.queued, 0,
        "healthy PG root was already outstanding from the original delete enqueue"
    );
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == committed.generation_id
    ));
}

#[test]
fn payload_lease_blocks_reclaim_across_cluster_handles() {
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
    let reader_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let reclaim_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&reader_cluster, &bucket, &key, b"shared lease");

    let lease = reader_cluster
        .acquire_object_payload_lease(&bucket, &key, committed.generation_id)
        .unwrap();
    assert_eq!(
        reclaim_cluster.object_payload_lease_count(&bucket, &key, committed.generation_id),
        1,
        "lease count must be visible through another cluster handle"
    );
    reclaim_cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(
        !reclaim_cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "storage-node-owned lease should block reclaim from another cluster handle"
    );

    drop(lease);
    assert_eq!(
        reader_cluster.object_payload_lease_count(&bucket, &key, committed.generation_id),
        0
    );
    assert!(
        reclaim_cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "reclaim should proceed after the cross-handle lease is released"
    );
}

#[test]
fn payload_lease_for_shard_locations_only_acquires_selected_storage_nodes() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"selected shard lease");
    let selected = committed.locations[0];

    let lease = cluster
        .acquire_object_payload_lease_for_shard_locations(
            &bucket,
            &key,
            committed.generation_id,
            &[selected],
        )
        .unwrap();
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let expected = usize::from(node_id == selected.node_id());
        assert_eq!(
            node.object_payload_lease_count(&bucket, &key, committed.generation_id),
            expected,
            "unexpected selected-shard lease count on node {node_id:?}"
        );
    }

    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(
        !cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "a lease on one selected shard owner must block whole-generation reclaim"
    );
    drop(lease);
    assert!(cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap());
}

#[test]
fn payload_lease_for_shard_locations_releases_partial_acquire_on_fence() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"partial acquire");
    let first = committed.locations[0];
    let fenced = committed
        .locations
        .iter()
        .copied()
        .find(|location| location.node_id() != first.node_id())
        .expect("EC placement should use at least two storage nodes");
    let fenced_node = map.node(fenced.node_id()).unwrap().storage_node();
    assert!(fenced_node.try_begin_object_payload_reclaim(&bucket, &key, committed.generation_id));

    match cluster.acquire_object_payload_lease_for_shard_locations(
        &bucket,
        &key,
        committed.generation_id,
        &[first, fenced],
    ) {
        Ok(_) => panic!("fenced shard location unexpectedly acquired a payload lease"),
        Err(error) => assert!(matches!(error, crate::StoreError::NotFound)),
    }
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        assert_eq!(
            node.object_payload_lease_count(&bucket, &key, committed.generation_id),
            0,
            "failed all-or-release acquisition leaked a lease on node {node_id:?}"
        );
    }
    fenced_node.finish_object_payload_reclaim(&bucket, &key, committed.generation_id, false);
}

#[test]
fn payload_lease_for_shard_locations_prevalidates_nodes_before_acquire() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let route = map.pg_routes.get_mut(&PgId::new(0)).unwrap();
    route.primary_node_id = NodeId::new(0);
    route.acting_set = Arc::from([NodeId::new(0), NodeId::new(99)]);

    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let generation_id = crate::GenerationId::MIN;
    let locations = [
        ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(0)),
            ShardIndex::new(0),
            NodeId::new(0),
        ),
        ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(0)),
            ShardIndex::new(1),
            NodeId::new(99),
        ),
    ];

    let err = match map.try_acquire_object_payload_lease_on_locations(
        &bucket,
        &key,
        generation_id,
        &locations,
    ) {
        Ok(_) => panic!("missing selected node unexpectedly acquired read handles"),
        Err(error) => error,
    };
    assert!(matches!(
        err,
        StoreError::NodeNotFound {
            node_id: 99,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
        }
    ));
    assert_eq!(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .object_payload_lease_count(&bucket, &key, generation_id),
        0,
        "missing later selected node must not leak an earlier acquired read handle"
    );
}

#[test]
fn payload_reclaim_in_progress_blocks_new_payload_leases() {
    let _serial = lock_payload_cleanup_hook_test();
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

    let cluster = crate::StorageCluster::from_local_map(Arc::new(map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"reclaim race payload");
    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let hook_gate = Arc::clone(&gate);
    let _hook_guard =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |_| {
            let (lock, cv) = &*hook_gate;
            let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
            if !state.0 {
                state.0 = true;
                cv.notify_all();
                while !state.1 {
                    state = cv.wait(state).unwrap_or_else(|e| e.into_inner());
                }
            }
            Ok(())
        }));

    let reclaim_cluster = Arc::clone(&cluster);
    let reclaim_bucket = bucket.clone();
    let reclaim_key = key.clone();
    let reclaim_generation_id = committed.generation_id;
    let reclaim_thread = std::thread::spawn(move || {
        reclaim_cluster.reclaim_object_payload_if_unleased(
            &reclaim_bucket,
            &reclaim_key,
            reclaim_generation_id,
        )
    });

    let (lock, cv) = &*gate;
    let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
    while !state.0 {
        state = cv.wait(state).unwrap_or_else(|e| e.into_inner());
    }
    let err = match cluster.acquire_object_payload_lease(&bucket, &key, committed.generation_id) {
        Ok(_) => panic!("new lease acquired after payload reclaim started"),
        Err(error) => error,
    };
    assert!(
        matches!(err, crate::StoreError::NotFound),
        "new leases must be rejected once reclaim starts deleting payload, got {err:?}"
    );
    state.1 = true;
    cv.notify_all();
    drop(state);

    assert!(reclaim_thread.join().unwrap().unwrap());
    assert!(!cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
}

#[test]
fn reclaim_payload_cleanup_failure_keeps_payload_lease_fence_until_retry() {
    let _serial = lock_payload_cleanup_hook_test();
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"cleanup fence payload");
    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let failed_ack_delete = Arc::new(AtomicBool::new(false));
    let failed_ack_delete_hook = Arc::clone(&failed_ack_delete);
    let hook_guard = cluster.test_install_before_metadata_primary_payload_ack_delete_hook(
        Arc::new(move |_shard_key| {
            if !failed_ack_delete_hook.swap(true, Ordering::SeqCst) {
                return Err(crate::StoreError::Io {
                    context: "injected reclaim ack delete failure",
                    source: std::io::Error::other("injected reclaim ack delete failure"),
                });
            }
            Ok(())
        }),
    );
    let err = cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(crate::StoreError::Io {
            context: "injected reclaim ack delete failure",
            ..
        })
    ));
    assert!(failed_ack_delete.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none(),
        "ack cleanup failure happens before the reclaim metadata delete command is installed"
    );
    assert!(cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
    for shard_index in 0..committed.written.ec.k + committed.written.ec.m {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    committed.written.data_pg_id,
                    committed.written.ec,
                    &committed.segment_okh,
                    committed.generation_id,
                    shard_index,
                )
                .unwrap(),
            "placed shard {shard_index} should already be deleted before ack cleanup fails"
        );
    }

    let err = match cluster.acquire_object_payload_lease(&bucket, &key, committed.generation_id) {
        Ok(_) => panic!("new lease acquired after payload cleanup failed mid-reclaim"),
        Err(error) => error,
    };
    assert!(
        matches!(err, crate::StoreError::NotFound),
        "failed mid-reclaim cleanup must keep leases fenced until retry converges, got {err:?}"
    );
    drop(hook_guard);

    assert!(cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert!(!cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
}

#[test]
fn reclaim_payload_metadata_delete_applies_to_object_pg_acting_set() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"reclaim payload");
    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(
            crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id,
            )
            .unwrap(),
            "delete should publish reclaim metadata on node {node_id:?}"
        );
    }

    assert!(cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(
            !crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id,
            )
            .unwrap(),
            "reclaim command should delete metadata on node {node_id:?}"
        );
    }
    for shard_index in 0..committed.written.ec.k + committed.written.ec.m {
        assert!(!cluster
            .test_payload_shard_file_exists(
                committed.written.data_pg_id,
                committed.written.ec,
                &committed.segment_okh,
                committed.generation_id,
                shard_index,
            )
            .unwrap());
    }
}

#[test]
fn reclaim_payload_metadata_delete_retry_reuses_pending_partial_command() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"retry payload");
    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let hook_guard =
        cluster.test_install_before_metadata_command_apply_hook(Arc::new(|node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::DeleteObjectPayloadReclaim(_)
            ) && node_id == NodeId::new(2)
            {
                return Err(crate::StoreError::Io {
                    context: "injected reclaim metadata command apply failure",
                    source: std::io::Error::other(
                        "injected reclaim metadata command apply failure",
                    ),
                });
            }
            Ok(())
        }));
    let err = cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(crate::StoreError::Io {
            context: "injected reclaim metadata command apply failure",
            ..
        })
    ));
    let pending = pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket)
        .expect("partial reclaim metadata delete must keep pending command");
    assert!(matches!(
        pending.payload(),
        MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
            if delete.matches_request(&bucket, &key, committed.generation_id)
    ));
    let err = match cluster.acquire_object_payload_lease(&bucket, &key, committed.generation_id) {
        Ok(_) => panic!("new lease acquired while reclaim metadata delete was pending"),
        Err(error) => error,
    };
    assert!(
            matches!(err, crate::StoreError::NotFound),
            "pending reclaim metadata delete must fence new leases after payload deletion starts, got {err:?}"
        );
    for (node_id, expected_exists) in [
        (NodeId::new(0), false),
        (NodeId::new(1), false),
        (NodeId::new(2), true),
    ] {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id,
            )
            .unwrap(),
            expected_exists,
            "partial apply state mismatch on node {node_id:?}"
        );
    }
    drop(hook_guard);

    assert!(cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(!crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id,
        )
        .unwrap());
    }
    let lease = cluster
        .acquire_object_payload_lease(&bucket, &key, committed.generation_id)
        .expect("converged reclaim metadata delete must clear the in-memory lease fence");
    drop(lease);
}

#[test]
fn composite_object_listings_fan_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let key_a = key_for_object_pg(topology, &bucket, 1, "dir/a/file-");
    let key_b = key_for_object_pg(topology, &bucket, 2, "dir/b/file-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for_with_okh(&cluster, &bucket, &key_b, [52; 16], b"payload-b");
    write_committed_direct_segment_for_with_okh(&cluster, &bucket, &key_a, [53; 16], b"payload-a");

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    let bridge_pg_a = bridge_node.get_pg(1).unwrap();
    crate::PgMetadataStore::delete_object_meta(&*bridge_pg_a, &bucket, &key_a).unwrap();
    drop(bridge_pg_a);
    let bridge_pg_b = bridge_node.get_pg(2).unwrap();
    crate::PgMetadataStore::delete_object_meta(&*bridge_pg_b, &bucket, &key_b).unwrap();
    drop(bridge_pg_b);
    assert!(bridge_node.test_get_object_meta(&bucket, &key_a).is_err());
    assert!(bridge_node.test_get_object_meta(&bucket, &key_b).is_err());

    let all_objects = cluster.list_all_objects_for_bucket(&bucket).unwrap();
    assert_eq!(
        all_objects
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );

    let listed = cluster
        .list_objects_for_bucket(&bucket, None, None, None, 100, 100)
        .unwrap();
    assert_eq!(
        listed
            .objects
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );

    let prefix = crate::ObjectKey::try_from("dir/".to_string()).unwrap();
    let delimited = cluster
        .list_objects_for_bucket(&bucket, Some(&prefix), Some("/"), None, 100, 100)
        .unwrap();
    assert!(delimited.objects.is_empty());
    assert_eq!(
        delimited
            .common_prefixes
            .iter()
            .map(crate::ObjectKey::as_str)
            .collect::<Vec<_>>(),
        vec!["dir/a/", "dir/b/"]
    );

    let all_versions = cluster
        .list_all_object_versions_for_bucket(&bucket)
        .unwrap();
    assert_eq!(
        all_versions
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );

    let listed_versions = cluster
        .list_object_versions_for_bucket(&bucket, None, None, None, None, 100)
        .unwrap();
    assert_eq!(
        listed_versions
            .versions
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );
}

#[test]
fn composite_bucket_listing_fan_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket_a, bucket_b) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "routed-bucket-a-"),
            bucket_for_pg(topology, 2, "routed-bucket-b-"),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let owner = crate::CanonicalUserId::from_principal("owner");
    seed_bucket_record(&map, NodeId::new(1), 1, &bucket_a, &owner);
    seed_bucket_record(&map, NodeId::new(2), 2, &bucket_b, &owner);

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    assert!(bridge_node.test_head_bucket_raw(&bucket_a).is_err());
    assert!(bridge_node.test_head_bucket_raw(&bucket_b).is_err());

    let buckets = cluster.list_buckets_for_owner(owner.as_str()).unwrap();
    assert_eq!(
        buckets
            .iter()
            .map(|bucket| &bucket.name)
            .collect::<Vec<_>>(),
        vec![&bucket_a, &bucket_b]
    );
}

#[test]
fn create_bucket_command_applies_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-create-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let created = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: true,
            public_write: false,
            versioning: crate::BucketVersioningState::Enabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    let created = match created {
        crate::BucketCreateAttemptOutcome::Created(info) => info,
        crate::BucketCreateAttemptOutcome::Exists(_) => {
            panic!("fresh bucket unexpectedly existed")
        }
    };

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, created.name);
        assert_eq!(info.owner_principal, created.owner_principal);
        assert_eq!(info.owner_canonical_id, created.owner_canonical_id);
        assert_eq!(info.created_at, created.created_at);
        assert_eq!(info.versioning, created.versioning);
        assert_eq!(info.acl_grants, created.acl_grants);
        assert_eq!(info.public_read, created.public_read);
        assert_eq!(info.public_write, created.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            created.bucket_execution_generation
        );
    }
    assert_bucket_execution_counter_on_acting_nodes(
        &map,
        &node_ids,
        1,
        created.bucket_execution_generation,
    );

    let exists = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: true,
            public_write: false,
            versioning: crate::BucketVersioningState::Enabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    assert!(matches!(
        exists,
        crate::BucketCreateAttemptOutcome::Exists(info) if info.name == bucket
    ));
}

#[test]
fn create_bucket_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-create-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));
    let create_config = || crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner,
        acl_grants: &acl_grants,
        public_read: true,
        public_write: false,
        versioning: crate::BucketVersioningState::Enabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };

    let err = cluster
        .create_bucket_with_config_and_load_info(&create_config())
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        let slot = pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .expect("partial create bucket should leave durable primary pending slot");
        assert_eq!(slot.scope_bucket.as_ref(), Some(&bucket));
    }

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(
                info.created_at, partial_info.created_at,
                "primary-first apply should create bucket on node {node_id:?} before the replica failure"
            );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).is_err(),
            "node {node_id:?} should not have the partially applied bucket"
        );
    }

    let retried = cluster
        .create_bucket_with_config_and_load_info(&create_config())
        .unwrap();
    assert!(matches!(
        retried,
        crate::BucketCreateAttemptOutcome::Created(info)
            if info.name == bucket
                && info.created_at == partial_info.created_at
                && info.bucket_execution_generation
                    == partial_info.bucket_execution_generation
    ));

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, partial_info.name);
        assert_eq!(info.owner_principal, partial_info.owner_principal);
        assert_eq!(info.owner_canonical_id, partial_info.owner_canonical_id);
        assert_eq!(info.created_at, partial_info.created_at);
        assert_eq!(info.state, partial_info.state);
        assert_eq!(info.versioning, partial_info.versioning);
        assert_eq!(info.object_lock, partial_info.object_lock);
        assert_eq!(info.acl_grants, partial_info.acl_grants);
        assert_eq!(info.public_read, partial_info.public_read);
        assert_eq!(info.public_write, partial_info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        assert!(pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn create_bucket_retries_partial_exact_command_conflict() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-create-exact-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let applied_by_hook = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let applied_by_hook_guard = Arc::clone(&applied_by_hook);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && !applied_by_hook_guard.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(NodeId::new(2)).unwrap().storage_node();
                    let pg = node.get_pg(1)?;
                    pg.apply_metadata_command_and_record(NodeId::new(2).as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual create bucket command apply failed: {error}")
                            }
                        })?;
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let created = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
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
        })
        .unwrap();
    assert!(matches!(
        created,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name == bucket
    ));
    assert!(applied_by_hook.load(Ordering::SeqCst));

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, bucket);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn create_bucket_retries_partial_exact_command_conflict_on_first_replica() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-create-first-exact-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let applied_by_hook = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let applied_by_hook_guard = Arc::clone(&applied_by_hook);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket.name == hook_bucket
                        && node_id == NodeId::new(0)
                        && !applied_by_hook_guard.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(NodeId::new(0)).unwrap().storage_node();
                    let pg = node.get_pg(1)?;
                    pg.apply_metadata_command_and_record(NodeId::new(0).as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual create bucket command apply failed: {error}")
                            }
                        })?;
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let created = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
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
        })
        .unwrap();
    assert!(matches!(
        created,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name == bucket
    ));
    assert!(applied_by_hook.load(Ordering::SeqCst));

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, bucket);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn create_bucket_drains_different_bucket_pending_command_on_same_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "partial-create-first-");
    let second_bucket = bucket_for_pg(topology, 1, "partial-create-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let fail_once_hook = Arc::clone(&fail_once);
    let first_bucket_for_hook = first_bucket.clone();
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket.name == first_bucket_for_hook
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected partial create bucket failure",
                        source: std::io::Error::other("injected partial create bucket failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: first_bucket.as_str(),
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
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected partial create bucket failure",
                ..
            })
        ),
        "expected injected partial create failure, got {err:?}"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &first_bucket).is_some());
    drop(hook_guard);

    let second = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: second_bucket.as_str(),
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
        })
        .unwrap();
    assert!(matches!(
        second,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name == second_bucket
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &second_bucket).is_none());

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let first = crate::PgMetadataStore::head_bucket_raw(&*pg, &first_bucket).unwrap();
        let second = crate::PgMetadataStore::head_bucket_raw(&*pg, &second_bucket).unwrap();
        assert_eq!(first.name, first_bucket);
        assert_eq!(second.name, second_bucket);
    }
}

#[test]
fn put_bucket_versioning_command_applies_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-versioning-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let original = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    };

    let updated = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
    assert!(updated.bucket_execution_generation > original.bucket_execution_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, updated.versioning);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn put_bucket_versioning_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-versioning-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        let slot = pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .expect("partial bucket versioning should leave durable primary pending slot");
        assert_eq!(slot.scope_bucket.as_ref(), Some(&bucket));
    }

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert_eq!(
        partial_info.versioning,
        crate::BucketVersioningState::Enabled
    );
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .versioning,
            crate::BucketVersioningState::Enabled,
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .versioning,
            crate::BucketVersioningState::Disabled,
            "node {node_id:?} should not have the partially applied versioning update"
        );
    }

    let retried = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(retried.versioning, crate::BucketVersioningState::Enabled);
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        assert!(pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn same_bucket_pending_metadata_command_drains_before_later_acl() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "pending-stream-acl-block-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));

    let pending = pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
        .expect("failed versioning command should remain pending");
    assert!(matches!(
        pending.payload(),
        MetadataCommandPayload::PutBucketVersioning(versioning)
            if versioning.bucket.name == bucket
                && versioning.bucket.versioning == crate::BucketVersioningState::Enabled
    ));
    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert_eq!(
        partial_info.versioning,
        crate::BucketVersioningState::Enabled
    );

    let acl_grants = crate::AclGrants::default();
    let acl_updated = cluster
        .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
        .unwrap();
    assert_eq!(
        acl_updated.versioning,
        crate::BucketVersioningState::Enabled
    );
    assert!(acl_updated.public_read);
    assert!(!acl_updated.public_write);
    assert!(
            acl_updated.bucket_execution_generation > partial_info.bucket_execution_generation,
            "later ACL command must reserve a newer execution generation after draining the pending versioning command"
        );
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            acl_updated.bucket_execution_generation
        );
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn bucket_update_cleans_terminal_pending_slot_before_new_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "terminal-pending-next-op-");
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &command)
        .unwrap();
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "test setup should leave a terminal durable pending slot"
    );

    let updated = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();

    assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
    assert!(updated.bucket_execution_generation > 1);
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "later bucket operation should clean the terminal slot before publishing its command"
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn put_bucket_acl_command_applies_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-acl-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let original = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    };
    let acl_grants = crate::AclGrants::default();

    let updated = cluster
        .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
        .unwrap();
    assert_eq!(updated.acl_grants, acl_grants);
    assert!(updated.public_read);
    assert!(!updated.public_write);
    assert!(updated.bucket_execution_generation > original.bucket_execution_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.acl_grants, updated.acl_grants);
        assert_eq!(info.public_read, updated.public_read);
        assert_eq!(info.public_write, updated.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn put_bucket_acl_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-acl-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketAcl(acl)
                    if acl.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(partial_info.public_read);
    assert!(!partial_info.public_write);
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(
            info.public_read && !info.public_write,
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(!info.public_read);
        assert!(
            !info.public_write,
            "node {node_id:?} should not have the partially applied ACL update"
        );
    }

    let retried = cluster
        .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
        .unwrap();
    assert!(retried.public_read);
    assert!(!retried.public_write);
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.acl_grants, acl_grants);
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn bucket_acl_drains_pending_completed_multipart_sequence_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "acl-drains-mpu-sequence-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            crate::ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
            AdvanceCompletedMultipartUploadSequenceCommand {
                bucket: bucket.clone(),
                completion_order: 7,
            },
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _serial = lock_metadata_command_apply_hook_test();
    let apply_count = Arc::new(AtomicUsize::new(0));
    let hook_bucket = bucket.clone();
    let apply_count_hook = Arc::clone(&apply_count);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(advance)
                    if advance.bucket == hook_bucket
                        && apply_count_hook.fetch_add(1, Ordering::SeqCst) == 1 =>
                {
                    return Err(StoreError::Io {
                        context: "injected completed multipart sequence apply failure",
                        source: std::io::Error::other(
                            "injected completed multipart sequence apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected completed multipart sequence apply failure",
                ..
            })
        ),
        "expected injected sequence apply failure, got {err:?}"
    );
    drop(hook_guard);

    let acl_grants = crate::AclGrants::default();
    let updated = cluster
        .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
        .unwrap();
    assert!(updated.public_read);
    assert!(!updated.public_write);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.acl_grants, updated.acl_grants);
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(info.completed_multipart_upload_sequence, 7);
    }
}

#[test]
fn existing_create_bucket_preserves_pending_acl_command_for_retry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-acl-create-exists-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketAcl(acl)
                    if acl.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));

    let pending_before = pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
        .expect("failed ACL command should remain pending");
    assert!(matches!(
        pending_before.payload(),
        MetadataCommandPayload::PutBucketAcl(acl)
            if acl.bucket.name == bucket && acl.bucket.public_read && !acl.bucket.public_write
    ));
    let partial_info = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(partial_info.public_read);
    assert!(!partial_info.public_write);
    let failed_replica_info = {
        let primary = map.node(NodeId::new(2)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(!failed_replica_info.public_read);
    assert!(!failed_replica_info.public_write);

    let attacker_owner = crate::CanonicalUserId::from_principal("attacker");
    let exists = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "attacker",
            owner_canonical_id: &attacker_owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    assert!(matches!(
        exists,
        crate::BucketCreateAttemptOutcome::Exists(info)
            if info.owner_principal == "owner"
                && info.owner_canonical_id
                    == crate::CanonicalUserId::from_principal("owner")
    ));

    assert!(
            pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
            "existing CreateBucket should drain and apply the pending ACL command before returning Exists"
        );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn bucket_acl_retry_rejects_same_acl_with_mismatched_post_image() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "acl-post-image-conflict-")
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let acl_grants = crate::AclGrants::default();
    let current = {
        let node = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap()
    };
    let mut update = PutBucketAclCommand::from_bucket(
        current.with_execution_generation(77),
        acl_grants.clone(),
        true,
        false,
    );
    update.bucket.bucket_policy_public = true;
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            PgId::new(1),
            MetadataCommandLogIndex::new(77).unwrap(),
        ),
        MetadataCommandPayload::PutBucketAcl(update),
    );
    insert_pending_metadata_command_for_test(&map, PgId::new(1), &bucket, &command);

    let err = cluster
        .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandContention {
                context: "conflicting pending put bucket acl command",
            })
        ),
        "expected conflicting pending ACL command, got {err:?}"
    );
    let info = cluster.head_bucket_info(&bucket).unwrap();
    assert!(!info.public_read);
    assert!(!info.bucket_policy_public);
}

#[test]
fn bucket_property_commands_apply_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-property-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let mut previous_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;
    let updated = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;

    let object_lock = crate::BucketObjectLockConfig {
        enabled: true,
        default_retention: Some(crate::ObjectLockDefaultRetention {
            mode: crate::ObjectLockMode::Governance,
            period: crate::RetentionPeriod::days(3).unwrap(),
        }),
    };
    let updated = cluster
        .put_bucket_object_lock_and_load_info(&bucket, object_lock)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.object_lock, object_lock);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let encryption = crate::BucketEncryptionConfig {
        default_encryption: Some(crate::ManagedEncryptionAlgorithm::Aes256),
        sse_c_blocked: false,
    };
    let updated = cluster
        .put_bucket_encryption_and_load_info(&bucket, encryption)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.encryption, encryption.effective());
        assert_eq!(
            crate::PgMetadataStore::get_bucket_encryption(&*pg, &bucket).unwrap(),
            encryption
        );
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let public_access_block = crate::PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: false,
        block_public_policy: true,
        restrict_public_buckets: false,
    };
    let updated = cluster
        .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, Some(public_access_block));
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_public_access_block_and_load_info(&bucket)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, None);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let ownership_controls = crate::BucketOwnershipControls {
        object_ownership: crate::BucketObjectOwnership::BucketOwnerPreferred,
    };
    let updated = cluster
        .put_bucket_ownership_controls_and_load_info(&bucket, ownership_controls)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.ownership_controls, Some(ownership_controls));
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_ownership_controls_and_load_info(&bucket)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.ownership_controls, None);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .put_bucket_abac_enabled_and_load_info(&bucket, true)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(info.bucket_abac_enabled);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }
}

#[test]
fn bucket_property_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-property-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let public_access_block = crate::PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: true,
        block_public_policy: false,
        restrict_public_buckets: true,
    };
    let expected_mutation = BucketPropertyMutation::PublicAccessBlock(Some(public_access_block));
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketProperty(property)
                    if property.bucket.name == hook_bucket
                        && property.effect == expected_mutation.effect()
                        && property.bucket.public_access_block == Some(public_access_block)
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert_eq!(partial_info.public_access_block, Some(public_access_block));
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .public_access_block,
            Some(public_access_block),
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .public_access_block,
            None,
            "node {node_id:?} should not have the partially applied property update"
        );
    }

    let retried = cluster
        .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
        .unwrap();
    assert_eq!(retried.public_access_block, Some(public_access_block));
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, Some(public_access_block));
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn invalid_bucket_property_command_does_not_poison_bucket_command_stream() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "invalid-property-no-poison-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;

    let invalid_object_lock = crate::BucketObjectLockConfig {
        enabled: true,
        default_retention: None,
    };
    let err = cluster
        .put_bucket_object_lock_and_load_info(&bucket, invalid_object_lock)
        .unwrap_err();
    match err {
        crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
            context: "put bucket object lock",
            source: rusqlite::Error::SqliteFailure(_, Some(message)),
        }) if message == "bucket object lock requires enabled versioning" => {}
        other => panic!("expected object-lock storage validation error, got {other:?}"),
    }
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "deterministic validation failures must not leave pending commands"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.object_lock, crate::BucketObjectLockConfig::default());
        assert_eq!(info.bucket_execution_generation, initial_generation);
    }

    let public_access_block = crate::PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: false,
        block_public_policy: true,
        restrict_public_buckets: false,
    };
    let updated = cluster
        .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
        .unwrap();
    assert_eq!(updated.public_access_block, Some(public_access_block));
    assert!(updated.bucket_execution_generation > initial_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, Some(public_access_block));
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn bucket_subresource_commands_apply_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-subresource-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let mut previous_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;

    let policy_body = r#"{"Statement":[]}"#;
    let updated = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Policy,
                body: policy_body,
                aux: crate::BucketSubresourceAux::policy(true),
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Policy,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, policy_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(stored.aux, crate::BucketSubresourceAux::policy(true));
        assert!(info.bucket_policy_present);
        assert!(info.bucket_policy_public);
        assert_eq!(info.bucket_policy_generation, 1);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let tags_body = "<Tagging><TagSet/></Tagging>";
    let updated = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Tagging,
                body: tags_body,
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, tags_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(stored.aux, crate::BucketSubresourceAux::None);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_subresource_and_load_info(&bucket, crate::BucketSubresourceKind::Tagging)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .is_none());
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let lifecycle_body = "<LifecycleConfiguration/>";
    let updated = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Lifecycle,
                body: lifecycle_body,
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Lifecycle,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, lifecycle_body);
        assert_eq!(stored.generation, Some(1));
        assert!(info.bucket_lifecycle_present);
        assert_eq!(info.bucket_lifecycle_generation, 1);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let cors_body = "<CORSConfiguration/>";
    let updated = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Cors,
                body: cors_body,
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Cors,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, cors_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_subresource_and_load_info(&bucket, crate::BucketSubresourceKind::Cors)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Cors,
        )
        .unwrap()
        .is_none());
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_subresource_and_load_info(&bucket, crate::BucketSubresourceKind::Policy)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Policy,
        )
        .unwrap()
        .is_none());
        assert!(!info.bucket_policy_present);
        assert!(!info.bucket_policy_public);
        assert_eq!(info.bucket_policy_generation, 2);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_subresource_and_load_info(&bucket, crate::BucketSubresourceKind::Lifecycle)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Lifecycle,
        )
        .unwrap()
        .is_none());
        assert!(!info.bucket_lifecycle_present);
        assert_eq!(info.bucket_lifecycle_generation, 2);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }
}

#[test]
fn bucket_subresource_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-subresource-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let policy_body = r#"{"Statement":[]}"#;
    let expected_mutation = BucketSubresourceMutation::Put {
        kind: crate::BucketSubresourceKind::Policy,
        body: policy_body.to_owned(),
        aux: crate::BucketSubresourceAux::policy(false),
    };
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketSubresource(subresource)
                    if subresource.name == hook_bucket
                        && subresource.mutation == expected_mutation
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Policy,
                body: policy_body,
                aux: crate::BucketSubresourceAux::policy(false),
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(partial_info.bucket_policy_present);
    assert!(!partial_info.bucket_policy_public);
    assert_eq!(partial_info.bucket_policy_generation, 1);
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(
            info.bucket_policy_present,
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(
            !info.bucket_policy_present,
            "node {node_id:?} should not have the partially applied policy"
        );
    }

    let retried = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Policy,
                body: policy_body,
                aux: crate::BucketSubresourceAux::policy(false),
            },
        )
        .unwrap();
    assert!(retried.bucket_policy_present);
    assert!(!retried.bucket_policy_public);
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Policy,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, policy_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(stored.aux, crate::BucketSubresourceAux::policy(false));
        assert!(info.bucket_policy_present);
        assert!(!info.bucket_policy_public);
        assert_eq!(info.bucket_policy_generation, 1);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn invalid_bucket_subresource_command_does_not_poison_bucket_command_stream() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "invalid-subresource-no-poison-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;

    let err = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Tagging,
                body: "<Tagging/>",
                aux: crate::BucketSubresourceAux::policy(true),
            },
        )
        .unwrap_err();
    match err {
        crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
            context: "put bucket subresource",
            source: rusqlite::Error::InvalidParameterName(message),
        }) if message.contains("Tagging does not support aux") => {}
        other => panic!("expected subresource storage validation error, got {other:?}"),
    }
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "deterministic validation failures must not leave pending commands"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.bucket_execution_generation, initial_generation);
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .is_none());
    }

    let tags_body = "<Tagging><TagSet/></Tagging>";
    let updated = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Tagging,
                body: tags_body,
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > initial_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, tags_body);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn finalized_bucket_delete_clears_pending_versioning_command_for_recreate() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-versioning-delete-recreate-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_some(),
        "failed versioning command should remain pending before delete"
    );
    let old_partial_generation = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
            .unwrap()
            .bucket_execution_generation
    };

    cluster.begin_bucket_delete(&bucket).unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "finalized delete must clear stale pending commands for the old bucket incarnation"
    );

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let recreated = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
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
        })
        .unwrap();
    let recreated_generation = match recreated {
        crate::BucketCreateAttemptOutcome::Created(info) => info.bucket_execution_generation,
        other => panic!("expected recreated bucket, got {other:?}"),
    };
    assert!(recreated_generation > old_partial_generation);

    let updated = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
    assert!(updated.bucket_execution_generation > recreated_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn finalized_bucket_delete_removes_replicated_create_rows() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-recreate-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let created_generation = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_head_bucket_raw(&bucket)
        .unwrap()
        .bucket_execution_generation;
    cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    let pre_delete_generation = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_head_bucket_raw(&bucket)
        .unwrap()
        .bucket_execution_generation;
    assert!(pre_delete_generation > created_generation);

    cluster.begin_bucket_delete(&bucket).unwrap();
    let deleting_generation = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_head_bucket_raw(&bucket)
        .unwrap()
        .bucket_execution_generation;
    assert!(deleting_generation > pre_delete_generation);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
        assert_eq!(info.bucket_execution_generation, deleting_generation);
    }
    assert_bucket_execution_counter_on_acting_nodes(&map, &node_ids, 1, deleting_generation);

    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert!(crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).is_err());
    }

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let recreated = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
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
        })
        .unwrap();
    assert!(matches!(
        recreated,
        crate::BucketCreateAttemptOutcome::Created(info)
            if info.name == bucket
                && info.bucket_execution_generation > pre_delete_generation
    ));

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .name,
            bucket
        );
    }
}

#[test]
fn finalized_bucket_delete_after_reopen_does_not_need_begin_waiter() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-finalize-reopen-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster.begin_bucket_delete(&bucket).unwrap();
    assert_clean_metadata_command_stream(&map, &[1]);
    drop(cluster);
    drop(map);

    let mut reopened = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    set_route_primary(&mut reopened, 1, NodeId::new(1));
    let reopened = Arc::new(reopened);
    let reopened_cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();

    assert_eq!(
        reopened_cluster
            .try_finalize_bucket_delete(&bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized,
        "finalization must not require the process that began DeleteBucket"
    );
    assert_eq!(
        reopened_cluster
            .try_finalize_bucket_delete(&bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::NotFound,
        "finalized delete should be idempotent after row removal"
    );
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn durable_bucket_finalize_scan_recovers_lost_local_queue_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let bucket = {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "delete-finalize-scan-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        cluster.begin_bucket_delete(&bucket).unwrap();
        bucket
    };

    let mut reopened = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    set_route_primary(&mut reopened, 1, NodeId::new(1));
    let reopened = Arc::new(reopened);
    let reopened_cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();

    let scan = reopened_cluster.enqueue_durable_bucket_delete_finalize_roots();
    assert_eq!(scan.errors, 0);
    assert_eq!(
        scan.queued, 1,
        "startup scan should rediscover the deleting bucket without an in-memory hint"
    );
    assert!(matches!(
        reopened_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_bucket))
            if queued_bucket == bucket
    ));
    assert_eq!(
        reopened_cluster
            .try_finalize_bucket_delete(&bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn durable_bucket_finalize_scan_prioritizes_expired_claimed_bucket() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket_a, bucket_b) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-finalize-a-"),
            bucket_for_pg(topology, 1, "delete-finalize-b-"),
        )
    };
    assert!(
        bucket_a < bucket_b,
        "test bucket names should exercise an earlier unclaimed bucket"
    );
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket_a);
    create_test_bucket(&cluster, &bucket_b);
    cluster.begin_bucket_delete(&bucket_b).unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let deleting_b =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket_b).unwrap();
    crate::PgMetadataStore::acquire_bucket_delete_finalize_claim(
        &*primary_pg,
        &bucket_b,
        deleting_b.bucket_incarnation_generation,
        "held-finalizer-claim-b",
        "external-worker",
        ClusterEpoch::INITIAL,
        10,
        Some(20),
        10,
    )
    .unwrap()
    .expect("later bucket should be claimable");
    drop(primary_pg);

    cluster.begin_bucket_delete(&bucket_a).unwrap();

    let scan = crate::clock::with_time_override(21, || {
        cluster.enqueue_durable_bucket_delete_finalize_roots()
    });
    assert_eq!(scan.errors, 0);
    assert_eq!(
        scan.queued, 2,
        "scan should enqueue the expired claimed bucket and the earlier deleting bucket"
    );
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_bucket))
            if queued_bucket == bucket_b
    ));
    assert_eq!(
        crate::clock::with_time_override(21, || { cluster.try_finalize_bucket_delete(&bucket_b) })
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized,
        "expired stale claim work should be recoverable from the durable scan"
    );
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_bucket))
            if queued_bucket == bucket_a
    ));
    assert_eq!(
        crate::clock::with_time_override(22, || { cluster.try_finalize_bucket_delete(&bucket_a) })
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn durable_bucket_finalize_scan_continues_after_unavailable_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-finalize-scan-later-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster.begin_bucket_delete(&bucket).unwrap();
    drop(cluster);

    let mut map = Arc::try_unwrap(map).expect("test should hold the only map reference");
    map.pg_routes.get_mut(&PgId::new(0)).unwrap().state = PgState::Peering;
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let scan = cluster.enqueue_durable_bucket_delete_finalize_roots();
    assert_eq!(
        scan.errors, 1,
        "unavailable PG should be reported in scan stats"
    );
    assert_eq!(
        scan.queued, 1,
        "scan should continue and enqueue the later healthy deleting bucket"
    );
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_bucket))
            if queued_bucket == bucket
    ));
}

#[test]
fn bucket_finalize_durable_claim_blocks_second_worker_until_released() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-finalize-claim-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster.begin_bucket_delete(&bucket).unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let deleting = crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
    let claimed_at = crate::clock::current_time_millis();
    let held = crate::PgMetadataStore::acquire_bucket_delete_finalize_claim(
        &*primary_pg,
        &bucket,
        deleting.bucket_incarnation_generation,
        "held-finalizer-claim",
        "external-worker",
        ClusterEpoch::INITIAL,
        claimed_at,
        claimed_at.checked_add(60_000),
        claimed_at,
    )
    .unwrap()
    .expect("test should be able to hold the finalizer claim");
    drop(primary_pg);

    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Pending,
        "a non-expired durable finalizer claim should block a second worker"
    );
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting
    );

    crate::PgMetadataStore::release_bucket_delete_finalize_claim(
        &*primary_pg,
        &bucket,
        deleting.bucket_incarnation_generation,
        &held.claim_id,
        &held.owner_token,
        held.cluster_epoch,
    )
    .unwrap();
    drop(primary_pg);
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
}

#[test]
fn finalized_bucket_delete_releases_finalizer_claim_for_next_same_pg_bucket() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket_a, bucket_b) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-finalize-release-a-"),
            bucket_for_pg(topology, 1, "delete-finalize-release-b-"),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket_a);
    create_test_bucket(&cluster, &bucket_b);

    cluster.begin_bucket_delete(&bucket_a).unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket_a).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );

    cluster.begin_bucket_delete(&bucket_b).unwrap();
    assert_eq!(
            cluster.try_finalize_bucket_delete(&bucket_b).unwrap(),
            crate::BucketDeleteFinalizeOutcome::Finalized,
            "a terminal bucket finalizer must release its singleton PG claim before unrelated same-PG work"
        );
}

#[test]
fn finalized_bucket_delete_waits_for_reclaim_then_finalizes_after_worker_progress() {
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
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"finalize reclaim");
    let lease = cluster
        .acquire_object_payload_lease(&bucket, &key, committed.generation_id)
        .unwrap();

    let delete_outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(matches!(
        delete_outcome.deleted,
        crate::DeletedCurrentObject::Live {
            generation_id,
            ..
        } if generation_id == committed.generation_id
    ));
    assert!(
        cluster
            .payload_reclaim_exists(&bucket, &key, committed.generation_id)
            .unwrap(),
        "object delete should leave payload reclaim metadata"
    );

    cluster.begin_bucket_delete(&bucket).unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Pending,
        "finalization must wait while reclaim metadata remains"
    );

    let released = lease.release();
    assert_eq!(released.remaining(), 0);
    assert!(
        cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "worker progress should clear the reclaim root after the read lease releases"
    );
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert_clean_metadata_command_stream(&map, &[1, object_pg]);
}

#[test]
fn finalized_bucket_delete_ignores_volatile_read_lease_without_reclaim_root() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-phantom-lease-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let phantom_key = crate::ObjectKey::try_from("phantom-read".to_string()).unwrap();
    let phantom_lease = cluster
        .acquire_object_payload_lease(&bucket, &phantom_key, crate::GenerationId::MIN)
        .unwrap();
    assert_eq!(cluster.bucket_object_payload_lease_count(&bucket), 1);

    cluster.begin_bucket_delete(&bucket).unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized,
        "volatile read handles without durable reclaim roots must not wedge bucket finalization"
    );
    drop(phantom_lease);
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn finalized_bucket_delete_preserves_unrelated_same_pg_pending_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (deleting_bucket, pending_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-pending-target-"),
            bucket_for_pg(topology, 1, "delete-pending-survivor-"),
        )
    };
    assert_ne!(deleting_bucket, pending_bucket);
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &deleting_bucket);
    create_test_bucket(&cluster, &pending_bucket);
    cluster.begin_bucket_delete(&deleting_bucket).unwrap();

    let pg_id = PgId::new(1);
    let pending_log_index = map.test_next_metadata_command_log_index(pg_id);
    let pending_command = {
        let primary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let current =
            crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &pending_bucket).unwrap();
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, pending_log_index),
            MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
                current.with_execution_generation(
                    primary_pg
                        .next_bucket_execution_generation_candidate()
                        .unwrap(),
                ),
                crate::BucketVersioningState::Enabled,
            )),
        )
    };
    insert_pending_metadata_command_for_test(&map, pg_id, &pending_bucket, &pending_command);

    assert_eq!(
        cluster
            .try_finalize_bucket_delete(&deleting_bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );

    assert!(
        pending_metadata_command_for_test(&map, pg_id, &pending_bucket).is_none(),
        "bucket finalization should drain same-PG pending work rather than dropping it"
    );
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(
            &*map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap(),
            &pending_bucket,
        )
        .unwrap()
        .versioning,
        crate::BucketVersioningState::Enabled
    );
}

#[test]
fn begin_bucket_delete_retries_when_pending_slot_wins_before_command_id() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-command-id-race-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard =
        cluster.test_install_before_bucket_delete_command_id_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let log_index = hook_map.test_next_metadata_command_log_index(pg_id);
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let current =
                crate::PgMetadataStore::head_bucket_record_raw(&*pg, &hook_bucket).unwrap();
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, log_index),
                MetadataCommandPayload::PutBucketVersioning(
                    PutBucketVersioningCommand::from_bucket(
                        current.with_execution_generation(
                            pg.next_bucket_execution_generation_candidate().unwrap(),
                        ),
                        crate::BucketVersioningState::Enabled,
                    ),
                ),
            );
            drop(pg);
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
        }));

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention { .. })
            ),
            "bucket delete owner should return retryable contention after a winning pending slot advances the bucket generation, got {err:?}"
        );
    cluster.begin_bucket_delete(&bucket).unwrap();

    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should install a contender before MarkBucketDeleting id allocation"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "bucket delete should drain the winning pending slot before retrying"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_retries_after_partial_mark_deleting_conflict() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-partial-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if node_id != NodeId::new(0) || hook_ran_for_closure.load(Ordering::SeqCst) {
                return Ok(());
            }
            match command.payload() {
                MetadataCommandPayload::MarkBucketDeleting(mark)
                    if mark.bucket_name() == &hook_bucket =>
                {
                    hook_ran_for_closure.store(true, Ordering::SeqCst);
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let pg = node.get_pg(command.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(node_id.as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual mark deleting command apply failed: {error}")
                            }
                        })?;
                    Err(StoreError::MetadataCommandLogConflict {
                        node_id: node_id.as_u32(),
                        pg_id: command.id().pg_id().get(),
                        cluster_epoch: command.id().cluster_epoch(),
                        log_index: command.id().log_index().get(),
                    })
                }
                _ => Ok(()),
            }
        },
    ));

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention { .. })
        ),
        "partial exact mark deleting conflict should ask the caller to retry, got {err:?}"
    );
    cluster.begin_bucket_delete(&bucket).unwrap();

    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should inject a command-log conflict after non-primary replicas apply"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "bucket delete should finish and clear the pending slot after retrying"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_reissues_stale_duplicate_mark_deleting_index() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let (occupant_bucket, delete_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, pg_id.get(), "delete-stale-occupant-"),
            bucket_for_pg(topology, pg_id.get(), "delete-stale-mark-"),
        )
    };
    create_test_bucket(&cluster, &occupant_bucket);
    create_test_bucket(&cluster, &delete_bucket);

    let stale_command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let occupant_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &occupant_bucket).unwrap();
    let occupant_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
            occupant_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
            crate::BucketVersioningState::Enabled,
        )),
    );
    let delete_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &delete_bucket).unwrap();
    let stale_delete_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            delete_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(primary_pg);

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &occupant_command)
            .unwrap();
    }
    force_insert_pending_metadata_command_for_test(
        &map,
        pg_id,
        &delete_bucket,
        &stale_delete_command,
    );

    cluster.begin_bucket_delete(&delete_bucket).unwrap();

    assert!(
        pending_metadata_command_for_test(&map, pg_id, &delete_bucket).is_none(),
        "stale duplicate-index mark command should be reissued and cleared"
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let occupant = crate::PgMetadataStore::head_bucket_raw(&*pg, &occupant_bucket).unwrap();
        assert_eq!(occupant.versioning, crate::BucketVersioningState::Enabled);
        let deleted = crate::PgMetadataStore::head_bucket_raw(&*pg, &delete_bucket).unwrap();
        assert_eq!(deleted.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_reissue_waits_for_primary_last_apply_window() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let (occupant_bucket, delete_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, pg_id.get(), "delete-window-occupant-"),
            bucket_for_pg(topology, pg_id.get(), "delete-window-mark-"),
        )
    };
    create_test_bucket(&cluster, &occupant_bucket);
    create_test_bucket(&cluster, &delete_bucket);

    let stale_command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let occupant_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &occupant_bucket).unwrap();
    let occupant_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
            occupant_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
            crate::BucketVersioningState::Enabled,
        )),
    );
    let delete_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &delete_bucket).unwrap();
    let stale_delete_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            delete_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &delete_bucket, &stale_delete_command);

    let primary_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release_primary = Arc::new((Mutex::new(false), Condvar::new()));
    let occupant_map = Arc::clone(&map);
    let occupant_command_for_thread = occupant_command.clone();
    let primary_gate_for_thread = Arc::clone(&primary_gate);
    let release_primary_for_thread = Arc::clone(&release_primary);
    let occupant_thread = std::thread::spawn(move || {
        let pg_lock = occupant_map.runtime_state().metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap();
        for node_id in [NodeId::new(1), NodeId::new(2)] {
            let pg = occupant_map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(pg_id.get())
                .unwrap();
            pg.apply_metadata_command_and_record(node_id.as_u32(), &occupant_command_for_thread)
                .unwrap();
        }
        {
            let (lock, cv) = &*primary_gate_for_thread;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        {
            let (lock, cv) = &*release_primary_for_thread;
            let _guard = cv
                .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |released| {
                    !*released
                })
                .unwrap();
        }
        let pg = occupant_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(0, &occupant_command_for_thread)
            .unwrap();
    });

    {
        let (lock, cv) = &*primary_gate;
        let guard = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |at_primary| {
                !*at_primary
            })
            .unwrap()
            .0;
        assert!(
            *guard,
            "occupant command should pause in the primary-last apply window"
        );
    }

    let reissue_cluster = cluster.clone();
    let stale_for_thread = stale_delete_command.clone();
    let reissue_thread = std::thread::spawn(move || {
        reissue_cluster
            .test_reissue_pending_metadata_command(pg_id, &stale_for_thread)
            .unwrap()
    });

    {
        let (lock, cv) = &*release_primary;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
    occupant_thread.join().unwrap();
    let replacement = reissue_thread
        .join()
        .unwrap()
        .expect("stale delete command should be reissued after in-flight apply finishes");
    assert_eq!(
        replacement.id().log_index().get(),
        stale_command_id.log_index().get() + 1
    );
    assert_eq!(replacement.payload(), stale_delete_command.payload());

    cluster.begin_bucket_delete(&delete_bucket).unwrap();

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let occupant = crate::PgMetadataStore::head_bucket_raw(&*pg, &occupant_bucket).unwrap();
        assert_eq!(occupant.versioning, crate::BucketVersioningState::Enabled);
        let deleted = crate::PgMetadataStore::head_bucket_raw(&*pg, &delete_bucket).unwrap();
        assert_eq!(deleted.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_retries_after_partial_object_pg_drain_conflict() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-object-pg-conflict-");
        let key = key_for_object_pg(topology, &bucket, 2, "key-");
        (bucket, key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let object_pg_id = PgId::new(2);
    let reservation_id = crate::SessionId::try_from("44".repeat(16)).unwrap();
    let command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(object_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            reservation_id.clone(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, object_pg_id, &bucket, &command);

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if node_id != NodeId::new(0) || hook_ran_for_closure.load(Ordering::SeqCst) {
                return Ok(());
            }
            match command.payload() {
                MetadataCommandPayload::ReserveObjectGeneration(reservation)
                    if reservation.bucket == hook_bucket && reservation.key == hook_key =>
                {
                    hook_ran_for_closure.store(true, Ordering::SeqCst);
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let pg = node.get_pg(command.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(node_id.as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual object PG command apply failed: {error}")
                            }
                        })?;
                    Err(StoreError::MetadataCommandLogConflict {
                        node_id: node_id.as_u32(),
                        pg_id: command.id().pg_id().get(),
                        cluster_epoch: command.id().cluster_epoch(),
                        log_index: command.id().log_index().get(),
                    })
                }
                _ => Ok(()),
            }
        },
    ));

    cluster.begin_bucket_delete(&bucket).unwrap();

    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should inject a command-log conflict after non-primary object replicas apply"
    );
    assert!(
        pending_metadata_command_for_test(&map, object_pg_id, &bucket).is_none(),
        "bucket delete should finish object-PG drain instead of surfacing a retryable conflict"
    );
    for node_id in node_ids {
        let bucket_pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);

        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id.get())
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*object_pg,
                &bucket,
                &key,
                &reservation_id,
            )
            .unwrap(),
            crate::GenerationId::MIN
        );
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id.get()]);
}

#[test]
fn begin_bucket_delete_skips_unrelated_all_pg_drain_slot() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, pending_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-unrelated-bucket-drain-"),
            bucket_for_pg(topology, 2, "delete-pending-bucket-drain-"),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    create_test_bucket(&cluster, &pending_bucket);

    let pending_pg_id = PgId::new(2);
    let command_id = cluster
        .next_bucket_metadata_command_id(pending_pg_id)
        .unwrap();
    let pending_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pending_pg_id)
        .unwrap();
    let pending_pg = pending_primary
        .storage_node()
        .get_pg(pending_pg_id.get())
        .unwrap();
    let current =
        crate::PgMetadataStore::head_bucket_record_raw(&*pending_pg, &pending_bucket).unwrap();
    let pending_command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            current.with_execution_generation(
                pending_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(pending_pg);
    insert_pending_metadata_command_for_test(
        &map,
        pending_pg_id,
        &pending_bucket,
        &pending_command,
    );

    cluster.begin_bucket_delete(&bucket).unwrap();

    assert!(
        pending_metadata_command_for_test(&map, pending_pg_id, &pending_bucket).is_some(),
        "bucket delete should not drain unrelated bucket-PG work found during all-PG scan"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let bucket_pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);

        let pending_pg = node.get_pg(pending_pg_id.get()).unwrap();
        let pending_info =
            crate::PgMetadataStore::head_bucket_raw(&*pending_pg, &pending_bucket).unwrap();
        assert_eq!(pending_info.state, crate::BucketState::Active);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn begin_bucket_delete_fails_closed_on_divergent_same_index_after_partial_apply() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-divergent-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let injected = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let injected_for_closure = Arc::clone(&injected);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::MarkBucketDeleting(mark)
                    if mark.bucket_name() == &hook_bucket
                        && node_id == NodeId::new(0)
                        && !injected_for_closure.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let node_pg = node.get_pg(pg_id.get()).unwrap();
                    let current =
                        crate::PgMetadataStore::head_bucket_record_raw(&*node_pg, &hook_bucket)
                            .unwrap();
                    let divergent = MetadataCommandEnvelope::new(
                        command.id(),
                        MetadataCommandPayload::PutBucketVersioning(
                            PutBucketVersioningCommand::from_bucket(
                                current.with_execution_generation(
                                    node_pg
                                        .next_bucket_execution_generation_candidate()
                                        .unwrap(),
                                ),
                                crate::BucketVersioningState::Enabled,
                            ),
                        ),
                    );
                    node_pg
                        .apply_metadata_command_and_record(node_id.as_u32(), &divergent)
                        .unwrap();
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();

    assert!(
        injected.load(Ordering::SeqCst),
        "test hook should inject a divergent same-index command on a replica"
    );
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandLogConflict { .. })
        ),
        "divergent same-index command log state must fail closed, got {err:?}"
    );
}

#[test]
fn begin_bucket_delete_partial_mark_deleting_reopens_and_converges() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-mark-reopen-")
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let command = {
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let now = crate::clock::current_time_millis();
        crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*primary_pg,
            &bucket,
            "delete-mark-reopen-drain",
            "delete-mark-reopen-owner",
            crate::ClusterEpoch::INITIAL,
            now,
            None,
        )
        .unwrap();
        let current =
            crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
                current.with_execution_generation(
                    primary_pg
                        .next_bucket_execution_generation_candidate()
                        .unwrap(),
                ),
            )),
        );
        primary_pg
            .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
            .unwrap();
        command
    };
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
    }
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "partial MarkBucketDeleting must leave the primary pending slot durable"
    );
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
    }
    assert!(
        pending_metadata_command_for_test(&reopened, pg_id, &bucket).is_none(),
        "open-time convergence should clear terminal MarkBucketDeleting pending slot"
    );
    let primary_pg = reopened
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*primary_pg, &bucket)
            .unwrap()
            .is_some(),
        "terminal delete drain should remain durable after reopen convergence"
    );
    drop(primary_pg);
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn bucket_update_fails_closed_on_divergent_same_index_after_partial_apply() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "bucket-update-divergent-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let injected = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let injected_for_closure = Arc::clone(&injected);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket_name() == &hook_bucket
                        && node_id == NodeId::new(0)
                        && !injected_for_closure.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let node_pg = node.get_pg(pg_id.get()).unwrap();
                    let current =
                        crate::PgMetadataStore::head_bucket_record_raw(&*node_pg, &hook_bucket)
                            .unwrap();
                    let divergent = MetadataCommandEnvelope::new(
                        command.id(),
                        MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                            current.with_execution_generation(
                                node_pg
                                    .next_bucket_execution_generation_candidate()
                                    .unwrap(),
                            ),
                            crate::AclGrants::default(),
                            false,
                            false,
                        )),
                    );
                    node_pg
                        .apply_metadata_command_and_record(node_id.as_u32(), &divergent)
                        .unwrap();
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap_err();

    assert!(
        injected.load(Ordering::SeqCst),
        "test hook should inject a divergent same-index command on a replica"
    );
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict { .. })
        ),
        "ordinary bucket-PG finish conflicts must fail closed, got {err:?}"
    );
}

#[test]
fn finalized_bucket_delete_fails_closed_on_active_replica() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-diverged-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster.begin_bucket_delete(&bucket).unwrap();

    let divergent_node = map.node(NodeId::new(0)).unwrap().storage_node();
    let divergent_pg = divergent_node.get_pg(1).unwrap();
    crate::PgMetadataStore::delete_finalized_bucket(&*divergent_pg, &bucket).unwrap();
    divergent_pg
        .refresh_metadata_command_state_digest()
        .unwrap();
    crate::PgMetadataStore::create_bucket(
        &*divergent_pg,
        &bucket,
        "owner",
        &crate::CanonicalUserId::from_principal("owner"),
        &crate::AclGrants::default(),
        false,
        false,
    )
    .unwrap();
    divergent_pg
        .refresh_metadata_command_state_digest()
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*divergent_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Active
    );
    drop(divergent_pg);

    let err = cluster.try_finalize_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(
                crate::MetadataError::BucketNotFinalizedForDelete {
                    state: crate::BucketState::Active
                }
            )
        ),
        "expected active replica to fail finalized delete, got {err:?}"
    );

    let divergent_pg = divergent_node.get_pg(1).unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*divergent_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Active
    );
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting
    );
}

#[test]
fn bucket_snapshot_pair_routes_to_bucket_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (source_bucket, destination_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "snapshot-source-"),
            bucket_for_pg(topology, 2, "snapshot-destination-"),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &source_bucket);
    create_test_bucket(&cluster, &destination_bucket);
    cluster
        .put_bucket_subresource_and_load_info(
            &source_bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Tagging,
                body:
                    "<Tagging><TagSet><Tag><Key>src</Key><Value>1</Value></Tag></TagSet></Tagging>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    cluster
        .put_bucket_subresource_and_load_info(
            &destination_bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Cors,
                body: "<CORSConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();

    let pair = cluster
        .load_bucket_snapshot_pair(
            (
                &source_bucket,
                crate::BucketSnapshotRequest {
                    tags: crate::BucketSnapshotTagsRequest::Always,
                    ..Default::default()
                },
            ),
            (
                &destination_bucket,
                crate::BucketSnapshotRequest {
                    cors: true,
                    ..Default::default()
                },
            ),
        )
        .unwrap();
    assert_eq!(pair.source().bucket.name, source_bucket);
    assert_eq!(pair.destination().bucket.name, destination_bucket);
    assert_eq!(
        pair.source().tags,
        crate::LoadedBucketSubresource::Loaded(
            "<Tagging><TagSet><Tag><Key>src</Key><Value>1</Value></Tag></TagSet></Tagging>"
                .to_string()
        )
    );
    assert_eq!(
        pair.destination().cors,
        crate::LoadedBucketSubresource::Loaded("<CORSConfiguration/>".to_string())
    );
}

#[test]
fn composite_multipart_and_lifecycle_scans_fan_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let upload_bucket = crate::BucketName::try_from("multipart-fanout-bucket".to_string()).unwrap();
    let lifecycle_bucket = bucket_for_pg(topology, 1, "lifecycle-bucket-");
    let aborting_bucket = bucket_for_pg(topology, 1, "aborting-bucket-");
    let key_a = key_for_object_pg(topology, &upload_bucket, 1, "uploads/a-");
    let key_b = key_for_object_pg(topology, &upload_bucket, 2, "uploads/b-");
    let aborting_key = key_for_object_pg(topology, &aborting_bucket, 2, "abort-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &lifecycle_bucket);
    create_test_bucket(&cluster, &aborting_bucket);
    cluster
        .put_bucket_subresource_and_load_info(
            &lifecycle_bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();

    let upload_a = upload_id_from_label("routedUploadA");
    let upload_b = upload_id_from_label("routedUploadB");
    let aborting_upload = upload_id_from_label("abortingUpload");
    seed_multipart_upload_record(
        &map,
        NodeId::new(1),
        1,
        &upload_bucket,
        &key_a,
        &upload_a,
        crate::UploadState::InProgress,
    );
    seed_multipart_upload_record(
        &map,
        NodeId::new(2),
        2,
        &upload_bucket,
        &key_b,
        &upload_b,
        crate::UploadState::InProgress,
    );
    seed_multipart_upload_record(
        &map,
        NodeId::new(2),
        2,
        &aborting_bucket,
        &aborting_key,
        &aborting_upload,
        crate::UploadState::Aborting,
    );

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    assert!(bridge_node
        .test_list_multipart_uploads_for_bucket(&upload_bucket)
        .unwrap()
        .is_empty());
    let all_uploads = cluster
        .list_all_multipart_uploads_for_bucket(&upload_bucket)
        .unwrap();
    assert_eq!(
        all_uploads
            .iter()
            .map(|upload| (&upload.key, &upload.upload_id))
            .collect::<Vec<_>>(),
        vec![(&key_a, &upload_a), (&key_b, &upload_b)]
    );

    let mut listed_uploads = cluster
        .list_multipart_uploads_for_bucket(&upload_bucket, None, None, None, 100, 100)
        .unwrap()
        .uploads;
    listed_uploads.sort_by(|left, right| left.key.cmp(&right.key));
    assert_eq!(
        listed_uploads
            .iter()
            .map(|upload| (&upload.key, &upload.upload_id))
            .collect::<Vec<_>>(),
        vec![(&key_a, &upload_a), (&key_b, &upload_b)]
    );

    let sweep = cluster.list_lifecycle_sweep_buckets().unwrap();
    assert_eq!(
        sweep
            .lifecycle_buckets
            .iter()
            .map(|bucket| &bucket.name)
            .collect::<Vec<_>>(),
        vec![&lifecycle_bucket]
    );
    assert_eq!(sweep.aborting_buckets, vec![aborting_bucket.clone()]);

    let roots = cluster.list_lifecycle_sweep_roots(0).unwrap();
    assert_eq!(
        roots
            .iter()
            .map(|root| (&root.bucket, root.source))
            .collect::<Vec<_>>(),
        vec![
            (
                &lifecycle_bucket,
                crate::LifecycleSweepRootSource::LifecycleConfig
            ),
            (
                &aborting_bucket,
                crate::LifecycleSweepRootSource::AbortingMultipartUpload,
            ),
        ]
    );
}

#[test]
fn completed_multipart_prune_fans_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = crate::BucketName::try_from("completed-prune-bucket".to_string()).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let older_key = key_for_object_pg(topology, &bucket, 1, "older-");
    let newer_key = key_for_object_pg(topology, &bucket, 2, "newer-");
    let post_prune_bucket = bucket_for_pg(topology, 1, "post-prune-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let older_upload = upload_id_from_label("olderCompleted");
    let newer_upload = upload_id_from_label("newerCompleted");
    seed_completed_multipart_upload_record(
        &map,
        NodeId::new(1),
        1,
        &bucket,
        &older_key,
        &older_upload,
        1,
    );
    seed_completed_multipart_upload_record(
        &map,
        NodeId::new(2),
        2,
        &bucket,
        &newer_key,
        &newer_upload,
        2,
    );

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    assert!(bridge_node
        .get_pg(1)
        .unwrap()
        .list_completed_multipart_uploads_for_bucket(bucket.as_str())
        .unwrap()
        .is_empty());
    assert!(bridge_node
        .get_pg(2)
        .unwrap()
        .list_completed_multipart_uploads_for_bucket(bucket.as_str())
        .unwrap()
        .is_empty());

    cluster
        .prune_completed_multipart_uploads_for_bucket_with_limit(&bucket, 1)
        .unwrap();

    let node_one_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::get_completed_multipart_upload(&*node_one_pg, &older_upload)
            .unwrap()
            .is_none(),
        "older routed completed-upload tombstone should be pruned"
    );
    let node_two_pg = map
        .node(NodeId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    assert!(
        crate::PgMetadataStore::get_completed_multipart_upload(&*node_two_pg, &newer_upload)
            .unwrap()
            .is_some(),
        "newer routed completed-upload tombstone should be retained"
    );
    drop(node_one_pg);
    drop(node_two_pg);

    create_test_bucket(&cluster, &post_prune_bucket);
}

#[test]
fn completed_multipart_prune_partial_command_retries_and_preserves_digest() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = crate::BucketName::try_from("completed-prune-retry-bucket".to_string()).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let key = key_for_object_pg(topology, &bucket, 1, "retry-");
    let post_prune_bucket = bucket_for_pg(topology, 1, "post-prune-retry-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let upload_id = upload_id_from_label("retryCompleted");
    for node_id in node_ids {
        seed_completed_multipart_upload_record(&map, node_id, 1, &bucket, &key, &upload_id, 1);
    }

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::DeleteCompletedMultipartUpload(_)
            ) && node_id == NodeId::new(2)
                && fail_once_hook.swap(false, Ordering::SeqCst)
            {
                return Err(StoreError::Io {
                    context: "injected completed multipart prune failure",
                    source: std::io::Error::other("injected completed multipart prune failure"),
                });
            }
            Ok(())
        },
    ));

    let err = cluster
        .prune_completed_multipart_uploads_for_bucket_with_limit(&bucket, 0)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io { .. })
        ),
        "expected injected partial prune failure, got {err:?}"
    );
    assert!(
        crate::PgMetadataStore::get_completed_multipart_upload(
            &*map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap(),
            &upload_id,
        )
        .unwrap()
        .is_none(),
        "first applied replica should have deleted the tombstone"
    );
    assert!(
        crate::PgMetadataStore::get_completed_multipart_upload(
            &*map
                .node(NodeId::new(2))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap(),
            &upload_id,
        )
        .unwrap()
        .is_some(),
        "failed replica should still have the tombstone before retry"
    );
    drop(hook_guard);

    cluster
        .prune_completed_multipart_uploads_for_bucket_with_limit(&bucket, 0)
        .unwrap();
    for node_id in node_ids {
        assert!(
            crate::PgMetadataStore::get_completed_multipart_upload(
                &*map.node(node_id).unwrap().storage_node().get_pg(1).unwrap(),
                &upload_id,
            )
            .unwrap()
            .is_none(),
            "retry should delete tombstone on node {node_id:?}"
        );
    }
    create_test_bucket(&cluster, &post_prune_bucket);
}

#[test]
fn bucket_delete_and_finalize_fan_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, live_key, tombstone_key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-bucket-");
        let live_key = key_for_object_pg(topology, &bucket, 2, "live-");
        let tombstone_key = key_for_object_pg(topology, &bucket, 2, "tombstone-");
        (bucket, live_key, tombstone_key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    write_committed_direct_segment_for_with_okh(&cluster, &bucket, &live_key, [61; 16], b"live");

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    assert!(bridge_node.test_get_object_meta(&bucket, &live_key).is_ok());

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "routed non-empty bucket should reject delete, got {err:?}"
    );
    {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
                .unwrap()
                .is_none(),
            "non-empty DeleteBucket must roll back the temporary durable drain"
        );
        let _ = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .expect("non-empty delete should leave the bucket active");
    }

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(2).unwrap();
        crate::PgMetadataStore::delete_object_meta(&*pg, &bucket, &live_key).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    let node_two = map.node(NodeId::new(2)).unwrap().storage_node();
    let completed_upload = upload_id_from_label("deleteCompleted");
    seed_completed_multipart_upload_record(
        &map,
        NodeId::new(2),
        2,
        &bucket,
        &tombstone_key,
        &completed_upload,
        1,
    );
    let node_two_pg = node_two.get_pg(2).unwrap();
    crate::PgMetadataStore::delete_object_meta(&*node_two_pg, &bucket, &tombstone_key).unwrap();
    node_two_pg.refresh_metadata_command_state_digest().unwrap();
    assert!(crate::PgMetadataStore::get_completed_multipart_upload(
        &*node_two_pg,
        &completed_upload
    )
    .unwrap()
    .is_some());
    drop(node_two_pg);

    cluster.begin_bucket_delete(&bucket).unwrap();
    {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
                .unwrap()
                .is_some(),
            "successful DeleteBucket begin should leave a terminal durable drain until finalize"
        );
    }
    assert!(
        matches!(
            cluster.begin_durable_bucket_delete_drain(&bucket).unwrap(),
            super::super::DurableBucketDeleteDrainBegin::AlreadyDeleting
        ),
        "durable delete-drain conflict must observe terminal Deleting as idempotent success"
    );
    cluster
        .begin_bucket_delete(&bucket)
        .expect("retrying DeleteBucket after MarkBucketDeleting should be idempotent");
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );

    assert!(map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_head_bucket_raw(&bucket)
        .is_err());
    let node_two_pg = node_two.get_pg(2).unwrap();
    assert!(
        crate::PgMetadataStore::get_completed_multipart_upload(&*node_two_pg, &completed_upload)
            .unwrap()
            .is_none(),
        "finalization should prune routed completed-upload tombstones"
    );
}

#[test]
fn bucket_control_plane_pending_install_waits_behind_durable_delete_drain() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "control-plane-drain-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(1);
    let primary = map.node(NodeId::new(1)).unwrap().storage_node();
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        super::super::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        super::super::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };

    let versioning_command_id = MetadataCommandId::new(
        crate::ClusterEpoch::INITIAL,
        pg_id,
        MetadataCommandLogIndex::new(2).unwrap(),
    );
    let versioning_command = {
        let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
        let current = crate::PgMetadataStore::head_bucket_record_raw(&*bucket_pg, &bucket)
            .unwrap()
            .with_execution_generation(
                bucket_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            );
        MetadataCommandEnvelope::new(
            versioning_command_id,
            MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
                current,
                crate::BucketVersioningState::Enabled,
            )),
        )
    };
    assert!(
        !cluster
            .try_set_bucket_control_pending_command_or_retry(pg_id, &bucket, &versioning_command)
            .unwrap(),
        "versioning command must not install while a durable delete drain is active"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "blocked bucket control-plane command must not leave a pending slot"
    );

    let lifecycle_command_id = MetadataCommandId::new(
        crate::ClusterEpoch::INITIAL,
        pg_id,
        MetadataCommandLogIndex::new(2).unwrap(),
    );
    let lifecycle_command = {
        let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
        let generation = bucket_pg
            .next_bucket_execution_generation_candidate()
            .unwrap();
        MetadataCommandEnvelope::new(
            lifecycle_command_id,
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                BucketSubresourceMutation::Put {
                    kind: crate::BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration/>".to_string(),
                    aux: crate::BucketSubresourceAux::None,
                },
                generation,
            )),
        )
    };
    assert!(
        !cluster
            .try_set_bucket_control_pending_command_or_retry(pg_id, &bucket, &lifecycle_command)
            .unwrap(),
        "lifecycle command must not install while a durable delete drain is active"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "blocked lifecycle command must not leave a pending slot"
    );

    let cors_command_id = MetadataCommandId::new(
        crate::ClusterEpoch::INITIAL,
        pg_id,
        MetadataCommandLogIndex::new(2).unwrap(),
    );
    let cors_command = {
        let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
        let generation = bucket_pg
            .next_bucket_execution_generation_candidate()
            .unwrap();
        MetadataCommandEnvelope::new(
            cors_command_id,
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                BucketSubresourceMutation::Put {
                    kind: crate::BucketSubresourceKind::Cors,
                    body: "<CORSConfiguration/>".to_string(),
                    aux: crate::BucketSubresourceAux::None,
                },
                generation,
            )),
        )
    };
    assert!(
        !cluster
            .try_set_bucket_control_pending_command_or_retry(pg_id, &bucket, &cors_command)
            .unwrap(),
        "CORS command must not install while a durable delete drain is active"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "blocked CORS command must not leave a pending slot"
    );

    cluster.clear_durable_bucket_delete_drain(&drain).unwrap();
    let versioned = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(versioned.versioning, crate::BucketVersioningState::Enabled);
    let lifecycle = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(lifecycle.bucket_lifecycle_present);
    cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Cors,
                body: "<CORSConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
    let cors = crate::PgMetadataStore::get_bucket_subresource(
        &*bucket_pg,
        &bucket,
        crate::BucketSubresourceKind::Cors,
    )
    .unwrap()
    .expect("CORS subresource should be installed after the drain clears");
    assert_eq!(cors.body, "<CORSConfiguration/>");
}

#[test]
fn begin_bucket_delete_waits_for_durable_reservation_and_post_drains_visible_write() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-durable-reservation-");
        let key = key_for_object_pg(topology, &bucket, 2, "key-");
        (bucket, key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(&bucket, "test-held-write", Some(key.as_str()))
        .unwrap();
    let bucket_write_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    let payload = b"visible";
    let generation_reservation_id = crate::SessionId::try_from("72".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &generation_reservation_id)
        .unwrap();
    let segment_okh = [71; 16];
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
    let commit_req = crate::CommitDirectPutObjectReq {
        bucket: bucket.clone(),
        key: key.clone(),
        generation_reservation_id,
        versioning: crate::BucketVersioningState::Disabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        generation_id,
        size: payload.len() as u64,
        etag_crc64: checksum::crc64::checksum(payload),
        ec: written.ec,
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        segment_index: 0,
        segment_crc64: Some(checksum::crc64::checksum(payload)),
        segment_okh,
        segment_vid: generation_id,
        data_pg_id: written.data_pg_id,
        bucket_write_reservation: bucket_write_proof,
    };

    let delete_started = Arc::new((Mutex::new(false), Condvar::new()));
    let delete_waiting = Arc::new((Mutex::new(false), Condvar::new()));
    let hook_bucket = bucket.clone();
    let delete_waiting_for_hook = Arc::clone(&delete_waiting);
    let _hook_guard =
        crate::node::install_bucket_scoped_test_hooks(crate::node::BucketScopedTestHooks {
            target: Some(hook_bucket),
            before_bucket_write_drain_wait: Some(Arc::new(move || {
                let (lock, cv) = &*delete_waiting_for_hook;
                *lock.lock().unwrap() = true;
                cv.notify_all();
            })),
            ..crate::node::BucketScopedTestHooks::default()
        });

    let delete_cluster = Arc::clone(&cluster);
    let delete_bucket = bucket.clone();
    let delete_started_for_thread = Arc::clone(&delete_started);
    let delete_thread = std::thread::spawn(move || {
        {
            let (lock, cv) = &*delete_started_for_thread;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        delete_cluster.begin_bucket_delete(&delete_bucket)
    });

    {
        let (lock, cv) = &*delete_started;
        let guard = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |started| {
                !*started
            })
            .unwrap()
            .0;
        assert!(*guard, "delete thread should start");
    }
    {
        let (lock, cv) = &*delete_waiting;
        let guard = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |waiting| {
                !*waiting
            })
            .unwrap()
            .0;
        assert!(
            *guard,
            "DeleteBucket should wait for the durable writer reservation before emptiness"
        );
    }

    {
        let pg_id = PgId::new(2);
        let shard_batch: Vec<(&ShardKey, WriteAck)> = written
            .written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        cluster
            .register_payload_shard_acks(written.data_pg_id, &shard_batch)
            .unwrap();
        let primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
            .unwrap();
        let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
        let command = cluster
            .prepare_commit_direct_put_object_command(
                pg_id,
                &pg,
                &commit_req,
                crate::VersionId::Null,
                commit_req.bucket_write_reservation.clone(),
            )
            .unwrap();
        pg.try_insert_pending_metadata_command_slot(
            primary.node_id().as_u32(),
            &command,
            Some(&bucket),
        )
        .unwrap();
    }

    let err = delete_thread.join().unwrap().unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "post-reservation drain/check should see the committed object, got {err:?}"
    );
    assert_bucket_write_reservations_released(&map, &bucket);
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .is_none(),
        "failed DeleteBucket should clear its temporary durable drain"
    );
}

#[test]
fn begin_bucket_delete_bounds_orphaned_durable_reservation_wait() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-orphan-reservation-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "test-orphaned-write",
            Some("orphaned-key"),
        )
        .unwrap();

    let started = std::time::Instant::now();
    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "DeleteBucket should not wait indefinitely for an orphaned durable reservation"
    );
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "orphaned durable reservation should make DeleteBucket retryable, got {err:?}"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .is_none(),
        "failed DeleteBucket should clear its temporary durable drain"
    );
    assert_eq!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .len(),
        1,
        "DeleteBucket must not silently drop another operation's durable reservation"
    );
    drop(bucket_pg);

    cluster
        .release_durable_bucket_write_reservation(reservation)
        .unwrap();
    cluster.begin_bucket_delete(&bucket).unwrap();
}

#[test]
fn begin_bucket_delete_bounds_active_delete_drain_wait() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-active-drain-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let drain = crate::PgMetadataStore::begin_durable_bucket_write_drain(
        &*bucket_pg,
        &bucket,
        "held-delete-drain",
        "other-delete-owner",
        crate::ClusterEpoch::INITIAL,
        crate::clock::current_time_millis(),
        None,
    )
    .unwrap();
    drop(bucket_pg);

    let started = std::time::Instant::now();
    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "DeleteBucket should not wait indefinitely behind another active delete drain"
    );
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention { .. })
        ),
        "active delete drain should make DeleteBucket return retryable contention, got {err:?}"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .as_ref()
            .map(|record| record.drain_id.as_str()),
        Some(drain.drain_id.as_str()),
        "DeleteBucket must not clear another caller's active delete drain"
    );
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Active,
        "timed out DeleteBucket begin must leave the bucket active"
    );
}

#[test]
fn begin_bucket_delete_drains_pending_delete_marker_before_emptiness_decision() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-marker-drain-");
        let key = key_for_object_pg(topology, &bucket, 2, "marker-");
        (bucket, key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();

    let pg_id = PgId::new(2);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let marker_version = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let object_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: acquire_test_bucket_write_proof(
                &cluster,
                &bucket,
                "delete-marker-drain-test",
                Some(key.as_str()),
            ),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: marker_version,
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence: object_pg
                .next_object_write_sequence(bucket.as_str(), key.as_str())
                .unwrap(),
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    drop(object_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "DeleteBucket should see the drained delete marker as bucket data, got {err:?}"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_version(
                    &*object_pg,
                    &bucket,
                    &key,
                    marker_version,
                ),
                Ok(crate::StoredObject::DeleteMarker(_))
            ),
            "DeleteBucket should converge the pending delete marker on node {node_id:?}"
        );
        let bucket_pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let bucket_info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .expect("BucketNotEmpty should leave the bucket active");
        assert_eq!(bucket_info.state, crate::BucketState::Active);
    }
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .is_none(),
        "BucketNotEmpty rollback should clear the durable delete drain"
    );
    drop(bucket_pg);
    assert_clean_metadata_command_stream(&map, &[1, 2]);
}

#[test]
fn begin_bucket_delete_drains_pending_specific_version_delete_that_empties_bucket() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "specific-delete-drain-");
        let key = key_for_object_pg(topology, &bucket, 2, "version-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    let committed = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [91; 16],
        [92; 16],
        b"delete the only version",
    );

    let pg_id = PgId::new(object_pg_id);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let object_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let stored = crate::PgMetadataStore::get_object_version(
        &*object_pg,
        &bucket,
        &key,
        committed.version_id,
    )
    .unwrap();
    let live = stored.as_live().unwrap();
    let payload = crate::StorageCluster::snapshot_live_object_payload_reclaim_command(
        &object_pg,
        &bucket,
        &key,
        live,
        crate::clock::current_time_millis(),
    )
    .unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
            bucket_write_reservation: acquire_test_bucket_write_proof(
                &cluster,
                &bucket,
                "specific-delete-drain-test",
                Some(key.as_str()),
            ),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: committed.version_id,
            target: DeleteObjectVersionTarget::Live {
                generation_id: live.generation_id,
                layout: live.layout,
                payload,
            },
        })),
    );
    drop(object_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    cluster.begin_bucket_delete(&bucket).unwrap();

    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_version(
                    &*object_pg,
                    &bucket,
                    &key,
                    committed.version_id,
                ),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "DeleteBucket should converge the pending specific-version delete on node {node_id:?}"
        );
        let bucket_pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let bucket_info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
        assert_eq!(bucket_info.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn begin_bucket_delete_drains_pending_lifecycle_current_expiry_marker() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, data_pg_id) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-lifecycle-current-");
        let key = key_for_object_pg(topology, &bucket, 2, "current-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));
    set_route_primary(&mut map, data_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let committed = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0x93; 16],
        [0x94; 16],
        b"lifecycle current before delete",
    );

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::InsertDeleteMarker(marker)
                    if marker.bucket == hook_bucket
                        && marker.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected lifecycle current expiry apply failure",
                        source: std::io::Error::other(
                            "injected lifecycle current expiry apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            committed.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |_, _| Ok::<_, ()>(true),
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected lifecycle current expiry apply failure",
                ..
            })
        ),
        "expected injected lifecycle current expiry failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_some(),
        "partial lifecycle current expiry command should remain pending"
    );

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "DeleteBucket should see the lifecycle delete marker as visible data, got {err:?}"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        let current = crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key)
            .expect("DeleteBucket should leave the lifecycle marker visible");
        assert!(
            matches!(current, crate::StoredObject::DeleteMarker(_)),
            "expected current delete marker on node {node_id:?}, got {current:?}"
        );
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn begin_bucket_delete_drains_pending_lifecycle_noncurrent_expiry() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, data_pg_id) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-lifecycle-noncurrent-");
        let key = key_for_object_pg(topology, &bucket, 2, "noncurrent-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));
    set_route_primary(&mut map, data_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let older = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xa1; 16],
        [0xa2; 16],
        b"older lifecycle version",
    );
    let current = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xa3; 16],
        [0xa4; 16],
        b"current lifecycle version",
    );

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let older_version = older.version_id;
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && delete.version_id == older_version
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected lifecycle noncurrent expiry apply failure",
                        source: std::io::Error::other(
                            "injected lifecycle noncurrent expiry apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&cluster, &bucket),
            |_, _| Ok::<_, ()>(HashSet::from([older.version_id])),
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected lifecycle noncurrent expiry apply failure",
                ..
            })
        ),
        "expected injected lifecycle noncurrent expiry failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_some(),
        "partial lifecycle noncurrent expiry command should remain pending"
    );

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
            ),
            "DeleteBucket should still see the current version after draining noncurrent expiry, got {err:?}"
        );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(
                &*object_pg,
                &bucket,
                &key,
                older.version_id,
            ),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        let visible = crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key)
            .expect("current version should remain visible");
        assert_eq!(visible.version_id(), current.version_id);
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn begin_bucket_delete_drains_pending_lifecycle_expired_delete_marker_cleanup() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, data_pg_id) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-lifecycle-marker-");
        let key = key_for_object_pg(topology, &bucket, 2, "marker-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));
    set_route_primary(&mut map, data_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let live = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xb1; 16],
        [0xb2; 16],
        b"live behind marker",
    );
    let marker = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::OwnerIdentity::from_principal("owner"),
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap();

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let marker_version = marker.version_id;
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && delete.version_id == marker_version
                        && matches!(
                            delete.target,
                            DeleteObjectVersionTarget::DeleteMarker { .. }
                        )
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected lifecycle delete-marker cleanup apply failure",
                        source: std::io::Error::other(
                            "injected lifecycle delete-marker cleanup apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .delete_expired_delete_marker_if_due(
            &bucket,
            &key,
            marker.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |_, _| Ok::<_, ()>(true),
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected lifecycle delete-marker cleanup apply failure",
                ..
            })
        ),
        "expected injected lifecycle delete-marker cleanup failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_some(),
        "partial lifecycle delete-marker cleanup command should remain pending"
    );

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
            ),
            "DeleteBucket should still see the revealed live version after draining marker cleanup, got {err:?}"
        );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(
                &*object_pg,
                &bucket,
                &key,
                marker.version_id,
            ),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        let visible = crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key)
            .expect("live version should be revealed after marker cleanup");
        assert_eq!(visible.version_id(), live.version_id);
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn stale_delete_drain_identity_cannot_clear_recreated_bucket_drain() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "stale-delete-drain-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster.begin_bucket_delete(&bucket).unwrap();

    let old_drain = {
        let node = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg_id = 1;
        let pg = node.get_pg(pg_id).unwrap();
        let record = crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .expect("DeleteBucket begin should leave a terminal durable drain");
        super::super::DurableBucketWriteDrain { pg_id, record }
    };
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    create_test_bucket(&cluster, &bucket);

    let new_drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        super::super::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        super::super::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("recreated active bucket should acquire a fresh delete drain")
        }
    };
    assert_ne!(
        old_drain.record.bucket_execution_generation, new_drain.record.bucket_execution_generation,
        "delete/recreate must produce a distinct bucket incarnation"
    );

    let err = cluster
        .clear_durable_bucket_delete_drain(&old_drain)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(
                crate::MetadataError::BucketWriteDrainNotFound { .. }
            )
        ),
        "stale drain cleanup should not match the recreated bucket, got {err:?}"
    );
    {
        let pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let current = crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .expect("fresh drain should remain installed");
        assert_eq!(current.drain_id, new_drain.record.drain_id);
        assert_eq!(
            current.bucket_execution_generation,
            new_drain.record.bucket_execution_generation
        );
    }

    cluster
        .clear_durable_bucket_delete_drain(&new_drain)
        .unwrap();
    let info = cluster.head_bucket_info(&bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Active);
}

#[test]
fn begin_bucket_delete_recovers_expired_durable_drain_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "expired-delete-drain-")
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let old_drain_id = "expired-delete-drain-before-reopen";
    {
        let pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let now = crate::clock::current_time_millis();
        crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*pg,
            &bucket,
            old_drain_id,
            "dead-delete-owner",
            crate::ClusterEpoch::INITIAL,
            now.saturating_sub(10),
            Some(now.saturating_sub(1)),
        )
        .unwrap();
    }
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let reopened_cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();
    reopened_cluster.begin_bucket_delete(&bucket).unwrap();

    {
        let pg = reopened
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let current = crate::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(current.state, crate::BucketState::Deleting);
        let drain = crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .expect(
                "DeleteBucket should leave a terminal drain after recovering the expired drain",
            );
        assert_ne!(
            drain.drain_id, old_drain_id,
            "expired pre-reopen drain must be rolled back by exact identity"
        );
    }
    assert!(
        matches!(
            reopened_cluster
                .begin_durable_bucket_delete_drain(&bucket)
                .unwrap(),
            super::super::DurableBucketDeleteDrainBegin::AlreadyDeleting
        ),
        "recovered terminal delete should be idempotent after reopen"
    );
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn storage_cluster_opens_local_node_map() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0, 1],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();

    assert_eq!(cluster.cluster_epoch(), ClusterEpoch::INITIAL);
    assert_eq!(cluster.operation_epoch(), ClusterEpoch::INITIAL);
    assert_eq!(cluster.metadata_node_id(), NodeId::new(0));
    assert_eq!(cluster.local_node_count(), 6);
    assert_eq!(cluster.local_node_ids().collect::<Vec<_>>(), node_ids);
    let routes = cluster.local_pg_routes().collect::<Vec<_>>();
    assert_eq!(routes.len(), 2);
    assert_eq!(
        cluster.local_pg_route(PgId::new(1)).unwrap().acting_set(),
        node_ids
    );
}

#[test]
fn places_payload_shards_deterministically_on_distinct_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
        NodeId::new(6),
        NodeId::new(7),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape)
            .unwrap();
    let data_pg_id = DataPgId::new(crate::PgId::new(3));

    let first = cluster
        .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
        .unwrap();
    let second = cluster
        .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(first.len(), usize::from(ec_shape.k + ec_shape.m));
    for (expected_index, location) in first.iter().enumerate() {
        assert_eq!(location.cluster_epoch(), ClusterEpoch::INITIAL);
        assert_eq!(location.data_pg_id(), data_pg_id);
        assert_eq!(
            location.shard_index(),
            ShardIndex::new(expected_index as u8)
        );
        assert!(
            cluster
                .local_node_ids()
                .any(|node_id| node_id == location.node_id()),
            "placed shard on unknown node {:?}",
            location.node_id()
        );
    }
    let distinct_nodes: BTreeSet<NodeId> = first.iter().map(ShardLocation::node_id).collect();
    assert_eq!(distinct_nodes.len(), first.len());
}

#[test]
fn payload_shard_node_selects_one_placed_shard() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let data_pg_id = DataPgId::new(crate::PgId::new(1));
    let locations = map
        .place_payload_shards(
            ClusterEpoch::INITIAL,
            data_pg_id,
            ec_shape,
            b"stable-payload-key",
        )
        .unwrap();

    let selected = map
        .payload_shard_node(
            ClusterEpoch::INITIAL,
            data_pg_id,
            ShardIndex::new(2),
            ec_shape,
            b"stable-payload-key",
        )
        .unwrap();

    assert_eq!(selected, locations[2].node_id());
}

#[test]
fn payload_shard_node_rejects_index_outside_ec_shape() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let err = map
        .payload_shard_node(
            ClusterEpoch::INITIAL,
            DataPgId::new(crate::PgId::new(0)),
            ShardIndex::new(ec_shape.k + ec_shape.m),
            ec_shape,
            b"stable-payload-key",
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::InvalidShardIndex {
            data_shards: 4,
            parity_shards: 2,
            shard_index: 6,
        }
    ));
}

#[test]
fn place_payload_shards_rejects_unknown_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();

    let err = map
        .place_payload_shards(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(99)),
            SharedStorageNode::DEFAULT_EC_SHAPE,
            b"stable-payload-key",
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::PgNotFound {
            pg_id: 99,
            cluster_epoch: ClusterEpoch::INITIAL,
        }
    ));
}

#[test]
fn place_payload_shards_rejects_stale_operation_epoch() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();

    let err = map
        .place_payload_shards(
            ClusterEpoch::new(2).unwrap(),
            DataPgId::new(PgId::new(0)),
            SharedStorageNode::DEFAULT_EC_SHAPE,
            b"stable-payload-key",
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::StalePayloadPlacement {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
}

#[test]
fn expired_route_maps_reject_payload_placement_and_shard_io() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let data_pg_id = DataPgId::new(PgId::new(0));
    map.route_map_valid_until_ms = Some(crate::clock::current_time_millis().saturating_add(60_000));

    let location = map
        .place_payload_shards(ClusterEpoch::INITIAL, data_pg_id, ec_shape, b"payload-key")
        .unwrap()[0];
    let key = ShardKey::new(&[61; 16], 1, location.shard_index().get());
    let ack = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"payload")
        .unwrap();
    assert_eq!(
        map.read_payload_shard(ClusterEpoch::INITIAL, location, &key, ack)
            .unwrap(),
        b"payload"
    );

    let expired_at = crate::clock::current_time_millis();
    map.route_map_valid_until_ms = Some(expired_at);
    let err = map
        .place_payload_shards(ClusterEpoch::INITIAL, data_pg_id, ec_shape, b"payload-key")
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RouteMapExpired {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms,
            now_ms,
        } if valid_until_ms == expired_at && now_ms >= expired_at
    ));

    let err = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"blocked")
        .unwrap_err();
    assert!(matches!(
        err,
        ShardIoError::RouteMapExpired {
            node_id,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms,
            now_ms,
        } if node_id == location.node_id().as_u32()
            && valid_until_ms == expired_at
            && now_ms >= expired_at
    ));

    let err = map
        .read_payload_shard(ClusterEpoch::INITIAL, location, &key, ack)
        .unwrap_err();
    assert!(matches!(
        err,
        ShardIoError::RouteMapExpired {
            node_id,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms,
            now_ms,
        } if node_id == location.node_id().as_u32()
            && valid_until_ms == expired_at
            && now_ms >= expired_at
    ));

    let err = map
        .delete_payload_shard(ClusterEpoch::INITIAL, location, &key)
        .unwrap_err();
    assert!(matches!(
        err,
        ShardIoError::RouteMapExpired {
            node_id,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms,
            now_ms,
        } if node_id == location.node_id().as_u32()
            && valid_until_ms == expired_at
            && now_ms >= expired_at
    ));
}

#[test]
fn storage_cluster_dispatches_payload_shard_io_to_placed_local_node() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let data_pg_id = DataPgId::new(crate::PgId::new(1));
    let location = cluster
        .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
        .unwrap()[3];
    let key = ShardKey::new(&[17; 16], 23, location.shard_index().get());

    let ack = cluster
        .write_payload_shard(location, &key, b"placed shard")
        .unwrap();

    assert_eq!(ack.stored_size, b"placed shard".len() as u64);
    assert_eq!(ack.crc64, checksum::crc64::checksum(b"placed shard"));
    assert_eq!(
        cluster.read_payload_shard(location, &key, ack).unwrap(),
        b"placed shard"
    );
    let mut dst = vec![0; b"placed shard".len()];
    cluster
        .read_payload_shard_into(location, &key, ack, &mut dst)
        .unwrap();
    assert_eq!(dst, b"placed shard");
    assert!(matches!(
        cluster.read_payload_shard(
            location,
            &key,
            WriteAck {
                crc64: ack.crc64 ^ 1,
                stored_size: ack.stored_size,
            },
        ),
        Err(ShardIoError::Store {
            source: StoreError::IntegrityError { .. },
            ..
        })
    ));

    let assigned_node = map.node(location.node_id()).unwrap();
    assert_eq!(
        assigned_node
            .storage_node()
            .read_shard_file(data_pg_id.get(), &key)
            .unwrap(),
        b"placed shard"
    );
    for other_node_id in node_ids {
        if other_node_id == location.node_id() {
            continue;
        }
        let other_node = map.node(other_node_id).unwrap();
        assert!(matches!(
            other_node
                .storage_node()
                .read_shard_file(data_pg_id.get(), &key),
            Err(StoreError::NotFound)
        ));
    }

    cluster.delete_payload_shard(location, &key).unwrap();
    assert!(matches!(
        cluster.read_payload_shard(location, &key, ack),
        Err(ShardIoError::Store {
            source: StoreError::NotFound,
            ..
        })
    ));
}

#[test]
fn storage_cluster_payload_write_uses_handle_operation_epoch() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let segment_okh = [43; 16];
    let generation_id = crate::GenerationId::MIN;

    let err = stale_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            b"stale epoch payload",
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::StalePayloadOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    for shard_index in 0..(ec_shape.k + ec_shape.m) {
        assert!(
            !current_cluster
                .test_payload_shard_file_exists(
                    0,
                    ec_shape,
                    &segment_okh,
                    generation_id,
                    shard_index,
                )
                .unwrap(),
            "stale operation epoch wrote shard {shard_index}"
        );
    }
}

#[test]
fn stale_storage_cluster_payload_read_reports_data_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let mut map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    Arc::get_mut(&mut map).unwrap().epoch = ClusterEpoch::new(2).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    let mut dst = Vec::new();

    let err = stale_cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: 0,
                segment_okh: [53; 16],
                segment_vid: crate::GenerationId::MIN,
                stored_size: 0,
                segment_crc64: Some(0),
                ec: ec_shape,
            },
            &mut dst,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::StalePayloadOperation {
            pg_id: 0,
            operation_epoch: ClusterEpoch::INITIAL,
            current_epoch,
        } if current_epoch == ClusterEpoch::new(2).unwrap()
    ));
    assert!(dst.is_empty());
}

#[test]
fn stale_storage_cluster_handle_cannot_use_current_epoch_location() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();
    let data_pg_id = DataPgId::new(PgId::new(0));
    let location = current_cluster
        .place_payload_shards(data_pg_id, ec_shape, b"current-epoch-location")
        .unwrap()[0];
    let key = ShardKey::new(&[47; 16], 1, location.shard_index().get());

    let err = stale_cluster
        .place_payload_shards(data_pg_id, ec_shape, b"stale-placement")
        .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::StalePayloadPlacement {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let err = stale_cluster
        .write_payload_shard(location, &key, b"must not write")
        .unwrap_err();
    assert!(matches!(
        err,
        ShardIoError::StaleOperationEpoch {
            node_id,
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if node_id == location.node_id().as_u32()
            && operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let node = map.node(location.node_id()).unwrap();
    assert!(matches!(
        node.storage_node().read_shard_file(data_pg_id.get(), &key),
        Err(StoreError::NotFound)
    ));
}

#[test]
fn stale_storage_cluster_handle_rejects_bucket_metadata_before_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();
    let bucket = crate::BucketName::try_from("stale-bucket".to_string()).unwrap();
    let owner_canonical_id = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let create = crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner_canonical_id,
        acl_grants: &acl_grants,
        public_read: false,
        public_write: false,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };

    let err = stale_cluster
        .create_bucket_with_config_and_load_info(&create)
        .unwrap_err();

    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        }) if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let err = current_cluster.head_bucket_info(&bucket).unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::BucketNotFound { .. })
    ));
}

#[test]
fn stale_storage_cluster_handle_rejects_object_metadata_before_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let reservation_id = crate::SessionId::try_from("02".repeat(16)).unwrap();

    let err = stale_cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap_err();

    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        }) if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let err = current_cluster
        .test_object_generation_reservation_for(&bucket, &key, &reservation_id)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Metadata(
            crate::MetadataError::ObjectGenerationReservationNotFound { .. }
        )
    ));
}

#[test]
fn stale_storage_cluster_handle_rejects_multipart_metadata_before_lookup() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let upload_id = crate::UploadId::try_from(".".repeat(128)).unwrap();

    let err = stale_cluster
        .load_multipart_upload(&bucket, &key, &upload_id)
        .unwrap_err();

    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        }) if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let err = current_cluster
        .load_multipart_upload(&bucket, &key, &upload_id)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::NoSuchUpload { .. })
    ));
}

#[test]
fn object_payload_lease_token_releases_after_cluster_epoch_transition() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let mut map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let generation_id = crate::GenerationId::MIN;

    let lease = current_cluster
        .acquire_object_payload_lease(&bucket, &key, generation_id)
        .unwrap();
    assert_eq!(
        current_cluster.object_payload_lease_count(&bucket, &key, generation_id),
        1
    );
    drop(current_cluster);

    Arc::get_mut(&mut map).unwrap().epoch = ClusterEpoch::new(2).unwrap();
    let current_epoch_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();

    let err = match stale_cluster.acquire_object_payload_lease(&bucket, &key, generation_id) {
        Ok(_) => panic!("stale cluster handle acquired a payload lease"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == ClusterEpoch::INITIAL
            && current_epoch == ClusterEpoch::new(2).unwrap()
    ));
    stale_cluster.enqueue_object_payload_reclaim(&bucket, &key, generation_id);
    assert!(current_epoch_cluster.try_take_reclaim_work().is_none());

    let released = lease.release();
    assert_eq!(released.remaining(), 0);
    assert_eq!(
        current_epoch_cluster.object_payload_lease_count(&bucket, &key, generation_id),
        0
    );

    released.enqueue_object_payload_reclaim();
    assert!(matches!(
        current_epoch_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == generation_id
    ));
}

#[test]
fn placed_payload_shard_io_rejects_key_location_shard_index_mismatch() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let data_pg_id = DataPgId::new(crate::PgId::new(1));
    let locations = cluster
        .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
        .unwrap();
    let shard_0_location = locations[0];
    let shard_3_location = locations[3];
    let shard_0_key = ShardKey::new(&[31; 16], 42, shard_0_location.shard_index().get());

    let err = cluster
        .write_payload_shard(shard_3_location, &shard_0_key, b"wrong shard")
        .unwrap_err();

    assert!(matches!(
        err,
        ShardIoError::ShardIndexMismatch {
            location_shard_index: 3,
            key_shard_index: 0,
            ..
        }
    ));
    assert!(matches!(
        map.node(shard_3_location.node_id())
            .unwrap()
            .storage_node()
            .read_shard_file(data_pg_id.get(), &shard_0_key),
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        map.node(shard_0_location.node_id())
            .unwrap()
            .storage_node()
            .read_shard_file(data_pg_id.get(), &shard_0_key),
        Err(StoreError::NotFound)
    ));

    let ack = cluster
        .write_payload_shard(shard_0_location, &shard_0_key, b"right shard")
        .unwrap();
    assert!(matches!(
        cluster.read_payload_shard(shard_3_location, &shard_0_key, ack),
        Err(ShardIoError::ShardIndexMismatch {
            location_shard_index: 3,
            key_shard_index: 0,
            ..
        })
    ));
    assert!(matches!(
        cluster.delete_payload_shard(shard_3_location, &shard_0_key),
        Err(ShardIoError::ShardIndexMismatch {
            location_shard_index: 3,
            key_shard_index: 0,
            ..
        })
    ));
    assert_eq!(
        cluster
            .read_payload_shard(shard_0_location, &shard_0_key, ack)
            .unwrap(),
        b"right shard"
    );
}

#[test]
fn payload_shard_io_rejects_stale_location_epoch() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let location = ShardLocation::new(
        ClusterEpoch::new(2).unwrap(),
        DataPgId::new(crate::PgId::new(0)),
        ShardIndex::new(0),
        NodeId::new(0),
    );
    let key = ShardKey::new(&[23; 16], 1, 0);

    let err = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"stale")
        .unwrap_err();

    assert!(matches!(
        err,
        ShardIoError::StaleLocation {
            location_epoch,
            current_epoch,
            ..
        } if location_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
}

#[test]
fn payload_shard_io_rejects_stale_operation_epoch_before_touching_node_store() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let location = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(0)),
        ShardIndex::new(0),
        NodeId::new(0),
    );
    let key = ShardKey::new(&[41; 16], 1, 0);

    let err = map
        .write_payload_shard(ClusterEpoch::new(2).unwrap(), location, &key, b"stale op")
        .unwrap_err();

    assert!(matches!(
        err,
        ShardIoError::StaleOperationEpoch {
            node_id: 0,
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let node = map.node(NodeId::new(0)).unwrap();
    assert!(matches!(
        node.storage_node().read_shard_file(0, &key),
        Err(StoreError::NotFound)
    ));
}

#[test]
fn payload_shard_io_rejects_node_outside_acting_set() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let location = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(crate::PgId::new(0)),
        ShardIndex::new(0),
        NodeId::new(99),
    );
    let key = ShardKey::new(&[29; 16], 1, 0);

    let err = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"unknown")
        .unwrap_err();

    assert!(matches!(
        err,
        ShardIoError::NodeNotInActingSet {
            node_id: 99,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
        }
    ));
}

#[test]
fn payload_placement_and_shard_io_reject_all_non_active_pg_states() {
    let non_active_states = [
        PgState::Peering,
        PgState::Degraded,
        PgState::Backfilling,
        PgState::Inconsistent,
    ];

    for state in non_active_states {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
        map.pg_routes.get_mut(&PgId::new(0)).unwrap().state = state;
        let location = ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(0)),
            ShardIndex::new(0),
            NodeId::new(0),
        );
        let key = ShardKey::new(&[47; 16], 1, 0);

        let err = map
            .place_payload_shards(
                ClusterEpoch::INITIAL,
                DataPgId::new(PgId::new(0)),
                ec_shape,
                b"non-active-placement",
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ClusterBuildError::PgNotActive {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));

        let err = map
            .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"non-active")
            .unwrap_err();
        assert!(matches!(
            err,
            ShardIoError::PgNotActive {
                node_id: 0,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));

        let err = map
            .read_payload_shard(
                ClusterEpoch::INITIAL,
                location,
                &key,
                WriteAck {
                    crc64: 0,
                    stored_size: 1,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ShardIoError::PgNotActive {
                node_id: 0,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));

        let err = map
            .delete_payload_shard(ClusterEpoch::INITIAL, location, &key)
            .unwrap_err();
        assert!(matches!(
            err,
            ShardIoError::PgNotActive {
                node_id: 0,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));

        assert!(matches!(
            map.node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .read_shard_file(0, &key),
            Err(StoreError::NotFound)
        ));
    }
}

#[test]
fn direct_put_payload_write_fails_closed_when_required_shard_node_leaves_acting_set() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, _object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation_id = crate::SessionId::try_from("56".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let segment_okh = [96; 16];
    let placement_key = super::super::segment_payload_placement_key(&segment_okh, generation_id);
    let locations = cluster
        .place_payload_shards(DataPgId::new(PgId::new(data_pg)), ec_shape, &placement_key)
        .unwrap();
    let (removed_shard_index, removed_node) = locations
        .iter()
        .enumerate()
        .rev()
        .map(|(index, location)| (index, location.node_id()))
        .find(|(_, node_id)| *node_id != NodeId::new(0))
        .expect("test placement should use a non-primary shard node");
    assert!(
        removed_shard_index > 0,
        "test must fail after at least one earlier shard write"
    );
    drop(cluster);

    {
        let route = Arc::get_mut(&mut map)
            .unwrap()
            .pg_routes
            .get_mut(&PgId::new(data_pg))
            .unwrap();
        let acting_set: Vec<NodeId> = node_ids
            .into_iter()
            .filter(|node_id| *node_id != removed_node)
            .collect();
        if route.primary_node_id == removed_node {
            route.primary_node_id = acting_set[0];
        }
        route.acting_set = Arc::from(acting_set);
    }
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            b"strict payload write requires every placed shard",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::NodeNotInActingSet {
            node_id,
            pg_id,
            cluster_epoch: ClusterEpoch::INITIAL,
        } if node_id == removed_node.as_u32() && pg_id == data_pg
    ));
    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    data_pg,
                    ec_shape,
                    &segment_okh,
                    generation_id,
                    shard_index,
                )
                .unwrap(),
            "failed strict payload write must not leave shard {shard_index}"
        );
    }
}

#[test]
fn placed_segment_recovery_propagates_non_active_pg_route() {
    let non_active_states = [
        PgState::Peering,
        PgState::Degraded,
        PgState::Backfilling,
        PgState::Inconsistent,
    ];

    for state in non_active_states {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
        let mut map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let segment = write_committed_direct_segment(&cluster, b"phase-six-seven-route-read");
        let data_pg_id = DataPgId::new(PgId::new(segment.written.data_pg_id));
        drop(cluster);

        Arc::get_mut(&mut map)
            .unwrap()
            .pg_routes
            .get_mut(&data_pg_id.pg_id())
            .unwrap()
            .state = state;
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let shard_size = segment
            .payload
            .len()
            .div_ceil(usize::from(segment.written.ec.k));
        let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
        let mut present_count = 0;

        let err = cluster
            .try_load_placed_segment_shard(
                segment.written.data_pg_id,
                &segment.segment_okh,
                segment.generation_id,
                &segment.locations,
                0,
                shard_size,
                &mut all_shards,
                &mut present_count,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::PgNotActive {
                pg_id,
                cluster_epoch,
                state: err_state,
            } if pg_id == data_pg_id.get()
                && cluster_epoch == ClusterEpoch::INITIAL
                && err_state == state
        ));
        assert_eq!(present_count, 0);
        assert!(all_shards.iter().all(Option::is_none));
    }
}

#[test]
fn placed_payload_delete_propagates_missing_pg_route() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();

    let err = cluster
        .delete_payload_shard_set(99, ec_shape, &[17; 16], crate::GenerationId::MIN)
        .unwrap_err();

    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::ClusterPgNotFound {
            pg_id: 99,
            cluster_epoch: ClusterEpoch::INITIAL,
        })
    ));
}

#[test]
fn placed_segment_recovery_propagates_missing_shard_pg_route() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-five-missing-pg-route");
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let mut locations = segment.locations.clone();
    locations[0] = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(99)),
        ShardIndex::new(0),
        locations[0].node_id(),
    );
    let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
    let mut present_count = 0;

    let err = cluster
        .try_load_placed_segment_shard(
            segment.written.data_pg_id,
            &segment.segment_okh,
            segment.generation_id,
            &locations,
            0,
            shard_size,
            &mut all_shards,
            &mut present_count,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::ShardPgNotFound {
            node_id,
            pg_id: 99,
            cluster_epoch: ClusterEpoch::INITIAL,
        } if node_id == locations[0].node_id().as_u32()
    ));
    assert_eq!(present_count, 0);
    assert!(all_shards.iter().all(Option::is_none));
}

#[test]
fn placed_segment_recovery_propagates_node_not_in_acting_set() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-five-acting-set-route");
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let mut locations = segment.locations.clone();
    locations[0] = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(segment.written.data_pg_id)),
        ShardIndex::new(0),
        NodeId::new(99),
    );
    let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
    let mut present_count = 0;

    let err = cluster
        .try_load_placed_segment_shard(
            segment.written.data_pg_id,
            &segment.segment_okh,
            segment.generation_id,
            &locations,
            0,
            shard_size,
            &mut all_shards,
            &mut present_count,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::NodeNotInActingSet {
            node_id: 99,
            pg_id,
            cluster_epoch: ClusterEpoch::INITIAL,
        } if pg_id == segment.written.data_pg_id
    ));
    assert_eq!(present_count, 0);
    assert!(all_shards.iter().all(Option::is_none));
}

#[test]
fn placed_segment_recovery_propagates_stale_shard_location() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-five-stale-location");
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let mut locations = segment.locations.clone();
    locations[0] = ShardLocation::new(
        ClusterEpoch::new(2).unwrap(),
        DataPgId::new(PgId::new(segment.written.data_pg_id)),
        ShardIndex::new(0),
        locations[0].node_id(),
    );
    let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
    let mut present_count = 0;

    let err = cluster
        .try_load_placed_segment_shard(
            segment.written.data_pg_id,
            &segment.segment_okh,
            segment.generation_id,
            &locations,
            0,
            shard_size,
            &mut all_shards,
            &mut present_count,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::StaleShardLocation {
            node_id,
            pg_id,
            location_epoch,
            current_epoch: ClusterEpoch::INITIAL,
        } if node_id == locations[0].node_id().as_u32()
            && pg_id == segment.written.data_pg_id
            && location_epoch == ClusterEpoch::new(2).unwrap()
    ));
    assert_eq!(present_count, 0);
    assert!(all_shards.iter().all(Option::is_none));
}

#[test]
fn placed_segment_recovery_propagates_shard_index_mismatch() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-five-shard-index-route");
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let mut locations = segment.locations.clone();
    locations[0] = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(segment.written.data_pg_id)),
        ShardIndex::new(1),
        locations[0].node_id(),
    );
    let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
    let mut present_count = 0;

    let err = cluster
        .try_load_placed_segment_shard(
            segment.written.data_pg_id,
            &segment.segment_okh,
            segment.generation_id,
            &locations,
            0,
            shard_size,
            &mut all_shards,
            &mut present_count,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::ShardIndexMismatch {
            node_id,
            pg_id,
            cluster_epoch: ClusterEpoch::INITIAL,
            location_shard_index: 1,
            key_shard_index: 0,
        } if node_id == locations[0].node_id().as_u32()
            && pg_id == segment.written.data_pg_id
    ));
    assert_eq!(present_count, 0);
    assert!(all_shards.iter().all(Option::is_none));
}

#[test]
fn placed_segment_recovery_wraps_node_store_error_with_shard_route() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let mut map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-five-shard-store-route");
    drop(cluster);

    Arc::get_mut(&mut map).unwrap().pg_routes.insert(
        PgId::new(99),
        LocalPgRoute::active(
            ClusterEpoch::INITIAL,
            PgId::new(99),
            NodeId::new(0),
            Arc::<[NodeId]>::from(node_ids.to_vec()),
        ),
    );
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let mut locations = segment.locations.clone();
    locations[0] = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(99)),
        ShardIndex::new(0),
        locations[0].node_id(),
    );
    let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
    let mut present_count = 0;

    let err = cluster
        .try_load_placed_segment_shard(
            segment.written.data_pg_id,
            &segment.segment_okh,
            segment.generation_id,
            &locations,
            0,
            shard_size,
            &mut all_shards,
            &mut present_count,
        )
        .unwrap_err();

    match err {
        StoreError::ShardStore {
            node_id,
            pg_id: 99,
            cluster_epoch: ClusterEpoch::INITIAL,
            source,
        } => {
            assert_eq!(node_id, locations[0].node_id().as_u32());
            assert!(matches!(*source, StoreError::PgNotFound { pg_id: 99 }));
        }
        other => panic!("expected shard store error with route context, got {other:?}"),
    }
    assert_eq!(present_count, 0);
    assert!(all_shards.iter().all(Option::is_none));
}

#[test]
fn placed_segment_recovery_treats_length_corrupt_shard_as_recoverable() {
    for extra_length in [false, true] {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let cluster = crate::StorageCluster::open_local_nodes(
            tmp.path(),
            &node_ids,
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap();
        let segment =
            write_committed_direct_segment(&cluster, b"phase-five-length-corrupt-payload");
        let shard_size = segment
            .payload
            .len()
            .div_ceil(usize::from(segment.written.ec.k));
        let corrupt_len = if extra_length { shard_size + 1 } else { 1 };
        let shard_path = cluster
            .test_payload_shard_file_path(
                segment.written.data_pg_id,
                segment.written.ec,
                &segment.segment_okh,
                segment.generation_id,
                0,
            )
            .unwrap();
        std::fs::write(&shard_path, vec![0xAB; corrupt_len]).unwrap();

        let mut recovered = Vec::new();
        cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: segment.written.data_pg_id,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.generation_id,
                    stored_size: segment.payload.len(),
                    segment_crc64: Some(checksum::crc64::checksum(&segment.payload)),
                    ec: segment.written.ec,
                },
                &mut recovered,
            )
            .unwrap();

        assert_eq!(
            recovered, segment.payload,
            "failed to recover when corrupt shard extra_length={extra_length}"
        );
    }
}

#[test]
fn payload_shard_io_rejects_unknown_pg_before_touching_node_store() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let location = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(99)),
        ShardIndex::new(0),
        NodeId::new(0),
    );
    let key = ShardKey::new(&[37; 16], 1, 0);

    let err = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"unknown pg")
        .unwrap_err();

    assert!(matches!(
        err,
        ShardIoError::PgNotFound {
            node_id: 0,
            pg_id: 99,
            cluster_epoch: ClusterEpoch::INITIAL,
        }
    ));
    assert!(!tmp.path().join("node-0000").join("pg-0099").exists());
}

#[test]
fn rejects_too_few_local_nodes_for_default_ec_shape_before_preparing_dirs() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
    ];
    let err = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0, 1],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::UnplaceableEcShape {
            data_shards: 4,
            parity_shards: 2,
            required_nodes: 6,
            node_count: 5,
        }
    ));
    for node_id in node_ids {
        assert!(
            !tmp.path()
                .join(format!("node-{:04}", node_id.as_u32()))
                .exists(),
            "placement validation must run before preparing local node directories"
        );
    }
}

#[test]
fn rejects_invalid_ec_shape_before_preparing_dirs() {
    let tmp = test_util::tempdir();
    let node_dir = tmp.path().join("node-0000");
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [LocalNodeStoreConfig::new(NodeId::new(0), &node_dir)],
        &[0],
        EcShape { k: 0, m: 2 },
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::InvalidEcShape {
            data_shards: 0,
            parity_shards: 2,
            ..
        }
    ));
    assert!(
        !node_dir.exists(),
        "EC shape validation must run before preparing local node directories"
    );
}

#[test]
fn rejects_duplicate_local_node_ids() {
    let tmp = test_util::tempdir();
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [
            LocalNodeStoreConfig::new(NodeId::new(0), tmp.path().join("a")),
            LocalNodeStoreConfig::new(NodeId::new(0), tmp.path().join("b")),
        ],
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap_err();

    assert!(matches!(err, ClusterBuildError::DuplicateNodeId { id: 0 }));
}

#[test]
fn rejects_empty_pg_set_before_preparing_dirs() {
    let tmp = test_util::tempdir();
    let node_dir = tmp.path().join("node-0000");
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [LocalNodeStoreConfig::new(NodeId::new(0), &node_dir)],
        &[],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap_err();

    assert!(matches!(err, ClusterBuildError::EmptyPgSet));
    assert!(
        !node_dir.exists(),
        "PG validation must run before preparing local node directories"
    );
}

#[test]
fn rejects_duplicate_pg_ids_before_preparing_dirs() {
    let tmp = test_util::tempdir();
    let node_dir = tmp.path().join("node-0000");
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [LocalNodeStoreConfig::new(NodeId::new(0), &node_dir)],
        &[0, 1, 1],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap_err();

    assert!(matches!(err, ClusterBuildError::DuplicatePgId { pg_id: 1 }));
    assert!(
        !node_dir.exists(),
        "PG validation must run before preparing local node directories"
    );
}

#[test]
fn rejects_duplicate_local_node_data_dirs() {
    let tmp = test_util::tempdir();
    let shared = tmp.path().join("shared");
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [
            LocalNodeStoreConfig::new(NodeId::new(0), &shared),
            LocalNodeStoreConfig::new(NodeId::new(1), &shared),
        ],
        &[0],
        EcShape { k: 1, m: 1 },
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::DuplicateDataDir {
            first_node_id: 0,
            duplicate_node_id: 1,
            ..
        }
    ));
    assert!(
        !shared.join("pg-0000").exists(),
        "duplicate directory validation must run before opening PG stores"
    );
}

#[test]
fn rejects_missing_metadata_primary() {
    let tmp = test_util::tempdir();
    let node_dir = tmp.path().join("a");
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [LocalNodeStoreConfig::new(NodeId::new(1), &node_dir)],
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::MetadataPrimaryNotFound { id: 0 }
    ));
    assert!(
        !node_dir.exists(),
        "metadata primary validation must run before preparing node directories"
    );
}
