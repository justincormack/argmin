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

mod bucket_commands;
mod bucket_delete;
mod command_fanout;
mod command_recovery;
mod direct_put;
mod metadata_replay;
mod multipart;
mod multipart_completion;
mod multipart_trace;
mod object_commands;
mod object_metadata;
mod object_read;
mod shard_scavenger;
mod stream_commands;
mod stream_put;
mod trace;
mod unix_clients;

use trace::{current_cluster, trace_node_ids};

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
