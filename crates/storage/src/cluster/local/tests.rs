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

fn placed_segment_shard_repair_work_item(index: usize) -> PlacedSegmentShardRepairWorkItem {
    let mut segment_okh = [0u8; 16];
    segment_okh[..8].copy_from_slice(&(index as u64).to_be_bytes());
    PlacedSegmentShardRepairWorkItem {
        request: crate::SegmentStoredBytesRequest {
            data_pg_id: 7,
            segment_okh,
            segment_vid: GenerationId::new(42).unwrap(),
            stored_size: 1024,
            segment_crc64: Some(index as u64),
            ec: EcShape { k: 4, m: 2 },
        },
        shard_index: ShardIndex::new((index % 6) as u8),
    }
}

#[test]
fn placed_segment_shard_repair_hint_queue_is_bounded() {
    let state = LocalClusterRuntimeState::new();
    for index in 0..LOCAL_PLACED_SEGMENT_SHARD_REPAIR_HINT_QUEUE_LIMIT {
        assert!(
            state.enqueue_placed_segment_shard_repair(placed_segment_shard_repair_work_item(index))
        );
    }

    assert!(
        !state.enqueue_placed_segment_shard_repair(placed_segment_shard_repair_work_item(
            LOCAL_PLACED_SEGMENT_SHARD_REPAIR_HINT_QUEUE_LIMIT
        ))
    );
    assert!(!state.enqueue_placed_segment_shard_repair(placed_segment_shard_repair_work_item(0)));

    for index in 0..LOCAL_PLACED_SEGMENT_SHARD_REPAIR_HINT_QUEUE_LIMIT {
        assert_eq!(
            state.try_take_placed_segment_shard_repair_work(),
            Some(placed_segment_shard_repair_work_item(index))
        );
    }
    assert!(state.try_take_placed_segment_shard_repair_work().is_none());
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
    let object_parts = crate::node_client::complete_multipart_expected_object_parts(
        req,
        crate::VersionId::Null,
        primary.storage_node().pg_topology(),
    );
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

fn set_route_state(map: &mut LocalClusterMap, pg_id: u32, state: PgState) {
    map.pg_routes.get_mut(&PgId::new(pg_id)).unwrap().state = state;
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
mod payload_reclaim;
mod placement;
mod shard_scavenger;
mod stream_commands;
mod stream_put;
mod trace;
mod unix_clients;

use trace::{current_cluster, trace_node_ids};
