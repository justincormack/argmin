// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cluster::{
    ShardScavengerReferenceScan, ShardScavengerRepairAuthority, ShardScavengerRepairCandidate,
};
use crate::types::ShardScavengerReferenceCursor;
use crate::SegmentStoredBytesRequest;

#[test]
fn shard_scavenger_audit_pass_pages_references_before_advancing_physical_pgs() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let cluster = crate::StorageCluster::open_static_local_nodes(
        tmp.path(),
        &node_ids,
        &pg_ids,
        EcShape { k: 2, m: 1 },
    )
    .unwrap();
    let mut pass = cluster.begin_shard_scavenger_audit_pass();

    for completed in 0..pg_ids.len() {
        assert!(
            cluster
                .audit_next_shard_scavenger_pg(&mut pass)
                .unwrap()
                .is_some(),
            "reference step {completed} must process exactly one metadata PG page"
        );
        assert_eq!(pass.next_reference_pg_index, completed + 1);
        assert_eq!(pass.next_pg_index, 0);
    }
    for completed in 0..pg_ids.len() {
        assert!(
            cluster
                .audit_next_shard_scavenger_pg(&mut pass)
                .unwrap()
                .is_some(),
            "physical step {completed} must process exactly one data PG"
        );
        assert_eq!(pass.next_pg_index, completed + 1);
    }
    assert!(
        cluster
            .audit_next_shard_scavenger_pg(&mut pass)
            .unwrap()
            .is_none(),
        "the pass must report completion only after every PG received its own step"
    );
}

#[test]
fn shard_scavenger_reference_page_failure_does_not_commit_cursor_or_partition() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let local_map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, EcShape { k: 2, m: 1 }).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg_id, _) = bucket_key_with_distinct_object_and_data_pg(topology);
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &key,
        b"reference page progress must be transactional",
    );
    let segment = crate::ObjectSegmentRecord {
        bucket,
        key,
        version_id: committed.version_id,
        segment_index: 0,
        size: committed.payload.len() as u64,
        segment_crc64: checksum::crc64::checksum(&committed.payload),
        segment_okh: committed.segment_okh,
        segment_vid: committed.generation_id,
        data_pg_id: committed.written.data_pg_id,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: committed.written.ec.k,
        ec_m: committed.written.ec.m,
    };
    map.metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(object_pg_id))
        .unwrap()
        .storage_node()
        .test_inject_exact_object_segment_unknown_data_pg(&segment, committed.generation_id)
        .unwrap();

    let mut pass = cluster.begin_shard_scavenger_audit_pass();
    loop {
        let before_pg_index = pass.next_reference_pg_index;
        let before_cursor = pass.reference_after.clone();
        let before_partition_count = pass.referenced_scans.len();
        match cluster.audit_next_shard_scavenger_pg(&mut pass) {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("malformed placement reference unexpectedly completed the pass"),
            Err(StoreError::ClusterPgNotFound { pg_id, .. }) => {
                assert_eq!(pg_id, u32::MAX);
                assert_eq!(pass.next_reference_pg_index, before_pg_index);
                assert_eq!(pass.reference_after, before_cursor);
                assert_eq!(pass.referenced_scans.len(), before_partition_count);
                assert!(!pass.referenced_scans.contains_key(&u32::MAX));
                break;
            }
            Err(error) => panic!("unexpected reference expansion failure: {error:?}"),
        }
    }
}

#[test]
fn shard_scavenger_conflicting_page_retry_does_not_duplicate_authorities_or_partitions() {
    let tmp = test_util::tempdir();
    let cluster = crate::StorageCluster::open_static_local_nodes(
        tmp.path(),
        &[NodeId::new(0)],
        &[0],
        EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let mut pass = cluster.begin_shard_scavenger_audit_pass();
    let shard_key = ShardKey::new(&[7; 16], 11, 0);
    let reference = crate::types::ShardScavengerPlacedShardSetReference {
        data_pg_id: 7,
        okh: [7; 16],
        generation_id: GenerationId::new(11).unwrap(),
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        stored_size: 16,
        crc64: 17,
        ec: EcShape { k: 1, m: 0 },
    };
    let authority = ShardScavengerRepairAuthority {
        source_pg_id: PgId::new(0),
        cursor: ShardScavengerReferenceCursor::ObjectSegment {
            bucket: BucketName::new("atomic-page").unwrap(),
            key: ObjectKey::new("key").unwrap(),
            version_id: 1,
            segment_index: 0,
        },
        reference: reference.clone(),
    };
    let work_item = PlacedSegmentShardRepairWorkItem {
        request: SegmentStoredBytesRequest {
            data_pg_id: 7,
            segment_okh: reference.okh,
            segment_vid: reference.generation_id,
            stored_size: reference.stored_size as usize,
            segment_crc64: reference.crc64,
            ec: reference.ec,
        },
        shard_index: shard_key.shard_index(),
    };
    let mut existing = ShardScavengerReferenceScan::default();
    existing.repair_work_by_shard.insert(
        shard_key.clone(),
        ShardScavengerRepairCandidate {
            work_item,
            authorities: vec![authority.clone()],
        },
    );
    pass.referenced_scans.insert(7, existing);

    for attempt in 0..2 {
        let mut conflicting_request = work_item.request;
        conflicting_request.segment_crc64 += 1;
        let mut conflicting = ShardScavengerReferenceScan::default();
        conflicting.repair_work_by_shard.insert(
            shard_key.clone(),
            ShardScavengerRepairCandidate {
                work_item: PlacedSegmentShardRepairWorkItem {
                    request: conflicting_request,
                    shard_index: work_item.shard_index,
                },
                authorities: vec![authority.clone()],
            },
        );
        let mut compatible_new_partition = ShardScavengerReferenceScan::default();
        compatible_new_partition
            .locations
            .insert((0, shard_key.clone()));
        let error = crate::StorageCluster::merge_shard_scavenger_reference_page(
            &mut pass,
            HashMap::from([(7, conflicting), (8, compatible_new_partition)]),
        )
        .expect_err("conflicting repair metadata must fail the complete page merge");
        assert!(matches!(error, StoreError::PayloadShardSetMismatch { .. }));
        assert_eq!(
            pass.referenced_scans[&7].repair_work_by_shard[&shard_key].authorities,
            std::slice::from_ref(&authority),
            "retry {attempt} must not append the already-seen authority"
        );
        assert!(!pass.referenced_scans.contains_key(&8));
    }
}

#[test]
fn shard_scavenger_transient_file_list_failure_retries_same_pg_with_references() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let local_map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, EcShape { k: 2, m: 1 }).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, _, _) = bucket_key_with_distinct_object_and_data_pg(topology);
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &key,
        b"retain the data-PG reference partition across transient scan failure",
    );

    let mut pass = cluster.begin_shard_scavenger_audit_pass();
    while pass.next_reference_pg_index < pass.pg_ids.len() {
        assert!(cluster
            .audit_next_shard_scavenger_pg(&mut pass)
            .unwrap()
            .is_some());
    }
    let target_index = pass
        .pg_ids
        .iter()
        .position(|pg_id| pg_id.get() == committed.written.data_pg_id)
        .unwrap();
    while pass.next_pg_index < target_index {
        assert!(cluster
            .audit_next_shard_scavenger_pg(&mut pass)
            .unwrap()
            .is_some());
    }
    assert!(pass
        .referenced_scans
        .contains_key(&committed.written.data_pg_id));

    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .test_fail_next_shard_scavenger_file_scan();
    let error = cluster
        .audit_next_shard_scavenger_pg(&mut pass)
        .expect_err("transient file-list failure must defer this PG");
    assert!(matches!(
        error,
        StoreError::Io {
            context: "list shard files for scavenger audit",
            ..
        }
    ));
    assert_eq!(pass.next_pg_index, target_index);
    assert!(pass
        .referenced_scans
        .contains_key(&committed.written.data_pg_id));

    assert!(cluster
        .audit_next_shard_scavenger_pg(&mut pass)
        .unwrap()
        .is_some());
    assert_eq!(pass.next_pg_index, target_index + 1);
    assert!(!pass
        .referenced_scans
        .contains_key(&committed.written.data_pg_id));
}

#[test]
fn shard_scavenger_does_not_revalidate_healthy_referenced_shards() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let local_map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, EcShape { k: 2, m: 1 }).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg_id, _) = bucket_key_with_distinct_object_and_data_pg(topology);
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let _committed = write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &key,
        b"healthy shard rows require no point revalidation",
    );

    cluster.audit_shard_storage_for_scavenger().unwrap();

    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(object_pg_id))
        .unwrap();
    let pg = primary.storage_node().get_pg(object_pg_id).unwrap();
    assert_eq!(
        pg.test_shard_scavenger_reference_match_calls(),
        0,
        "exact metadata revalidation must be reserved for missing shard locations"
    );
}

#[test]
fn cluster_shard_scavenger_records_file_without_row_observations() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let local_map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0], EcShape { k: 2, m: 1 }).unwrap();
    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(local_map)).unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("77777777777777777777777777777777".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &[0xe7; 16],
            b"background shard scavenger audit candidate",
        )
        .unwrap();

    let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
    assert!(
        written.written_shards.iter().all(|shard| {
            observations.iter().any(|observation| {
                observation.reason == crate::ShardScavengerObservationReason::FileWithoutShardRow
                    && observation.resolved_at.is_none()
                    && observation.key.data_pg_id == written.data_pg_id
                    && observation.key.shard_key == shard.key
            })
        }),
        "shard scavenger audit did not record file-without-row observations; observations={observations:?}"
    );
}

#[test]
fn cluster_shard_scavenger_marks_slow_writer_candidate_and_resolves_after_publish() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, _object_pg, _data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("67676767676767676767676767676767".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"slow writer shard scavenger candidate";
    let segment_okh = [0xd4; 16];
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
    let shard_batch: Vec<(&crate::ShardKey, crate::WriteAck)> = written
        .written_shards
        .iter()
        .map(|shard| (&shard.key, shard.ack))
        .collect();
    cluster
        .register_payload_shard_acks(written.data_pg_id, &shard_batch)
        .unwrap();

    let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
    let unreferenced: Vec<_> = observations
        .iter()
        .filter(|observation| {
            observation.reason
                == crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile
                && observation.key.data_pg_id == written.data_pg_id
                && written
                    .written_shards
                    .iter()
                    .any(|shard| shard.key == observation.key.shard_key)
        })
        .collect();
    assert_eq!(
        unreferenced.len(),
        written.written_shards.len(),
        "pre-publish shards with rows and files should be audit candidates, not deletion proof"
    );
    for shard in &written.written_shards {
        assert!(
            cluster
                .test_payload_shard_file_exists(
                    written.data_pg_id,
                    written.ec,
                    &segment_okh,
                    generation_id,
                    shard.key.shard_index().get(),
                )
                .unwrap(),
            "audit-only scavenger must not delete slow-writer shard files"
        );
    }

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
    let _outcome = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap();

    let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
    let unresolved_unreferenced = observations.iter().any(|observation| {
        observation.reason == crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile
            && observation.resolved_at.is_none()
            && observation.key.data_pg_id == written.data_pg_id
            && written
                .written_shards
                .iter()
                .any(|shard| shard.key == observation.key.shard_key)
    });
    assert!(
        !unresolved_unreferenced,
        "published metadata reference should resolve the apparent orphan observations"
    );
}

#[test]
fn cluster_shard_scavenger_audit_does_not_wait_for_non_primary_pg_mutex() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut local_map, 0, NodeId::new(0));
    let map = Arc::new(local_map);
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());

    let non_primary_pg_guard = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(0)
        .unwrap();
    let audit_cluster = Arc::clone(&cluster);
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let result = audit_cluster
            .audit_shard_storage_for_scavenger()
            .map(|observations| observations.len());
        tx.send(result).unwrap();
    });

    let audit_result = rx.recv_timeout(std::time::Duration::from_secs(1));
    drop(non_primary_pg_guard);
    assert!(
        audit_result.is_ok(),
        "cluster shard-scavenger audit should not wait for non-primary PgStore mutex"
    );
    assert_eq!(audit_result.unwrap().unwrap(), 0);
    handle.join().unwrap();
}

#[test]
fn cluster_shard_scavenger_treats_pending_direct_put_as_referenced() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg, _data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("69696969696969696969696969696969".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"pending direct put shard scavenger reference";
    let segment_okh = [0xd6; 16];
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
    let shard_batch: Vec<(&crate::ShardKey, crate::WriteAck)> = written
        .written_shards
        .iter()
        .map(|shard| (&shard.key, shard.ack))
        .collect();
    cluster
        .register_payload_shard_acks(written.data_pg_id, &shard_batch)
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
    let pg_id = PgId::new(object_pg);
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
    drop(pg);

    let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
    let unresolved_unreferenced = observations.iter().any(|observation| {
        observation.reason == crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile
            && observation.resolved_at.is_none()
            && observation.key.data_pg_id == written.data_pg_id
            && written
                .written_shards
                .iter()
                .any(|shard| shard.key == observation.key.shard_key)
    });
    assert!(
        !unresolved_unreferenced,
        "pending metadata command payload references should suppress apparent orphan observations"
    );
}

#[test]
fn cluster_shard_scavenger_treats_pending_multipart_completion_as_referenced() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg, _data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let (req, expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "mpuscavenge");
    let pg_id = PgId::new(object_pg);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let (command, _) =
        pending_multipart_completion_command_for_test(&map, &cluster, pg_id, &req, 1234);
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    pg.try_insert_pending_metadata_command_slot(
        primary.node_id().as_u32(),
        &command,
        Some(&bucket),
    )
    .unwrap();
    assert!(
        pg.test_delete_multipart_part_segments_for_upload(&req.upload_id)
            .unwrap()
            > 0
    );
    drop(pg);

    let ec = EcShape {
        k: expected_segment.ec_k,
        m: expected_segment.ec_m,
    };
    let shard_keys = (0..(ec.k + ec.m))
        .map(|shard_index| {
            ShardKey::new(
                &expected_segment.segment_okh,
                expected_segment.segment_vid.get(),
                shard_index,
            )
        })
        .collect::<Vec<_>>();

    let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
    let unresolved_unreferenced = observations.iter().any(|observation| {
        observation.reason == crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile
            && observation.resolved_at.is_none()
            && observation.key.data_pg_id == expected_segment.data_pg_id
            && shard_keys.contains(&observation.key.shard_key)
    });
    assert!(
            !unresolved_unreferenced,
            "pending multipart completion payload references should suppress apparent orphan observations"
        );

    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    assert!(pg.test_clear_pending_metadata_command_slot().unwrap());
    drop(pg);

    let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
    let reported = observations
        .iter()
        .filter(|observation| {
            observation.reason
                == crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile
                && observation.resolved_at.is_none()
                && observation.key.data_pg_id == expected_segment.data_pg_id
                && shard_keys.contains(&observation.key.shard_key)
        })
        .count();
    assert_eq!(
            reported,
            shard_keys.len(),
            "without the pending multipart completion, every shard in the detached segment is an audit candidate"
        );
}

#[test]
fn cluster_shard_scavenger_reports_wrong_node_file_and_expected_missing_file() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, _object_pg, _data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("68686868686868686868686868686868".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"wrong-node shard scavenger candidate";
    let segment_okh = [0xd5; 16];
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
    let _outcome = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap();

    let misplaced_shard = &written.written_shards[0].key;
    let data_pg = DataPgId::new_for_test(PgId::new(written.data_pg_id));
    let placement_key =
        super::super::super::segment_payload_placement_key(&segment_okh, generation_id);
    let locations = cluster
        .place_payload_shards(data_pg, written.ec, &placement_key)
        .unwrap();
    let expected_node_id = locations[usize::from(misplaced_shard.shard_index().get())].node_id();
    let wrong_node_id = node_ids
        .into_iter()
        .find(|node_id| *node_id != expected_node_id)
        .unwrap();
    let expected_path = cluster
        .test_payload_shard_file_path(
            written.data_pg_id,
            written.ec,
            &segment_okh,
            generation_id,
            misplaced_shard.shard_index().get(),
        )
        .unwrap();
    let shard_bytes = std::fs::read(&expected_path).unwrap();
    std::fs::remove_file(&expected_path).unwrap();
    let wrong_path = map
        .node(wrong_node_id)
        .unwrap()
        .data_dir()
        .join(format!("pg-{:04}", written.data_pg_id))
        .join("shards")
        .join(misplaced_shard.hex_prefix())
        .join(misplaced_shard.hex());
    std::fs::create_dir_all(wrong_path.parent().unwrap()).unwrap();
    std::fs::write(&wrong_path, shard_bytes).unwrap();

    let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
    assert!(
        observations.iter().any(|observation| {
            observation.reason == crate::ShardScavengerObservationReason::ShardRowWithoutFile
                && observation.resolved_at.is_none()
                && observation.key.node_id == expected_node_id.as_u32()
                && observation.key.data_pg_id == written.data_pg_id
                && observation.key.shard_key == *misplaced_shard
        }),
        "missing referenced shard must be reported at the expected placement node"
    );
    assert!(
        observations.iter().any(|observation| {
            observation.reason
                == crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile
                && observation.resolved_at.is_none()
                && observation.key.node_id == wrong_node_id.as_u32()
                && observation.key.data_pg_id == written.data_pg_id
                && observation.key.shard_key == *misplaced_shard
        }),
        "same shard key on the wrong node must remain a distinct physical observation"
    );

    let repair_rows = cluster
        .list_placed_segment_shard_repairs(written.data_pg_id)
        .unwrap();
    assert_eq!(repair_rows.len(), 1);
    let expected_work = crate::types::PlacedSegmentShardRepairWorkItem {
        request: crate::SegmentStoredBytesRequest {
            data_pg_id: written.data_pg_id,
            segment_okh,
            segment_vid: generation_id,
            stored_size: payload.len(),
            segment_crc64: checksum::crc64::checksum(payload),
            ec: written.ec,
        },
        shard_index: misplaced_shard.shard_index(),
    };
    assert_eq!(repair_rows[0].work_item, expected_work);
    assert_eq!(repair_rows[0].observation_count, 2);
    assert_eq!(
        cluster.try_take_placed_segment_shard_repair_work(),
        Some(expected_work)
    );
}

#[test]
fn cluster_shard_scavenger_revalidates_repair_after_reference_snapshot_changes() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, _object_pg, _data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"stale repair reference");
    let missing_shard = &committed.written.written_shards[0];
    let missing_path = cluster
        .test_payload_shard_file_path(
            committed.written.data_pg_id,
            committed.written.ec,
            &committed.segment_okh,
            committed.generation_id,
            missing_shard.key.shard_index().get(),
        )
        .unwrap();
    std::fs::remove_file(&missing_path).unwrap();

    let mut pass = cluster.begin_shard_scavenger_audit_pass();
    while pass.next_reference_pg_index < pass.pg_ids.len() {
        assert!(cluster
            .audit_next_shard_scavenger_pg(&mut pass)
            .unwrap()
            .is_some());
    }
    assert_eq!(pass.next_pg_index, 0);
    let referenced_scan = pass
        .referenced_scans
        .get(&committed.written.data_pg_id)
        .expect("reference discovery must partition the object into its data PG");
    assert!(committed.written.written_shards.iter().all(|shard| {
        referenced_scan
            .locations
            .iter()
            .any(|(_, key)| key == &shard.key)
    }));
    assert!(pass.referenced_scans.iter().all(|(data_pg_id, scan)| {
        *data_pg_id == committed.written.data_pg_id
            || committed
                .written
                .written_shards
                .iter()
                .all(|shard| scan.locations.iter().all(|(_, key)| key != &shard.key))
    }));

    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();

    let mut observations = Vec::new();
    while let Some(pg_observations) = cluster.audit_next_shard_scavenger_pg(&mut pass).unwrap() {
        observations.extend(pg_observations);
    }
    assert!(
        pass.referenced_scans.is_empty(),
        "each data PG must consume its prebuilt reference partition exactly once"
    );
    assert!(observations.iter().all(|observation| {
        observation.reason != crate::ShardScavengerObservationReason::ShardRowWithoutFile
            || observation.key.shard_key != missing_shard.key
            || observation.resolved_at.is_some()
    }));

    assert!(
        cluster
            .list_placed_segment_shard_repairs(committed.written.data_pg_id)
            .unwrap()
            .is_empty(),
        "a reference removed after paging must not authorize durable repair"
    );
}

#[test]
fn cluster_shard_scavenger_accepts_any_live_exact_repair_authority() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let local_map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, EcShape { k: 2, m: 1 }).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg, _) = bucket_key_with_distinct_object_and_data_pg(topology);
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &key,
        b"applied and terminal-pending repair authority",
    );
    let missing_shard = committed.written.written_shards[0].clone();
    let missing_path = cluster
        .test_payload_shard_file_path(
            committed.written.data_pg_id,
            committed.written.ec,
            &committed.segment_okh,
            committed.generation_id,
            missing_shard.key.shard_index().get(),
        )
        .unwrap();
    std::fs::remove_file(missing_path).unwrap();

    let pg_id = PgId::new(object_pg);
    let pending = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
            object: crate::PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: committed.version_id,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                generation_id: committed.generation_id,
                size: committed.payload.len() as u64,
                etag: crate::ObjectEtag::single_part(checksum::crc64::checksum(&committed.payload)),
                ec: committed.written.ec,
                layout: crate::ObjectLayout::Standard,
                tags: None,
                metadata_blob: Some(crate::SerializedMetadataBlob::default()),
                system_metadata_blob: Some(crate::SerializedSystemMetadataBlob::default()),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            },
            segments: vec![crate::ObjectSegmentRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: committed.version_id,
                segment_index: 0,
                size: committed.payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(&committed.payload),
                segment_okh: committed.segment_okh,
                segment_vid: committed.generation_id,
                data_pg_id: committed.written.data_pg_id,
                placement_cluster_epoch: ClusterEpoch::INITIAL,
                ec_k: committed.written.ec.k,
                ec_m: committed.written.ec.m,
            }],
            generation_reservation_id: crate::SessionId::try_from("73".repeat(16)).unwrap(),
            write_sequence: 1,
            last_modified_millis: 1,
            stale_payload: None,
            bucket_write_reservation: acquire_test_bucket_write_proof(
                &cluster,
                &bucket,
                crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
            ),
        })),
    );
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(object_pg).unwrap();
    pg.try_insert_pending_metadata_command_slot(
        primary.node_id().as_u32(),
        &pending,
        Some(&bucket),
    )
    .unwrap();
    drop(pg);

    let mut pass = cluster.begin_shard_scavenger_audit_pass();
    while pass.next_reference_pg_index < pass.pg_ids.len() {
        assert!(cluster
            .audit_next_shard_scavenger_pg(&mut pass)
            .unwrap()
            .is_some());
    }
    let candidate = pass
        .referenced_scans
        .get(&committed.written.data_pg_id)
        .and_then(|scan| scan.repair_work_by_shard.get(&missing_shard.key))
        .expect("the missing shard must retain both discovered authorities");
    assert_eq!(
        candidate.authorities.len(),
        2,
        "the applied object and terminal pending command must not overwrite each other"
    );

    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(object_pg).unwrap();
    assert!(pg.test_clear_pending_metadata_command_slot().unwrap());
    drop(pg);

    while cluster
        .audit_next_shard_scavenger_pg(&mut pass)
        .unwrap()
        .is_some()
    {}
    assert!(cluster
        .list_placed_segment_shard_repairs(committed.written.data_pg_id)
        .unwrap()
        .iter()
        .any(|repair| repair.work_item.shard_index == missing_shard.key.shard_index()));
}

#[test]
fn cluster_shard_scavenger_scan_incomplete_suppresses_negative_reference_claims() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let data_pg_id = 2;
    let primary = local_map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(data_pg_id))
        .unwrap();
    let primary_node_id = primary.node_id().as_u32();
    let malformed_node_id = NodeId::new(1);
    let candidate_key = ShardKey::new(&[0xd7; 16], 42, 1);
    let pg = primary.storage_node().get_pg(data_pg_id).unwrap();
    crate::traits::ShardStore::write_shard(&*pg, &candidate_key, b"candidate").unwrap();
    drop(pg);

    let malformed_dir = local_map
        .node(malformed_node_id)
        .unwrap()
        .data_dir()
        .join(format!("pg-{data_pg_id:04}"))
        .join("shards")
        .join("aa");
    std::fs::create_dir_all(&malformed_dir).unwrap();
    let malformed_path = malformed_dir.join("not-a-shard-key");
    std::fs::write(&malformed_path, b"junk").unwrap();

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(local_map)).unwrap();
    let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
    assert!(
        observations.iter().any(|observation| {
            observation.reason == crate::ShardScavengerObservationReason::ScanIncomplete
                && observation.resolved_at.is_none()
                && observation.key.node_id == malformed_node_id.as_u32()
                && observation.key.data_pg_id == data_pg_id
                && observation
                    .last_error
                    .as_deref()
                    .is_some_and(|error| error.contains("not-a-shard-key"))
        }),
        "incomplete shard file scan should be persisted as an audit observation"
    );
    assert!(
        observations.iter().all(|observation| {
            observation.reason
                != crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile
                || observation.key.shard_key != candidate_key
                || observation.resolved_at.is_some()
        }),
        "negative-reference candidates from an incomplete scan must not be reported"
    );

    std::fs::remove_file(&malformed_path).unwrap();
    let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
    assert!(
        observations.iter().any(|observation| {
            observation.reason == crate::ShardScavengerObservationReason::ScanIncomplete
                && observation.key.node_id == malformed_node_id.as_u32()
                && observation.key.data_pg_id == data_pg_id
                && observation.resolved_at.is_some()
        }),
        "a later complete scan should resolve the scan-incomplete observation"
    );
    assert!(
        observations.iter().any(|observation| {
            observation.reason
                == crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile
                && observation.resolved_at.is_none()
                && observation.key.node_id == primary_node_id
                && observation.key.data_pg_id == data_pg_id
                && observation.key.shard_key == candidate_key
        }),
        "once the scan completes, the apparent unreferenced shard can be reported"
    );
}

#[test]
fn cluster_shard_scavenger_reference_scan_failure_suppresses_negative_reference_claims() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let data_pg_id = 2;
    let primary = local_map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(data_pg_id))
        .unwrap();
    let primary_node_id = primary.node_id().as_u32();
    let candidate_key = ShardKey::new(&[0xd8; 16], 42, 1);
    let pg = primary.storage_node().get_pg(data_pg_id).unwrap();
    crate::traits::ShardStore::write_shard(&*pg, &candidate_key, b"candidate").unwrap();
    drop(pg);

    let reference_pg_id = 1;
    let reference_primary = local_map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(reference_pg_id))
        .unwrap();
    let reference_pg = reference_primary
        .storage_node()
        .get_pg(reference_pg_id)
        .unwrap();
    let malformed_bytes = b"not a metadata command".to_vec();
    let malformed_checksum = checksum::crc64::checksum(&malformed_bytes);
    reference_pg
        .test_insert_raw_pending_metadata_command_slot(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(reference_pg_id),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            malformed_checksum,
            &malformed_bytes,
            None,
        )
        .unwrap();
    reference_pg
        .test_set_pending_reclaim_reference_count(1)
        .unwrap();
    drop(reference_pg);

    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let mut pass = cluster.begin_shard_scavenger_audit_pass();
    assert!(cluster
        .audit_next_shard_scavenger_pg(&mut pass)
        .unwrap()
        .is_some());
    let error = cluster
        .audit_next_shard_scavenger_pg(&mut pass)
        .expect_err("malformed reference page must defer the pass");
    assert!(matches!(
        error,
        StoreError::ShardScavengerScanIncomplete {
            context: "decode pending command shard scavenger reference index",
            ..
        }
    ));
    assert_eq!(
        pass.next_reference_pg_index, reference_pg_id as usize,
        "a failed page must not advance the reference cursor"
    );

    let reference_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(reference_pg_id))
        .unwrap();
    let reference_pg = reference_primary
        .storage_node()
        .get_pg(reference_pg_id)
        .unwrap();
    assert!(reference_pg
        .test_clear_pending_metadata_command_slot()
        .unwrap());
    drop(reference_pg);

    let mut observations = Vec::new();
    while let Some(pg_observations) = cluster.audit_next_shard_scavenger_pg(&mut pass).unwrap() {
        observations.extend(pg_observations);
    }
    assert!(observations.iter().all(|observation| {
        observation.reason != crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile
            || observation.key.shard_key != candidate_key
            || observation.resolved_at.is_some()
    }));

    let mut confirmed_pass = cluster.begin_shard_scavenger_audit_pass_with_prior(std::mem::take(
        &mut pass.current_unreferenced,
    ));
    observations.clear();
    while let Some(pg_observations) = cluster
        .audit_next_shard_scavenger_pg(&mut confirmed_pass)
        .unwrap()
    {
        observations.extend(pg_observations);
    }
    assert!(
        observations.iter().any(|observation| {
            observation.reason
                == crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile
                && observation.resolved_at.is_none()
                && observation.key.node_id == primary_node_id
                && observation.key.data_pg_id == data_pg_id
                && observation.key.shard_key == candidate_key
        }),
        "once the reference scan completes, the apparent unreferenced shard can be reported"
    );
}
