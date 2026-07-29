use super::*;

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
    let expected_work = crate::PlacedSegmentShardRepairWorkItem {
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
    assert_eq!(repair_rows[0].observation_count, 1);
    assert_eq!(
        cluster.try_take_placed_segment_shard_repair_work(),
        Some(expected_work)
    );
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
    drop(reference_pg);

    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
    assert!(
        observations.iter().any(|observation| {
            observation.reason == crate::ShardScavengerObservationReason::ScanIncomplete
                && observation.resolved_at.is_none()
                && observation.key.node_id == primary_node_id
                && observation.key.data_pg_id == data_pg_id
                && observation.last_error.as_deref().is_some_and(|error| {
                    error.contains("reference scan failed")
                        && error.contains(
                            "decode pending metadata command for shard scavenger references",
                        )
                })
        }),
        "reference scan failures should be persisted as scan-incomplete observations"
    );
    assert!(
        observations.iter().all(|observation| {
            observation.reason
                != crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile
                || observation.key.shard_key != candidate_key
                || observation.resolved_at.is_some()
        }),
        "negative-reference candidates from an incomplete reference scan must not be reported"
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

    let observations = cluster.audit_shard_storage_for_scavenger().unwrap();
    assert!(
        observations.iter().any(|observation| {
            observation.reason == crate::ShardScavengerObservationReason::ScanIncomplete
                && observation.key.node_id == primary_node_id
                && observation.key.data_pg_id == data_pg_id
                && observation.resolved_at.is_some()
        }),
        "a later complete reference scan should resolve scan-incomplete observations"
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
        "once the reference scan completes, the apparent unreferenced shard can be reported"
    );
}
