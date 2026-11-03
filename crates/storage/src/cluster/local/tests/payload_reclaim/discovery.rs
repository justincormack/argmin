// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn durable_reclaim_discovery_passes_deferred_and_leased_roots_after_restart() {
    for leased in [false, true] {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let pg_ids = [0, 1, 2, 3];
        let ec = EcShape { k: 2, m: 1 };
        let bucket = BucketName::try_from("bucket").unwrap();
        let (first, second) = {
            let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec).unwrap());
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let first_key = key_for_object_pg(topology, &bucket, 1, "a-deferred-");
            let second_key = key_for_object_pg(topology, &bucket, 1, "z-ready-");
            assert!(first_key < second_key);
            assert_eq!(topology.object_pg_for(&bucket, &first_key), 1);
            assert_eq!(topology.object_pg_for(&bucket, &second_key), 1);
            let cluster = StorageCluster::from_static_local_map(map).unwrap();
            let first = write_committed_direct_segment_for_with_okh(
                &cluster,
                &bucket,
                &first_key,
                [0x81; 16],
                b"protected first payload",
            );
            let second = write_committed_direct_segment_for_with_okh(
                &cluster,
                &bucket,
                &second_key,
                [0x82; 16],
                b"independent second payload",
            );
            for key in [&first_key, &second_key] {
                cluster
                    .delete_current_object_if(&bucket, key, |_| Ok::<(), ()>(()))
                    .unwrap()
                    .unwrap();
            }
            ((first_key, first), (second_key, second))
        };
        // Reopen, rather than relying on the deletion-generated queue hints.
        let cluster = StorageCluster::from_static_local_map(Arc::new(
            LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec).unwrap(),
        ))
        .unwrap();
        assert!(cluster.try_take_reclaim_work().is_none());
        let first_root = (bucket.clone(), first.0.clone(), first.1.generation_id);
        let second_root = (bucket.clone(), second.0.clone(), second.1.generation_id);
        let lease = leased.then(|| {
            cluster
                .acquire_object_payload_lease(&bucket, &first.0, first.1.generation_id)
                .unwrap()
        });
        let excluded = if leased {
            HashSet::new()
        } else {
            HashSet::from([first_root.clone()])
        };
        let scan = |excluded: &HashSet<_>| {
            let batch = cluster.enqueue_durable_reclaim_work_batch_excluding(
                None,
                pg_ids.len(),
                excluded,
                &HashSet::new(),
                &HashSet::new(),
            );
            assert_eq!(batch.outcome, crate::DurableReclaimScanOutcome::Complete);
            assert_eq!(batch.next_pg_id, None);
            assert!(batch.retry_pass_required);
        };
        let take_payload = || loop {
            match cluster.try_take_reclaim_work() {
                Some(ReclaimWorkItem::BucketDelete(root)) => {
                    // Successful reclaim also wakes bucket finalization. This
                    // fixture keeps the bucket live; consume only its wake hint.
                    assert_eq!(root.bucket, bucket);
                    assert_eq!(root.bucket_incarnation_generation, 1);
                    cluster.finish_bucket_delete_finalize_work(&root);
                }
                work => break work,
            }
        };
        scan(&excluded);
        assert!(take_payload().is_none(), "first root must remain deferred");
        scan(&excluded);
        assert_eq!(
            take_payload(),
            Some(ReclaimWorkItem::ObjectPayload(second_root.clone()))
        );
        assert!(cluster
            .reclaim_object_payload_if_unleased(&bucket, &second.0, second.1.generation_id)
            .unwrap());
        cluster.finish_object_payload_reclaim_work(&bucket, &second.0, second.1.generation_id);
        assert!(!cluster
            .payload_reclaim_exists(&bucket, &second.0, second.1.generation_id)
            .unwrap());
        assert!(cluster
            .payload_reclaim_exists(&bucket, &first.0, first.1.generation_id)
            .unwrap());
        let mut stored = Vec::new();
        cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: first.1.written.data_pg_id,
                    segment_okh: first.1.segment_okh,
                    segment_vid: first.1.generation_id,
                    stored_size: first.1.encrypted.storage_bytes.len(),
                    segment_crc64: first.1.encrypted.storage_crc64,
                    ec,
                },
                &mut stored,
            )
            .unwrap();
        assert_eq!(stored, first.1.encrypted.storage_bytes);

        drop(lease);
        scan(&HashSet::new());
        assert_eq!(
            take_payload(),
            Some(ReclaimWorkItem::ObjectPayload(first_root))
        );
        assert!(cluster
            .reclaim_object_payload_if_unleased(&bucket, &first.0, first.1.generation_id)
            .unwrap());
        cluster.finish_object_payload_reclaim_work(&bucket, &first.0, first.1.generation_id);
        let clean = cluster.enqueue_durable_reclaim_work_batch_excluding(
            None,
            pg_ids.len(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
        );
        assert!(!clean.retry_pass_required);
        assert!(take_payload().is_none());
    }
}
