// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn direct_put_winner_between_pending_lookup_and_snapshot_preserves_condition() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
            &[0],
            EcShape { k: 2, m: 1 },
        )
        .unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let bucket = BucketName::try_from("snapshot-cleanup-race".to_owned()).unwrap();
    let key = ObjectKey::try_from("object".to_owned()).unwrap();
    create_test_bucket(&cluster, &bucket);
    let prepare = |label: &str, byte: u8, payload: &[u8]| {
        let reservation_id = crate::tests::stream_session_id(label);
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let written = write_encrypted_direct_put_segment_payload_shards(
            &cluster,
            &bucket,
            &key,
            generation_id,
            0,
            &[byte; 16],
            payload,
        )
        .unwrap();
        let request = direct_put_commit_req(
            &cluster,
            DirectPutCommitReqFixture {
                bucket: &bucket,
                key: &key,
                reservation_id,
                generation_id,
                payload,
                segment_okh: [byte; 16],
                written: &written,
            },
        );
        (request, written)
    };
    let (original, original_written) = prepare("original", 0x81, b"original object");
    cluster
        .commit_direct_put_object_from_payload_shards(
            &original,
            &original_written.written_shards,
            |_| Ok::<(), ()>(()),
        )
        .unwrap()
        .unwrap();
    let (loser, loser_written) = prepare("loser", 0x82, b"losing conditional object");
    let (winner, winner_written) = prepare("winner", 0x83, b"winning conditional object");
    let original_etag = crate::ObjectEtag::single_part(original.etag_crc64);
    let winner_etag = crate::ObjectEtag::single_part(winner.etag_crc64);
    assert_ne!(original_etag, winner_etag);

    // Publish the real winner after the loser observes an empty pending slot,
    // but before it loads the object. Keep only the winner's terminal cleanup
    // outstanding: the losing snapshot must already contain the winning ETag.
    let defer_cleanup = Arc::new(AtomicBool::new(true));
    let defer_for_hook = Arc::clone(&defer_cleanup);
    let winner_generation = winner.generation_id;
    let _cleanup_hook = cluster.test_install_global_metadata_command_terminal_slot_removal_hook(
        Arc::new(move |command| {
            matches!(command.payload(), MetadataCommandPayload::CommitDirectPutObject(commit)
                if commit.object.generation_id == winner_generation)
                && defer_for_hook.load(Ordering::SeqCst)
        }),
    );
    let snapshot_calls = Arc::new(AtomicUsize::new(0));
    let calls_for_hook = Arc::clone(&snapshot_calls);
    let hook_cluster = Arc::clone(&cluster);
    let hook_winner = winner.clone();
    let winner_shards = winner_written.written_shards.clone();
    let (published_tx, published_rx) = std::sync::mpsc::sync_channel(1);
    let _snapshot_hook = cluster.test_install_direct_put_snapshot_read_hook(Arc::new(move || {
        if calls_for_hook.fetch_add(1, Ordering::SeqCst) == 0 {
            hook_cluster
                .commit_direct_put_object_from_payload_shards(
                    &hook_winner,
                    &winner_shards,
                    |snapshot| {
                        assert_eq!(snapshot.existing_etag, Some(original_etag.format()));
                        Ok::<(), ()>(())
                    },
                )
                .unwrap()
                .unwrap();
            published_tx.send(()).unwrap();
        }
        Ok(())
    }));

    let mut action_calls = 0;
    let result = thread::scope(|scope| {
        let map = &map;
        let cluster = &cluster;
        let bucket = &bucket;
        scope.spawn(move || {
            published_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let pg_id = PgId::new(0);
            let command = pending_metadata_command_for_test(map, pg_id, bucket).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            // Do not rescue the caller before it actually encounters the
            // recovery handoff. No synthetic contention/error is injected.
            while !map
                .runtime_state()
                .test_metadata_command_recovery_handoff_requested(pg_id, &command)
            {
                assert!(Instant::now() < deadline, "loser never requested recovery");
                thread::yield_now();
            }
            defer_cleanup.store(false, Ordering::SeqCst);
            assert_eq!(
                cluster
                    .drain_pending_metadata_command_with_authorized_recovery_route(
                        pg_id, &command, cluster,
                    )
                    .unwrap(),
                PendingMetadataCommandOutcome::Applied,
            );
        });
        cluster.commit_direct_put_object_from_payload_shards(
            &loser,
            &loser_written.written_shards,
            |snapshot| {
                action_calls += 1;
                assert_eq!(snapshot.existing_etag, Some(winner_etag.format()));
                assert_ne!(snapshot.existing_etag, Some(original_etag.format()));
                Err::<(), _>("If-Match precondition failed")
            },
        )
    });
    if let Err(error) = &result {
        eprintln!(
            "direct_put_error cause_label={}: {error:?}",
            error.diagnostic_cause_label()
        );
    }
    assert!(matches!(
        result.unwrap(),
        Err("If-Match precondition failed")
    ));
    assert_eq!(action_calls, 1);
    assert_eq!(snapshot_calls.load(Ordering::SeqCst), 2);
    assert_single_generation_release(&map, 0, &bucket, &key, &loser.generation_reservation_id);
    assert_direct_payload_staging_cleaned(
        &map,
        &cluster,
        &bucket,
        &key,
        &loser.generation_reservation_id,
        DirectPayloadTestIdentity {
            data_pg_id: loser_written.data_pg_id,
            ec: loser_written.ec,
            segment_okh: loser.segment_okh,
            segment_vid: loser.generation_id,
        },
    );
    assert_direct_payload_shards_exist(
        &cluster,
        DirectPayloadTestIdentity {
            data_pg_id: winner_written.data_pg_id,
            ec: winner_written.ec,
            segment_okh: winner.segment_okh,
            segment_vid: winner.generation_id,
        },
    );
    for node in map.nodes.values() {
        let pg = node.storage_node().get_pg(0).unwrap();
        let current = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(
            current.as_live().unwrap().generation_id,
            winner.generation_id
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
    assert!(pending_metadata_command_for_test(&map, PgId::new(0), &bucket).is_none());
}
