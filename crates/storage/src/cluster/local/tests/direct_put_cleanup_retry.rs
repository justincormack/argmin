// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone, Copy)]
enum CleanupMode {
    Retry,
    Discard,
    CrossedKey,
    CrossedBucket,
}

#[test]
fn direct_put_cleanup_reports_physical_failures_and_retries_before_generation_release() {
    for mode in [CleanupMode::Retry, CleanupMode::Discard] {
        for fail_file in [true, false] {
            check_physical_cleanup_failure(fail_file, mode);
        }
    }
}

#[test]
fn crossed_route_discard_reports_physical_failure_without_drop_retry() {
    for mode in [CleanupMode::CrossedKey, CleanupMode::CrossedBucket] {
        for fail_file in [true, false] {
            check_physical_cleanup_failure(fail_file, mode);
        }
    }
}

fn check_physical_cleanup_failure(fail_file: bool, mode: CleanupMode) {
    let tmp = test_util::tempdir();
    let ec = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &trace_node_ids(), &[0], ec).unwrap());
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("cleanup-retry");
    let key = crate::tests::object_key("payload");
    let reservation_id = crate::tests::stream_session_id("cleanup-retry");
    create_test_bucket(&cluster, &bucket);
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let fixture = crate::tests::sse_s3_segment_test_fixture(b"retryable cleanup payload", 0);
    let reservation = route
        .test_reserve_generation_with_id(&reservation_id)
        .unwrap();
    let payload = route
        .write_direct_object_payload(
            reservation.bind_encryption(&fixture.encryption),
            fixture.logical_size,
            &fixture.storage_bytes,
        )
        .unwrap();
    let identity = direct_payload_test_identity(&payload);
    let shards = payload.written.written_shards.clone();
    // Model the state after acknowledgement registration but before terminal
    // ownership. Use the real encrypted write's exact acknowledgements.
    cluster
        .test_register_payload_shard_acks(identity.data_pg_id, &shards)
        .unwrap();
    assert_direct_payload_shards_exist(&cluster, identity);
    let ack_node = map
        .metadata_pg_primary_node(cluster.operation_epoch(), PgId::new(identity.data_pg_id))
        .unwrap()
        .storage_node();
    for shard in &shards {
        ack_node
            .get_pg(identity.data_pg_id)
            .unwrap()
            .validate_written_shard_ack(&shard.key, shard.ack)
            .unwrap();
    }
    let file_calls = Arc::new(AtomicUsize::new(0));
    let ack_calls = Arc::new(AtomicUsize::new(0));
    let observed_files = Arc::clone(&file_calls);
    let observed_acks = Arc::clone(&ack_calls);
    let file_hook =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |key| {
            observed_files.fetch_add(1, Ordering::SeqCst);
            if fail_file && key.shard_index().get() == 0 {
                return Err(StoreError::Io {
                    context: "cleanup retry file failure",
                    source: std::io::Error::other("injected"),
                });
            }
            Ok(())
        }));
    let ack_hook = cluster.test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(
        move |key| {
            observed_acks.fetch_add(1, Ordering::SeqCst);
            if !fail_file && key.shard_index().get() == 0 {
                return Err(StoreError::Io {
                    context: "cleanup retry ack failure",
                    source: std::io::Error::other("injected"),
                });
            }
            Ok(())
        },
    ));
    let (error, cleanup) = if matches!(mode, CleanupMode::Retry) {
        let mut cleanup = payload.into_cleanup();
        (cleanup.finish().unwrap_err(), Some(cleanup))
    } else {
        let crossed_key = crate::tests::object_key("crossed-key");
        let crossed_bucket = crate::tests::bucket_name("crossed-bucket");
        if matches!(mode, CleanupMode::CrossedBucket) {
            create_test_bucket(&cluster, &crossed_bucket);
        }
        let discard_route = match mode {
            CleanupMode::CrossedKey => admission
                .active_put_object_route(&bucket, &crossed_key)
                .unwrap(),
            CleanupMode::CrossedBucket => admission
                .active_put_object_route(&crossed_bucket, &key)
                .unwrap(),
            CleanupMode::Discard => admission.active_put_object_route(&bucket, &key).unwrap(),
            CleanupMode::Retry => unreachable!(),
        };
        (
            discard_route
                .discard_direct_object_payload(payload)
                .unwrap_err(),
            None,
        )
    };
    assert_eq!(error.diagnostic_cause_label(), "store_io_failure");
    assert_eq!(file_calls.load(Ordering::SeqCst), usize::from(ec.k + ec.m));
    assert_eq!(ack_calls.load(Ordering::SeqCst), usize::from(ec.k + ec.m));
    for shard in &shards {
        assert_eq!(
            cluster
                .test_payload_shard_file_exists(
                    identity.data_pg_id,
                    ec,
                    &identity.segment_okh,
                    identity.segment_vid,
                    shard.key.shard_index().get(),
                )
                .unwrap(),
            fail_file && shard.key.shard_index().get() == 0
        );
        let result = ack_node
            .get_pg(identity.data_pg_id)
            .unwrap()
            .validate_written_shard_ack(&shard.key, shard.ack);
        if !fail_file && shard.key.shard_index().get() == 0 {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(StoreError::NotFound)), "{result:?}");
        }
    }
    for node in map.nodes.values() {
        let pg = node.storage_node().get_pg(0).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id
            )
            .unwrap(),
            identity.segment_vid
        );
    }
    // Consuming discard reports the failure without hiding a second attempt
    // in Drop. Its lost retry custody remains a durable-owner obligation.
    let Some(mut cleanup) = cleanup else {
        return;
    };
    drop(file_hook);
    drop(ack_hook);
    let observed_files = Arc::clone(&file_calls);
    let observed_acks = Arc::clone(&ack_calls);
    let _file_hook =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |_| {
            observed_files.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
    let _ack_hook =
        cluster.test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(move |_| {
            observed_acks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
    cleanup.finish().unwrap();
    cleanup.finish().unwrap();
    drop(cleanup);
    assert_eq!(
        file_calls.load(Ordering::SeqCst),
        2 * usize::from(ec.k + ec.m)
    );
    assert_eq!(
        ack_calls.load(Ordering::SeqCst),
        2 * usize::from(ec.k + ec.m)
    );
    assert_direct_payload_staging_cleaned(&map, &cluster, &bucket, &key, &reservation_id, identity);
    for shard in &shards {
        assert!(matches!(
            ack_node
                .get_pg(identity.data_pg_id)
                .unwrap()
                .validate_written_shard_ack(&shard.key, shard.ack),
            Err(StoreError::NotFound)
        ));
    }
    assert_single_generation_release(&map, 0, &bucket, &key, &reservation_id);
}
