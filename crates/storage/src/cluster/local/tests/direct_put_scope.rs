// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn reservations(
    map: &LocalClusterMap,
    bucket: &crate::BucketName,
) -> Vec<crate::BucketWriteReservationRecord> {
    let pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(0)
        .unwrap();
    crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, bucket).unwrap()
}

#[test]
fn direct_put_scope_releases_early_failure_without_touching_foreign_proof() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("scope-early");
    let key = crate::tests::object_key("key");
    create_test_bucket(&cluster, &bucket);
    let _foreign = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let before = reservations(&map, &bucket);
    assert_eq!(before.len(), 1);
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let result = route
        .with_direct_put_scope(Default::default(), |_, scope| {
            let generation = scope.reserve_generation().unwrap();
            let metadata_before = cluster
                .test_object_pg_metadata_proof(&bucket, &key)
                .unwrap();
            let error = scope.reserve_generation().unwrap_err();
            assert_eq!(error.diagnostic_cause_label(), "invalid_request");
            assert_eq!(
                cluster
                    .test_object_pg_metadata_proof(&bucket, &key)
                    .unwrap(),
                metadata_before
            );
            assert_eq!(reservations(&map, &bucket).len(), 2);
            assert_ne!(
                scope.reservation_proof().reservation_id,
                before[0].reservation_id
            );
            drop(generation);
            Err::<(), _>("encryption failed before terminal publication")
        })
        .unwrap();
    assert_eq!(result, Err("encryption failed before terminal publication"));
    assert_eq!(reservations(&map, &bucket), before);
}

#[test]
fn direct_put_scope_rejects_exact_proof_substitution_and_commits_own_proof() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("scope-proof");
    let key = crate::tests::object_key("key");
    create_test_bucket(&cluster, &bucket);
    let data = b"scope-bound encrypted payload";
    let fixture = crate::tests::sse_s3_segment_test_fixture(data, 0);
    let mut prepared = prepared_encrypted_direct_put(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        data,
        &fixture,
    );
    let before = reservations(&map, &bucket);
    assert_eq!(before.len(), 1);
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();

    for crossed in [true, false] {
        route
            .with_direct_put_scope(Default::default(), |_, scope| {
                let generation = scope.reserve_generation().unwrap();
                let reservation_id = generation.reservation_id.clone();
                let committed = committed_test_object_encryption(
                    &fixture,
                    scope.reservation_proof(),
                    &bucket,
                    &key,
                    generation.generation_id(),
                );
                let authority = generation
                    .bind_encryption(&fixture.encryption)
                    .bind_standard_segment(fixture.logical_size)
                    .bind_committed_encryption(&committed)
                    .unwrap();
                prepared.encryption = committed;
                if !crossed {
                    prepared.bucket_write_reservation = scope.reservation_proof().clone();
                }
                let payload = scope
                    .write_payload(authority, &fixture.storage_bytes)
                    .unwrap();
                let identity = direct_payload_test_identity(&payload);
                assert_direct_payload_shards_exist(&cluster, identity);
                let result = scope.commit(payload, &prepared, |_| Ok::<(), ()>(()));
                if crossed {
                    let error = result.unwrap_err();
                    assert_eq!(error.diagnostic_cause_label(), "invalid_request");
                    assert_direct_payload_staging_cleaned(
                        &map,
                        &cluster,
                        &bucket,
                        &key,
                        &reservation_id,
                        identity,
                    );
                    assert_eq!(
                        reservations(&map, &bucket).len(),
                        2,
                        "rejection must not release either proof before callback returns"
                    );
                } else {
                    let outcome = result.unwrap().unwrap();
                    assert_eq!(outcome.live_size, data.len() as u64);
                    assert_direct_payload_shards_exist(&cluster, identity);
                    let error = scope.reserve_generation().unwrap_err();
                    assert_eq!(error.diagnostic_cause_label(), "invalid_request");
                }
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            reservations(&map, &bucket),
            before,
            "foreign proof must remain byte-for-byte unchanged"
        );
    }
}

#[test]
fn direct_put_scope_rejects_crossed_generation_and_payload_for_same_object() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("scope-crossing");
    let key = crate::tests::object_key("key");
    create_test_bucket(&cluster, &bucket);
    let fixture = crate::tests::sse_s3_segment_test_fixture(b"crossed scopes", 0);
    let mut prepared = prepared_encrypted_direct_put(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        b"crossed scopes",
        &fixture,
    );
    let before = reservations(&map, &bucket);
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    for cross_generation in [true, false] {
        route
            .with_direct_put_scope(Default::default(), |_, outer| {
                route
                    .with_direct_put_scope(Default::default(), |_, inner| {
                        let both = reservations(&map, &bucket);
                        assert_eq!(both.len(), 3);
                        let generation = outer.reserve_generation().unwrap();
                        if cross_generation {
                            let authority = generation
                                .bind_encryption(&fixture.encryption)
                                .bind_standard_segment(fixture.logical_size);
                            let error = inner
                                .write_payload(authority, &fixture.storage_bytes)
                                .unwrap_err();
                            assert_eq!(error.diagnostic_cause_label(), "invalid_request");
                            assert_eq!(reservations(&map, &bucket), both);
                            return Ok::<(), ()>(());
                        }
                        let reservation_id = generation.reservation_id.clone();
                        prepared.encryption = committed_test_object_encryption(
                            &fixture,
                            outer.reservation_proof(),
                            &bucket,
                            &key,
                            generation.generation_id(),
                        );
                        prepared.bucket_write_reservation = inner.reservation_proof().clone();
                        let authority = generation
                            .bind_encryption(&fixture.encryption)
                            .bind_standard_segment(fixture.logical_size)
                            .bind_committed_encryption(&prepared.encryption)
                            .unwrap();
                        let payload = outer
                            .write_payload(authority, &fixture.storage_bytes)
                            .unwrap();
                        let identity = direct_payload_test_identity(&payload);
                        assert_direct_payload_shards_exist(&cluster, identity);
                        let error = inner
                            .commit(payload, &prepared, |_| Ok::<(), ()>(()))
                            .unwrap_err();
                        assert_eq!(error.diagnostic_cause_label(), "invalid_request");
                        assert_direct_payload_staging_cleaned(
                            &map,
                            &cluster,
                            &bucket,
                            &key,
                            &reservation_id,
                            identity,
                        );
                        assert_eq!(reservations(&map, &bucket), both);
                        Ok::<(), ()>(())
                    })
                    .unwrap()
                    .unwrap();
                assert_eq!(reservations(&map, &bucket).len(), 2);
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
        assert_eq!(reservations(&map, &bucket), before);
    }
}

#[test]
fn direct_put_scope_preserves_terminal_proof_until_trailing_recovery() {
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
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("scope-recovery");
    let key = crate::tests::object_key("key");
    create_test_bucket(&cluster, &bucket);
    let data = b"terminal publication hands proof to recovery";
    let fixture = crate::tests::sse_s3_segment_test_fixture(data, 0);
    let mut prepared = prepared_encrypted_direct_put(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        data,
        &fixture,
    );
    let before = reservations(&map, &bucket);
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission.active_put_object_route(&bucket, &key).unwrap();
    let _serial = lock_metadata_command_apply_hook_test();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_hook = Arc::clone(&attempts);
    let hook_bucket = bucket.clone();
    let hook = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if matches!(command.payload(), MetadataCommandPayload::CommitDirectPutObject(commit)
            if commit.object.bucket == hook_bucket && node_id == NodeId::new(2))
            {
                attempts_hook.fetch_add(1, Ordering::SeqCst);
                return Err(StoreError::StorageRpc {
                    node_id: node_id.as_u32(),
                    operation: "apply metadata command",
                    failure: crate::storage_rpc::StorageRpcErrorCode::MetadataCommandContention,
                    detail: crate::error::StorageNodeFailureDetail::new(
                        "scope trailing replica unavailable",
                    ),
                });
            }
            Ok(())
        },
    ));
    let owned = route
        .with_direct_put_scope(Default::default(), |_, scope| {
            let generation = scope.reserve_generation().unwrap();
            prepared.encryption = committed_test_object_encryption(
                &fixture,
                scope.reservation_proof(),
                &bucket,
                &key,
                generation.generation_id(),
            );
            prepared.bucket_write_reservation = scope.reservation_proof().clone();
            let authority = generation
                .bind_encryption(&fixture.encryption)
                .bind_standard_segment(fixture.logical_size)
                .bind_committed_encryption(&prepared.encryption)
                .unwrap();
            let payload = scope
                .write_payload(authority, &fixture.storage_bytes)
                .unwrap();
            scope
                .commit(payload, &prepared, |_| Ok::<(), ()>(()))
                .unwrap()
                .unwrap();
            Ok::<_, ()>(scope.reservation_proof().clone())
        })
        .unwrap()
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    let after = reservations(&map, &bucket);
    assert_eq!(
        after.len(),
        2,
        "wrapper must not release a command-owned proof"
    );
    assert!(after
        .iter()
        .any(|record| record.reservation_id == owned.reservation_id));
    assert!(after.contains(&before[0]));
    let pending = pending_metadata_command_for_test(&map, PgId::new(0), &bucket).unwrap();
    drop(hook);
    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_route(
                PgId::new(0),
                &pending,
                &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert_eq!(reservations(&map, &bucket), before);
    assert!(pending_metadata_command_for_test(&map, PgId::new(0), &bucket).is_none());
}
