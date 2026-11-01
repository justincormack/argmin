// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_client::BucketWriteReservationNodeClient;
use crate::node_runtime::pg_store::HeartbeatEffect;

fn seeded_heartbeat_fixture(
    node: &Arc<SharedStorageNode>,
    config: &StorageNodeProcessConfig,
) -> CreationFixture {
    let mut fixture = CreationFixture::new(node, false);
    {
        let pg = node.get_pg(0).unwrap();
        create_probe_bucket_direct(&pg, &fixture.request.bucket);
        for id in ["canary", "target"] {
            let record = pg
                .acquire_durable_bucket_write_reservation(DurableBucketWriteReservationAcquire {
                    name: &fixture.request.bucket,
                    reservation_id: id,
                    owner_token: "owner",
                    cluster_epoch: config.cluster_epoch,
                    operation_kind: PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                    created_at: 1_000,
                    lease_deadline: 20_000,
                    target_context: Some(fixture.request.key.as_str()),
                })
                .unwrap();
            if id == "target" {
                fixture.proof = BucketWriteReservationProof::from(&record);
            }
        }
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    fixture.seed_session(node, config);
    fixture
}

#[test]
fn embedded_heartbeat_checks_deadline_inside_both_write_transactions() {
    heartbeat_deadline_matrix(false);
}

#[test]
fn unix_heartbeat_checks_intersected_deadline_inside_both_write_transactions() {
    heartbeat_deadline_matrix(true);
}

fn heartbeat_deadline_matrix(remote: bool) {
    for effect in [
        HeartbeatEffect::BucketReservation,
        HeartbeatEffect::StreamProof,
    ] {
        for (server_first, absent_client) in [(false, false), (true, false), (true, true)] {
            if !remote && server_first {
                continue;
            }
            let time = crate::clock::test_time_override_guard(1_000);
            let tmp = test_util::tempdir();
            let mut config = test_config(&tmp);
            config.default_ec_shape = EcShape { k: 1, m: 0 };
            config.route_map_validity =
                RouteMapValidity::until_ms(if server_first { 5_000 } else { 10_000 }).unwrap();
            private_socket_dir(config.socket_path.parent().unwrap());
            let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
            let node = server.test_storage_node();
            let fixture = seeded_heartbeat_fixture(&node, &config);
            let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
            let bucket_pg = BucketPgId::new_for_test(PgId::new(0));
            let mut renewed_proof = fixture.proof.clone();
            // Model the actual two-hop protocol: the bucket renewal has already
            // committed before the object-PG proof update is attempted.
            if effect == HeartbeatEffect::StreamProof {
                let record = local
                    .open_bucket_write_reservation_route(
                        config.cluster_epoch,
                        bucket_pg,
                        &fixture.request.bucket,
                    )
                    .unwrap()
                    .heartbeat_durable_bucket_write_reservation_with_effect_fence(
                        &fixture.proof,
                        30_000,
                        AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                    )
                    .unwrap();
                renewed_proof = BucketWriteReservationProof::from(&record);
            }
            let observe = || {
                let pg = node.get_pg(0).unwrap();
                (
                    pg.metadata_command_checkpoint(config.node_id.as_u32(), config.cluster_epoch)
                        .unwrap(),
                    pg.pending_metadata_command_envelope(
                        config.node_id.as_u32(),
                        config.cluster_epoch,
                    )
                    .unwrap(),
                    pg.durable_bucket_write_reservations(&fixture.request.bucket)
                        .unwrap(),
                    pg.get_stream_upload(&fixture.request.session_id).unwrap(),
                )
            };
            let before = observe();
            let fence = if absent_client {
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch)
            } else {
                AdmittedRouteEffectFence::bounded(
                    config.cluster_epoch,
                    if server_first { 10_000 } else { 5_000 },
                    if server_first { 9_000 } else { 4_000 },
                )
            };
            let mut renewed_config = config.clone();
            renewed_config.route_map_validity = RouteMapValidity::until_ms(20_000).unwrap();
            let ran = Arc::new(AtomicBool::new(false));
            let serving = if remote {
                let server = Arc::clone(&server);
                let hook_server = Arc::clone(&server);
                let renewed_config = renewed_config.clone();
                let ran = Arc::clone(&ran);
                Some(thread::spawn(move || {
                    let time = crate::clock::test_time_override_guard(1_000);
                    let control = time.control();
                    PgStore::test_with_before_heartbeat_effect_hook(
                        move |actual, in_transaction| {
                            assert_eq!(actual, effect);
                            assert!(in_transaction);
                            hook_server
                                .install_control_plane_runtime_config(renewed_config)
                                .unwrap();
                            control.set(4_500);
                            ran.store(true, Ordering::SeqCst);
                        },
                        || server.accept_one().unwrap(),
                    );
                    // Exactly one failed request and one freshly admitted retry.
                    server.accept_one().unwrap();
                }))
            } else {
                None
            };
            let unix = UnixStorageNodeClient::new(
                config.node_id,
                config.cluster_epoch,
                config.socket_path.clone(),
            )
            .with_pg_topology(Arc::new(crate::PgTopology::new(&config.pg_ids).unwrap()));
            let bucket_client: &dyn BucketWriteReservationNodeClient =
                if remote { &unix } else { &local };
            let object_client: &dyn ObjectMutationMetadataNodeClient =
                if remote { &unix } else { &local };
            let bucket_route = bucket_client
                .open_bucket_write_reservation_route(
                    config.cluster_epoch,
                    bucket_pg,
                    &fixture.request.bucket,
                )
                .unwrap();
            let stream_route = object_client
                .open_stream_upload_session_metadata_route(
                    config.cluster_epoch,
                    ObjectMetadataPgId::new_for_test(PgId::new(0)),
                    &fixture.request.bucket,
                    &fixture.request.key,
                    &fixture.request.session_id,
                )
                .unwrap();
            let attempt = |fence| -> Result<(), StoreError> {
                match effect {
                    HeartbeatEffect::BucketReservation => bucket_route
                        .heartbeat_durable_bucket_write_reservation_with_effect_fence(
                            &fixture.proof,
                            30_000,
                            fence,
                        )
                        .map(|_| ())
                        .map_err(|e| match e {
                            BucketSnapshotLoadError::Store(e) => e,
                            e => panic!("unexpected heartbeat error: {e:?}"),
                        }),
                    HeartbeatEffect::StreamProof => stream_route
                        .update_put_bucket_write_reservation(&fixture.proof, &renewed_proof, fence)
                        .map_err(|e| match e {
                            ObjectPgActionError::Store(e) => e,
                            e => panic!("unexpected proof-update error: {e:?}"),
                        }),
                }
            };
            let error = if remote {
                attempt(fence).expect_err(&format!(
                    "{effect:?} server_first={server_first} absent_client={absent_client}"
                ))
            } else {
                let control = time.control();
                let ran = Arc::clone(&ran);
                let server = Arc::clone(&server);
                PgStore::test_with_before_heartbeat_effect_hook(
                    move |actual, in_transaction| {
                        assert_eq!(actual, effect);
                        assert!(in_transaction);
                        server
                            .install_control_plane_runtime_config(renewed_config)
                            .unwrap();
                        control.set(4_500);
                        ran.store(true, Ordering::SeqCst);
                    },
                    || attempt(fence),
                )
                .unwrap_err()
            };
            assert!(ran.load(Ordering::SeqCst));
            assert!(if remote {
                matches!(error, StoreError::StorageRpc { failure: StorageRpcErrorCode::StaleShardLocation, .. })
            } else {
                matches!(error, StoreError::RouteMapExpired { valid_until_ms: 5_000, now_ms: 4_500, .. })
            }, "{effect:?} remote={remote} server_first={server_first} absent_client={absent_client}: {error:?}");
            assert_eq!(
                observe(),
                before,
                "failed heartbeat must preserve both complete records"
            );
            attempt(AdmittedRouteEffectFence::bounded(
                config.cluster_epoch,
                20_000,
                19_000,
            ))
            .unwrap();
            let after = observe();
            assert_ne!(after, before);
            assert_eq!(after.2[0], before.2[0], "independent canary reservation");
            match effect {
                HeartbeatEffect::BucketReservation => {
                    assert_eq!(after.2[1].lease_deadline, 30_000);
                    assert_eq!(after.3, before.3, "first hop does not update session");
                }
                HeartbeatEffect::StreamProof => {
                    assert_eq!(
                        after.2, before.2,
                        "second hop preserves committed first hop"
                    );
                    let mut expected = before.3.clone();
                    expected.bucket_write_reservation = Some(renewed_proof);
                    assert_eq!(after.3, expected);
                }
            }
            if let Some(serving) = serving {
                serving.join().unwrap();
            }
        }
    }
}

#[test]
fn unix_heartbeat_preserves_old_proof_epoch_across_both_mutations() {
    let _time = crate::clock::test_time_override_guard(1_000);
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.default_ec_shape = EcShape { k: 1, m: 0 };
    config.route_map_validity = RouteMapValidity::until_ms(10_000).unwrap();
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let node = server.test_storage_node();
    let fixture = seeded_heartbeat_fixture(&node, &config);
    config.historical_pg_routes.extend(config.pg_routes.clone());
    config.cluster_epoch = ClusterEpoch::new(config.cluster_epoch.get() + 1).unwrap();
    for route in &mut config.pg_routes {
        route.cluster_epoch = config.cluster_epoch;
    }
    server
        .install_control_plane_runtime_config(config.clone())
        .unwrap();
    let serving = {
        let server = Arc::clone(&server);
        thread::spawn(move || {
            let _time = crate::clock::test_time_override_guard(1_000);
            server.accept_one().unwrap();
            server.accept_one().unwrap();
        })
    };
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(crate::PgTopology::new(&config.pg_ids).unwrap()));
    let fence = AdmittedRouteEffectFence::bounded(config.cluster_epoch, 10_000, 9_000);
    let renewed = client
        .open_bucket_write_reservation_route(
            config.cluster_epoch,
            BucketPgId::new_for_test(PgId::new(0)),
            &fixture.request.bucket,
        )
        .unwrap()
        .heartbeat_durable_bucket_write_reservation_with_effect_fence(&fixture.proof, 30_000, fence)
        .unwrap();
    let renewed = BucketWriteReservationProof::from(&renewed);
    client
        .open_stream_upload_session_metadata_route(
            config.cluster_epoch,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &fixture.request.bucket,
            &fixture.request.key,
            &fixture.request.session_id,
        )
        .unwrap()
        .update_put_bucket_write_reservation(&fixture.proof, &renewed, fence)
        .unwrap();
    serving.join().unwrap();
    assert_eq!(renewed.cluster_epoch, fixture.proof.cluster_epoch);
    assert_ne!(renewed.cluster_epoch, config.cluster_epoch);
    assert_eq!(renewed.lease_deadline, 30_000);
    assert!(renewed.has_same_stable_identity(&fixture.proof));
    let pg = node.get_pg(0).unwrap();
    assert_eq!(
        pg.get_stream_upload(&fixture.request.session_id)
            .unwrap()
            .bucket_write_reservation,
        Some(renewed.clone())
    );
    assert_eq!(
        BucketWriteReservationProof::from(
            &pg.durable_bucket_write_reservation(
                &fixture.request.bucket,
                &fixture.proof.reservation_id
            )
            .unwrap()
            .unwrap()
        ),
        renewed
    );
}
