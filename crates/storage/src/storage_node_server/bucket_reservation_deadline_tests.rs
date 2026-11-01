// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_client::BucketWriteReservationNodeClient;
use crate::node_runtime::client_facade::LocalStorageNodeClient;

#[test]
fn embedded_bucket_reservation_checks_deadline_inside_sql_transaction() {
    reservation_deadline_matrix(false);
}

#[test]
fn unix_bucket_reservation_checks_intersected_deadline_inside_sql_transaction() {
    reservation_deadline_matrix(true);
}

fn reservation_deadline_matrix(remote: bool) {
    for (server_first, absent_client) in [(false, false), (true, false), (true, true)] {
        if !remote && server_first {
            continue;
        }
        let time = crate::clock::test_time_override_guard(1_000);
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity =
            RouteMapValidity::until_ms(if server_first { 5_000 } else { 10_000 }).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let node = server.test_storage_node();
        let bucket = crate::tests::bucket_name("reservation-effect-deadline");
        let acquire = |reservation_id| DurableBucketWriteReservationAcquire {
            name: &bucket,
            reservation_id,
            owner_token: "owner-token",
            cluster_epoch: config.cluster_epoch,
            operation_kind:
                crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            created_at: 1_000,
            lease_deadline: 20_000,
            target_context: Some("key"),
        };
        let canary = {
            let pg = node.get_pg(0).unwrap();
            create_probe_bucket_direct(&pg, &bucket);
            let record = pg
                .acquire_durable_bucket_write_reservation(acquire("canary"))
                .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            record
        };
        let observe = || {
            let pg = node.get_pg(0).unwrap();
            (
                pg.metadata_command_checkpoint(config.node_id.as_u32(), config.cluster_epoch)
                    .unwrap(),
                pg.pending_metadata_command_envelope(config.node_id.as_u32(), config.cluster_epoch)
                    .unwrap(),
                pg.durable_bucket_write_reservations(&bucket).unwrap(),
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
        let mut renewed = config.clone();
        renewed.route_map_validity = RouteMapValidity::until_ms(20_000).unwrap();
        let hook_ran = Arc::new(AtomicBool::new(false));
        let serving = if remote {
            let server = Arc::clone(&server);
            let hook_server = Arc::clone(&server);
            let renewed = renewed.clone();
            let ran = Arc::clone(&hook_ran);
            Some(thread::spawn(move || {
                let time = crate::clock::test_time_override_guard(1_000);
                let control = time.control();
                PgStore::test_with_before_bucket_reservation_effect_hook(
                    move |in_transaction| {
                        assert!(in_transaction);
                        hook_server
                            .install_control_plane_runtime_config(renewed)
                            .unwrap();
                        control.set(4_500);
                        ran.store(true, Ordering::SeqCst);
                    },
                    || server.accept_one().unwrap(),
                );
                // Fresh admission, followed by exact idempotent retry.
                server.accept_one().unwrap();
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
        let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
        let client: &dyn BucketWriteReservationNodeClient = if remote { &unix } else { &local };
        let route = client
            .open_bucket_write_reservation_route(
                config.cluster_epoch,
                BucketPgId::new_for_test(PgId::new(0)),
                &bucket,
            )
            .unwrap();
        let attempt = |fence| {
            route.acquire_durable_bucket_write_reservation_with_effect_fence(
                acquire("target"),
                fence,
            )
        };
        let error = if remote {
            attempt(fence).expect_err(&format!(
                "server_first={server_first} absent_client={absent_client}"
            ))
        } else {
            let control = time.control();
            let ran = Arc::clone(&hook_ran);
            let server = Arc::clone(&server);
            PgStore::test_with_before_bucket_reservation_effect_hook(
                move |in_transaction| {
                    assert!(in_transaction);
                    server
                        .install_control_plane_runtime_config(renewed)
                        .unwrap();
                    control.set(4_500);
                    ran.store(true, Ordering::SeqCst);
                },
                || attempt(fence),
            )
            .unwrap_err()
        };
        assert!(
            hook_ran.load(Ordering::SeqCst),
            "must reach SQLite write admission before expiry"
        );
        assert!(
            if remote {
                matches!(
                    &error,
                    BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                        failure: StorageRpcErrorCode::StaleShardLocation,
                        ..
                    })
                )
            } else {
                matches!(
                    &error,
                    BucketSnapshotLoadError::Store(StoreError::RouteMapExpired {
                        valid_until_ms: 5_000,
                        now_ms: 4_500,
                        ..
                    })
                )
            },
            "remote={remote} server_first={server_first} absent_client={absent_client}: {error:?}"
        );
        assert_eq!(
            observe(),
            before,
            "expiry must preserve complete reservation and checkpoint state"
        );
        let fresh = AdmittedRouteEffectFence::bounded(config.cluster_epoch, 20_000, 19_000);
        let record = attempt(fresh).unwrap();
        assert_eq!(record.reservation_id, "target");
        let after = observe();
        assert_eq!(attempt(fresh).unwrap(), record);
        assert_eq!(
            observe(),
            after,
            "exact retry must not create another reservation"
        );
        assert_eq!(
            node.get_pg(0)
                .unwrap()
                .durable_bucket_write_reservation(&bucket, "canary")
                .unwrap(),
            Some(canary)
        );
        if let Some(serving) = serving {
            serving.join().unwrap();
        }
    }
}
