// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_client::MetadataCommandNodeClient;
use crate::node_runtime::client_facade::LocalStorageNodeClient;

#[derive(Clone, Copy, Debug)]
enum InsertApi {
    Generic,
    GenericUntil,
    ClassifiedUntil,
    BucketControl,
    BucketControlUntil,
}

impl InsertApi {
    fn command(self) -> MetadataCommandEnvelope {
        match self {
            Self::Generic | Self::GenericUntil | Self::ClassifiedUntil => {
                test_metadata_command(0, 1)
            }
            Self::BucketControl | Self::BucketControlUntil => {
                test_bucket_control_metadata_command(0, 1)
            }
        }
    }

    fn insert(
        self,
        client: &dyn MetadataCommandNodeClient,
        command: &MetadataCommandEnvelope,
        fence: AdmittedRouteEffectFence,
    ) -> Result<(), StoreError> {
        let pg = command.id().pg_id();
        let bucket = command.bucket_name();
        let deadline = Instant::now() + Duration::from_secs(30);
        match self {
            Self::Generic => client.try_insert_pending_metadata_command_slot_with_effect_fence(
                pg,
                command,
                Some(bucket),
                fence,
            ),
            Self::GenericUntil => client
                .try_insert_pending_metadata_command_slot_with_effect_fence_until(
                    pg,
                    command,
                    Some(bucket),
                    fence,
                    deadline,
                ),
            Self::ClassifiedUntil => client
                .try_insert_pending_metadata_command_slot_with_effect_fence_classified_until(
                    pg,
                    command,
                    Some(bucket),
                    fence,
                    deadline,
                )
                .map_err(|error| {
                    assert_eq!(
                        error.kind(),
                        MetadataCommandApplyErrorKind::Definitive,
                        "{error:?}"
                    );
                    error.into_source()
                }),
            Self::BucketControl => client
                .try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence(
                    pg, command, bucket, fence,
                )
                .map(|_| ()),
            Self::BucketControlUntil => client
                .try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence_until(
                    pg, command, bucket, fence, deadline,
                )
                .map(|_| ()),
        }
    }
}

#[test]
fn embedded_pending_slot_checks_deadline_inside_sql_transaction() {
    pending_deadline_matrix(false);
}

#[test]
fn unix_pending_slot_checks_intersected_deadline_inside_sql_transaction() {
    pending_deadline_matrix(true);
}

fn pending_deadline_matrix(remote: bool) {
    for api in [
        InsertApi::Generic,
        InsertApi::GenericUntil,
        InsertApi::ClassifiedUntil,
        InsertApi::BucketControl,
        InsertApi::BucketControlUntil,
    ] {
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
            let command = api.command();
            let bucket = command.bucket_name();
            {
                let pg = node.get_pg(0).unwrap();
                create_probe_bucket_direct(&pg, bucket);
                pg.acquire_durable_bucket_write_reservation(DurableBucketWriteReservationAcquire {
                    name: bucket,
                    reservation_id: "pending-effect-canary",
                    owner_token: "canary-owner",
                    cluster_epoch: config.cluster_epoch,
                    operation_kind: crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                    created_at: 1_000,
                    lease_deadline: 20_000,
                    target_context: Some("object"),
                }).unwrap();
                pg.refresh_metadata_command_state_digest().unwrap();
            }
            let observe = || {
                let pg = node.get_pg(0).unwrap();
                (
                    pg.metadata_command_replica_state().unwrap(),
                    pg.pending_metadata_command_slot(config.node_id.as_u32(), config.cluster_epoch)
                        .unwrap(),
                    pg.durable_bucket_write_reservations(bucket).unwrap(),
                )
            };
            let before = observe();
            let before_checkpoint = node
                .get_pg(0)
                .unwrap()
                .metadata_command_checkpoint(config.node_id.as_u32(), config.cluster_epoch)
                .unwrap();
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
            let ran = Arc::new(AtomicBool::new(false));
            let serving = if remote {
                let server = Arc::clone(&server);
                let hook_server = Arc::clone(&server);
                let ran = Arc::clone(&ran);
                let renewed = renewed.clone();
                Some(thread::spawn(move || {
                    let time = crate::clock::test_time_override_guard(1_000);
                    let control = time.control();
                    PgStore::test_with_before_pending_slot_effect_hook(
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
                    // Fresh insertion, then exact retry, each on a new RPC connection.
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
            );
            let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
            let client: &dyn MetadataCommandNodeClient = if remote { &unix } else { &local };
            let attempt = |fence| api.insert(client, &command, fence);
            let error = if remote {
                attempt(fence).expect_err(&format!(
                    "{api:?}: server_first={server_first} absent_client={absent_client}"
                ))
            } else {
                let control = time.control();
                let ran = Arc::clone(&ran);
                let server = Arc::clone(&server);
                PgStore::test_with_before_pending_slot_effect_hook(
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
            assert!(ran.load(Ordering::SeqCst), "must reach SQLite admission");
            assert!(if remote {
                matches!(error, StoreError::StorageRpc { failure: StorageRpcErrorCode::StaleShardLocation, .. })
            } else {
                matches!(error, StoreError::RouteMapExpired { valid_until_ms: 5_000, now_ms: 4_500, .. })
            }, "{api:?} remote={remote} server_first={server_first} absent_client={absent_client}: {error:?}");
            assert_eq!(
                observe(),
                before,
                "expiry must leave checkpoint, slot and reservations exact"
            );
            assert_eq!(
                node.get_pg(0)
                    .unwrap()
                    .metadata_command_checkpoint(config.node_id.as_u32(), config.cluster_epoch)
                    .unwrap(),
                before_checkpoint
            );
            let fresh = AdmittedRouteEffectFence::bounded(config.cluster_epoch, 20_000, 19_000);
            attempt(fresh).unwrap();
            let installed = observe();
            let slot = installed.1.as_ref().unwrap();
            assert_eq!(slot.command_bytes, command.command_bytes());
            assert_eq!(slot.id, command.id());
            assert!(!slot.publication_started);
            assert_eq!(installed.0, before.0);
            assert_eq!(installed.2, before.2);
            attempt(fresh).unwrap();
            assert_eq!(
                observe(),
                installed,
                "exact retry must preserve all slot state"
            );
            if let Some(serving) = serving {
                serving.join().unwrap();
            }
        }
    }
}
