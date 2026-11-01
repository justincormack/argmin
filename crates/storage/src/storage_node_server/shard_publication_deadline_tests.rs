// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::client_facade::LocalStorageNodeClient;

fn file(node: &SharedStorageNode, key: &ShardKey) -> Option<Vec<u8>> {
    match node.read_shard_file(0, key) {
        Ok(data) => Some(data),
        Err(StoreError::NotFound) => None,
        Err(error) => panic!("unexpected file read error: {error:?}"),
    }
}

#[test]
fn embedded_shard_publication_rechecks_captured_deadline_before_rename() {
    publication_deadline_matrix(false);
}

#[test]
fn unix_shard_publication_rechecks_intersected_deadline_before_link() {
    publication_deadline_matrix(true);
}

fn publication_deadline_matrix(remote: bool) {
    for (server_first, absent_client) in [(false, false), (true, false), (true, true)] {
        if !remote && server_first {
            continue;
        }
        for occupied in [false, true] {
            let time = crate::clock::test_time_override_guard(1_000);
            let tmp = test_util::tempdir();
            let mut config = test_config(&tmp);
            config.route_map_validity =
                RouteMapValidity::until_ms(if server_first { 5_000 } else { 10_000 }).unwrap();
            private_socket_dir(config.socket_path.parent().unwrap());
            let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
            let node = server.test_storage_node();
            let key = test_shard_key(0);
            let canary = ShardKey::new(&[0x61; 16], 99, 0);
            let payload = if remote && occupied {
                b"original shard".as_slice()
            } else {
                b"new shard".as_slice()
            };
            let (canary_ack, old_ack) = {
                let pg = node.get_pg(0).unwrap();
                (
                    pg.write_shard(&canary, b"independent shard").unwrap(),
                    occupied.then(|| pg.write_shard(&key, b"original shard").unwrap()),
                )
            };
            let before = file(&node, &key);
            let tmp_path = config.data_dir.join("pg-0000/tmp");
            assert!(std::fs::read_dir(&tmp_path).unwrap().next().is_none());
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
                let renewed = renewed.clone();
                let ran = Arc::clone(&ran);
                let tmp_path = tmp_path.clone();
                Some(thread::spawn(move || {
                    let time = crate::clock::test_time_override_guard(1_000);
                    let control = time.control();
                    PgStore::test_with_before_shard_publication_hook(
                        move || {
                            assert_eq!(std::fs::read_dir(&tmp_path).unwrap().count(), 1);
                            hook_server
                                .install_control_plane_runtime_config(renewed)
                                .unwrap();
                            control.set(4_500);
                            ran.store(true, Ordering::SeqCst);
                        },
                        || server.accept_one().unwrap(),
                    );
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
            let client: &dyn PlacedShardNodeClient = if remote { &unix } else { &local };
            let route = client
                .open_placed_shard_route(
                    crate::cluster::ShardLocation::new(
                        config.cluster_epoch,
                        DataPgId::new_for_test(PgId::new(0)),
                        key.shard_index(),
                        config.node_id,
                    ),
                    &key,
                )
                .unwrap();
            let attempt = |fence| route.write_placed_shard_with_effect_fence(payload, fence);
            let error = if remote {
                attempt(fence).expect_err(&format!(
                    "server_first={server_first} absent_client={absent_client} occupied={occupied}"
                ))
            } else {
                let control = time.control();
                let ran = Arc::clone(&ran);
                let server = Arc::clone(&server);
                let tmp_path = tmp_path.clone();
                PgStore::test_with_before_shard_publication_hook(
                    move || {
                        assert_eq!(std::fs::read_dir(&tmp_path).unwrap().count(), 1);
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
            assert!(ran.load(Ordering::SeqCst));
            assert!(
                if remote {
                    matches!(
                        error,
                        StoreError::StorageRpc {
                            failure: StorageRpcErrorCode::StaleShardLocation,
                            ..
                        }
                    )
                } else {
                    matches!(
                        error,
                        StoreError::RouteMapExpired {
                            valid_until_ms: 5_000,
                            now_ms: 4_500,
                            ..
                        }
                    )
                },
                "{error:?}"
            );
            assert_eq!(file(&node, &key), before);
            assert!(std::fs::read_dir(&tmp_path).unwrap().next().is_none());
            assert_eq!(file(&node, &canary).unwrap(), b"independent shard");
            {
                let pg = node.get_pg(0).unwrap();
                pg.validate_written_shard_ack(&canary, canary_ack).unwrap();
                if let Some(ack) = old_ack {
                    pg.validate_written_shard_ack(&key, ack).unwrap();
                } else {
                    assert!(matches!(pg.stat_shard(&key), Err(StoreError::NotFound)));
                }
            }
            let ack = attempt(AdmittedRouteEffectFence::bounded(
                config.cluster_epoch,
                20_000,
                19_000,
            ))
            .unwrap();
            assert_eq!(ack.stored_size, payload.len() as u64);
            assert_eq!(ack.crc64, checksum::crc64::checksum(payload));
            assert_eq!(file(&node, &key).unwrap(), payload);
            assert!(std::fs::read_dir(&tmp_path).unwrap().next().is_none());
            if let Some(serving) = serving {
                serving.join().unwrap();
            }
        }
    }
}
