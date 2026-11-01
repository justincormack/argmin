// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_client::ShardAckNodeClient;

#[test]
fn unix_shard_ack_rechecks_captured_route_inside_transaction_after_renewal() {
    for occupied in [false, true] {
        let _time = crate::clock::test_time_override_guard(1_000);
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let node = server.test_storage_node();
        let key = test_shard_key(0);
        let next = ShardKey::new(&[0x62; 16], 1, 0);
        let canary = ShardKey::new(&[0x63; 16], 1, 0);
        // Files are already durable; this RPC independently publishes ack rows.
        let ack = node.write_shard_file(0, &key, b"first shard").unwrap();
        let next_ack = node.write_shard_file(0, &next, b"second shard").unwrap();
        let canary_ack = {
            let pg = node.get_pg(0).unwrap();
            if occupied {
                pg.register_written_shard(&key, ack).unwrap();
            }
            pg.write_shard(&canary, b"independent shard").unwrap()
        };
        let ran = Arc::new(AtomicBool::new(false));
        let serving = {
            let server = Arc::clone(&server);
            let hook_server = Arc::clone(&server);
            let ran = Arc::clone(&ran);
            let mut renewed = config.clone();
            renewed.route_map_validity = RouteMapValidity::until_ms(20_000).unwrap();
            thread::spawn(move || {
                let time = crate::clock::test_time_override_guard(1_000);
                let control = time.control();
                PgStore::test_with_before_shard_ack_effect_hook(
                    move |in_transaction| {
                        assert!(in_transaction);
                        hook_server.install_control_plane_runtime_config(renewed).unwrap();
                        control.set(4_500);
                        ran.store(true, Ordering::SeqCst);
                    },
                    || server.accept_one().unwrap(),
                );
                // Fresh authority on the renewed map must allow the same batch.
                server.accept_one().unwrap();
                server.accept_one().unwrap();
            })
        };
        let client = UnixStorageNodeClient::new(config.node_id, config.cluster_epoch, config.socket_path.clone());
        let route = client.open_shard_ack_route(config.cluster_epoch, DataPgId::new_for_test(PgId::new(0))).unwrap();
        let batch = [(&key, ack), (&next, next_ack)];
        let error = route.register_shard_acks(&batch).unwrap_err();
        assert!(ran.load(Ordering::SeqCst));
        assert!(matches!(error, StoreError::StorageRpc { failure: StorageRpcErrorCode::StaleShardLocation, .. }), "{error:?}");
        {
            let pg = node.get_pg(0).unwrap();
            if occupied {
                pg.validate_written_shard_ack(&key, ack).unwrap();
            } else {
                assert!(matches!(pg.stat_shard(&key), Err(StoreError::NotFound)));
            }
            assert!(matches!(pg.stat_shard(&next), Err(StoreError::NotFound)));
            pg.validate_written_shard_ack(&canary, canary_ack).unwrap();
        }
        assert_eq!(node.read_shard_file(0, &key).unwrap(), b"first shard");
        assert_eq!(node.read_shard_file(0, &next).unwrap(), b"second shard");
        assert_eq!(node.read_shard_file(0, &canary).unwrap(), b"independent shard");
        route.register_shard_acks(&batch).unwrap();
        route.register_shard_acks(&batch).unwrap();
        serving.join().unwrap();
        let pg = node.get_pg(0).unwrap();
        for (key, ack) in batch {
            pg.validate_written_shard_ack(key, ack).unwrap();
        }
    }
}
