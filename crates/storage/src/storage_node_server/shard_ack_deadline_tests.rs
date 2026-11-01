// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_client::ShardAckNodeClient;

#[test]
fn unix_shard_ack_rechecks_captured_route_inside_transaction_after_renewal() {
    for (server_first, absent_client) in [(false, false), (true, false), (true, true)] {
        deadline_matrix(server_first, absent_client, false);
    }
}

#[test]
fn tls_shard_ack_rebinds_frontend_deadline_with_different_monotonic_origin() {
    for (server_first, absent_client) in [(false, false), (true, false), (true, true)] {
        deadline_matrix(server_first, absent_client, true);
    }
}

fn deadline_matrix(server_first: bool, absent_client: bool, tls: bool) {
    for occupied in [false, true] {
        let _time = crate::clock::test_time_override_guard(3_750);
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.route_map_validity = RouteMapValidity::until_ms(if server_first { 5_000 } else { 10_000 }).unwrap();
        private_socket_dir(config.socket_path.parent().unwrap());
        let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let server = Arc::new(if tls {
            PreparedStorageNodeServer::new(config.clone())
                .with_rpc_auth(storage_rpc_server_auth(&credential))
                .with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                    "127.0.0.1:0".parse().unwrap(), storage_rpc_tls_server_config(),
                )]).bind().unwrap()
        } else { StorageNodeServer::bind(config.clone()).unwrap() });
        let address = tls.then(|| server.tcp_listener_addr_for_test());
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
                let time = crate::clock::test_time_override_guard(3_750);
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
        let batch = [(&key, ack), (&next, next_ack)];
        // The frontend's monotonic clock origin deliberately differs from the
        // receiving node's. Exercise the real sender conversion, not raw RPC.
        // Stay within authentication's future-skew allowance while crossing
        // the 4,000 ms effective route cutoff. Fresh requests see the new time.
        let attempt = |fresh: bool| crate::clock::with_time_and_monotonic_override(if fresh {4_500} else {3_750}, 900_000, || {
            let client = if let Some(address) = address {
                let endpoint = StorageRpcClientEndpoint::tcp_with_config(
                    format!("tcp://localhost:{}", address.port()), vec![address], "localhost",
                    storage_rpc_tls_client_config(),
                ).unwrap();
                UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                    config.node_id, config.cluster_epoch, endpoint,
                    LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                    Some(storage_rpc_client_auth(credential.clone(), 9)),
                )
            } else { UnixStorageNodeClient::new(config.node_id, config.cluster_epoch, config.socket_path.clone()) };
            let route = client.open_shard_ack_route(config.cluster_epoch, DataPgId::new_for_test(PgId::new(0))).unwrap();
            let fence = if absent_client { AdmittedRouteEffectFence::unbounded(config.cluster_epoch) }
                else if fresh || server_first { AdmittedRouteEffectFence::bounded(config.cluster_epoch, 20_000, 918_000) }
                else { AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 900_250) };
            route.register_shard_acks_with_effect_fence(&batch, fence)
        });
        let error = attempt(false).unwrap_err();
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
        attempt(true).unwrap();
        attempt(true).unwrap();
        serving.join().unwrap();
        let pg = node.get_pg(0).unwrap();
        for (key, ack) in batch {
            pg.validate_written_shard_ack(key, ack).unwrap();
        }
    }
}

#[test]
fn embedded_shard_ack_delegates_fence_past_pg_admission() {
    use crate::node_runtime::client_facade::LocalStorageNodeClient;
    let time = crate::clock::test_time_override_guard(1_000);
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let node = server.test_storage_node();
    let key = test_shard_key(0);
    let ack = node.write_shard_file(0, &key, b"durable payload").unwrap();
    let client = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
    let route = client.open_shard_ack_route(config.cluster_epoch, DataPgId::new_for_test(PgId::new(0))).unwrap();
    let control = time.control();
    let error = PgStore::test_with_before_shard_ack_effect_hook(move |in_transaction| {
        assert!(in_transaction);
        control.set(4_500);
    }, || route.register_shard_acks_with_effect_fence(&[(&key, ack)], AdmittedRouteEffectFence::bounded(config.cluster_epoch, 5_000, 4_000))).unwrap_err();
    assert!(matches!(error, StoreError::RouteMapExpired { .. }), "{error:?}");
    assert!(matches!(node.get_pg(0).unwrap().stat_shard(&key), Err(StoreError::NotFound)));
    assert_eq!(node.read_shard_file(0, &key).unwrap(), b"durable payload");
    // Maintenance/convergence still has no frontend deadline. This does not
    // grant a fresh request permission to choose that path.
    route.register_shard_acks(&[(&key, ack)]).unwrap();
    node.get_pg(0).unwrap().validate_written_shard_ack(&key, ack).unwrap();
}
