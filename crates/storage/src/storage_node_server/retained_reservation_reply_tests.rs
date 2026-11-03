// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn authenticated_retained_reservation_release_response_failure(tcp: bool) {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let credential = storage_rpc_auth_test_credential(ControlPlaneAuthPrincipal::Frontend {
        instance_id: "frontend-1".to_owned(),
    });
    let prepared = PreparedStorageNodeServer::new(config.clone())
        .with_rpc_auth(storage_rpc_server_auth(&credential));
    let server = Arc::new(
        if tcp {
            prepared.with_rpc_listeners(vec![StorageNodeRpcListenerConfig::tls_tcp_with_config(
                "127.0.0.1:0".parse().unwrap(),
                storage_rpc_tls_server_config(),
            )])
        } else {
            prepared
        }
        .bind()
        .unwrap(),
    );
    let endpoint = if tcp {
        let address = server.tcp_listener_addr_for_test();
        StorageRpcClientEndpoint::tcp_with_config(
            format!("tcp://localhost:{}", address.port()),
            vec![address],
            "localhost",
            storage_rpc_tls_client_config(),
        )
        .unwrap()
    } else {
        StorageRpcClientEndpoint::unix(config.socket_path.clone())
    };
    let bucket = crate::tests::bucket_name("retained-release-reply");
    let key = crate::tests::object_key("same-subject");
    let pg_id = server._node.bucket_metadata_pg_for(&bucket);
    let pg = server._node.get_pg(pg_id.pg_id().get()).unwrap();
    create_probe_bucket_direct(&pg, &bucket);
    let now = crate::clock::current_time_millis();
    let acquire = |reservation_id, owner_token| {
        PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id,
                owner_token,
                cluster_epoch: config.cluster_epoch,
                operation_kind:
                    crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                created_at: now,
                lease_deadline: now.saturating_add(60_000),
                target_context: Some(key.as_str()),
            },
        )
        .unwrap()
    };
    let released = acquire("released-reservation", "released-owner");
    let canary = acquire("canary-reservation", "canary-owner");
    let proof = BucketWriteReservationProof::from(&released);
    pg.refresh_metadata_command_state_digest().unwrap();
    let initial = PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket).unwrap();
    assert_eq!(initial.len(), 2);
    assert!(initial.contains(&released));
    assert!(initial.contains(&canary));
    let initial_log = pg.metadata_command_replica_state().unwrap();
    drop(pg);

    let replies = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&replies);
    server.set_response_envelope_test_hook(Arc::new(move |kind, envelope| {
        if kind == StorageRpcMessageKind::ProofRelease
            && observed.fetch_add(1, Ordering::SeqCst) == 0
        {
            // Dispatch has committed the deletion. Invalidate its authenticated
            // reply, so the client cannot trust that the release completed.
            *envelope.last_mut().unwrap() ^= 1;
        }
    }));
    let auth = storage_rpc_client_auth(credential, 9);
    let new_client = || {
        UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            config.node_id,
            config.cluster_epoch,
            endpoint.clone(),
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(Arc::clone(&auth)),
        )
    };
    let accepting = Arc::clone(&server);
    let join = thread::spawn(move || accepting.accept_one());
    let client = new_client();
    let route = client
        .open_retained_bucket_write_reservation_route(pg_id, &bucket)
        .unwrap();
    let error = route
        .release_metadata_command_bucket_write_reservation(&proof)
        .unwrap_err();
    assert!(
        matches!(
            &error,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc { failure, detail, .. })
                if *failure == StorageRpcErrorCode::PayloadDecode
                    && detail.as_str().starts_with("storage RPC authentication failed:")
        ),
        "{error:?}"
    );
    drop(route);
    drop(client);
    assert!(join.join().unwrap().is_ok());
    assert_eq!(replies.load(Ordering::SeqCst), 1, "no hidden retry");

    let assert_released = || {
        let pg = server._node.get_pg(pg_id.pg_id().get()).unwrap();
        assert_eq!(
            PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket).unwrap(),
            vec![canary.clone()],
            "only the exact reservation may be released"
        );
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, initial_log.applied_log_index);
        assert_eq!(state.applied_log_hash, initial_log.applied_log_hash);
    };
    assert_released();

    // Discard the failed connection and all client-side route state. Exact
    // release on a fresh authenticated connection confirms the absent record.
    let accepting = Arc::clone(&server);
    let join = thread::spawn(move || accepting.accept_one());
    let client = new_client();
    let route = client
        .open_retained_bucket_write_reservation_route(pg_id, &bucket)
        .unwrap();
    for _ in 0..2 {
        route
            .release_metadata_command_bucket_write_reservation(&proof)
            .unwrap();
        assert_released();
    }
    drop(route);
    drop(client);
    assert!(join.join().unwrap().is_ok());
    assert_eq!(replies.load(Ordering::SeqCst), 3);
}

#[test]
fn authenticated_unix_retained_reservation_release_response_failure() {
    authenticated_retained_reservation_release_response_failure(false);
}

#[test]
fn authenticated_tls_tcp_retained_reservation_release_response_failure() {
    authenticated_retained_reservation_release_response_failure(true);
}
