// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Receiver-only integration: inherited receipt claims are not physical-write
//! evidence. Consuming frontend custody/remote installation remains a next slice.
use super::*;
use crate::storage_rpc::complete_stream_append_install as install;

fn prepared(session: &UnixStorageNodeMetadataCommandSession, bytes: &[u8]) -> Vec<u8> {
    let response = session
        .test_stream_append_prepare_request(bytes.to_vec())
        .unwrap()
        .unwrap();
    let request = prepare::encode_request(&prepare::decode_request(bytes).unwrap()).unwrap();
    let command = request.decode_response(&response).unwrap();
    install::encode_request(install::Request {
        preparation: bytes,
        command,
    })
    .unwrap()
}

#[test]
fn staging_remote_append_install_receiver_exact_retry_and_damaged_indexes_unix_tls() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for appended in [false, true] {
                with_request(tls, target.clone(), appended, |f, session, bytes, _| {
                    let request = prepared(session, bytes);
                    let before = f.observe();
                    let shadows = f.shadows();
                    let server = &f.remote(NodeId::new(4)).unwrap().server;
                    server.test_stream_append_install_admission(&request);
                    assert_eq!(f.observe(), before);
                    let response = session
                        .test_stream_append_install_request(request.clone())
                        .unwrap()
                        .unwrap();
                    install::require_confirmation(&request, &response).unwrap();
                    let after = f.observe();
                    assert_ne!(after[4], before[4], "positive mutation canary");
                    assert_eq!(after[..4], before[..4], "only primary slot changes");
                    assert_eq!(f.shadows(), shadows);
                    let node = f.node(NodeId::new(4));
                    let pg_id = f.context.id().pg_id().get();
                    {
                        let pg = node.get_pg(pg_id).unwrap();
                        let pending = pg
                            .inspect_complete_pending_command(4, &topology())
                            .unwrap()
                            .unwrap();
                        assert_eq!(
                            pending.id.log_index().get(),
                            f.context.id().log_index().get() + 1
                        );
                        assert!(!pending.publication_started);
                    }
                    let response = session
                        .test_stream_append_install_request(request.clone())
                        .unwrap()
                        .unwrap();
                    install::require_confirmation(&request, &response).unwrap();
                    // SQLite's INSERT .. DO NOTHING and exact side-index checks
                    // must not change the durable rows or count on exact retry.
                    assert_eq!(f.observe(), after);
                    node.get_pg(pg_id)
                        .unwrap()
                        .conn
                        .execute_batch("DELETE FROM metadata_command_pending_staging_protection")
                        .unwrap();
                    let damaged = f.observe();
                    assert!(session
                        .test_stream_append_install_request(request)
                        .unwrap()
                        .is_err());
                    assert_eq!(f.observe(), damaged, "no repair masquerading as retry");
                });
            }
        }
    }
}

#[test]
fn staging_remote_append_install_receiver_rejects_crossed_complete_commands_before_mutation() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_request(tls, target, false, |f, session, bytes, _| {
                let request = prepared(session, bytes);
                let original = install::decode_request(&request).unwrap();
                let context = ContextualCommand::decode_command(
                    original.command.command,
                    original.command.checksum,
                    &topology(),
                )
                .unwrap();
                let before = f.observe();
                for field in [
                    "payload",
                    "witness",
                    "index",
                    "cutoff",
                    "participants",
                    "issuer",
                ] {
                    let crossed = context.test_cross_remote_append_reply(field);
                    let encoded = crossed.encode_command().unwrap();
                    let request = install::encode_request(install::Request {
                        preparation: bytes,
                        command: crate::storage_rpc::complete_applied::BorrowedRequest {
                            node_id: original.command.node_id,
                            epoch: original.command.epoch,
                            pg_id: original.command.pg_id,
                            checksum: encoded.checksum_crc64(),
                            command: encoded.bytes(),
                        },
                    })
                    .unwrap();
                    let error = session
                        .test_stream_append_install_request(request)
                        .unwrap()
                        .unwrap_err();
                    assert!(
                        error.message.contains("remote append"),
                        "{field}: {error:?}"
                    );
                    assert_eq!(f.observe(), before, "{field}");
                }
                let response = session
                    .test_stream_append_install_request(request.clone())
                    .unwrap()
                    .unwrap();
                install::require_confirmation(&request, &response).unwrap();
            });
        }
    }
}

#[test]
fn staging_remote_append_install_receiver_fences_client_and_server_expiry_at_sql_effect() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for server_first in [false, true] {
                with_request(tls, target.clone(), false, |f, session, bytes, _| {
                    let server = &f.remote(NodeId::new(4)).unwrap().server;
                    let mut claims = prepare::decode_request(bytes).unwrap();
                    claims.receiver.deadline.portable_wall_valid_until_ms =
                        crate::clock::current_time_millis() + 10_000;
                    let request = prepare::encode_request(&claims).unwrap();
                    if server_first {
                        server.test_receiver_route_validity(
                            crate::clock::current_time_millis() + 5_000,
                        );
                    }
                    server.set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == Kind::StagingStreamAppendPrepare {
                            StorageNodeServer::test_expire_stream_append_at_install(server_first);
                        }
                    }));
                    let request = prepared(session, request.bytes());
                    let before = f.observe();
                    let error = session
                        .test_stream_append_install_request(request)
                        .unwrap()
                        .unwrap_err();
                    assert!(
                        error.message.contains("expired") || error.message.contains("cutoff"),
                        "{error:?}"
                    );
                    assert_eq!(f.observe(), before, "no slot/index/cursor/log mutation");
                });
            }
        }
    }
}

#[test]
fn staging_remote_append_install_receiver_lost_success_keeps_exact_pending_custody() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_request(tls, target, false, |f, session, bytes, _| {
                let request = prepared(session, bytes);
                let primary = f.remote(NodeId::new(4)).unwrap();
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(|kind, _| {
                        if kind == Kind::StagingStreamAppendInstall {
                            StorageNodeServer::test_lose_stream_append_reply();
                        }
                    }));
                assert!(session
                    .test_stream_append_install_request(request.clone())
                    .is_err());
                let after = f.observe();
                let node = f.node(NodeId::new(4));
                assert!(node
                    .get_pg(f.context.id().pg_id().get())
                    .unwrap()
                    .inspect_complete_pending_command(4, &topology())
                    .unwrap()
                    .is_some());
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(|_, _| {}));
                // Receiver-only replay after reconnect: this does not authorize
                // the owning sender to discard its original section/history.
                let retry = primary
                    .client
                    .open_metadata_command_critical_section_until(
                        f.context.id().pg_id(),
                        deadline(),
                    )
                    .unwrap();
                let reply = retry
                    .test_stream_append_install_request(request.clone())
                    .unwrap()
                    .unwrap();
                install::require_confirmation(&request, &reply).unwrap();
                assert_eq!(f.observe(), after);
            });
        }
    }
}
