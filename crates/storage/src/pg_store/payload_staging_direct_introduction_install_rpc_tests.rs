// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Receiver socket evidence only: raw test transport does not mint/reset sender custody.
use super::*;
use crate::node_client::UnixStorageNodeMetadataCommandSession;
use crate::node_runtime::server::StorageNodeServer;
use crate::storage_rpc::complete_direct_introduction_install as install;

fn with_prepared(
    tls: bool,
    action: impl FnOnce(
        &fanout_mixed::Mixed,
        &UnixStorageNodeMetadataCommandSession,
        &StorageNodeServer,
        &[u8],
    ),
) {
    let (f, owner, proof, _dir) = fixture(tls);
    let primary = f.remote(NodeId::new(4)).unwrap();
    let _running = primary.run();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .complete_fanout_route(f.context.id().pg_id(), f.context.id().cluster_epoch())
        .unwrap();
    let session = primary
        .client
        .open_complete_fanout_primary(&route, deadline())
        .unwrap();
    let before = f.observe();
    let prepared = route
        .prepare_direct_introduction_remote(
            &primary.client,
            &session,
            (
                &owner.subject.bucket,
                &owner.subject.key,
                &owner.subject.id.attempt,
                &proof,
            ),
            deadline(),
        )
        .unwrap();
    let (request, response) = prepared.test_bytes();
    let bytes = install::encode_request(install::Request {
        preparation: request,
        response,
    })
    .unwrap();
    assert_eq!(f.observe(), before);
    let shadows = f.shadows();
    action(&f, &session, &primary.server, &bytes);
    assert_eq!(f.shadows(), shadows);
    drop(prepared);
    drop(session);
}

fn require_pending(f: &fanout_mixed::Mixed, request: &[u8]) {
    let node = f.node(NodeId::new(4));
    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
    let input = install::decode_request(request).unwrap();
    let prep = wire::encode_request(&wire::decode_request(input.preparation).unwrap()).unwrap();
    let response = prep.decode_response(input.response).unwrap();
    let stored: Vec<u8> = pg
        .conn
        .query_row(
            "SELECT command_bytes FROM metadata_command_pending_slot",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, response.command.command);
    assert!(pg
        .pending_staging_protection(&topology())
        .unwrap()
        .unwrap()
        .window
        .is_some());
    assert_eq!(
        pg.pending_metadata_command_route_references(4)
            .unwrap()
            .unwrap()
            .id,
        f.context.id()
    );
}

#[test]
fn staging_direct_introduction_install_rpc_exact_retry_and_crossed_exchange() {
    for tls in [false, true] {
        with_prepared(tls, |f, session, _, request| {
            let before = f.observe();
            for field in ["proof", "cutoff", "command"] {
                let input = install::decode_request(request).unwrap();
                let mut logical = wire::decode_request(input.preparation).unwrap();
                let original = wire::encode_request(&logical).unwrap();
                let response = original.decode_response(input.response).unwrap();
                let mut cutoff = response.cutoff;
                let mut bytes = response.command.command.to_vec();
                let mut checksum = response.command.checksum;
                match field {
                    "proof" => logical.proof.owner_token.push('x'),
                    "cutoff" => cutoff.portable_wall_valid_until_ms -= 1,
                    "command" => {
                        *bytes.last_mut().unwrap() ^= 1;
                        checksum = checksum::crc64::checksum(&bytes);
                    }
                    _ => unreachable!(),
                }
                let prepared = wire::encode_request(&logical).unwrap();
                let response = wire::encode_response(
                    &prepared,
                    rpc::complete_applied::BorrowedRequest {
                        checksum,
                        command: &bytes,
                        ..response.command
                    },
                    cutoff,
                )
                .unwrap();
                let crossed = install::encode_request(install::Request {
                    preparation: prepared.bytes(),
                    response: &response,
                })
                .unwrap();
                let error = session
                    .test_direct_introduction_install_request(crossed)
                    .unwrap()
                    .unwrap_err();
                assert!(
                    error.message.contains("retained connection custody"),
                    "{field}: {error:?}"
                );
                assert_eq!(f.observe(), before);
            }
            let response = session
                .test_direct_introduction_install_request(request.to_vec())
                .unwrap()
                .unwrap();
            install::require_confirmation(request, &response).unwrap();
            require_pending(f, request);
            let installed = f.observe();
            assert_ne!(installed[4], before[4]);
            assert_eq!(
                &installed[..4],
                &before[..4],
                "only primary pending state changes"
            );
            let response = session
                .test_direct_introduction_install_request(request.to_vec())
                .unwrap()
                .unwrap();
            install::require_confirmation(request, &response).unwrap();
            assert_eq!(
                f.observe(),
                installed,
                "exact retry must not rewrite indexes or slot"
            );
        });
    }
}

#[test]
fn staging_direct_introduction_install_rpc_reopen_cannot_recreate_preparation() {
    for tls in [false, true] {
        with_prepared(tls, |f, session, _, request| {
            let before = f.observe();
            session.test_close_primary_section();
            let primary = f.remote(NodeId::new(4)).unwrap();
            let new = primary
                .client
                .open_metadata_command_critical_section_until(f.context.id().pg_id(), deadline())
                .unwrap();
            let error = new
                .test_direct_introduction_install_request(request.to_vec())
                .unwrap()
                .unwrap_err();
            assert!(
                error.message.contains("retained connection custody"),
                "{error:?}"
            );
            assert_eq!(f.observe(), before);
        });
    }
}

#[test]
fn staging_direct_introduction_install_rpc_lost_reply_keeps_exact_pending_state() {
    for tls in [false, true] {
        with_prepared(tls, |f, session, server, request| {
            server.set_response_frame_test_hook(Arc::new(|kind, _| {
                if kind == Kind::StagingDirectIntroductionInstall {
                    StorageNodeServer::test_lose_direct_introduction_reply();
                }
            }));
            assert!(session
                .test_direct_introduction_install_request(request.to_vec())
                .is_err());
            require_pending(f, request);
            let after = f.observe();
            assert!(session
                .test_direct_introduction_install_request(request.to_vec())
                .is_err());
            assert_eq!(f.observe(), after);
            server.set_response_frame_test_hook(Arc::new(|_, _| {}));
            let primary = f.remote(NodeId::new(4)).unwrap();
            let new = primary
                .client
                .open_metadata_command_critical_section_until(f.context.id().pg_id(), deadline())
                .unwrap();
            assert!(new
                .test_direct_introduction_install_request(request.to_vec())
                .unwrap()
                .is_err());
            assert_eq!(
                f.observe(),
                after,
                "new session must not recreate owning history"
            );
        });
    }
}

#[test]
fn staging_direct_introduction_install_rpc_frozen_complete_composite() {
    let req = rpc::decode_storage_rpc_frame(&crate::tests::frozen_hex_fixture(include_str!("../storage_rpc/testdata/direct_introduction_rpc_v37_payload_v1_command_v14_context_v1_request.hex"))).unwrap();
    let resp = rpc::decode_storage_rpc_frame(&crate::tests::frozen_hex_fixture(include_str!("../storage_rpc/testdata/direct_introduction_rpc_v37_payload_v1_command_v14_context_v1_reply.hex"))).unwrap();
    let response = rpc::decode_storage_rpc_response_payload(&resp.payload)
        .unwrap()
        .unwrap();
    let bytes = install::encode_request(install::Request {
        preparation: &req.payload,
        response: &response,
    })
    .unwrap();
    let mut frozen = crate::tests::frozen_hex_fixture("4449495101002d010000");
    frozen.extend_from_slice(&req.payload);
    frozen.extend_from_slice(&crate::tests::frozen_hex_fixture("96020000"));
    frozen.extend_from_slice(&response);
    assert_eq!(bytes, frozen);
    assert_eq!(bytes.len(), 977);
    assert_eq!(
        install::confirmation(&bytes).unwrap(),
        crate::tests::frozen_hex_fixture(
            "4449494301007cd4962755ee7e8cb834eef48fa86250d3be72ab8d6490fb7e71c1ff037430a7"
        )
    );
    let input = install::decode_request(&bytes).unwrap();
    let original = wire::encode_request(&wire::decode_request(input.preparation).unwrap()).unwrap();
    let response = original.decode_response(input.response).unwrap();
    ContextualCommand::decode_command(
        response.command.command,
        response.command.checksum,
        &topology(),
    )
    .unwrap();
}

#[test]
fn staging_direct_introduction_install_rpc_transaction_failures_retain_original_submission() {
    for tls in [false, true] {
        for lose_confirmation in [false, true] {
            let (f, owner, proof, _dir) = fixture(tls);
            let primary = f.remote(NodeId::new(4)).unwrap();
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == KIND {
                        StorageNodeServer::test_introduction_install_fault(lose_confirmation);
                    }
                }));
            let _running = primary.run();
            let admission = f.handle.admit_current_route().unwrap();
            let route = admission
                .complete_fanout_route(f.context.id().pg_id(), f.context.id().cluster_epoch())
                .unwrap();
            let session = primary
                .client
                .open_complete_fanout_primary(&route, deadline())
                .unwrap();
            let prepared = route
                .prepare_direct_introduction_remote(
                    &primary.client,
                    &session,
                    (
                        &owner.subject.bucket,
                        &owner.subject.key,
                        &owner.subject.id.attempt,
                        &proof,
                    ),
                    deadline(),
                )
                .unwrap();
            let (request, response) = prepared.test_bytes();
            let request = install::encode_request(install::Request {
                preparation: request,
                response,
            })
            .unwrap();
            let before = f.observe();
            let error = session
                .test_direct_introduction_install_request(request.clone())
                .unwrap()
                .unwrap_err();
            if lose_confirmation {
                assert!(
                    error
                        .message
                        .contains("lost complete installation confirmation"),
                    "{error:?}"
                );
                require_pending(&f, &request);
                let installed = f.observe();
                let response = session
                    .test_direct_introduction_install_request(request.clone())
                    .unwrap()
                    .unwrap();
                install::require_confirmation(&request, &response).unwrap();
                assert_eq!(
                    f.observe(),
                    installed,
                    "uncertain retry must keep exact original submission"
                );
            } else {
                assert!(error.message.contains("expired"), "{error:?}");
                assert_eq!(
                    f.observe(),
                    before,
                    "expired at SQLite effect: no slot or sidecars"
                );
            }
            drop(prepared);
            drop(session);
        }
    }
}
