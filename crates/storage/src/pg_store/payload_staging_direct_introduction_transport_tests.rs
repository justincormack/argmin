// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed authenticated Unix/TLS preparation: no pending or owner mutation.
use super::*;
use crate::storage_rpc::{
    self as rpc, complete_direct_introduction as wire, StorageRpcMessageKind as Kind,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const KIND: Kind = Kind::StagingDirectIntroductionPrepare;

#[path = "payload_staging_direct_introduction_install_rpc_tests.rs"]
mod installation;
#[path = "payload_staging_direct_introduction_request_remote_tests.rs"]
mod request;
#[path = "payload_staging_direct_introduction_sender_tests.rs"]
mod sender;

#[test]
fn staging_direct_introduction_transport_frozen_complete_composite() {
    let owner = super::fixture(StagingTarget::DirectPut);
    let logical = wire::Request {
        route: rpc::complete_settled_prefix::Request {
            node: NodeId::new(4),
            epoch: owner.subject.originating_epoch,
            pg: owner.subject.object_pg,
            topology: rpc::complete_settled_prefix::topology_digest(&topology()),
            deadline: rpc::StorageRpcOperationDeadline {
                portable_wall_valid_until_ms: 10_000,
            },
        },
        bucket: owner.subject.bucket.clone(),
        key: owner.subject.key.clone(),
        attempt: owner.subject.id.attempt.clone(),
        proof: proof(
            &owner.subject,
            PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
        ),
    };
    let input = wire::encode_request(&logical).unwrap();
    let line = include_str!("testdata/staging_context_carrier_v14_context_v1.hex")
        .lines()
        .find(|line| line.starts_with("direct "))
        .unwrap();
    let columns: Vec<_> = line.split_whitespace().collect();
    let checksum = u64::from_str_radix(columns[1], 16).unwrap();
    let command = crate::tests::frozen_hex_fixture(columns[3]);
    let context = ContextualCommand::decode_command(&command, checksum, &topology()).unwrap();
    context
        .require_remote_direct_introduction(&logical, 8_000)
        .unwrap();
    let reply = wire::encode_response(
        &input,
        rpc::complete_applied::BorrowedRequest {
            node_id: logical.route.node,
            pg_id: logical.route.pg,
            epoch: logical.route.epoch,
            checksum,
            command: &command,
        },
        rpc::StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: 8_000,
        },
    )
    .unwrap();
    for (literal, payload) in [
        (include_str!("../storage_rpc/testdata/direct_introduction_rpc_v37_payload_v1_command_v14_context_v1_request.hex"), input.bytes().to_vec()),
        (include_str!("../storage_rpc/testdata/direct_introduction_rpc_v37_payload_v1_command_v14_context_v1_reply.hex"), rpc::encode_storage_rpc_success_response(&reply)),
    ] {
        let frame = rpc::encode_storage_rpc_frame(17, KIND, &payload).unwrap();
        let frozen = crate::tests::frozen_hex_fixture(literal);
        assert_eq!(frame, frozen);
        let decoded = rpc::read_storage_rpc_frame_from(&mut std::io::Cursor::new(frozen)).unwrap();
        assert_eq!(decoded.request_id, 17);
        assert_eq!(decoded.kind, KIND);
        assert_eq!(decoded.payload, payload);
    }
}

fn fixture(
    tls: bool,
) -> (
    fanout_mixed::Mixed,
    StagingOwnerRecord,
    BucketWriteReservationProof,
    test_util::TempDir,
) {
    let (
        Setup {
            _dir,
            map,
            handle,
            context,
        },
        owner,
        proof,
    ) = inputs();
    (
        fanout_mixed::Mixed::from_settled_fixture(map, handle, context, [(4, tls)]),
        owner,
        proof,
        _dir,
    )
}

#[test]
fn staging_direct_introduction_transport_unix_tls_retains_exact_session_and_no_effects() {
    for tls in [false, true] {
        let (f, owner, proof, _dir) = fixture(tls);
        let primary = f.remote(NodeId::new(4)).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        primary
            .server
            .set_response_frame_test_hook(Arc::new(move |kind, _| {
                if kind == KIND {
                    count.fetch_add(1, Ordering::SeqCst);
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
        let before = f.observe();
        let shadows = f.shadows();
        let subject = (
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        );
        let prepared = route
            .prepare_direct_introduction_remote(&primary.client, &session, subject, deadline())
            .unwrap();
        assert_eq!(prepared.id(), f.context.id());
        let (request, reply) = prepared.test_bytes();
        let request = wire::encode_request(&wire::decode_request(request).unwrap()).unwrap();
        let response = request.decode_response(reply).unwrap();
        assert_eq!(
            ContextualCommand::decode_command(
                response.command.command,
                response.command.checksum,
                &topology()
            )
            .unwrap()
            .id(),
            prepared.id()
        );
        assert_eq!(
            prepared.test_bounds().1,
            response.cutoff.portable_wall_valid_until_ms
        );
        prepared.require_valid().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.observe(), before);
        // The receiver already owns this preparation: a second frame must not
        // construct a fresh submission, even with a newly supplied work budget.
        let error = route
            .prepare_direct_introduction_remote(&primary.client, &session, subject, deadline())
            .err()
            .expect("second claim");
        assert!(
            matches!(&error, crate::node_runtime::pg_store::StagingWindowConfirmationError::Store(StoreError::StorageRpc { detail, .. })
            if detail.as_str().contains("preparation already claimed")),
            "{error:?}"
        );
        prepared.require_valid().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(f.observe(), before);
        session.test_close_primary_section();
        let replacement = primary
            .client
            .open_complete_fanout_primary(&route, deadline())
            .unwrap();
        assert!(
            prepared.require_valid().is_err(),
            "reopened session renewed captured authority"
        );
        assert_eq!(f.observe(), before);
        drop(prepared);
        drop(replacement);
        drop(session);
        assert_eq!(f.observe(), before, "preparation Drop mutated storage");
        assert_eq!(f.shadows(), shadows);
    }
}

#[test]
fn staging_direct_introduction_transport_lost_reply_does_not_install_or_retry() {
    for tls in [false, true] {
        let (f, owner, proof, _dir) = fixture(tls);
        let primary = f.remote(NodeId::new(4)).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        primary.server.set_response_frame_test_hook(Arc::new(move |kind, _| {
            if kind == KIND {
                count.fetch_add(1, Ordering::SeqCst);
                crate::storage_node_server::StorageNodeServer::test_lose_direct_introduction_reply();
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
        let before = f.observe();
        assert!(route
            .prepare_direct_introduction_remote(
                &primary.client,
                &session,
                (
                    &owner.subject.bucket,
                    &owner.subject.key,
                    &owner.subject.id.attempt,
                    &proof
                ),
                deadline()
            )
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.observe(), before);
        drop(session);
        assert_eq!(f.observe(), before);
    }
}

#[test]
fn staging_direct_introduction_transport_authenticated_crossed_replies_export_no_holder() {
    for tls in [false, true] {
        for field in ["digest", "proof", "cutoff", "checksum", "expired"] {
            let (f, owner, proof, _dir) = fixture(tls);
            let primary = f.remote(NodeId::new(4)).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&calls);
            let token = proof.owner_token.as_bytes().to_vec();
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind != KIND {
                        return;
                    }
                    count.fetch_add(1, Ordering::SeqCst);
                    let mut body = rpc::decode_storage_rpc_response_payload(&frame.payload)
                        .unwrap()
                        .unwrap();
                    match field {
                        "digest" => body[6] ^= 1,
                        "cutoff" => body[70..78].copy_from_slice(&u64::MAX.to_le_bytes()),
                        "expired" => body[70..78].copy_from_slice(&1u64.to_le_bytes()),
                        "checksum" => body[78 + 16] ^= 1,
                        "proof" => {
                            let command = rpc::complete_applied::decode(&body[78..]).unwrap();
                            let mut bytes = command.command.to_vec();
                            let offset =
                                bytes.windows(token.len()).position(|s| s == token).unwrap();
                            bytes[offset] ^= 1;
                            let checksum = checksum::crc64::checksum(&bytes);
                            // This is a valid complete context with the WRONG proof,
                            // not rejection at a checksum or structural decoder.
                            ContextualCommand::decode_command(&bytes, checksum, &topology())
                                .unwrap();
                            let encoded = rpc::complete_applied::encode(
                                rpc::complete_applied::BorrowedRequest {
                                    checksum,
                                    command: &bytes,
                                    ..command
                                },
                            )
                            .unwrap();
                            body[38..70]
                                .copy_from_slice(&rpc::complete_stream_allocation::digest(&bytes));
                            body.truncate(78);
                            body.extend_from_slice(&encoded);
                        }
                        _ => unreachable!(),
                    }
                    frame.payload = rpc::encode_storage_rpc_success_response(&body);
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
            let before = f.observe();
            let error = route
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
                .err()
                .expect("crossed reply");
            if field == "proof" {
                assert!(
                    error.to_string().contains("reply differs from request"),
                    "{error}"
                );
            }
            if field == "expired" {
                assert!(
                    matches!(
                        error,
                        crate::node_runtime::pg_store::StagingWindowConfirmationError::Store(
                            StoreError::OperationDeadlineExceeded { .. }
                        )
                    ),
                    "{error:?}"
                );
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(f.observe(), before);
            // Do not drop the rejected session to make this assertion true:
            // validation must already have invalidated it before returning.
            let retry = route
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
                .err()
                .expect("rejected preparation session remained reusable");
            assert!(matches!(retry,
                crate::node_runtime::pg_store::StagingWindowConfirmationError::Store(StoreError::RouteCapabilitySubjectMismatch { operation })
                if operation.contains("exact open primary session")));
            assert_eq!(calls.load(Ordering::SeqCst), 1, "retry reached the peer");
            assert_eq!(f.observe(), before);
            // A new section can be acquired while the old session value still
            // exists. This proves eventual peer teardown, not synchronous EOF.
            let count = Arc::clone(&calls);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == KIND {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            let replacement = primary
                .client
                .open_complete_fanout_primary(&route, deadline())
                .unwrap();
            let prepared = route
                .prepare_direct_introduction_remote(
                    &primary.client,
                    &replacement,
                    (
                        &owner.subject.bucket,
                        &owner.subject.key,
                        &owner.subject.id.attempt,
                        &proof,
                    ),
                    deadline(),
                )
                .unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            assert_eq!(f.observe(), before);
            drop(prepared);
            drop(replacement);
            drop(session);
        }
    }
}

#[test]
fn staging_direct_introduction_transport_unvalidated_reply_drop_and_unwind_close_session() {
    for tls in [false, true] {
        for unwind in [false, true] {
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
            let request = wire::encode_request(&wire::Request {
                route: rpc::complete_settled_prefix::Request {
                    node: route.primary(),
                    pg: route.pg_id(),
                    epoch: route.epoch(),
                    topology: rpc::complete_settled_prefix::topology_digest(route.topology()),
                    deadline: rpc::StorageRpcOperationDeadline::from_instant(deadline()),
                },
                bucket: owner.subject.bucket.clone(),
                key: owner.subject.key.clone(),
                attempt: owner.subject.id.attempt.clone(),
                proof,
            })
            .unwrap();
            let reached = std::cell::Cell::new(false);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let reply = session
                    .borrow_complete_primary(
                        &primary.client,
                        route.pg_id(),
                        route.epoch(),
                        deadline(),
                    )
                    .unwrap()
                    .prepare_direct_introduction(&request)
                    .unwrap();
                request.decode_response(reply.bytes()).unwrap();
                reached.set(true);
                if unwind {
                    panic!("interrupted introduction reply validation");
                }
                drop(reply);
            }));
            assert!(
                reached.get(),
                "must receive a valid claimed preparation first"
            );
            assert_eq!(result.is_err(), unwind);
            assert!(session
                .borrow_complete_primary(&primary.client, route.pg_id(), route.epoch(), deadline())
                .is_err());
            assert_eq!(f.observe(), before);
            drop(session);
        }
    }
}

#[test]
fn staging_direct_introduction_transport_preflight_and_expiry_are_effect_free() {
    for tls in [false, true] {
        let (f, owner, proof, _dir) = fixture(tls);
        let primary = f.remote(NodeId::new(4)).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        primary
            .server
            .set_response_frame_test_hook(Arc::new(move |kind, _| {
                if kind == KIND {
                    count.fetch_add(1, Ordering::SeqCst);
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
        let before = f.observe();
        let mut crossed = proof.clone();
        crossed.owner_token = "x".repeat(257);
        assert!(route
            .prepare_direct_introduction_remote(
                &primary.client,
                &session,
                (
                    &owner.subject.bucket,
                    &owner.subject.key,
                    &owner.subject.id.attempt,
                    &crossed
                ),
                deadline()
            )
            .is_err());
        let subject = (
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        );
        assert!(route
            .prepare_direct_introduction_remote(&primary.client, &session, subject, Instant::now())
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let foreign_key = (0..100)
            .map(|n| ObjectKey::try_from(format!("crossed-{n}")).unwrap())
            .find(|key| topology().object_pg_for(&owner.subject.bucket, key) != route.pg_id().get())
            .unwrap();
        let mut foreign_proof = proof.clone();
        foreign_proof.target_context = Some(foreign_key.to_string());
        assert!(route
            .prepare_direct_introduction_remote(
                &primary.client,
                &session,
                (
                    &owner.subject.bucket,
                    &foreign_key,
                    &owner.subject.id.attempt,
                    &foreign_proof
                ),
                deadline()
            )
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let prepared = route
            .prepare_direct_introduction_remote(&primary.client, &session, subject, deadline())
            .unwrap();
        let bounds = prepared.test_bounds();
        crate::clock::with_time_override(bounds.1, || assert!(prepared.require_valid().is_err()));
        assert_eq!(prepared.test_bounds(), bounds);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.observe(), before);
        drop(prepared);
        drop(session);
    }
}
