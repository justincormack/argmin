// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0
//! Installed Unix/TLS allocation under the real remote primary's issuer.
use super::*;
use crate::node_runtime::traits::PgMetadataStore;
use crate::storage_rpc::{complete_stream_allocation as wire, StorageRpcMessageKind as Kind};
use crate::types::ExpectedStreamAppendState;
use crate::StreamUploadTarget;
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "payload_staging_remote_append_receiver_tests.rs"]
mod append_receiver;

fn diagnostic(error: &crate::node_runtime::pg_store::StagingWindowConfirmationError) -> String {
    match error {
        crate::node_runtime::pg_store::StagingWindowConfirmationError::Store(
            StoreError::StorageRpc { detail, .. },
        ) => detail.as_str().to_owned(),
        _ => error.to_string(),
    }
}

pub(in super::super) fn setup(
    tls: bool,
    target: StagingTarget,
    appended: bool,
) -> (
    test_util::TempDir,
    Mixed,
    Option<BucketWriteReservationProof>,
) {
    setup_with_endpoints(target, appended, [(4, tls), (0, !tls)])
}

pub(super) fn setup_with_endpoints<const N: usize>(
    target: StagingTarget,
    appended: bool,
    endpoints: [(u32, bool); N],
) -> (
    test_util::TempDir,
    Mixed,
    Option<BucketWriteReservationProof>,
) {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    let Setup {
        _dir,
        map,
        handle,
        context,
    } = super::super::allocation::setup_live(target.clone(), false);
    let mut f = Mixed::from_settled_fixture(map, handle, context, endpoints);
    let mut owner = fresh_capture::template(target);
    let node = f.node(NodeId::new(4));
    let pg = node.get_pg(owner.subject.object_pg.get()).unwrap();
    owner.subject.payload_generation = pg
        .next_generation_id(&owner.subject.bucket, &owner.subject.key)
        .unwrap();
    owner.subject.issuer = pg.stream_allocation_issuer(NodeId::new(4)).unwrap();
    let intro = ContextualCommand::from_described(candidate::capture(
        &pg,
        &owner,
        introduction::command(&owner),
        false,
    ));
    drop(pg);
    if appended {
        for n in 0..5 {
            apply(
                &f.node(NodeId::new(n))
                    .get_pg(intro.id().pg_id().get())
                    .unwrap(),
                &intro,
            );
        }
        let pg = node.get_pg(owner.subject.object_pg.get()).unwrap();
        let snapshot = pg.command_build_snapshot().unwrap();
        owner = append::load(&pg, &owner);
        snapshot.finish().unwrap();
        fresh_append_capture::prepare(&pg, &owner);
        f.context = ContextualCommand::from_described(candidate::capture(
            &pg,
            &owner,
            append::command(&owner),
            true,
        ));
    } else {
        f.context = intro;
    }
    for n in 0..5 {
        f.node(NodeId::new(n))
            .get_pg(f.context.id().pg_id().get())
            .unwrap()
            .recover_complete_command_state(
                PgStoreRecoveryContext::for_node(NodeId::new(n)),
                &topology(),
            )
            .unwrap();
    }
    let proof = (!appended && matches!(owner.subject.target, StagingTarget::UploadPart { .. }))
        .then(|| proof(&owner.subject, owner.subject.creation_operation()));
    (_dir, f, proof)
}

pub(super) fn inputs(
    f: &Mixed,
    target: StagingTarget,
) -> (
    StagingOwnerRecord,
    crate::PrepareStreamUploadSegmentAppendReq,
    ExpectedStreamAppendState,
) {
    let node = f.node(NodeId::new(4));
    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
    let snapshot = pg.command_build_snapshot().unwrap();
    let owner = append::load(&pg, &fresh_capture::template(target));
    let expected = preparation::expected(&pg, &owner);
    let request = crate::PrepareStreamUploadSegmentAppendReq {
        session_id: owner.subject.id.attempt.clone(),
        segment_index: owner.windows.last().unwrap().index,
        size: 4,
        segment_crc64: 17,
        payload_crc64: 23,
    };
    snapshot.finish().unwrap();
    (owner, request, expected)
}

fn reopened_session_matrix(tls: bool) {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            for history in ["installed", "inspected", "uncertain"] {
                let (_dir, f, proof) = setup(tls, target.clone(), appended);
                run_terminal(&f, proof.as_ref(), history, |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    let confirmed = primary
                        .confirm(terminal, deadline(), |_| {})
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let original = confirmed.installation_observation();
                    let (owner, request, expected) = inputs(f, target.clone());
                    let Primary::Remote(client, session) = primary else {
                        unreachable!()
                    };
                    let id = f.context.id();
                    let before = f.observe();
                    let shadows = f.shadows();
                    let node = f.node(NodeId::new(4));
                    let cursor = node
                        .get_pg(id.pg_id().get())
                        .unwrap()
                        .get_stream_upload(&request.session_id)
                        .unwrap()
                        .next_segment_vid;
                    session.test_close_primary_section();
                    let replacement = client
                        .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                        .unwrap();
                    // The same client, endpoint, node, epoch, PG and held-section
                    // checks all succeed; only confirmation identity differs.
                    drop(
                        replacement
                            .borrow_complete_primary(
                                client,
                                id.pg_id(),
                                id.cluster_epoch(),
                                deadline(),
                            )
                            .unwrap(),
                    );
                    let calls = Arc::new(AtomicUsize::new(0));
                    let received = Arc::clone(&calls);
                    f.remote(NodeId::new(4))
                        .unwrap()
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, _| {
                            if kind == Kind::ObjectStreamSegmentAppendPrepare {
                                received.fetch_add(1, Ordering::SeqCst);
                            }
                        }));
                    let failure = confirmed
                        .allocate_stream_remote(client, &replacement, &request, &expected)
                        .err()
                        .expect("reopened session must not allocate a segment");
                    assert_eq!(failure.id(), id);
                    assert_eq!(failure.test_installation_observation(), original);
                    assert!(
                        matches!(
                            failure.error(),
                            E::Store(StoreError::RouteCapabilitySubjectMismatch {
                                operation:
                                    "confirmed window requires its original remote primary session",
                            })
                        ),
                        "{}",
                        diagnostic(failure.error())
                    );
                    assert_eq!(
                        calls.load(Ordering::SeqCst),
                        0,
                        "reject before allocation RPC"
                    );
                    assert_eq!(f.observe(), before);
                    assert_eq!(f.shadows(), shadows);
                    drop(failure);
                    assert_eq!(f.observe(), before, "Drop must preserve durable custody");
                    let pg = node.get_pg(id.pg_id().get()).unwrap();
                    assert_eq!(
                        pg.get_stream_upload(&request.session_id)
                            .unwrap()
                            .next_segment_vid,
                        cursor
                    );
                    let snapshot = pg.command_build_snapshot().unwrap();
                    assert_eq!(append::load(&pg, &owner), owner);
                    snapshot.finish().unwrap();
                });
            }
        }
    }
}

#[test]
fn staging_stream_allocation_remote_unix_rejects_reopened_same_client_session() {
    reopened_session_matrix(false);
}

#[test]
fn staging_stream_allocation_remote_tls_rejects_reopened_same_client_session() {
    reopened_session_matrix(true);
}

fn matrix(tls: bool) {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            for history in ["inspected", "installed", "uncertain"] {
                let (_dir, f, proof) = setup(tls, target.clone(), appended);
                run_terminal(&f, proof.as_ref(), history, |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    let confirmed = primary
                        .confirm(terminal, deadline(), |_| {})
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let (owner, request, expected) = inputs(f, target.clone());
                    let observe = || {
                        (0..5)
                            .map(|member| {
                                let node = f.node(NodeId::new(member));
                                let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                                (
                                    pg.metadata_state_digest().unwrap(),
                                    pg.metadata_command_replica_state().unwrap(),
                                    complete_pending::observe(&pg),
                                    complete_maintenance::log_state(&pg),
                                )
                            })
                            .collect::<Vec<_>>()
                    };
                    let before = observe();
                    let Primary::Remote(client, session) = primary else {
                        unreachable!()
                    };
                    let retry = confirmed.test_stream_allocation_bytes(&request, &expected);
                    f.remote(NodeId::new(4))
                        .unwrap()
                        .server
                        .test_stream_allocation_admission(&retry);
                    let allocated = confirmed
                        .allocate_stream_remote(client, session, &request, &expected)
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_eq!(
                        allocated.segment().segment_vid,
                        owner.windows.last().unwrap().start
                    );
                    assert_eq!(allocated.segment().segment_crc64, 17);
                    let node = f.node(NodeId::new(4));
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    assert_eq!(
                        pg.get_stream_upload(&request.session_id)
                            .unwrap()
                            .next_segment_vid,
                        owner.windows.last().unwrap().end
                    );
                    pg.inspect_complete_settled_prefix(
                        4,
                        f.context.id().cluster_epoch(),
                        &topology(),
                    )
                    .unwrap()
                    .finish()
                    .unwrap();
                    drop(pg);
                    // This covers canonical state, log and pending custody,
                    // not the deliberately local cursor.
                    assert_eq!(observe(), before);
                    let consumed = f.observe();
                    let error = session
                        .test_stream_allocation_payload(retry, deadline())
                        .unwrap_err();
                    let diagnostic = diagnostic(&error.into());
                    assert!(diagnostic.contains("unconsumed"), "{diagnostic}");
                    assert_eq!(
                        f.observe(),
                        consumed,
                        "a duplicate wire request cannot consume another slot"
                    );
                    drop(allocated);
                });
            }
        }
    }
}

#[test]
fn staging_stream_allocation_remote_unix_targets_windows_histories() {
    matrix(false);
}
#[test]
fn staging_stream_allocation_remote_tls_targets_windows_histories() {
    matrix(true);
}

#[test]
fn staging_stream_allocation_remote_bad_or_lost_reply_retains_consumed_custody() {
    for tls in [false, true] {
        for fault in ["digest", "segment", "error", "disconnect", "closed"] {
            let (_dir, f, proof) = setup(tls, multipart_target(), false);
            run_terminal(
                &f,
                proof.as_ref(),
                "uncertain",
                |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    let confirmed = primary
                        .confirm(terminal, deadline(), |_| {})
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let (owner, request, expected) = inputs(f, multipart_target());
                    let calls = Arc::new(AtomicUsize::new(0));
                    let observed = Arc::clone(&calls);
                    f.remote(NodeId::new(4)).unwrap().server.set_response_frame_test_hook(Arc::new(
                        move |kind, bytes| {
                            use crate::storage_rpc as rpc;
                            if kind != Kind::ObjectStreamSegmentAppendPrepare { return; }
                            observed.fetch_add(1, Ordering::SeqCst);
                            if fault == "disconnect" {
                                crate::node_runtime::server::StorageNodeServer::test_lose_stream_allocation_reply();
                                return;
                            }
                            let envelope = rpc::decode_storage_rpc_response_payload_with_connection_disposition(&bytes.payload).unwrap();
                            let reusable = envelope.connection_reusable && fault != "closed";
                            let mut payload = envelope.response.unwrap();
                            match fault {
                                "error" => bytes.payload = rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                                    code: rpc::StorageRpcErrorCode::TransportTimeout,
                                    message: "injected lost allocation reply".into(),
                                }).unwrap(),
                                "digest" | "segment" | "closed" => {
                                    if fault == "digest" { payload[6] ^= 1; }
                                    else if fault == "segment" {
                                        let digest = payload[6..38].try_into().unwrap();
                                        let mut segment = wire::decode_response(&payload, digest).unwrap();
                                        segment.segment_vid = GenerationId::new(segment.segment_vid.get() + 1).unwrap();
                                        payload = wire::encode_response(digest, &segment);
                                    }
                                    bytes.payload = rpc::encode_storage_rpc_success_response(&payload);
                                }
                                _ => unreachable!(),
                            }
                            rpc::set_storage_rpc_response_connection_reusable(&mut bytes.payload, reusable).unwrap();
                        }
                    ));
                    let Primary::Remote(client, session) = primary else {
                        unreachable!()
                    };
                    let failure = confirmed
                        .allocate_stream_remote(client, session, &request, &expected)
                        .err()
                        .expect("fault must not return a grant");
                    assert_eq!(failure.id(), f.context.id());
                    let diagnostic = match fault {
                        "digest" => "request/response",
                        "segment" => "response segment",
                        "closed" => "lost held section",
                        _ => "lost allocation reply",
                    };
                    if fault == "disconnect" {
                        assert!(matches!(failure.error(), crate::node_runtime::pg_store::StagingWindowConfirmationError::Store(StoreError::StorageRpc { operation: "read metadata command session RPC response", .. })), "{}", failure.error());
                    } else {
                        assert!(
                            self::diagnostic(failure.error()).contains(diagnostic),
                            "{}",
                            self::diagnostic(failure.error())
                        );
                    }
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    drop(failure);
                    let node = f.node(NodeId::new(4));
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    assert_eq!(
                        pg.get_stream_upload(&request.session_id)
                            .unwrap()
                            .next_segment_vid,
                        owner.windows[0].end
                    );
                    let snapshot = pg.command_build_snapshot().unwrap();
                    assert_eq!(append::load(&pg, &owner), owner);
                    snapshot.finish().unwrap();
                },
            );
        }
    }
}

#[test]
fn staging_stream_allocation_remote_update_expiry_preserves_cursor_and_owner() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            let (_dir, f, proof) = setup(tls, target.clone(), false);
            run_terminal(
                &f,
                proof.as_ref(),
                "installed",
                |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    f.remote(NodeId::new(4)).unwrap().server.set_response_frame_test_hook(Arc::new(|kind, _| {
                    if kind == Kind::MetadataCommandReplicaState { crate::node_runtime::server::StorageNodeServer::test_expire_stream_allocation_at_update(); }
                }));
                    let confirmed = primary
                        .confirm(terminal, deadline(), |_| {})
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let (_, request, expected) = inputs(f, target.clone());
                    let before = f.observe();
                    let Primary::Remote(client, session) = primary else {
                        unreachable!()
                    };
                    let failure = confirmed
                        .allocate_stream_remote(client, session, &request, &expected)
                        .err()
                        .expect("expiry must not return allocation");
                    let message = diagnostic(failure.error());
                    assert!(
                        message.contains("expired") || message.contains("deadline"),
                        "{message}"
                    );
                    assert_eq!(failure.id(), f.context.id());
                    assert_eq!(f.observe(), before);
                    drop(failure);
                    assert_eq!(f.observe(), before);
                },
            );
        }
    }
}

#[test]
fn staging_stream_allocation_remote_renewal_does_not_extend_captured_route_wait() {
    let (_dir, f, proof) = setup(false, StagingTarget::StreamPut, false);
    let mut request_bytes = None;
    run_terminal(
        &f,
        proof.as_ref(),
        "installed",
        |f, primary, mut terminal| {
            primary.retire(&mut terminal);
            let confirmed = primary
                .confirm(terminal, deadline(), |_| {})
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let (_, request, expected) = inputs(f, StagingTarget::StreamPut);
            request_bytes = Some(confirmed.test_stream_allocation_bytes(&request, &expected));
        },
    );
    let before = f.observe();
    f.remote(NodeId::new(4))
        .unwrap()
        .server
        .test_stream_allocation_route_expiry(request_bytes.as_ref().unwrap());
    assert_eq!(f.observe(), before);
}

#[test]
fn staging_stream_allocation_remote_crossed_inputs_reject_before_transport() {
    for tls in [false, true] {
        for fault in ["session", "generation", "target", "index"] {
            let (_dir, f, proof) = setup(tls, StagingTarget::StreamPut, false);
            run_terminal(
                &f,
                proof.as_ref(),
                "inspected",
                |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    let confirmed = primary
                        .confirm(terminal, deadline(), |_| {})
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let (_, mut request, mut expected) = inputs(f, StagingTarget::StreamPut);
                    let ExpectedStreamAppendState::PutObject {
                        session: record,
                        generation_id,
                    } = &mut expected
                    else {
                        unreachable!()
                    };
                    match fault {
                        "session" => {
                            record.session_id =
                                SessionId::try_from("fedcba9876543210fedcba9876543210").unwrap()
                        }
                        "generation" => {
                            *generation_id = GenerationId::new(generation_id.get() + 1).unwrap()
                        }
                        "target" => {
                            record.target = StreamUploadTarget::UploadPart {
                                upload_id: crate::tests::multipart_upload_id("crossed-target"),
                                part_number: 1,
                            }
                        }
                        "index" => request.segment_index += 1,
                        _ => unreachable!(),
                    }
                    f.remote(NodeId::new(4))
                        .unwrap()
                        .server
                        .set_response_frame_test_hook(Arc::new(|kind, _| {
                            assert_ne!(
                                kind,
                                Kind::ObjectStreamSegmentAppendPrepare,
                                "crossed input reached transport"
                            )
                        }));
                    let before = f.observe();
                    let Primary::Remote(client, session) = primary else {
                        unreachable!()
                    };
                    let failure = confirmed
                        .allocate_stream_remote(client, session, &request, &expected)
                        .err()
                        .expect("crossed input must fail");
                    assert!(diagnostic(failure.error()).contains("allocation input"));
                    drop(failure);
                    assert_eq!(f.observe(), before);
                },
            );
        }
    }
}

#[test]
fn staging_stream_allocation_remote_receiver_rechecks_captured_prefix_and_session() {
    for tls in [false, true] {
        for fault in ["prefix", "session", "topology", "node", "deadline"] {
            let (_dir, f, proof) = setup(tls, StagingTarget::StreamPut, false);
            run_terminal(
                &f,
                proof.as_ref(),
                "installed",
                |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    let confirmed = primary
                        .confirm(terminal, deadline(), |_| {})
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let (_, request, expected) = inputs(f, StagingTarget::StreamPut);
                    let bytes = confirmed.test_stream_allocation_bytes(&request, &expected);
                    let mut crossed = wire::decode_request(&bytes).unwrap();
                    match fault {
                        "prefix" => crossed.prefix.applied_log_index += 1,
                        "session" => {
                            let Some(ExpectedStreamAppendState::PutObject { session, .. }) =
                                &mut crossed.preparation.expected_append_state
                            else {
                                unreachable!()
                            };
                            session.created_at += 1;
                        }
                        "topology" => crossed.window.route.topology[0] ^= 1,
                        "node" => {
                            crossed.window.route.node = NodeId::new(0);
                            crossed.preparation.object.node_id = NodeId::new(0);
                        }
                        "deadline" => {
                            crossed.window.route.deadline.portable_wall_valid_until_ms = 1
                        }
                        _ => unreachable!(),
                    }
                    let before = f.observe();
                    let Primary::Remote(_, session) = primary else {
                        unreachable!()
                    };
                    let error = session
                        .test_stream_allocation_payload(
                            wire::encode_request(&crossed).unwrap(),
                            deadline(),
                        )
                        .unwrap_err();
                    let message = diagnostic(&error.into());
                    let expected = match fault {
                        "prefix" => "prefixes differ",
                        "session" => "captured session authority",
                        "topology" => "topology/prefix",
                        "node" => "node",
                        "deadline" => "deadline expired",
                        _ => unreachable!(),
                    };
                    assert!(message.contains(expected), "{fault}: {message}");
                    assert_eq!(f.observe(), before);
                    drop(confirmed);
                    assert_eq!(f.observe(), before);
                },
            );
        }
    }
}
