// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! All-remote application keeps physical custody and the original primary section.
use super::*;
use crate::node_runtime::pg_store::{
    ApplicationObservation as Applied, PublicationObservation as Published,
};
use crate::node_runtime::traits::PgMetadataStore;

const APPLY: Kind = Kind::MetadataCommandApplyAndRecord;

#[test]
fn staging_remote_append_application_holds_primary_session_through_replica_confirmation() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_submission!(
                tls,
                target,
                false,
                "installed",
                |f, session, submission, owner, shards| {
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    submission
                        .install_remote(&primary.client, &session)
                        .unwrap();
                    let mut publication = submission
                        .into_publication()
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    publication
                        .publish_remote(&primary.client, &session)
                        .unwrap();
                    let witness = f.remote(NodeId::new(0)).unwrap();
                    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
                    let (release_tx, release_rx) = std::sync::mpsc::channel();
                    let release_rx = std::sync::Mutex::new(release_rx);
                    witness
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, _| {
                            if kind == APPLY {
                                ready_tx.send(()).unwrap();
                                release_rx
                                    .lock()
                                    .unwrap()
                                    .recv_timeout(std::time::Duration::from_secs(5))
                                    .unwrap();
                            }
                        }));
                    std::thread::scope(|scope| {
                        let session_ref = &session;
                        let worker = scope.spawn(move || {
                        ready_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
                        let result = session_ref.borrow_complete_primary(&primary.client, f.context.id().pg_id(), f.context.id().cluster_epoch(), Instant::now() + std::time::Duration::from_millis(30));
                        let error = result.err();
                        release_tx.send(()).unwrap();
                        assert!(error.expect("application must retain primary mutex until replica confirmation").to_string().contains("borrow complete primary session"));
                    });
                        publication
                            .apply_remote(&primary.client, &session, &witness.client)
                            .unwrap();
                        worker.join().unwrap();
                    });
                    assert_eq!(publication.application(witness.node), Applied::Applied);
                    let after = f.observe();
                    drop(publication);
                    require_payload(f, owner, shards, true);
                    assert_eq!(f.observe(), after);
                }
            );
        }
    }
}

#[test]
fn staging_remote_append_application_orders_all_members_and_retains_payload() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for appended in [false, true] {
                with_submission!(
                    tls,
                    target.clone(),
                    appended,
                    "installed",
                    |f, session, submission, owner, shards| {
                        let primary = f.remote(NodeId::new(4)).unwrap();
                        submission
                            .install_remote(&primary.client, &session)
                            .unwrap();
                        let history = submission.test_binding_state();
                        let mut publication = submission
                            .into_publication()
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        let id = publication.id();
                        let cutoff = publication.test_cutoff();
                        let before = f.observe();
                        assert!(publication
                            .apply_remote(
                                &primary.client,
                                &session,
                                &f.remote(NodeId::new(0)).unwrap().client
                            )
                            .is_err());
                        assert_eq!(
                            publication.application(NodeId::new(0)),
                            Applied::NoAttemptObserved
                        );
                        assert_eq!(f.observe(), before);
                        publication
                            .publish_remote(&primary.client, &session)
                            .unwrap();
                        let marked = f.observe();
                        for node in [4, 2] {
                            assert!(publication
                                .apply_remote(
                                    &primary.client,
                                    &session,
                                    &f.remote(NodeId::new(node)).unwrap().client
                                )
                                .is_err());
                            assert_eq!(
                                publication.application(NodeId::new(node)),
                                Applied::NoAttemptObserved
                            );
                        }
                        assert_eq!(f.observe(), marked);
                        for node_id in [0, 4, 2, 3, 1] {
                            let peer = f.remote(NodeId::new(node_id)).unwrap();
                            let before = f.observe();
                            let state = publication
                                .apply_remote(&primary.client, &session, &peer.client)
                                .unwrap();
                            assert_eq!(state.applied_log_index, id.log_index().get());
                            assert_eq!(publication.application(peer.node), Applied::Applied);
                            let node = f.node(peer.node);
                            let pg = node.get_pg(id.pg_id().get()).unwrap();
                            let segments =
                                pg.list_stream_segments(&owner.subject.id.attempt).unwrap();
                            assert_eq!(segments.len(), usize::from(appended) + 1);
                            let segment = segments.last().unwrap();
                            let window = owner.windows.last().unwrap();
                            assert_eq!(segment.segment_okh, window.segment_hash);
                            assert_eq!(segment.segment_vid, window.start);
                            assert_eq!(segment.size, 4);
                            let snapshot = pg.command_build_snapshot().unwrap();
                            let advanced = append::load(&pg, owner);
                            snapshot.finish().unwrap();
                            assert_eq!(advanced.subject, owner.subject);
                            assert_eq!(advanced.resources, owner.resources);
                            assert_eq!(advanced.windows.len(), owner.windows.len() + 1);
                            assert_eq!(advanced.references.len(), usize::from(appended) + 1);
                            drop(pg);
                            let applied = f.observe();
                            for other in 0..5 {
                                if other != node_id as usize {
                                    assert_eq!(applied[other], before[other]);
                                }
                            }
                            publication
                                .apply_remote(&primary.client, &session, &peer.client)
                                .unwrap();
                            assert_eq!(f.observe(), applied, "exact apply replay is non-mutating");
                            require_payload(f, owner, shards, true);
                        }
                        assert_eq!(publication.test_binding_state(), history);
                        assert_eq!(publication.test_cutoff(), cutoff);
                        assert_eq!(publication.publication(), Published::MarkerConfirmed);
                        let before = f.observe();
                        let failure = publication
                            .into_terminal()
                            .err()
                            .expect("generic terminal cannot discard the remote session identity");
                        let publication = failure.into_publication();
                        for node in 0..5 {
                            assert_eq!(
                                publication.application(NodeId::new(node)),
                                Applied::Applied
                            );
                        }
                        drop(publication);
                        require_payload(f, owner, shards, true);
                        assert_eq!(f.observe(), before);
                    }
                );
            }
        }
    }
}

#[test]
fn staging_remote_append_application_rejects_wrong_primary_reconnect_and_expiry_without_effects() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_submission!(
                tls,
                target,
                false,
                "uncertain",
                |f, session, submission, owner, shards| {
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    submission
                        .install_remote(&primary.client, &session)
                        .unwrap();
                    let mut publication = submission
                        .into_publication()
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    publication
                        .publish_remote(&primary.client, &session)
                        .unwrap();
                    let before = f.observe();
                    let history = publication.test_binding_state();
                    let count = Arc::new(AtomicUsize::new(0));
                    for peer in &f.remote {
                        let calls = Arc::clone(&count);
                        peer.server
                            .set_response_frame_test_hook(Arc::new(move |kind, _| {
                                if kind == APPLY {
                                    calls.fetch_add(1, Ordering::SeqCst);
                                }
                            }));
                    }
                    let witness = f.remote(NodeId::new(0)).unwrap();
                    for peer in [witness, primary] {
                        let duplicate = crate::node_client::UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                            peer.node,
                            f.context.id().cluster_epoch(),
                            peer.client.test_execution_endpoint().clone(),
                            crate::node_client::LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
                            None,
                        );
                        assert!(publication
                            .apply_remote(&primary.client, &session, &duplicate)
                            .unwrap_err()
                            .to_string()
                            .contains("exact admitted peer client"));
                    }
                    assert!(publication
                        .apply_remote(&witness.client, &session, &witness.client)
                        .is_err());
                    crate::clock::with_time_override(publication.test_cutoff().1, || {
                        assert!(publication
                            .apply_remote(&primary.client, &session, &witness.client)
                            .is_err());
                    });
                    session.test_close_primary_section();
                    let replacement = primary
                        .client
                        .open_metadata_command_critical_section_until(
                            f.context.id().pg_id(),
                            deadline(),
                        )
                        .unwrap();
                    let error = publication
                        .apply_remote(&primary.client, &replacement, &witness.client)
                        .unwrap_err();
                    assert!(
                        error
                            .to_string()
                            .contains("original remote primary session"),
                        "{error}"
                    );
                    assert_eq!(count.load(Ordering::SeqCst), 0);
                    for node in 0..5 {
                        assert_eq!(
                            publication.application(NodeId::new(node)),
                            Applied::NoAttemptObserved
                        );
                    }
                    assert_eq!(publication.test_binding_state(), history);
                    drop(publication);
                    require_payload(f, owner, shards, true);
                    assert_eq!(f.observe(), before);
                }
            );
        }
    }
}

#[test]
fn staging_remote_append_application_bad_confirmations_keep_order_and_exact_retry() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for fault in ["digest", "error", "empty"] {
                with_submission!(
                    tls,
                    target.clone(),
                    false,
                    "installed",
                    |f, session, submission, owner, shards| {
                        let primary = f.remote(NodeId::new(4)).unwrap();
                        submission
                            .install_remote(&primary.client, &session)
                            .unwrap();
                        let mut publication = submission
                            .into_publication()
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        publication
                            .publish_remote(&primary.client, &session)
                            .unwrap();
                        let history = publication.test_binding_state();
                        let cutoff = publication.test_cutoff();
                        for node in [0, 4, 2] {
                            let peer = f.remote(NodeId::new(node)).unwrap();
                            let replies = Arc::new(std::sync::Mutex::new(None));
                            let saved = Arc::clone(&replies);
                            peer.server.set_response_frame_test_hook(Arc::new(
                                move |kind, frame| {
                                    if kind != APPLY {
                                        return;
                                    }
                                    let mut body =
                                        rpc::decode_storage_rpc_response_payload(&frame.payload)
                                            .unwrap()
                                            .unwrap();
                                    *saved.lock().unwrap() = Some(body.clone());
                                    match fault {
                                        "digest" => {
                                            let mut reply =
                                                rpc::complete_application::decode_confirmation(
                                                    &body,
                                                )
                                                .unwrap();
                                            reply.command_sha256[0] ^= 1;
                                            body = rpc::complete_application::encode_confirmation(
                                                reply,
                                            );
                                        }
                                        "empty" => body.clear(),
                                        "error" => {}
                                        _ => unreachable!(),
                                    }
                                    frame.payload = if fault == "error" {
                                        rpc::encode_storage_rpc_error_response(
                                            &rpc::StorageRpcErrorResponse {
                                                code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                                message: "injected after committed append apply"
                                                    .into(),
                                            },
                                        )
                                        .unwrap()
                                    } else {
                                        rpc::encode_storage_rpc_success_response(&body)
                                    };
                                    rpc::set_storage_rpc_response_connection_reusable(
                                        &mut frame.payload,
                                        true,
                                    )
                                    .unwrap();
                                },
                            ));
                            assert!(publication
                                .apply_remote(&primary.client, &session, &peer.client)
                                .is_err());
                            assert_eq!(publication.application(peer.node), Applied::Unconfirmed);
                            let applied = f.observe();
                            if node != 2 {
                                let blocked = if node == 0 { 4 } else { 2 };
                                assert!(publication
                                    .apply_remote(
                                        &primary.client,
                                        &session,
                                        &f.remote(NodeId::new(blocked)).unwrap().client
                                    )
                                    .is_err());
                                assert_eq!(
                                    publication.application(NodeId::new(blocked)),
                                    Applied::NoAttemptObserved
                                );
                            }
                            crate::clock::with_time_override(cutoff.1, || {
                                assert!(publication
                                    .apply_remote(&primary.client, &session, &peer.client)
                                    .is_err());
                            });
                            assert_eq!(publication.application(peer.node), Applied::Unconfirmed);
                            assert_eq!(f.observe(), applied);
                            let expected = replies.lock().unwrap().take().unwrap();
                            peer.server.set_response_frame_test_hook(Arc::new(
                                move |kind, frame| {
                                    if kind == APPLY {
                                        assert_eq!(
                                            rpc::decode_storage_rpc_response_payload(
                                                &frame.payload
                                            )
                                            .unwrap()
                                            .unwrap(),
                                            expected
                                        );
                                    }
                                },
                            ));
                            publication
                                .apply_remote(&primary.client, &session, &peer.client)
                                .unwrap();
                            assert_eq!(publication.application(peer.node), Applied::Applied);
                            assert_eq!(f.observe(), applied);
                            peer.server
                                .set_response_frame_test_hook(Arc::new(|kind, frame| {
                                    if kind == APPLY {
                                        frame.payload =
                                            rpc::encode_storage_rpc_success_response(&[]);
                                        rpc::set_storage_rpc_response_connection_reusable(
                                            &mut frame.payload,
                                            true,
                                        )
                                        .unwrap();
                                    }
                                }));
                            assert!(publication
                                .apply_remote(&primary.client, &session, &peer.client)
                                .is_err());
                            assert_eq!(publication.application(peer.node), Applied::Applied);
                            assert_eq!(f.observe(), applied);
                            peer.server
                                .set_response_frame_test_hook(Arc::new(|_, _| {}));
                        }
                        assert_eq!(publication.test_binding_state(), history);
                        assert_eq!(publication.test_cutoff(), cutoff);
                        drop(publication);
                        require_payload(f, owner, shards, true);
                    }
                );
            }
        }
    }
}

#[test]
fn staging_remote_append_application_primary_not_sent_and_unwind_keep_history() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_submission!(
                tls,
                target,
                false,
                "installed",
                |f, session, submission, owner, shards| {
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    submission
                        .install_remote(&primary.client, &session)
                        .unwrap();
                    let mut publication = submission
                        .into_publication()
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    publication
                        .publish_remote(&primary.client, &session)
                        .unwrap();
                    publication
                        .apply_remote(
                            &primary.client,
                            &session,
                            &f.remote(NodeId::new(0)).unwrap().client,
                        )
                        .unwrap();
                    let before = f.observe();
                    session.test_append_apply_before_dispatch(false);
                    assert!(publication
                        .apply_remote(&primary.client, &session, &primary.client)
                        .unwrap_err()
                        .to_string()
                        .contains("not sent"));
                    assert_eq!(
                        publication.application(primary.node),
                        Applied::NoAttemptObserved
                    );
                    session.test_append_apply_before_dispatch(true);
                    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || publication.apply_remote(&primary.client, &session, &primary.client)
                    ))
                    .is_err());
                    assert_eq!(publication.application(primary.node), Applied::Unconfirmed);
                    session.test_append_apply_before_dispatch(false);
                    assert!(publication
                        .apply_remote(&primary.client, &session, &primary.client)
                        .is_err());
                    assert_eq!(publication.application(primary.node), Applied::Unconfirmed);
                    assert_eq!(f.observe(), before);
                    publication
                        .apply_remote(&primary.client, &session, &primary.client)
                        .unwrap();
                    assert_eq!(publication.application(primary.node), Applied::Applied);
                    let after = f.observe();
                    drop(publication);
                    require_payload(f, owner, shards, true);
                    assert_eq!(f.observe(), after);
                }
            );
        }
    }
}

#[test]
fn staging_remote_append_application_lost_or_closed_reply_retains_custody_and_primary_identity() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for (lost_node, lose_reply) in [(0, true), (4, true), (2, true), (4, false)] {
                with_submission!(
                    tls,
                    target.clone(),
                    false,
                    "installed",
                    |f, session, submission, owner, shards| {
                        let primary = f.remote(NodeId::new(4)).unwrap();
                        submission
                            .install_remote(&primary.client, &session)
                            .unwrap();
                        let mut publication = submission
                            .into_publication()
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        publication
                            .publish_remote(&primary.client, &session)
                            .unwrap();
                        for node in [0, 4, 2] {
                            if node == lost_node {
                                break;
                            }
                            publication
                                .apply_remote(
                                    &primary.client,
                                    &session,
                                    &f.remote(NodeId::new(node)).unwrap().client,
                                )
                                .unwrap();
                        }
                        let lost = f.remote(NodeId::new(lost_node)).unwrap();
                        lost.server
                            .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                                if kind == APPLY {
                                    if lose_reply {
                                        StorageNodeServer::test_lose_application_reply();
                                    } else {
                                        rpc::set_storage_rpc_response_connection_reusable(
                                            &mut frame.payload,
                                            false,
                                        )
                                        .unwrap();
                                    }
                                }
                            }));
                        let result =
                            publication.apply_remote(&primary.client, &session, &lost.client);
                        assert_eq!(result.is_err(), lose_reply);
                        let expected = if lose_reply {
                            Applied::Unconfirmed
                        } else {
                            Applied::Applied
                        };
                        assert_eq!(publication.application(lost.node), expected);
                        let after = f.observe();
                        lost.server
                            .set_response_frame_test_hook(Arc::new(|_, _| {}));
                        if lost_node == 4 {
                            assert!(publication
                                .apply_remote(&primary.client, &session, &lost.client)
                                .is_err());
                            session.test_close_primary_section();
                            let replacement = primary
                                .client
                                .open_metadata_command_critical_section_until(
                                    f.context.id().pg_id(),
                                    deadline(),
                                )
                                .unwrap();
                            assert!(publication
                                .apply_remote(&primary.client, &replacement, &lost.client)
                                .unwrap_err()
                                .to_string()
                                .contains("original remote primary session"));
                            assert_eq!(publication.application(lost.node), expected);
                        } else {
                            publication
                                .apply_remote(&primary.client, &session, &lost.client)
                                .unwrap();
                            assert_eq!(publication.application(lost.node), Applied::Applied);
                        }
                        assert_eq!(f.observe(), after);
                        drop(publication);
                        require_payload(f, owner, shards, true);
                        assert_eq!(f.observe(), after);
                    }
                );
            }
        }
    }
}
