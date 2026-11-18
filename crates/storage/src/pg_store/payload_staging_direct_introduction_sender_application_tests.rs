// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh introductions, real remote primary, and both embedded/remote witnesses.
use super::*;
use crate::node_runtime::clients::UnixStorageNodeClient;
use crate::node_runtime::pg_store::ApplicationObservation as Applied;

const APPLY: Kind = Kind::MetadataCommandApplyAndRecord;

macro_rules! with_application {
    ($endpoints:expr, |$f:ident, $session:ident, $publication:ident, $owner:ident, $proof:ident| $body:block) => {
        with_application!(@consuming $endpoints, |$f, $session, $publication, $owner, $proof| {
            let bounds = $publication.test_bounds();
            $body
            assert_eq!($publication.test_bounds(), bounds);
            assert_eq!($publication.test_installation(), InstallationObservation::Installed);
            let after = $f.observe();
            drop($publication);
            assert_eq!($f.observe(), after, "Drop does not retire or clean");
        });
    };
    (@consuming $endpoints:expr, |$f:ident, $session:ident, $publication:ident, $owner:ident, $proof:ident| $body:block) => {
        with_application!(@until deadline(), $endpoints, |$f, $session, $publication, $owner, $proof| $body);
    };
    (@until $until:expr, $endpoints:expr, |$f:ident, $session:ident, $publication:ident, $owner:ident, $proof:ident| $body:block) => {{
        let (setup, $owner, $proof) = inputs();
        let Setup { _dir, map, handle, context } = setup;
        let $f = fanout_mixed::Mixed::from_settled_fixture(map, handle, context, $endpoints);
        let _running: Vec<_> = $f.remote.iter().map(fanout_mixed::Remote::run).collect();
        let primary = $f.remote(NodeId::new(4)).unwrap();
        let admission = $f.handle.admit_current_route().unwrap();
        let route = admission.complete_fanout_route($f.context.id().pg_id(), $f.context.id().cluster_epoch()).unwrap();
        let until = $until;
        let $session = primary.client.open_complete_fanout_primary(&route, until).unwrap();
        let prepared = route.prepare_direct_introduction_remote(&primary.client, &$session,
            (&$owner.subject.bucket, &$owner.subject.key, &$owner.subject.id.attempt, &$proof), until).unwrap_or_else(|e| panic!("introduction preparation: {e}"));
        let mut submission = prepared.into_submission();
        submission.install().unwrap();
        let mut $publication = submission.into_publication().unwrap_or_else(|e| panic!("{}", e.error()));
        let shadows = $f.shadows();
        $body
        assert_eq!($f.shadows(), shadows);
    }};
}

macro_rules! apply_member {
    ($f:ident, $publication:ident, $node:expr) => {{
        let node = NodeId::new($node);
        if let Some(peer) = $f.remote(node) {
            $publication.apply_remote(&peer.client)
        } else {
            $publication.apply_embedded_replica(node)
        }
    }};
}

#[path = "payload_staging_direct_introduction_sender_terminal_tests.rs"]
mod terminal;

#[test]
fn staging_direct_introduction_sender_application_orders_mixed_cohort_and_preserves_exact_owners() {
    for tls in [false, true] {
        for witness in [0, 3] {
            with_application!(
                [(4, tls), (witness, !tls), (2, tls)],
                |f, session, publication, owner, proof| {
                    let before = f.observe();
                    assert!(apply_member!(f, publication, 0)
                        .unwrap_err()
                        .to_string()
                        .contains("marker is not confirmed"));
                    assert!(publication.completion().is_err());
                    assert_eq!(f.observe(), before);
                    publication.publish().unwrap();
                    let marked = f.observe();
                    for (node, diagnostic) in
                        [(4, "witness confirmation"), (2, "primary confirmation")]
                    {
                        assert!(apply_member!(f, publication, node)
                            .unwrap_err()
                            .to_string()
                            .contains(diagnostic));
                        assert_eq!(
                            publication.application(NodeId::new(node)),
                            Applied::NoAttemptObserved
                        );
                    }
                    assert!(publication.apply_embedded_replica(NodeId::new(99)).is_err());
                    assert!(publication.apply_embedded_replica(NodeId::new(4)).is_err());
                    for peer in &f.remote {
                        let crossed = UnixStorageNodeClient::new(
                            peer.node,
                            f.context.id().cluster_epoch(),
                            test_util::tempdir().path().join("absent.sock"),
                        );
                        assert!(publication
                            .apply_remote(&crossed)
                            .unwrap_err()
                            .to_string()
                            .contains("exact admitted peer client"));
                    }
                    assert_eq!(f.observe(), marked);
                    let mut expected = None;
                    for member in [0, 4, 2, 3, 1] {
                        assert!(publication.completion().is_err());
                        let before = f.observe();
                        let state = apply_member!(f, publication, member).unwrap();
                        assert_eq!(state.applied_log_index, publication.id().log_index().get());
                        assert_eq!(
                            publication.application(NodeId::new(member)),
                            Applied::Applied
                        );
                        let after = f.observe();
                        for other in 0..5 {
                            if other != member as usize {
                                assert_eq!(after[other], before[other]);
                            }
                        }
                        let node = f.node(NodeId::new(member));
                        let pg = node.get_pg(publication.id().pg_id().get()).unwrap();
                        let snapshot = pg.command_build_snapshot().unwrap();
                        let stored = sql::load(
                            &pg.conn,
                            publication.id().pg_id(),
                            &owner.subject.id.attempt,
                        )
                        .unwrap()
                        .unwrap();
                        snapshot.finish().unwrap();
                        assert_eq!(stored.subject.bucket, owner.subject.bucket);
                        assert_eq!(stored.subject.key, owner.subject.key);
                        assert_eq!(stored.subject.id.introduction, publication.id());
                        assert_eq!(
                            stored.resources.creation,
                            ResourceResponsibility::Live(StagingBucketResource::capture(
                                &proof,
                                PgId::new(topology().bucket_pg_for(&proof.bucket))
                            ))
                        );
                        if let Some(expected) = &expected {
                            assert_eq!(&stored, expected);
                        } else {
                            expected = Some(stored);
                        }
                        drop(pg);
                        apply_member!(f, publication, member).unwrap();
                        assert_eq!(
                            f.observe(),
                            after,
                            "exact application replay is non-mutating"
                        );
                    }
                    let completion = publication.completion().unwrap();
                    assert_eq!(completion.id(), publication.id());
                    assert_eq!(completion.confirmed_peers(), 5);
                    completion.require_valid().unwrap();
                    let before = f.observe();
                    crate::clock::with_time_override(publication.test_bounds().1, || {
                        assert!(completion.require_valid().is_err());
                    });
                    session.test_close_primary_section();
                    assert!(completion.require_valid().is_err());
                    assert_eq!(f.observe(), before);
                }
            );
        }
    }
}

#[test]
fn staging_direct_introduction_sender_application_remote_uncertainty_blocks_order_until_exact_retry(
) {
    for tls in [false, true] {
        for fault in ["digest", "empty", "error"] {
            with_application!(
                [(4, tls), (0, !tls), (2, tls)],
                |f, session, publication, owner, proof| {
                    publication.publish().unwrap();
                    for member in [0, 4, 2] {
                        let peer = f.remote(NodeId::new(member)).unwrap();
                        peer.server
                            .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                                if kind != APPLY {
                                    return;
                                }
                                let mut body =
                                    rpc::decode_storage_rpc_response_payload(&frame.payload)
                                        .unwrap()
                                        .unwrap();
                                match fault {
                                    "digest" => {
                                        let mut reply =
                                            rpc::complete_application::decode_confirmation(&body)
                                                .unwrap();
                                        reply.command_sha256[0] ^= 1;
                                        body =
                                            rpc::complete_application::encode_confirmation(reply);
                                    }
                                    "empty" => body.clear(),
                                    "error" => (),
                                    _ => unreachable!(),
                                }
                                frame.payload = if fault == "error" {
                                    rpc::encode_storage_rpc_error_response(
                                        &rpc::StorageRpcErrorResponse {
                                            code: rpc::StorageRpcErrorCode::ResourceExhausted,
                                            message: "injected after introduction apply".into(),
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
                            }));
                        assert!(publication.apply_remote(&peer.client).is_err());
                        assert_eq!(publication.application(peer.node), Applied::Unconfirmed);
                        let applied = f.observe();
                        assert!(publication.completion().is_err());
                        if member != 2 {
                            assert!(
                                apply_member!(f, publication, if member == 0 { 4 } else { 2 })
                                    .is_err()
                            );
                        }
                        assert_eq!(f.observe(), applied);
                        peer.server
                            .set_response_frame_test_hook(Arc::new(|_, _| {}));
                        publication.apply_remote(&peer.client).unwrap();
                        assert_eq!(publication.application(peer.node), Applied::Applied);
                        assert_eq!(f.observe(), applied);
                    }
                    for member in [3, 1] {
                        apply_member!(f, publication, member).unwrap();
                    }
                    publication.completion().unwrap().require_valid().unwrap();
                }
            );
        }
    }
}

#[test]
fn staging_direct_introduction_sender_application_lost_reply_never_replaces_primary_session() {
    for tls in [false, true] {
        for (member, lost) in [(0, true), (4, true), (4, false), (2, true)] {
            with_application!(
                [(4, tls), (0, !tls), (2, tls)],
                |f, session, publication, owner, proof| {
                    publication.publish().unwrap();
                    if member != 0 {
                        apply_member!(f, publication, 0).unwrap();
                    }
                    if member == 2 {
                        apply_member!(f, publication, 4).unwrap();
                    }
                    let peer = f.remote(NodeId::new(member)).unwrap();
                    peer.server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            if kind == APPLY {
                                if lost {
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
                    assert_eq!(publication.apply_remote(&peer.client).is_err(), lost);
                    let expected = if lost {
                        Applied::Unconfirmed
                    } else {
                        Applied::Applied
                    };
                    assert_eq!(publication.application(peer.node), expected);
                    let after = f.observe();
                    assert_eq!(
                        f.node(peer.node)
                            .get_pg(publication.id().pg_id().get())
                            .unwrap()
                            .metadata_command_replica_state()
                            .unwrap()
                            .applied_log_index,
                        publication.id().log_index().get()
                    );
                    assert!(publication.completion().is_err());
                    if member == 4 {
                        session.test_close_primary_section();
                        let _new = peer
                            .client
                            .open_metadata_command_critical_section_until(
                                publication.id().pg_id(),
                                deadline(),
                            )
                            .unwrap();
                        for target in [4, 2, 1] {
                            assert!(apply_member!(f, publication, target).is_err());
                        }
                        assert_eq!(publication.application(peer.node), expected);
                    } else {
                        // Replica connections may reconnect; the original primary
                        // section remains the unique serialization/custody owner.
                        peer.server
                            .set_response_frame_test_hook(Arc::new(|_, _| {}));
                        publication.apply_remote(&peer.client).unwrap();
                        assert_eq!(publication.application(peer.node), Applied::Applied);
                    }
                    assert_eq!(
                        f.observe(),
                        after,
                        "no repeat mutation after lost confirmation"
                    );
                }
            );
        }
    }
}

#[test]
fn staging_direct_introduction_sender_application_not_sent_unwind_and_expiry_preserve_history() {
    for tls in [false, true] {
        with_application!(
            [(4, tls), (2, !tls)],
            |f, session, publication, owner, proof| {
                publication.publish().unwrap();
                apply_member!(f, publication, 0).unwrap();
                let before = f.observe();
                session.test_append_apply_before_dispatch(false);
                assert!(apply_member!(f, publication, 4).is_err());
                assert_eq!(
                    publication.application(NodeId::new(4)),
                    Applied::NoAttemptObserved
                );
                session.test_append_apply_before_dispatch(true);
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| apply_member!(
                        f,
                        publication,
                        4
                    )))
                    .is_err()
                );
                assert_eq!(
                    publication.application(NodeId::new(4)),
                    Applied::Unconfirmed
                );
                session.test_append_apply_before_dispatch(false);
                assert!(apply_member!(f, publication, 4).is_err());
                assert_eq!(
                    publication.application(NodeId::new(4)),
                    Applied::Unconfirmed
                );
                assert_eq!(f.observe(), before);
                apply_member!(f, publication, 4).unwrap();
                let after = f.observe();
                crate::clock::with_time_override(publication.test_bounds().1, || {
                    for member in [2, 1, 4] {
                        assert!(apply_member!(f, publication, member).is_err());
                    }
                });
                assert_eq!(publication.application(NodeId::new(4)), Applied::Applied);
                let primary = f.remote(NodeId::new(4)).unwrap();
                session.test_close_primary_section();
                let _new = primary
                    .client
                    .open_metadata_command_critical_section_until(
                        publication.id().pg_id(),
                        deadline(),
                    )
                    .unwrap();
                for member in [2, 1, 4] {
                    assert!(apply_member!(f, publication, member).is_err());
                }
                assert_eq!(
                    publication.application(NodeId::new(1)),
                    Applied::NoAttemptObserved
                );
                assert_eq!(
                    publication.application(NodeId::new(2)),
                    Applied::NoAttemptObserved
                );
                assert_eq!(publication.application(NodeId::new(4)), Applied::Applied);
                assert_eq!(f.observe(), after);
            }
        );
    }
}

#[test]
fn staging_direct_introduction_sender_application_embedded_commit_failure_retains_custody() {
    for tls in [false, true] {
        for (stage, unwind) in [
            ("before-commit", false),
            ("after-commit", false),
            ("after-commit", true),
        ] {
            with_application!([(4, tls)], |f, session, publication, owner, proof| {
                publication.publish().unwrap();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    publication.test_apply_embedded_replica(NodeId::new(0), |at| {
                        if at == stage {
                            assert!(!unwind, "injected after commit");
                            return Err(StoreError::RouteCapabilitySubjectMismatch {
                                operation: "injected introduction apply failure",
                            });
                        }
                        Ok(())
                    })
                }));
                if unwind {
                    assert!(result.is_err());
                } else {
                    assert!(result.unwrap().is_err());
                }
                assert_eq!(
                    publication.application(NodeId::new(0)),
                    Applied::Unconfirmed
                );
                let before = f.observe();
                assert!(apply_member!(f, publication, 4).is_err());
                assert_eq!(f.observe(), before);
                apply_member!(f, publication, 0).unwrap();
                assert_eq!(publication.application(NodeId::new(0)), Applied::Applied);
                let applied = f.observe();
                if stage == "after-commit" {
                    assert_eq!(before, applied);
                }
                apply_member!(f, publication, 0).unwrap();
                assert_eq!(f.observe(), applied);
            });
        }
    }
}

#[test]
fn staging_direct_introduction_sender_application_rechecks_expiry_at_embedded_effect() {
    for tls in [false, true] {
        with_application!([(4, tls)], |f, session, publication, owner, proof| {
            publication.publish().unwrap();
            let before = f.observe();
            // Embedded admission carries the already-bound std::time::Instant,
            // not a fresh wall-clock interpretation at each effect.
            let cutoff = publication.test_bounds().0;
            let mut reached = false;
            assert!(publication
                .test_apply_embedded_replica(NodeId::new(0), |stage| {
                    if stage == "before-effect" {
                        reached = true;
                        assert!(Instant::now() < cutoff, "must expire after admission");
                        while let Some(remaining) = cutoff.checked_duration_since(Instant::now()) {
                            std::thread::sleep(remaining);
                        }
                    }
                    Ok(())
                })
                .is_err());
            assert!(
                reached,
                "must expire inside the transaction, not at preflight"
            );
            assert_eq!(
                publication.application(NodeId::new(0)),
                Applied::Unconfirmed
            );
            assert_eq!(
                f.observe(),
                before,
                "no owner, reservation or prefix effect"
            );
        });
    }
}

#[test]
fn staging_direct_introduction_sender_application_holds_original_primary_through_confirmation() {
    for tls in [false, true] {
        for remote in [false, true] {
            let witness = if remote { 0 } else { 2 };
            with_application!(
                [(4, tls), (witness, !tls)],
                |f, session, publication, owner, proof| {
                    publication.publish().unwrap();
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
                    let (release_tx, release_rx) = std::sync::mpsc::channel();
                    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
                    if remote {
                        let ready = ready_tx.clone();
                        let release = Arc::clone(&release_rx);
                        f.remote(NodeId::new(0))
                            .unwrap()
                            .server
                            .set_response_frame_test_hook(Arc::new(move |kind, _| {
                                if kind == APPLY {
                                    ready.send(()).unwrap();
                                    release
                                        .lock()
                                        .unwrap()
                                        .recv_timeout(std::time::Duration::from_secs(5))
                                        .unwrap();
                                }
                            }));
                    }
                    std::thread::scope(|scope| {
                        let session_ref = &session;
                        let id = publication.id();
                        let worker = scope.spawn(move || {
                            ready_rx
                                .recv_timeout(std::time::Duration::from_secs(5))
                                .unwrap();
                            let error = session_ref
                                .borrow_complete_primary(
                                    &primary.client,
                                    id.pg_id(),
                                    id.cluster_epoch(),
                                    Instant::now() + std::time::Duration::from_millis(30),
                                )
                                .err();
                            release_tx.send(()).unwrap();
                            assert!(error
                                .expect("borrow must span confirmation")
                                .to_string()
                                .contains("borrow complete primary session"));
                        });
                        if remote {
                            apply_member!(f, publication, 0).unwrap();
                        } else {
                            publication
                                .test_apply_embedded_replica(NodeId::new(0), |stage| {
                                    if stage == "after-commit" {
                                        ready_tx.send(()).unwrap();
                                        release_rx
                                            .lock()
                                            .unwrap()
                                            .recv_timeout(std::time::Duration::from_secs(5))
                                            .unwrap();
                                    }
                                    Ok(())
                                })
                                .unwrap();
                        }
                        worker.join().unwrap();
                    });
                    assert_eq!(publication.application(NodeId::new(0)), Applied::Applied);
                }
            );
        }
    }
}
