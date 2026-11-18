// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! A genuinely embedded replica applies under retained remote-primary custody.
use super::*;
use crate::node_runtime::pg_store::ApplicationObservation as Applied;

fn embedded() -> NodeId {
    NodeId::new(1)
}

macro_rules! with_mixed_publication {
    ($tls:expr, $origin:expr, |$f:ident, $session:ident, $publication:ident, $owner:ident, $shards:ident, $command:ident| $body:block) => {
        with_mixed_publication!(@endpoints [(0, $tls), (2, $tls), (3, $tls), (4, $tls)], $origin, |$f, $session, $publication, $owner, $shards, $command| $body);
    };
    (@endpoints $endpoints:expr, $origin:expr, |$f:ident, $session:ident, $publication:ident, $owner:ident, $shards:ident, $command:ident| $body:block) => {
        with_writer_endpoints(
            StagingTarget::DirectPut,
            false,
            $origin,
            $endpoints,
            |$f, mut writer, $owner, $shards| {
                if $f.remote.len() == 4 {
                assert!($f.remote(embedded()).is_none());
                let participants = &$owner.windows[0].participants;
                assert!(
                    !participants.files.contains(&embedded()),
                    "physical participants: {:?}",
                    participants.files
                );
                assert_ne!(participants.acknowledgement_primary, embedded());
                }
                let $session;
                let witnessed = witnessed!($f, writer, $owner, $shards, $session);
                let evidence = crate::tests::sse_s3_segment_test_fixture(b"data", 0)
                    .committed_encryption_evidence(witnessed.encryption_subject());
                let client = &$f.remote(NodeId::new(4)).unwrap().client;
                let prepared = witnessed
                    .prepare_terminal_remote(client, &$session, &input(&evidence), &evidence)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                let $command = prepared.test_command();
                let mut submission = prepared.into_submission();
                submission.install_remote(client, &$session).unwrap();
                let mut $publication = submission
                    .into_publication()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                $body
            },
        );
    };
}

#[path = "payload_staging_remote_direct_terminal_tests.rs"]
mod terminal;

#[test]
fn staging_mixed_direct_application_orders_members_and_transfers_exact_references() {
    for tls in [false, true] {
        for origin in ["installed", "inspected", "uncertain"] {
            with_mixed_publication!(
                tls,
                origin,
                |f, session, publication, owner, shards, command| {
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    let history = publication.test_binding_state();
                    let cutoff = publication.test_cutoff();
                    let before = f.observe();
                    assert!(publication
                        .apply_embedded_replica(&primary.client, &session, embedded())
                        .is_err());
                    assert_eq!(f.observe(), before);
                    publication
                        .publish_remote(&primary.client, &session)
                        .unwrap();
                    for n in [0, 4] {
                        let before = f.observe();
                        assert!(publication
                            .apply_embedded_replica(&primary.client, &session, embedded())
                            .is_err());
                        assert_eq!(
                            publication.application(embedded()),
                            Applied::NoAttemptObserved
                        );
                        assert_eq!(f.observe(), before);
                        publication
                            .apply_remote(
                                &primary.client,
                                &session,
                                &f.remote(NodeId::new(n)).unwrap().client,
                            )
                            .unwrap();
                    }
                    let before = f.observe();
                    let state = publication
                        .apply_embedded_replica(&primary.client, &session, embedded())
                        .unwrap();
                    assert_eq!(state.applied_log_index, publication.id().log_index().get());
                    assert_eq!(publication.application(embedded()), Applied::Applied);
                    let applied = f.observe();
                    assert_ne!(applied[1], before[1]);
                    for n in [0, 2, 3, 4] {
                        assert_eq!(applied[n], before[n]);
                    }
                    {
                        let node = f.node(embedded());
                        let pg = node.get_pg(publication.id().pg_id().get()).unwrap();
                        object::assert_object(&pg, &command);
                        let snapshot = pg.command_build_snapshot().unwrap();
                        assert_eq!(append::load(&pg, owner), object::expected(owner, &command));
                        snapshot.finish().unwrap();
                    }
                    publication
                        .apply_embedded_replica(&primary.client, &session, embedded())
                        .unwrap();
                    assert_eq!(f.observe(), applied, "exact replay must not mutate SQL");
                    for n in [2, 3] {
                        publication
                            .apply_remote(
                                &primary.client,
                                &session,
                                &f.remote(NodeId::new(n)).unwrap().client,
                            )
                            .unwrap();
                    }
                    for n in 0..5 {
                        assert_eq!(publication.application(NodeId::new(n)), Applied::Applied);
                        let node = f.node(NodeId::new(n));
                        let pg = node.get_pg(publication.id().pg_id().get()).unwrap();
                        object::assert_object(&pg, &command);
                        let snapshot = pg.command_build_snapshot().unwrap();
                        assert_eq!(append::load(&pg, owner), object::expected(owner, &command));
                        snapshot.finish().unwrap();
                    }
                    assert_eq!(publication.test_binding_state(), history);
                    assert_eq!(publication.test_cutoff(), cutoff);
                    let before = f.observe();
                    let failure = publication
                        .into_terminal()
                        .err()
                        .expect("must retain original remote section");
                    assert!(failure
                        .error()
                        .to_string()
                        .contains("remote direct terminal"));
                    let publication = failure.into_publication();
                    assert_eq!(publication.application(embedded()), Applied::Applied);
                    drop(publication);
                    require_payload(f, owner, shards, true);
                    assert_eq!(f.observe(), before);
                }
            );
        }
    }
}

#[test]
fn staging_mixed_direct_application_holds_primary_through_embedded_confirmation() {
    for tls in [false, true] {
        with_mixed_publication!(
            tls,
            "installed",
            |f, session, publication, owner, shards, _command| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                publication
                    .publish_remote(&primary.client, &session)
                    .unwrap();
                for n in [0, 4] {
                    publication
                        .apply_remote(
                            &primary.client,
                            &session,
                            &f.remote(NodeId::new(n)).unwrap().client,
                        )
                        .unwrap();
                }
                let (ready_tx, ready_rx) = std::sync::mpsc::channel();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                std::thread::scope(|scope| {
                    let session_ref = &session;
                    let worker = scope.spawn(move || {
                        ready_rx
                            .recv_timeout(std::time::Duration::from_secs(5))
                            .unwrap();
                        let error = session_ref
                            .borrow_complete_primary(
                                &primary.client,
                                f.context.id().pg_id(),
                                f.context.id().cluster_epoch(),
                                Instant::now() + std::time::Duration::from_millis(30),
                            )
                            .err();
                        release_tx.send(()).unwrap();
                        assert!(error
                            .expect("primary borrow must span embedded confirmation")
                            .to_string()
                            .contains("borrow complete primary session"));
                    });
                    publication
                        .test_apply_embedded_replica(
                            &primary.client,
                            &session,
                            embedded(),
                            |stage| {
                                if stage == "after-commit" {
                                    ready_tx.send(()).unwrap();
                                    release_rx
                                        .recv_timeout(std::time::Duration::from_secs(5))
                                        .unwrap();
                                }
                                Ok(())
                            },
                        )
                        .unwrap();
                    worker.join().unwrap();
                });
                assert_eq!(publication.application(embedded()), Applied::Applied);
                let after = f.observe();
                drop(publication);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), after);
            }
        );
    }
}

#[test]
fn staging_mixed_direct_application_rejects_crossed_members_reconnect_and_expiry() {
    for tls in [false, true] {
        with_mixed_publication!(
            tls,
            "uncertain",
            |f, session, publication, owner, shards, _command| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                publication
                    .publish_remote(&primary.client, &session)
                    .unwrap();
                for n in [0, 4] {
                    publication
                        .apply_remote(
                            &primary.client,
                            &session,
                            &f.remote(NodeId::new(n)).unwrap().client,
                        )
                        .unwrap();
                }
                let before = f.observe();
                let history = publication.test_binding_state();
                let cutoff = publication.test_cutoff();
                for n in [2, 4, 99] {
                    assert!(publication
                        .apply_embedded_replica(&primary.client, &session, NodeId::new(n))
                        .is_err());
                }
                assert!(publication
                    .apply_embedded_replica(
                        &f.remote(NodeId::new(0)).unwrap().client,
                        &session,
                        embedded()
                    )
                    .is_err());
                crate::clock::with_time_override(cutoff.1, || {
                    assert!(publication
                        .apply_embedded_replica(&primary.client, &session, embedded())
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
                    .apply_embedded_replica(&primary.client, &replacement, embedded())
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("original remote primary session"),
                    "{error}"
                );
                assert_eq!(
                    publication.application(embedded()),
                    Applied::NoAttemptObserved
                );
                assert_eq!(publication.test_binding_state(), history);
                assert_eq!(publication.test_cutoff(), cutoff);
                drop(publication);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), before);
            }
        );
    }
}

#[test]
fn staging_mixed_direct_application_uncertainty_retains_custody_and_exact_retry() {
    for tls in [false, true] {
        for (stage, unwind) in [
            ("before-commit", false),
            ("after-commit", false),
            ("after-commit", true),
        ] {
            with_mixed_publication!(
                tls,
                "installed",
                |f, session, publication, owner, shards, command| {
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    publication
                        .publish_remote(&primary.client, &session)
                        .unwrap();
                    for n in [0, 4] {
                        publication
                            .apply_remote(
                                &primary.client,
                                &session,
                                &f.remote(NodeId::new(n)).unwrap().client,
                            )
                            .unwrap();
                    }
                    let history = publication.test_binding_state();
                    let cutoff = publication.test_cutoff();
                    let mut reached = false;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        publication.test_apply_embedded_replica(
                            &primary.client,
                            &session,
                            embedded(),
                            |at| {
                                if at == stage {
                                    reached = true;
                                    assert!(!unwind, "mixed direct reply unwind");
                                    return Err(StoreError::OperationDeadlineExceeded {
                                        context: "mixed direct reply lost",
                                    });
                                }
                                Ok(())
                            },
                        )
                    }));
                    assert!(reached);
                    if unwind {
                        assert!(result.is_err());
                    } else {
                        assert!(result
                            .unwrap()
                            .unwrap_err()
                            .to_string()
                            .contains("mixed direct reply lost"));
                    }
                    assert_eq!(publication.application(embedded()), Applied::Unconfirmed);
                    {
                        let node = f.node(embedded());
                        let pg = node.get_pg(publication.id().pg_id().get()).unwrap();
                        let snapshot = pg.command_build_snapshot().unwrap();
                        assert_eq!(
                            append::load(&pg, owner),
                            if stage == "after-commit" {
                                object::expected(owner, &command)
                            } else {
                                owner.clone()
                            }
                        );
                        snapshot.finish().unwrap();
                    }
                    let uncertain = f.observe();
                    crate::clock::with_time_override(cutoff.1, || {
                        assert!(publication
                            .apply_embedded_replica(&primary.client, &session, embedded())
                            .is_err());
                    });
                    assert_eq!(publication.application(embedded()), Applied::Unconfirmed);
                    assert_eq!(f.observe(), uncertain);
                    publication
                        .apply_embedded_replica(&primary.client, &session, embedded())
                        .unwrap();
                    assert_eq!(publication.application(embedded()), Applied::Applied);
                    if stage == "after-commit" {
                        assert_eq!(
                            f.observe(),
                            uncertain,
                            "committed retry cannot repeat mutations"
                        );
                    }
                    {
                        let node = f.node(embedded());
                        let pg = node.get_pg(publication.id().pg_id().get()).unwrap();
                        object::assert_object(&pg, &command);
                    }
                    let before = f.observe();
                    publication
                        .apply_embedded_replica(&primary.client, &session, embedded())
                        .unwrap();
                    assert_eq!(f.observe(), before);
                    // A later local failure must not erase confirmed application.
                    assert!(publication
                        .test_apply_embedded_replica(&primary.client, &session, embedded(), |at| {
                            if at == "after-commit" {
                                return Err(StoreError::OperationDeadlineExceeded {
                                    context: "confirmed retry reply lost",
                                });
                            }
                            Ok(())
                        })
                        .is_err());
                    assert_eq!(publication.application(embedded()), Applied::Applied);
                    assert_eq!(publication.test_binding_state(), history);
                    assert_eq!(publication.test_cutoff(), cutoff);
                    assert_eq!(f.observe(), before);
                    drop(publication);
                    require_payload(f, owner, shards, true);
                    assert_eq!(f.observe(), before);
                }
            );
        }
    }
}
