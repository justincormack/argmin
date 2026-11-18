// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Remote primary custody survives application to a genuinely embedded replica.
use super::*;
use crate::node_runtime::pg_store::ApplicationObservation as Applied;
use crate::node_runtime::traits::PgMetadataStore;

fn embedded(f: &Mixed) -> NodeId {
    (0..5)
        .map(NodeId::new)
        .find(|n| f.remote(*n).is_none())
        .unwrap()
}

#[test]
fn staging_mixed_append_application_holds_primary_through_embedded_confirmation() {
    for tls in [false, true] {
        with_submission!(@with with_mixed_writer, tls, StagingTarget::StreamPut, false, "installed", |f, session, submission, owner, shards| {
            let primary = f.remote(NodeId::new(4)).unwrap();
            submission.install_remote(&primary.client, &session).unwrap();
            let mut publication = submission.into_publication().unwrap_or_else(|e| panic!("{}", e.error()));
            publication.publish_remote(&primary.client, &session).unwrap();
            for n in [0, 4] {
                publication.apply_remote(&primary.client, &session, &f.remote(NodeId::new(n)).unwrap().client).unwrap();
            }
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            std::thread::scope(|scope| {
                let session_ref = &session;
                let worker = scope.spawn(move || {
                    ready_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
                    let error = session_ref.borrow_complete_primary(&primary.client, f.context.id().pg_id(), f.context.id().cluster_epoch(), Instant::now() + std::time::Duration::from_millis(30)).err();
                    release_tx.send(()).unwrap();
                    assert!(error.expect("embedded confirmation must retain primary mutex").to_string().contains("borrow complete primary session"));
                });
                publication.test_apply_embedded_replica(&primary.client, &session, embedded(f), |stage| {
                    if stage == "after-commit" {
                        ready_tx.send(()).unwrap();
                        release_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
                    }
                    Ok(())
                }).unwrap();
                worker.join().unwrap();
            });
            assert_eq!(publication.application(embedded(f)), Applied::Applied);
            require_payload(f, owner, shards, true);
        });
    }
}

pub(super) fn with_mixed_writer(
    tls: bool,
    target: StagingTarget,
    appended: bool,
    history: &str,
    action: impl FnOnce(&Mixed, Writer<'_>, &StagingOwnerRecord, &[Vec<u8>]),
) {
    assert!(
        !appended,
        "this fixture selects the initial physical window"
    );
    let embedded_node = if matches!(target, StagingTarget::UploadPart { .. }) {
        3
    } else {
        1
    };
    let endpoints: Vec<_> = (0..5)
        .filter(|n| *n != embedded_node)
        .map(|n| (n, tls))
        .collect();
    with_writer_endpoints::<4>(
        target,
        appended,
        history,
        endpoints
            .try_into()
            .unwrap_or_else(|_| panic!("four remote endpoints required")),
        |f, writer, owner, shards| {
            assert!(f.remote(embedded(f)).is_none());
            let participants = &owner.windows.last().unwrap().participants;
            assert!(
                !participants.files.contains(&embedded(f)),
                "physical participants: {:?}",
                participants.files
            );
            assert_ne!(participants.acknowledgement_primary, embedded(f));
            action(f, writer, owner, shards);
        },
    );
}

#[test]
fn staging_mixed_append_application_orders_replicas_and_preserves_exact_replay() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_submission!(@with with_mixed_writer, tls, target, false, "installed", |f, session, submission, owner, shards| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                submission.install_remote(&primary.client, &session).unwrap();
                let mut publication = submission.into_publication().unwrap_or_else(|e| panic!("{}", e.error()));
                let before = f.observe();
                assert!(publication.apply_embedded_replica(&primary.client, &session, embedded(f)).is_err());
                assert_eq!(f.observe(), before);
                publication.publish_remote(&primary.client, &session).unwrap();
                for n in [0, 4] {
                    let before = f.observe();
                    assert!(publication.apply_embedded_replica(&primary.client, &session, embedded(f)).is_err());
                    assert_eq!(f.observe(), before);
                    publication.apply_remote(&primary.client, &session, &f.remote(NodeId::new(n)).unwrap().client).unwrap();
                }
                let before = f.observe();
                publication.apply_embedded_replica(&primary.client, &session, embedded(f)).unwrap();
                assert_eq!(publication.application(embedded(f)), Applied::Applied);
                let applied = f.observe();
                let local = embedded(f).as_u32() as usize;
                assert_ne!(applied[local], before[local]);
                for n in 0..5 { if n != local { assert_eq!(applied[n], before[n]); } }
                let node = f.node(embedded(f));
                let pg = node.get_pg(owner.subject.object_pg.get()).unwrap();
                let segments = pg.list_stream_segments(&owner.subject.id.attempt).unwrap();
                assert_eq!(segments.len(), 1);
                let window = owner.windows.last().unwrap();
                assert_eq!(segments[0].segment_okh, window.segment_hash);
                assert_eq!(segments[0].segment_vid, window.start);
                assert_eq!(segments[0].size, 4);
                let snapshot = pg.command_build_snapshot().unwrap();
                let advanced = append::load(&pg, owner);
                snapshot.finish().unwrap();
                assert_eq!(advanced.resources, owner.resources);
                assert_eq!(advanced.windows.len(), owner.windows.len() + 1);
                assert_eq!(advanced.references.len(), 1);
                drop(pg);
                publication.apply_embedded_replica(&primary.client, &session, embedded(f)).unwrap();
                assert_eq!(f.observe(), applied, "exact replay must not mutate SQL");
                for n in (1..4).filter(|n| NodeId::new(*n) != embedded(f)) {
                    publication.apply_remote(&primary.client, &session, &f.remote(NodeId::new(n)).unwrap().client).unwrap();
                }
                let before = f.observe();
                let failure = publication.into_terminal().err().expect("generic terminal cannot discard the remote session identity");
                let publication = failure.into_publication();
                for n in 0..5 { assert_eq!(publication.application(NodeId::new(n)), Applied::Applied); }
                drop(publication);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), before);
            });
        }
    }
}

#[test]
fn staging_mixed_append_application_rejects_crossed_members_reconnect_and_expiry() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_submission!(@with with_mixed_writer, tls, target, false, "installed", |f, session, submission, owner, shards| {
                let primary = f.remote(NodeId::new(4)).unwrap();
                submission.install_remote(&primary.client, &session).unwrap();
                let mut publication = submission.into_publication().unwrap_or_else(|e| panic!("{}", e.error()));
                publication.publish_remote(&primary.client, &session).unwrap();
                for n in [0, 4] {
                    publication.apply_remote(&primary.client, &session, &f.remote(NodeId::new(n)).unwrap().client).unwrap();
                }
                let before = f.observe();
                for n in [2, 4, 99] {
                    assert!(publication.apply_embedded_replica(&primary.client, &session, NodeId::new(n)).is_err());
                }
                assert!(publication.apply_embedded_replica(&f.remote(NodeId::new(0)).unwrap().client, &session, embedded(f)).is_err());
                crate::clock::with_time_override(publication.test_cutoff().1, || {
                    assert!(publication.apply_embedded_replica(&primary.client, &session, embedded(f)).is_err());
                });
                session.test_close_primary_section();
                let replacement = primary.client.open_metadata_command_critical_section_until(f.context.id().pg_id(), deadline()).unwrap();
                let error = publication.apply_embedded_replica(&primary.client, &replacement, embedded(f)).unwrap_err();
                assert!(error.to_string().contains("original remote primary session"), "{error}");
                assert_eq!(publication.application(embedded(f)), Applied::NoAttemptObserved);
                drop(publication);
                require_payload(f, owner, shards, true);
                assert_eq!(f.observe(), before);
            });
        }
    }
}

#[test]
fn staging_mixed_append_application_uncertainty_retains_custody_and_exact_retry() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for (stage, unwind) in [
                ("before-commit", false),
                ("after-commit", false),
                ("after-commit", true),
            ] {
                with_submission!(@with with_mixed_writer, tls, target.clone(), false, "installed", |f, session, submission, owner, shards| {
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    submission.install_remote(&primary.client, &session).unwrap();
                    let mut publication = submission.into_publication().unwrap_or_else(|e| panic!("{}", e.error()));
                    publication.publish_remote(&primary.client, &session).unwrap();
                    for n in [0, 4] {
                        publication.apply_remote(&primary.client, &session, &f.remote(NodeId::new(n)).unwrap().client).unwrap();
                    }
                    let mut reached = false;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        publication.test_apply_embedded_replica(&primary.client, &session, embedded(f), |at| {
                            if at == stage {
                                reached = true;
                                assert!(!unwind, "mixed append reply unwind");
                                return Err(StoreError::OperationDeadlineExceeded { context: "mixed append reply lost" });
                            }
                            Ok(())
                        })
                    }));
                    assert!(reached);
                    if unwind { assert!(result.is_err()); } else {
                        assert!(result.unwrap().unwrap_err().to_string().contains("mixed append reply lost"));
                    }
                    assert_eq!(publication.application(embedded(f)), Applied::Unconfirmed);
                    let node = f.node(embedded(f));
                    assert_eq!(node.get_pg(owner.subject.object_pg.get()).unwrap().list_stream_segments(&owner.subject.id.attempt).unwrap().len(), usize::from(stage == "after-commit"));
                    crate::clock::with_time_override(publication.test_cutoff().1, || {
                        assert!(publication.apply_embedded_replica(&primary.client, &session, embedded(f)).is_err());
                    });
                    assert_eq!(publication.application(embedded(f)), Applied::Unconfirmed);
                    publication.apply_embedded_replica(&primary.client, &session, embedded(f)).unwrap();
                    assert_eq!(publication.application(embedded(f)), Applied::Applied);
                    let before = f.observe();
                    publication.apply_embedded_replica(&primary.client, &session, embedded(f)).unwrap();
                    assert_eq!(f.observe(), before);
                    drop(publication);
                    require_payload(f, owner, shards, true);
                });
            }
        }
    }
}
