// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh introduction through all original embedded members. Completion remains
//! borrowed evidence; no terminal/physical effect is selected by these tests.
use super::*;
use crate::node_runtime::pg_store::ApplicationObservation as Applied;

pub(super) fn members(
    f: &Setup,
) -> Vec<(
    Vec<crate::node_runtime::pg_store::command_log::MetadataCheckpointTableBlock>,
    u64,
    u64,
)> {
    (0..5)
        .map(|member| {
            let node = f.map.node(NodeId::new(member)).unwrap().test_node();
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            (
                introduction::state(&pg),
                pg.metadata_command_replica_state()
                    .unwrap()
                    .applied_log_index,
                pg.conn.total_changes(),
            )
        })
        .collect()
}

#[test]
fn staging_direct_introduction_application_orders_original_cohort_and_requires_every_member() {
    let clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let mut captured = section
        .prepare_direct_introduction(
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        )
        .unwrap();
    captured.install().unwrap();
    let mut publication = captured
        .into_publication()
        .unwrap_or_else(|e| panic!("{}", e.error()));
    let cutoffs = publication.test_original_deadlines();
    let before = members(&f);
    assert!(publication
        .apply(NodeId::new(0))
        .unwrap_err()
        .to_string()
        .contains("marker is not confirmed"));
    assert!(publication.completion().is_err());
    assert_eq!(members(&f), before);
    publication.publish().unwrap();
    let before = members(&f);
    for (node, diagnostic) in [
        (4, "witness confirmation"),
        (2, "primary confirmation"),
        (99, "not in the admitted acting set"),
    ] {
        let error = publication.apply(NodeId::new(node)).unwrap_err();
        assert!(error.to_string().contains(diagnostic), "{error:?}");
        assert_eq!(
            publication.application(NodeId::new(node)),
            Applied::NoAttemptObserved
        );
    }
    assert_eq!(members(&f), before);
    // Deliberately not acting-set order: fixture [4, 2, 0, 3, 1] has witness 0.
    for (position, node) in [0, 4, 2, 3, 1].into_iter().enumerate() {
        assert!(publication.completion().is_err());
        let state = publication.apply(NodeId::new(node)).unwrap();
        assert_eq!(state.applied_log_index, publication.id().log_index().get());
        assert_eq!(publication.application(NodeId::new(node)), Applied::Applied);
        assert_eq!(
            members(&f)
                .iter()
                .filter(|(_, index, _)| *index == state.applied_log_index)
                .count(),
            position + 1
        );
    }
    let mut expected = None;
    for member in 0..5 {
        let node = f.map.node(NodeId::new(member)).unwrap().test_node();
        let pg = node.get_pg(section.pg_id().get()).unwrap();
        let snapshot = pg.command_build_snapshot().unwrap();
        let stored = sql::load(&pg.conn, section.pg_id(), &owner.subject.id.attempt)
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
    }
    let completion = publication.completion().unwrap();
    assert_eq!(completion.id(), f.context.id());
    assert_eq!(completion.confirmed_members().unwrap(), 5);
    assert!(started(&f), "completion retired the pending slot");
    assert_eq!(publication.test_original_deadlines(), cutoffs);
    clock.set(100_000);
    assert!(
        completion.confirmed_members().is_err(),
        "borrowed evidence outlived authority"
    );
    let before_drop = members(&f);
    drop(publication);
    assert_eq!(members(&f), before_drop, "Drop performed terminal cleanup");
}

#[test]
fn staging_direct_introduction_application_lost_reply_and_unwind_retain_each_confirmation() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let mut captured = section
        .prepare_direct_introduction(
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        )
        .unwrap();
    captured.install().unwrap();
    let mut publication = captured
        .into_publication()
        .unwrap_or_else(|e| panic!("{}", e.error()));
    publication.publish().unwrap();
    for (node, fault) in [
        (0, "lost-reply"),
        (4, "none"),
        (2, "before-commit"),
        (3, "unwind"),
        (1, "none"),
    ] {
        if fault != "none" {
            let before = members(&f);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                publication.test_apply(NodeId::new(node), |point| {
                    if point == "after-commit" && fault == "unwind" {
                        panic!("lost application confirmation");
                    }
                    if (point == "after-commit" && fault == "lost-reply")
                        || (point == "before-commit" && fault == "before-commit")
                    {
                        return Err(StoreError::RouteCapabilitySubjectMismatch {
                            operation: "injected introduction application failure",
                        });
                    }
                    Ok(())
                })
            }));
            if fault == "unwind" {
                assert!(result.is_err());
            } else {
                assert!(result
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("injected introduction application failure"));
            }
            assert_eq!(
                publication.application(NodeId::new(node)),
                Applied::Unconfirmed
            );
            assert_eq!(publication.publication(), Published::MarkerConfirmed);
            assert_eq!(
                publication.test_installation(),
                InstallationObservation::Installed
            );
            assert!(publication.completion().is_err());
            let cutoffs = publication.test_original_deadlines();
            let before_handoff = members(&f);
            let failure = match publication.into_terminal() {
                Ok(_) => panic!("uncertain application authorized terminal handoff"),
                Err(failure) => failure,
            };
            assert!(failure.error().to_string().contains("confirmed"));
            publication = failure.into_publication();
            assert_eq!(publication.test_original_deadlines(), cutoffs);
            assert_eq!(
                publication.application(NodeId::new(node)),
                Applied::Unconfirmed
            );
            assert_eq!(members(&f), before_handoff);
            let after = members(&f);
            if fault == "before-commit" {
                assert_eq!(after[node as usize].0, before[node as usize].0);
                assert_eq!(after[node as usize].1, 0);
            } else {
                assert_eq!(after[node as usize].1, publication.id().log_index().get());
            }
            if node == 0 {
                assert!(publication
                    .apply(NodeId::new(4))
                    .unwrap_err()
                    .to_string()
                    .contains("witness confirmation"));
                assert_eq!(members(&f), after);
            }
        }
        publication.apply(NodeId::new(node)).unwrap();
        assert_eq!(publication.application(NodeId::new(node)), Applied::Applied);
        let before_retry = members(&f);
        publication.apply(NodeId::new(node)).unwrap();
        assert_eq!(members(&f), before_retry, "exact replay mutated state");
    }
    assert_eq!(
        publication
            .completion()
            .unwrap()
            .confirmed_members()
            .unwrap(),
        5
    );
    let before_drop = members(&f);
    drop(publication);
    assert_eq!(members(&f), before_drop);
}

#[test]
fn staging_direct_introduction_terminal_handoff_preserves_custody_without_effects_or_renewal() {
    for expire_before_handoff in [false, true] {
        let clock = crate::clock::test_time_override_guard(2_000);
        let (f, owner, proof) = inputs();
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                f.context.id().pg_id(),
                f.context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let mut captured = section
            .prepare_direct_introduction(
                &owner.subject.bucket,
                &owner.subject.key,
                &owner.subject.id.attempt,
                &proof,
            )
            .unwrap();
        captured.install().unwrap();
        let mut publication = captured
            .into_publication()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let cutoffs = publication.test_original_deadlines();
        let before = members(&f);
        let failure = match publication.into_terminal() {
            Ok(_) => panic!("unpublished introduction granted terminal custody"),
            Err(failure) => failure,
        };
        publication = failure.into_publication();
        assert_eq!(publication.publication(), Published::NoAttemptObserved);
        assert_eq!(members(&f), before);
        publication.publish().unwrap();
        for node in [0, 4, 2, 3, 1] {
            let before = members(&f);
            let failure = match publication.into_terminal() {
                Ok(_) => panic!("incomplete cohort granted terminal custody"),
                Err(failure) => failure,
            };
            assert!(failure.error().to_string().contains("confirmed"));
            publication = failure.into_publication();
            assert_eq!(publication.test_original_deadlines(), cutoffs);
            assert_eq!(
                publication.test_installation(),
                InstallationObservation::Installed
            );
            assert_eq!(members(&f), before);
            publication.apply(NodeId::new(node)).unwrap();
        }
        let before = members(&f);
        if expire_before_handoff {
            clock.set(100_000);
            let failure = match publication.into_terminal() {
                Ok(_) => panic!("expired introduction granted terminal custody"),
                Err(failure) => failure,
            };
            let publication = failure.into_publication();
            assert_eq!(publication.test_original_deadlines(), cutoffs);
            for node in 0..5 {
                assert_eq!(publication.application(NodeId::new(node)), Applied::Applied);
            }
            drop(publication);
        } else {
            let terminal = publication
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(terminal.id(), f.context.id());
            assert_eq!(terminal.test_original_deadlines(), cutoffs);
            terminal.require_valid().unwrap();
            assert!(started(&f), "handoff retired the introduction slot");
            assert_eq!(members(&f), before);
            clock.set(100_000);
            assert!(terminal.require_valid().is_err());
            for node in 0..5 {
                assert_eq!(terminal.application(NodeId::new(node)), Applied::Applied);
            }
            drop(terminal);
        }
        assert_eq!(members(&f), before, "handoff or Drop mutated custody");
        assert!(started(&f));
    }
}

#[test]
fn staging_direct_introduction_application_expiry_at_commit_rolls_back_without_resetting_history() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route
        .lock_until(Instant::now() + Duration::from_secs(4))
        .unwrap();
    let mut captured = section
        .prepare_direct_introduction(
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        )
        .unwrap();
    captured.install().unwrap();
    let mut publication = captured
        .into_publication()
        .unwrap_or_else(|e| panic!("{}", e.error()));
    publication.publish().unwrap();
    let before = members(&f);
    let (until, _) = publication.test_original_deadlines();
    let mut reached = false;
    let error = publication
        .test_apply(NodeId::new(0), |point| {
            if point == "before-commit" {
                reached = true;
                std::thread::sleep(until.saturating_duration_since(Instant::now()));
            }
            Ok(())
        })
        .unwrap_err();
    assert!(reached, "{error:?}");
    assert!(error.to_string().contains("deadline"), "{error:?}");
    let after = members(&f);
    for (before, after) in before.iter().zip(&after) {
        assert_eq!(before.0, after.0);
        assert_eq!(before.1, after.1);
    }
    assert_eq!(
        publication.application(NodeId::new(0)),
        Applied::Unconfirmed
    );
    assert_eq!(publication.publication(), Published::MarkerConfirmed);
    assert!(publication.apply(NodeId::new(0)).is_err());
    assert!(publication.completion().is_err());
    assert_eq!(members(&f), after);
    drop(publication);
    assert_eq!(members(&f), after);
}
