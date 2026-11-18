// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Original embedded-cohort application of the owning source/cleanup pair.
//! Metadata-only fixtures; this does not exercise physical cleanup or release.

use super::abandonment_preparation::{handoff, setup, setup_with_seed};
use super::*;
use crate::node_runtime::pg_store::{
    AbandonedPartInstallationObservation as I, ApplicationObservation as A,
    PublicationObservation as P,
};

fn injected() -> StoreError {
    StoreError::OperationDeadlineExceeded {
        context: "injected owned cleanup application failure",
    }
}

#[test]
fn staging_abandoned_part_application_original_cohort_converges_without_detaching_custody() {
    for count in [0, 2] {
        for origin in ["inspected", "installed", "uncertain"] {
            let dir = test_util::tempdir();
            let (map, handle, source, cleanup, (record, raw)) =
                setup_with_seed(&dir, count, |pg, owner| {
                    (owner.clone(), part::command(pg, owner))
                });
            let admission = handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    source.id().pg_id(),
                    source.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let mut owner = handoff(&map, &admission, &section, &source, origin, deadline())
                .prepare_part_cleanup(&section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            owner.install_cleanup(&section).unwrap();
            owner.publish_cleanup(&section).unwrap();
            let history = owner.installation_observation();
            let shared = map
                .runtime_state()
                .metadata_command_pg_lock(source.id().pg_id());
            let mut expected = None;
            // Deliberately not acting-set order [4, 2, 0, 3, 1]. Witness is 0.
            for id in [0, 4, 3, 1, 2] {
                let node = map.node(NodeId::new(id)).unwrap().test_node();
                assert_eq!(
                    owner.cleanup_application(NodeId::new(id)),
                    A::NoAttemptObserved
                );
                let mut effect = false;
                let state = owner
                    .test_apply_cleanup_embedded(&section, NodeId::new(id), |at| {
                        effect |= at == "after-effect";
                        Ok(())
                    })
                    .unwrap();
                assert!(
                    effect,
                    "fresh cleanup must execute the transactional transfer"
                );
                assert_eq!(state.applied_log_index, cleanup.id().log_index().get());
                assert_eq!(owner.cleanup_application(NodeId::new(id)), A::Applied);
                assert_eq!(owner.cleanup_publication(), P::MarkerConfirmed);
                assert_eq!(owner.cleanup_installation(), I::Installed);
                assert_eq!(owner.installation_observation(), history);
                owner.assert_contexts_for_test(&source, &cleanup, &topology());
                let pg = node.get_pg(source.id().pg_id().get()).unwrap();
                let inspection = pg.command_build_snapshot().unwrap();
                abandonment::assert_closed(&pg, &record, &raw);
                inspection.finish().unwrap();
                let materialized = (
                    introduction::state(&pg),
                    complete_maintenance::log_state(&pg),
                    state,
                );
                if let Some(expected) = &expected {
                    assert_eq!(&materialized, expected, "all members must converge exactly");
                } else {
                    expected = Some(materialized);
                }
                let after = (snapshot(&pg), pg.conn.total_changes());
                drop(pg);
                owner
                    .test_apply_cleanup_embedded(&section, NodeId::new(id), |at| {
                        assert_ne!(at, "before-effect", "exact replay must not transfer again");
                        Ok(())
                    })
                    .unwrap();
                let pg = node.get_pg(source.id().pg_id().get()).unwrap();
                assert_eq!((snapshot(&pg), pg.conn.total_changes()), after);
                assert!(
                    shared.try_lock().is_none(),
                    "original primary serialization stays held"
                );
            }
            // Application is not slot retirement, resource release or physical cleanup.
            let node = map.node(NodeId::new(4)).unwrap().test_node();
            let pg = node.get_pg(source.id().pg_id().get()).unwrap();
            assert!(started(&pg));
            let after = (snapshot(&pg), pg.conn.total_changes());
            drop(pg);
            drop(owner);
            let pg = node.get_pg(source.id().pg_id().get()).unwrap();
            assert_eq!((snapshot(&pg), pg.conn.total_changes()), after);
        }
    }
}

#[test]
fn staging_abandoned_part_application_requires_confirmed_publication_and_witness_order() {
    let dir = test_util::tempdir();
    let (map, handle, source, _) = setup(&dir, 2);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            source.id().pg_id(),
            source.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let mut owner = handoff(&map, &admission, &section, &source, "uncertain", deadline())
        .prepare_part_cleanup(&section)
        .unwrap_or_else(|e| panic!("{}", e.error()));
    owner.install_cleanup(&section).unwrap();
    for publication in [P::NoAttemptObserved, P::Unconfirmed] {
        if publication == P::Unconfirmed {
            owner
                .test_publish_cleanup(&section, |at| {
                    if at == "after-commit" {
                        Err(injected())
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
        }
        let node = map.node(NodeId::new(0)).unwrap().test_node();
        let before = snapshot(&node.get_pg(source.id().pg_id().get()).unwrap());
        let error = owner
            .test_apply_cleanup_embedded(&section, NodeId::new(0), |_| {
                panic!("unconfirmed publication reached apply")
            })
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("publication marker is not confirmed"),
            "{error}"
        );
        assert_eq!(owner.cleanup_publication(), publication);
        assert_eq!(
            owner.cleanup_application(NodeId::new(0)),
            A::NoAttemptObserved
        );
        assert_eq!(
            snapshot(&node.get_pg(source.id().pg_id().get()).unwrap()),
            before
        );
    }
    owner.publish_cleanup(&section).unwrap();
    for (id, diagnostic) in [
        (4, "witness confirmation"),
        (2, "primary confirmation"),
        (99, "not in the admitted acting set"),
    ] {
        let error = owner
            .test_apply_cleanup_embedded(&section, NodeId::new(id), |_| {
                panic!("wrong order/member reached apply")
            })
            .unwrap_err();
        assert!(error.to_string().contains(diagnostic), "{id}: {error}");
        assert_eq!(
            owner.cleanup_application(NodeId::new(id)),
            A::NoAttemptObserved
        );
    }
    owner
        .apply_cleanup_embedded(&section, NodeId::new(0))
        .unwrap();
    let error = owner
        .apply_cleanup_embedded(&section, NodeId::new(2))
        .unwrap_err();
    assert!(
        error.to_string().contains("primary confirmation"),
        "{error}"
    );
    for id in [4, 2, 1, 3] {
        owner
            .apply_cleanup_embedded(&section, NodeId::new(id))
            .unwrap();
    }
}

#[test]
fn staging_abandoned_part_application_errors_and_unwind_keep_per_member_uncertainty() {
    for id in [0, 4] {
        for stage in [
            "before-effect",
            "after-effect",
            "before-commit",
            "after-commit",
        ] {
            for unwind in [false, true] {
                let dir = test_util::tempdir();
                let (map, handle, source, cleanup) = setup(&dir, 2);
                let admission = handle.admit_current_route().unwrap();
                let route = admission
                    .embedded_publication_route(
                        NodeId::new(4),
                        source.id().pg_id(),
                        source.id().cluster_epoch(),
                    )
                    .unwrap();
                let section = route.lock_until(deadline()).unwrap();
                let mut owner =
                    handoff(&map, &admission, &section, &source, "uncertain", deadline())
                        .prepare_part_cleanup(&section)
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                owner.install_cleanup(&section).unwrap();
                owner.publish_cleanup(&section).unwrap();
                if id == 4 {
                    owner
                        .apply_cleanup_embedded(&section, NodeId::new(0))
                        .unwrap();
                }
                let history = owner.installation_observation();
                let node = map.node(NodeId::new(id)).unwrap().test_node();
                let before = snapshot(&node.get_pg(source.id().pg_id().get()).unwrap());
                let mut reached = false;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    owner.test_apply_cleanup_embedded(&section, NodeId::new(id), |at| {
                        if at == stage {
                            reached = true;
                            assert!(!unwind, "injected cleanup application unwind");
                            return Err(injected());
                        }
                        Ok(())
                    })
                }));
                assert!(reached, "{id} {stage}");
                if unwind {
                    assert!(result.is_err());
                } else {
                    assert!(result.unwrap().is_err());
                }
                assert_eq!(owner.cleanup_application(NodeId::new(id)), A::Unconfirmed);
                assert_eq!(
                    owner.cleanup_application(NodeId::new(2)),
                    A::NoAttemptObserved
                );
                assert_eq!(owner.cleanup_publication(), P::MarkerConfirmed);
                assert_eq!(owner.cleanup_installation(), I::Installed);
                assert_eq!(owner.installation_observation(), history);
                owner.assert_contexts_for_test(&source, &cleanup, &topology());
                let pg = node.get_pg(source.id().pg_id().get()).unwrap();
                assert!(pg.conn.is_autocommit());
                pg.require_usable_connection().unwrap();
                if stage == "after-commit" {
                    assert_ne!(snapshot(&pg), before);
                } else {
                    assert_eq!(snapshot(&pg), before);
                }
                drop(pg);
                let next = if id == 0 { 4 } else { 2 };
                owner
                    .test_apply_cleanup_embedded(&section, NodeId::new(next), |_| {
                        panic!("unconfirmed predecessor allowed apply")
                    })
                    .unwrap_err();
                owner
                    .test_apply_cleanup_embedded(&section, NodeId::new(id), |at| {
                        if stage == "after-commit" {
                            assert_ne!(at, "before-effect", "lost-reply replay repeated transfer");
                        }
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(owner.cleanup_application(NodeId::new(id)), A::Applied);
                let pg = node.get_pg(source.id().pg_id().get()).unwrap();
                let after = (snapshot(&pg), pg.conn.total_changes());
                drop(pg);
                // A failed confirmed retry must preserve the observed fact.
                owner
                    .test_apply_cleanup_embedded(&section, NodeId::new(id), |at| {
                        assert_ne!(at, "before-effect");
                        if at == "before-commit" {
                            Err(injected())
                        } else {
                            Ok(())
                        }
                    })
                    .unwrap_err();
                assert_eq!(owner.cleanup_application(NodeId::new(id)), A::Applied);
                let pg = node.get_pg(source.id().pg_id().get()).unwrap();
                assert_eq!((snapshot(&pg), pg.conn.total_changes()), after);
            }
        }
    }
}

#[test]
fn staging_abandoned_part_application_original_deadline_reaches_effect_and_commit() {
    for stage in ["before-effect", "before-commit"] {
        let dir = test_util::tempdir();
        let (map, handle, source, cleanup) = setup(&dir, 2);
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                source.id().pg_id(),
                source.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let cutoff = Instant::now() + Duration::from_secs(2);
        let mut owner = handoff(&map, &admission, &section, &source, "uncertain", cutoff)
            .prepare_part_cleanup(&section)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        owner.install_cleanup(&section).unwrap();
        owner.publish_cleanup(&section).unwrap();
        let node = map.node(NodeId::new(0)).unwrap().test_node();
        let before = snapshot(&node.get_pg(source.id().pg_id().get()).unwrap());
        let mut reached = false;
        let error = owner
            .test_apply_cleanup_embedded(&section, NodeId::new(0), |at| {
                if at == stage {
                    reached = true;
                    std::thread::sleep(
                        cutoff.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
                    );
                }
                Ok(())
            })
            .unwrap_err();
        assert!(reached);
        assert!(error.to_string().contains("deadline"), "{error}");
        assert_eq!(owner.cleanup_application(NodeId::new(0)), A::Unconfirmed);
        assert_eq!(
            snapshot(&node.get_pg(source.id().pg_id().get()).unwrap()),
            before
        );
        drop(section);
        let renewed = route.lock_until(deadline()).unwrap();
        owner
            .test_apply_cleanup_embedded(&renewed, NodeId::new(0), |_| {
                panic!("retry renewed original cutoff")
            })
            .unwrap_err();
        assert_eq!(owner.cleanup_application(NodeId::new(0)), A::Unconfirmed);
        assert_eq!(owner.cleanup_publication(), P::MarkerConfirmed);
        assert_eq!(owner.cleanup_installation(), I::Installed);
        owner.assert_contexts_for_test(&source, &cleanup, &topology());
        assert_eq!(
            snapshot(&node.get_pg(source.id().pg_id().get()).unwrap()),
            before
        );
    }
}

#[test]
fn staging_abandoned_part_application_foreign_section_preserves_all_histories() {
    let dir = test_util::tempdir();
    let (map, handle, source, cleanup) = setup(&dir, 2);
    let other_dir = test_util::tempdir();
    let (other_map, other_handle, _, _) = setup(&other_dir, 2);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            source.id().pg_id(),
            source.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let mut owner = handoff(&map, &admission, &section, &source, "uncertain", deadline())
        .prepare_part_cleanup(&section)
        .unwrap_or_else(|e| panic!("{}", e.error()));
    owner.install_cleanup(&section).unwrap();
    owner.publish_cleanup(&section).unwrap();
    let history = owner.installation_observation();
    let other_admission = other_handle.admit_current_route().unwrap();
    let other_route = other_admission
        .embedded_publication_route(
            NodeId::new(4),
            source.id().pg_id(),
            source.id().cluster_epoch(),
        )
        .unwrap();
    let other_section = other_route.lock_until(deadline()).unwrap();
    let observe = |map: &crate::LocalClusterMap| {
        let node = map.node(NodeId::new(0)).unwrap().test_node();
        let pg = node.get_pg(source.id().pg_id().get()).unwrap();
        (snapshot(&pg), pg.conn.total_changes())
    };
    for expected in [A::NoAttemptObserved, A::Unconfirmed, A::Applied] {
        if expected == A::Unconfirmed {
            owner
                .test_apply_cleanup_embedded(&section, NodeId::new(0), |at| {
                    if at == "after-commit" {
                        Err(injected())
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
        }
        if expected == A::Applied {
            owner
                .apply_cleanup_embedded(&section, NodeId::new(0))
                .unwrap();
        }
        let before = (observe(&map), observe(&other_map));
        let error = owner
            .test_apply_cleanup_embedded(&other_section, NodeId::new(0), |_| {
                panic!("foreign runtime reached apply")
            })
            .unwrap_err();
        assert!(error.to_string().contains("subject"), "{error}");
        assert_eq!(owner.cleanup_application(NodeId::new(0)), expected);
        assert_eq!(owner.installation_observation(), history);
        owner.assert_contexts_for_test(&source, &cleanup, &topology());
        assert_eq!((observe(&map), observe(&other_map)), before);
    }
}

#[test]
fn staging_abandoned_part_application_damaged_replica_keeps_partial_fanout_custody() {
    let dir = test_util::tempdir();
    let (map, handle, source, cleanup) = setup(&dir, 2);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            source.id().pg_id(),
            source.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let mut owner = handoff(&map, &admission, &section, &source, "uncertain", deadline())
        .prepare_part_cleanup(&section)
        .unwrap_or_else(|e| panic!("{}", e.error()));
    owner.install_cleanup(&section).unwrap();
    owner.publish_cleanup(&section).unwrap();
    for id in [0, 4] {
        owner
            .apply_cleanup_embedded(&section, NodeId::new(id))
            .unwrap();
    }
    let history = owner.installation_observation();
    let node = map.node(NodeId::new(2)).unwrap().test_node();
    let pg = node.get_pg(source.id().pg_id().get()).unwrap();
    pg.conn
        .execute_batch("DELETE FROM metadata_command_log")
        .unwrap();
    let before = (snapshot(&pg), pg.conn.total_changes());
    drop(pg);
    owner
        .apply_cleanup_embedded(&section, NodeId::new(2))
        .unwrap_err();
    assert_eq!(owner.cleanup_application(NodeId::new(2)), A::Unconfirmed);
    for id in [1, 3] {
        owner
            .apply_cleanup_embedded(&section, NodeId::new(id))
            .unwrap();
    }
    for id in [0, 4, 1, 3] {
        assert_eq!(owner.cleanup_application(NodeId::new(id)), A::Applied);
    }
    assert_eq!(owner.cleanup_installation(), I::Installed);
    assert_eq!(owner.cleanup_publication(), P::MarkerConfirmed);
    assert_eq!(owner.installation_observation(), history);
    owner.assert_contexts_for_test(&source, &cleanup, &topology());
    let pg = node.get_pg(source.id().pg_id().get()).unwrap();
    assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
    drop(pg);
    let primary = map.node(NodeId::new(4)).unwrap().test_node();
    let pg = primary.get_pg(source.id().pg_id().get()).unwrap();
    assert!(started(&pg));
    let before = (snapshot(&pg), pg.conn.total_changes());
    drop(pg);
    drop(owner);
    let pg = primary.get_pg(source.id().pg_id().get()).unwrap();
    assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
}
