// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Current embedded-primary publication of an owned, source-bearing cleanup.
//! Metadata-only tests: no frontend activation, physical cleanup or fanout claim.

use super::abandonment_preparation::{handoff, setup};
use super::*;
use crate::node_runtime::pg_store::{
    AbandonedPartInstallationObservation as I, PreparedAbandonedPart, PublicationObservation as P,
};
use crate::{LocalClusterMap, StorageClusterRouteAdmission};

fn prepared<'a>(
    map: &LocalClusterMap,
    admission: &'a StorageClusterRouteAdmission,
    section: &crate::cluster::EmbeddedPublicationSection<'_>,
    source: &ContextualCommand,
    origin: &str,
    cutoff: Instant,
) -> PreparedAbandonedPart<'a> {
    handoff(map, admission, section, source, origin, cutoff)
        .prepare_part_cleanup(section)
        .unwrap_or_else(|e| panic!("{}", e.error()))
}

fn injected() -> StoreError {
    StoreError::OperationDeadlineExceeded {
        context: "injected cleanup publication failure",
    }
}

#[test]
fn staging_abandoned_part_publication_rejects_identical_cleanup_with_detached_deadline() {
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
    let mut owner = prepared(&map, &admission, &section, &source, "uncertain", deadline());
    owner.install_cleanup(&section).unwrap();
    let detached = pending(&cleanup);
    let topology = topology();
    let detached = detached
        .publication_request(NodeId::new(4), &topology)
        .unwrap();
    section
        .test_with_publication_authority(&detached, deadline(), |pg, authority| {
            let before = (snapshot(pg), pg.conn.total_changes());
            owner.test_with_publication_permit(|permit| {
                let bound =
                    CompletePublicationRequest::abandoned_part_cleanup_request(permit).unwrap();
                assert_eq!(bound.id(), detached.id());
                assert_eq!(bound.command_sha256(), detached.command_sha256());
                assert_eq!(bound.node_id(), detached.node_id());
                assert_eq!(bound.topology(), detached.topology());
                bound.require_abandoned_part_binding(permit).unwrap();
                let error = permit
                    .publish_in_store(pg, authority, &detached, |_| {
                        panic!("detached deadline reached SQL")
                    })
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("exact cleanup/primary/deadline owner"),
                    "{error}"
                );
            });
            assert_eq!((snapshot(pg), pg.conn.total_changes()), before);
            Ok(())
        })
        .unwrap();
    assert_eq!(owner.cleanup_publication(), P::NoAttemptObserved);
    assert_eq!(owner.cleanup_installation(), I::Installed);
    owner.publish_cleanup(&section).unwrap();
    assert_eq!(owner.cleanup_publication(), P::MarkerConfirmed);
}

#[test]
fn staging_abandoned_part_publication_keeps_source_and_only_commits_marker() {
    for count in [0, 2] {
        for origin in ["inspected", "installed", "uncertain"] {
            let dir = test_util::tempdir();
            let (map, handle, source, cleanup) = setup(&dir, count);
            let admission = handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    source.id().pg_id(),
                    source.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let mut owner = prepared(&map, &admission, &section, &source, origin, deadline());
            let history = owner.installation_observation();
            let node = map.node(NodeId::new(4)).unwrap().test_node();
            let before = snapshot(&node.get_pg(source.id().pg_id().get()).unwrap());
            let error = owner
                .test_publish_cleanup(&section, |_| panic!("unsent cleanup reached publication"))
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("requires confirmed installation"),
                "{error}"
            );
            assert_eq!(owner.cleanup_publication(), P::NoAttemptObserved);
            assert_eq!(
                snapshot(&node.get_pg(source.id().pg_id().get()).unwrap()),
                before
            );
            owner.install_cleanup(&section).unwrap();
            let before = without_marker(&node.get_pg(source.id().pg_id().get()).unwrap());
            let mut trial = false;
            owner
                .test_publish_cleanup(&section, |stage| {
                    trial |= stage == "after-trial";
                    Ok(())
                })
                .unwrap();
            assert!(
                trial,
                "source-bearing cleanup must reach shared trial application"
            );
            assert_eq!(owner.cleanup_publication(), P::MarkerConfirmed);
            assert_eq!(owner.cleanup_installation(), I::Installed);
            assert_eq!(owner.installation_observation(), history);
            owner.assert_contexts_for_test(&source, &cleanup, &topology());
            let pg = node.get_pg(source.id().pg_id().get()).unwrap();
            assert!(started(&pg));
            assert_eq!(without_marker(&pg), before, "trial effects must roll back");
            let after = (snapshot(&pg), pg.conn.total_changes());
            drop(pg);
            owner
                .test_publish_cleanup(&section, |_| panic!("confirmed publication redispatched"))
                .unwrap();
            drop(owner);
            let pg = node.get_pg(source.id().pg_id().get()).unwrap();
            assert_eq!(
                (snapshot(&pg), pg.conn.total_changes()),
                after,
                "retry/Drop cannot release custody"
            );
        }
    }
}

#[test]
fn staging_abandoned_part_publication_requires_confirmation_after_install_reply_loss() {
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
    let mut owner = prepared(&map, &admission, &section, &source, "uncertain", deadline());
    owner
        .test_install_cleanup(&section, |at| {
            if at == "after-commit" {
                Err(injected())
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert_eq!(owner.cleanup_installation(), I::Unconfirmed);
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(source.id().pg_id().get()).unwrap();
    let before = (snapshot(&pg), pg.conn.total_changes());
    drop(pg);
    let error = owner
        .test_publish_cleanup(&section, |_| {
            panic!("uncertain installation reached publication")
        })
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires confirmed installation"),
        "{error}"
    );
    assert_eq!(owner.cleanup_publication(), P::NoAttemptObserved);
    assert_eq!(owner.cleanup_installation(), I::Unconfirmed);
    let pg = node.get_pg(source.id().pg_id().get()).unwrap();
    assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
    drop(pg);
    owner.install_cleanup(&section).unwrap();
    owner.publish_cleanup(&section).unwrap();
    assert_eq!(owner.cleanup_publication(), P::MarkerConfirmed);
    owner.assert_contexts_for_test(&source, &cleanup, &topology());
}

#[test]
fn staging_abandoned_part_publication_errors_and_unwind_retain_pair_and_uncertainty() {
    for stage in [
        "before-trial",
        "after-trial",
        "before-marker",
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
            let mut owner = prepared(&map, &admission, &section, &source, "uncertain", deadline());
            owner.install_cleanup(&section).unwrap();
            let history = owner.installation_observation();
            let node = map.node(NodeId::new(4)).unwrap().test_node();
            let before = without_marker(&node.get_pg(source.id().pg_id().get()).unwrap());
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                owner.test_publish_cleanup(&section, |at| {
                    if at == stage {
                        assert!(!unwind, "interrupt cleanup publication at {stage}");
                        return Err(injected());
                    }
                    Ok(())
                })
            }));
            if unwind {
                assert!(outcome.is_err());
            } else {
                assert!(outcome
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("injected cleanup publication"));
            }
            assert_eq!(owner.cleanup_publication(), P::Unconfirmed);
            assert_eq!(owner.cleanup_installation(), I::Installed);
            assert_eq!(owner.installation_observation(), history);
            owner.assert_contexts_for_test(&source, &cleanup, &topology());
            let pg = node.get_pg(source.id().pg_id().get()).unwrap();
            assert!(pg.conn.is_autocommit());
            pg.require_usable_connection().unwrap();
            assert_eq!(without_marker(&pg), before);
            assert_eq!(started(&pg), stage == "after-commit");
            drop(pg);
            owner.publish_cleanup(&section).unwrap();
            assert_eq!(owner.cleanup_publication(), P::MarkerConfirmed);
            assert_eq!(owner.cleanup_installation(), I::Installed);
            assert_eq!(owner.installation_observation(), history);
        }
    }
}

#[test]
fn staging_abandoned_part_publication_revalidates_source_slot_and_indexes() {
    for sql in [
        "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
        "DELETE FROM metadata_command_pending_route_dependencies",
        "DELETE FROM metadata_command_pending_staging_protection",
        "DELETE FROM metadata_command_log",
        "DELETE FROM metadata_command_pending_slot",
    ] {
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
        let mut owner = prepared(&map, &admission, &section, &source, "uncertain", deadline());
        owner.install_cleanup(&section).unwrap();
        let history = owner.installation_observation();
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(source.id().pg_id().get()).unwrap();
        pg.conn.execute_batch(sql).unwrap();
        let before = (snapshot(&pg), pg.conn.total_changes());
        drop(pg);
        owner.publish_cleanup(&section).expect_err(sql);
        assert_eq!(owner.cleanup_publication(), P::Unconfirmed);
        assert_eq!(owner.cleanup_installation(), I::Installed);
        assert_eq!(owner.installation_observation(), history);
        owner.assert_contexts_for_test(&source, &cleanup, &topology());
        let pg = node.get_pg(source.id().pg_id().get()).unwrap();
        assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
    }
}

#[test]
fn staging_abandoned_part_publication_original_cutoff_reaches_marker_and_commit() {
    for stage in ["before-marker", "before-commit"] {
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
        let mut owner = prepared(&map, &admission, &section, &source, "uncertain", cutoff);
        owner.install_cleanup(&section).unwrap();
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let before = snapshot(&node.get_pg(source.id().pg_id().get()).unwrap());
        let mut reached = false;
        let error = owner
            .test_publish_cleanup(&section, |at| {
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
        assert_eq!(owner.cleanup_publication(), P::Unconfirmed);
        assert_eq!(owner.cleanup_installation(), I::Installed);
        drop(section);
        let renewed = route.lock_until(deadline()).unwrap();
        owner
            .test_publish_cleanup(&renewed, |_| {
                panic!("expired source reached publication retry")
            })
            .unwrap_err();
        assert_eq!(owner.cleanup_publication(), P::Unconfirmed);
        owner.assert_contexts_for_test(&source, &cleanup, &topology());
        assert_eq!(
            snapshot(&node.get_pg(source.id().pg_id().get()).unwrap()),
            before
        );
    }
}

#[test]
fn staging_abandoned_part_publication_foreign_section_preserves_every_history_state() {
    let dir = test_util::tempdir();
    let (map, handle, source, cleanup) = setup(&dir, 2);
    let foreign_dir = test_util::tempdir();
    let (foreign_map, foreign_handle, _, _) = setup(&foreign_dir, 2);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            source.id().pg_id(),
            source.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let mut owner = prepared(&map, &admission, &section, &source, "uncertain", deadline());
    owner.install_cleanup(&section).unwrap();
    let history = owner.installation_observation();
    let foreign_admission = foreign_handle.admit_current_route().unwrap();
    let foreign_route = foreign_admission
        .embedded_publication_route(
            NodeId::new(4),
            source.id().pg_id(),
            source.id().cluster_epoch(),
        )
        .unwrap();
    let foreign_section = foreign_route.lock_until(deadline()).unwrap();
    let observe = |map: &LocalClusterMap| {
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(source.id().pg_id().get()).unwrap();
        (snapshot(&pg), pg.conn.total_changes())
    };
    for expected in [P::NoAttemptObserved, P::Unconfirmed, P::MarkerConfirmed] {
        if expected == P::Unconfirmed {
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
        if expected == P::MarkerConfirmed {
            owner.publish_cleanup(&section).unwrap();
        }
        let before = (observe(&map), observe(&foreign_map));
        owner
            .test_publish_cleanup(&foreign_section, |_| {
                panic!("foreign section reached publication")
            })
            .unwrap_err();
        assert_eq!(owner.cleanup_publication(), expected);
        assert_eq!(owner.cleanup_installation(), I::Installed);
        assert_eq!(owner.installation_observation(), history);
        owner.assert_contexts_for_test(&source, &cleanup, &topology());
        assert_eq!((observe(&map), observe(&foreign_map)), before);
    }
}

#[test]
fn staging_abandoned_part_publication_uncertain_applied_replay_does_not_repeat_transfers() {
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
    let mut owner = prepared(&map, &admission, &section, &source, "uncertain", deadline());
    owner.install_cleanup(&section).unwrap();
    owner
        .test_publish_cleanup(&section, |at| {
            if at == "after-commit" {
                Err(injected())
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert_eq!(owner.cleanup_publication(), P::Unconfirmed);
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(source.id().pg_id().get()).unwrap();
    apply(&pg, &cleanup);
    let before = (snapshot(&pg), pg.conn.total_changes());
    drop(pg);
    let mut inspected = false;
    owner
        .test_publish_cleanup(&section, |at| {
            assert_ne!(
                at, "after-trial",
                "applied replay must not repeat proof transfers"
            );
            inspected |= at == "before-commit";
            Ok(())
        })
        .unwrap();
    assert!(inspected);
    assert_eq!(owner.cleanup_publication(), P::MarkerConfirmed);
    assert_eq!(owner.cleanup_installation(), I::Installed);
    owner.assert_contexts_for_test(&source, &cleanup, &topology());
    let pg = node.get_pg(source.id().pg_id().get()).unwrap();
    assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
}
