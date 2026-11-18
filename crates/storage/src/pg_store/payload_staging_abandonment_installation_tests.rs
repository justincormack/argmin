// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One real pending replacement and retained history. No shard effects or live
//! builder selection. Faults use the same transaction as admitted installation.

use super::abandonment_preparation::{handoff, setup};
use super::*;
use crate::node_runtime::pg_store::AbandonedPartInstallationObservation as O;

fn injected() -> StoreError {
    StoreError::OperationDeadlineExceeded {
        context: "injected abandoned part installation failure",
    }
}

#[test]
fn staging_abandonment_installation_preserves_source_history_resources_and_exact_replay() {
    for count in [0, 2] {
        for origin in ["inspected", "installed", "uncertain"] {
            let dir = test_util::tempdir();
            let (map, handle, context, cleanup) = setup(&dir, count);
            let admission = handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    context.id().pg_id(),
                    context.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let source = handoff(&map, &admission, &section, &context, origin, deadline());
            let history = source.installation_observation();
            let mut prepared = source
                .prepare_part_cleanup(&section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let node = map.node(NodeId::new(4)).unwrap().test_node();
            let pg = node.get_pg(context.id().pg_id().get()).unwrap();
            let state = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let log = complete_maintenance::log_state(&pg);
            let before = snapshot(&pg);
            drop(pg);
            assert_eq!(prepared.cleanup_installation(), O::NoAttemptObserved);
            prepared.install_cleanup(&section).unwrap();
            assert_eq!(prepared.cleanup_installation(), O::Installed);
            assert_eq!(prepared.installation_observation(), history);
            prepared.assert_contexts_for_test(&context, &cleanup, &topology());
            let pg = node.get_pg(context.id().pg_id().get()).unwrap();
            assert_ne!(snapshot(&pg), before);
            assert_eq!(
                introduction::state(&pg),
                state,
                "no resource release or owner mutation"
            );
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            assert_eq!(complete_maintenance::log_state(&pg), log);
            let pending = pg
                .inspect_complete_pending_command(4, &topology())
                .unwrap()
                .unwrap();
            assert_eq!(pending.id, cleanup.id());
            assert!(!pending.publication_started);
            let after = (snapshot(&pg), pg.conn.total_changes());
            drop(pg);
            prepared
                .test_install_cleanup(&section, |stage| {
                    assert_ne!(stage, "before-effect");
                    Ok(())
                })
                .unwrap();
            drop(prepared);
            let pg = node.get_pg(context.id().pg_id().get()).unwrap();
            assert_eq!(
                (snapshot(&pg), pg.conn.total_changes()),
                after,
                "replay and Drop must not mutate"
            );
        }
    }
}

#[test]
fn staging_abandonment_installation_confirmed_retry_never_redispatches_or_regresses() {
    for sql in [
        "",
        "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
        "DELETE FROM metadata_command_pending_route_dependencies",
        "DELETE FROM metadata_command_pending_staging_protection",
        "DELETE FROM metadata_command_pending_slot",
    ] {
        let dir = test_util::tempdir();
        let (map, handle, context, cleanup) = setup(&dir, 2);
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                context.id().pg_id(),
                context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let source = handoff(
            &map,
            &admission,
            &section,
            &context,
            "uncertain",
            deadline(),
        );
        let history = source.installation_observation();
        let mut prepared = source
            .prepare_part_cleanup(&section)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        prepared.install_cleanup(&section).unwrap();
        assert_eq!(prepared.cleanup_installation(), O::Installed);
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(context.id().pg_id().get()).unwrap();
        pg.conn.execute_batch(sql).unwrap();
        let before = (snapshot(&pg), pg.conn.total_changes());
        drop(pg);
        let mut called = false;
        prepared
            .test_install_cleanup(&section, |_| {
                called = true;
                Err(injected())
            })
            .unwrap();
        assert!(
            !called,
            "confirmed installation reached storage retry: {sql}"
        );
        assert_eq!(prepared.cleanup_installation(), O::Installed);
        assert_eq!(prepared.installation_observation(), history);
        prepared.assert_contexts_for_test(&context, &cleanup, &topology());
        let pg = node.get_pg(context.id().pg_id().get()).unwrap();
        assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
    }
}

#[test]
fn staging_abandonment_installation_confirmed_retry_still_rejects_expired_section() {
    let dir = test_util::tempdir();
    let (map, handle, context, cleanup) = setup(&dir, 2);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            context.id().pg_id(),
            context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let source = handoff(
        &map,
        &admission,
        &section,
        &context,
        "uncertain",
        deadline(),
    );
    let history = source.installation_observation();
    let mut prepared = source
        .prepare_part_cleanup(&section)
        .unwrap_or_else(|e| panic!("{}", e.error()));
    prepared.install_cleanup(&section).unwrap();
    assert_eq!(prepared.cleanup_installation(), O::Installed);
    drop(section);
    let cutoff = Instant::now() + Duration::from_secs(2);
    let expired_section = route.lock_until(cutoff).unwrap();
    std::thread::sleep(cutoff.saturating_duration_since(Instant::now()) + Duration::from_millis(1));
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(context.id().pg_id().get()).unwrap();
    let before = (snapshot(&pg), pg.conn.total_changes());
    drop(pg);
    let error = prepared
        .test_install_cleanup(&expired_section, |_| {
            panic!("expired section reached storage retry")
        })
        .unwrap_err();
    assert!(error.to_string().contains("deadline"), "{error}");
    assert_eq!(prepared.cleanup_installation(), O::Installed);
    assert_eq!(prepared.installation_observation(), history);
    prepared.assert_contexts_for_test(&context, &cleanup, &topology());
    let pg = node.get_pg(context.id().pg_id().get()).unwrap();
    assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
}

#[test]
fn staging_abandonment_installation_errors_and_unwind_keep_pair_retryable() {
    for stage in [
        "before-effect",
        "after-slot",
        "before-commit",
        "after-commit",
    ] {
        for unwind in [false, true] {
            let dir = test_util::tempdir();
            let (map, handle, context, cleanup) = setup(&dir, 2);
            let admission = handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    context.id().pg_id(),
                    context.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let source = handoff(
                &map,
                &admission,
                &section,
                &context,
                "uncertain",
                deadline(),
            );
            let history = source.installation_observation();
            let mut prepared = source
                .prepare_part_cleanup(&section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let node = map.node(NodeId::new(4)).unwrap().test_node();
            let before = snapshot(&node.get_pg(context.id().pg_id().get()).unwrap());
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                prepared.test_install_cleanup(&section, |at| {
                    if at == stage {
                        assert!(!unwind, "interrupt cleanup installation at {stage}");
                        return Err(injected());
                    }
                    Ok(())
                })
            }));
            if unwind {
                assert!(result.is_err());
            } else {
                assert!(result
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("injected abandoned part"));
            }
            assert_eq!(prepared.cleanup_installation(), O::Unconfirmed);
            assert_eq!(prepared.installation_observation(), history);
            prepared.assert_contexts_for_test(&context, &cleanup, &topology());
            let pg = node.get_pg(context.id().pg_id().get()).unwrap();
            assert!(pg.conn.is_autocommit());
            pg.require_usable_connection().unwrap();
            assert_eq!(snapshot(&pg) == before, stage != "after-commit");
            drop(pg);
            prepared.install_cleanup(&section).unwrap();
            assert_eq!(prepared.cleanup_installation(), O::Installed);
            assert_eq!(prepared.installation_observation(), history);
        }
    }
}

#[test]
fn staging_abandonment_installation_revalidates_current_closure_in_write_transaction() {
    let dir = test_util::tempdir();
    let (map, handle, context, cleanup, (append, next)) =
        super::abandonment_preparation::setup_with_seed(&dir, 0, |pg, owner| {
            (append::command(owner), append::next(pg, owner))
        });
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            context.id().pg_id(),
            context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let source = handoff(
        &map,
        &admission,
        &section,
        &context,
        "uncertain",
        deadline(),
    );
    let mut prepared = source
        .prepare_part_cleanup(&section)
        .unwrap_or_else(|e| panic!("{}", e.error()));
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(context.id().pg_id().get()).unwrap();
    let prefix = pg.metadata_command_replica_state().unwrap();
    // Same command-log prefix, but a new coherent staged segment/owner window.
    // This isolates closure recapture from the independent prefix comparison.
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    // Capture precedes the runtime reopen/epoch transition: that transition
    // revokes fresh allocation by the old issuer, not an already captured
    // append's deterministic application. Keep the prefix unchanged here to
    // isolate exact closure comparison from the separate prefix check.
    super::super::super::descriptor::apply_append_for_test(&pg, &topology(), &append, &next)
        .unwrap();
    pg.conn.execute_batch("COMMIT").unwrap();
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    let before = snapshot(&pg);
    drop(pg);
    let error = prepared.install_cleanup(&section).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("closure changed since preparation"),
        "{error}"
    );
    prepared.assert_contexts_for_test(&context, &cleanup, &topology());
    assert_eq!(prepared.cleanup_installation(), O::Unconfirmed);
    assert_eq!(
        snapshot(&node.get_pg(context.id().pg_id().get()).unwrap()),
        before
    );
}

#[test]
fn staging_abandonment_installation_exact_retry_rejects_damaged_indexes_without_repair() {
    for sql in [
        "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
        "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed'",
        "DELETE FROM metadata_command_pending_route_dependencies",
        "DELETE FROM metadata_command_pending_staging_protection",
        "DELETE FROM metadata_command_pending_placed_reference_pages",
    ] {
        let dir = test_util::tempdir();
        let (map, handle, context, cleanup) = setup(&dir, 2);
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                context.id().pg_id(),
                context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let source = handoff(
            &map,
            &admission,
            &section,
            &context,
            "uncertain",
            deadline(),
        );
        let mut prepared = source
            .prepare_part_cleanup(&section)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        prepared
            .test_install_cleanup(&section, |at| {
                if at == "after-commit" {
                    Err(injected())
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(context.id().pg_id().get()).unwrap();
        pg.conn.execute_batch(sql).unwrap();
        let before = (snapshot(&pg), pg.conn.total_changes());
        drop(pg);
        prepared.install_cleanup(&section).expect_err(sql);
        assert_eq!(prepared.cleanup_installation(), O::Unconfirmed);
        prepared.assert_contexts_for_test(&context, &cleanup, &topology());
        let pg = node.get_pg(context.id().pg_id().get()).unwrap();
        assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
    }
}

#[test]
fn staging_abandonment_installation_fences_first_effect_and_commit_and_never_resets_uncertainty() {
    for stage in ["before-effect", "before-commit"] {
        let dir = test_util::tempdir();
        let (map, handle, context, _) = setup(&dir, 2);
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                context.id().pg_id(),
                context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let cutoff = Instant::now() + Duration::from_secs(2);
        let source = handoff(&map, &admission, &section, &context, "uncertain", cutoff);
        let mut prepared = source
            .prepare_part_cleanup(&section)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let before = snapshot(&node.get_pg(context.id().pg_id().get()).unwrap());
        let mut reached = false;
        let error = prepared
            .test_install_cleanup(&section, |at| {
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
        assert_eq!(prepared.cleanup_installation(), O::Unconfirmed);
        drop(section);
        let renewed_section = route.lock_until(deadline()).unwrap();
        assert!(prepared.install_cleanup(&renewed_section).is_err());
        assert_eq!(prepared.cleanup_installation(), O::Unconfirmed);
        assert_eq!(
            snapshot(&node.get_pg(context.id().pg_id().get()).unwrap()),
            before
        );
    }
}

#[test]
fn staging_abandonment_installation_replay_preserves_started_and_applied_cleanup() {
    for applied in [false, true] {
        let dir = test_util::tempdir();
        let (map, handle, context, cleanup) = setup(&dir, 2);
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                context.id().pg_id(),
                context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let source = handoff(
            &map,
            &admission,
            &section,
            &context,
            "uncertain",
            deadline(),
        );
        let mut prepared = source
            .prepare_part_cleanup(&section)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        prepared
            .test_install_cleanup(&section, |at| {
                if at == "after-commit" {
                    Err(injected())
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert_eq!(prepared.cleanup_installation(), O::Unconfirmed);
        let evidence = pending(&cleanup);
        section
            .start_complete_publication(
                &evidence
                    .publication_request(NodeId::new(4), &topology())
                    .unwrap(),
                deadline(),
            )
            .unwrap();
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(context.id().pg_id().get()).unwrap();
        if applied {
            apply(&pg, &cleanup);
        }
        let before = (snapshot(&pg), pg.conn.total_changes());
        drop(pg);
        let mut inspected = false;
        prepared
            .test_install_cleanup(&section, |at| {
                assert_ne!(at, "before-effect");
                inspected |= at == "before-commit";
                Ok(())
            })
            .unwrap();
        assert!(inspected, "uncertain replay must validate durable state");
        assert_eq!(prepared.cleanup_installation(), O::Installed);
        let pg = node.get_pg(context.id().pg_id().get()).unwrap();
        assert!(
            pg.inspect_complete_pending_command(4, &topology())
                .unwrap()
                .unwrap()
                .publication_started
        );
        assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
    }
}

#[test]
fn staging_abandonment_installation_prefix_change_rejects_without_recapture_or_effects() {
    let dir = test_util::tempdir();
    let (map, handle, context, cleanup) = setup(&dir, 2);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            context.id().pg_id(),
            context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let source = handoff(
        &map,
        &admission,
        &section,
        &context,
        "uncertain",
        deadline(),
    );
    let mut prepared = source
        .prepare_part_cleanup(&section)
        .unwrap_or_else(|e| panic!("{}", e.error()));
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(context.id().pg_id().get()).unwrap();
    let subject = fixture(multipart_target()).subject;
    let other = crate::metadata_command::MetadataCommandEnvelope::new(
        cleanup.id(),
        crate::metadata_command::MetadataCommandPayload::ReserveObjectVersion(
            crate::metadata_command::ReserveObjectVersionCommand::new(
                subject.bucket,
                subject.key,
                crate::VersionId::from_u64(500),
            ),
        ),
    );
    let other = application::ordinary_context(&other, &topology()).unwrap();
    apply(&pg, &other);
    assert_ne!(
        pg.metadata_command_replica_state().unwrap(),
        prepared.prefix()
    );
    let before = (snapshot(&pg), pg.conn.total_changes());
    drop(pg);
    let error = prepared
        .install_cleanup(&section)
        .unwrap_err()
        .into_store_error()
        .unwrap();
    assert!(
        matches!(error, StoreError::MetadataCommandLogConflict { .. }),
        "{error}"
    );
    prepared.assert_contexts_for_test(&context, &cleanup, &topology());
    let pg = node.get_pg(context.id().pg_id().get()).unwrap();
    assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
}

#[test]
fn staging_abandonment_installation_foreign_admission_cannot_mutate_or_reset_history() {
    let dir = test_util::tempdir();
    let (map, handle, context, _) = setup(&dir, 2);
    let foreign_dir = test_util::tempdir();
    let (foreign_map, foreign_handle, _, _) = setup(&foreign_dir, 2);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            context.id().pg_id(),
            context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let source = handoff(
        &map,
        &admission,
        &section,
        &context,
        "uncertain",
        deadline(),
    );
    let history = source.installation_observation();
    let mut prepared = source
        .prepare_part_cleanup(&section)
        .unwrap_or_else(|e| panic!("{}", e.error()));
    let foreign_admission = foreign_handle.admit_current_route().unwrap();
    let foreign_route = foreign_admission
        .embedded_publication_route(
            NodeId::new(4),
            context.id().pg_id(),
            context.id().cluster_epoch(),
        )
        .unwrap();
    let foreign_section = foreign_route.lock_until(deadline()).unwrap();
    let observe = |map: &crate::LocalClusterMap| {
        snapshot(
            &map.node(NodeId::new(4))
                .unwrap()
                .test_node()
                .get_pg(context.id().pg_id().get())
                .unwrap(),
        )
    };
    for expected in [O::NoAttemptObserved, O::Unconfirmed, O::Installed] {
        if expected == O::Unconfirmed {
            prepared
                .test_install_cleanup(&section, |at| {
                    if at == "after-commit" {
                        Err(injected())
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
        }
        if expected == O::Installed {
            prepared.install_cleanup(&section).unwrap();
        }
        let before = (observe(&map), observe(&foreign_map));
        prepared
            .test_install_cleanup(&foreign_section, |_| {
                panic!("foreign admission reached transaction")
            })
            .unwrap_err();
        assert_eq!(prepared.cleanup_installation(), expected);
        assert_eq!(prepared.installation_observation(), history);
        assert_eq!((observe(&map), observe(&foreign_map)), before);
    }
    prepared.install_cleanup(&section).unwrap();
    assert_eq!(prepared.cleanup_installation(), O::Installed);
}

#[test]
fn staging_abandonment_installation_failed_rollback_retires_connection_until_reopen() {
    let dir = test_util::tempdir();
    let (before, pg_id) = {
        let (map, handle, context, _) = setup(&dir, 2);
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                context.id().pg_id(),
                context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let source = handoff(
            &map,
            &admission,
            &section,
            &context,
            "uncertain",
            deadline(),
        );
        let mut prepared = source
            .prepare_part_cleanup(&section)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let pg_id = context.id().pg_id().get();
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(pg_id).unwrap();
        let before = snapshot(&pg);
        pg.fail_metadata_command_rollbacks.set(1);
        drop(pg);
        let error = prepared
            .test_install_cleanup(&section, |at| {
                if at == "after-slot" {
                    Err(injected())
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert!(error.to_string().contains("rollback failed"), "{error}");
        assert_eq!(prepared.cleanup_installation(), O::Unconfirmed);
        let access = node
            .get_pg(pg_id)
            .err()
            .expect("retired PG remained accessible");
        assert!(
            access.to_string().contains("requires reopen and recovery"),
            "{access}"
        );
        assert!(prepared.install_cleanup(&section).is_err());
        assert_eq!(prepared.cleanup_installation(), O::Unconfirmed);
        (before, pg_id)
    };
    // All original runtimes/handles are gone before reopening these files.
    let pg = PgStore::open(&dir.path().join("4").join(format!("pg-{pg_id:04}")), pg_id).unwrap();
    pg.require_usable_connection().unwrap();
    assert_eq!(snapshot(&pg), before);
}
