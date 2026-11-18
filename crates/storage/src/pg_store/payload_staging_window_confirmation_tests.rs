// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact active-frontier confirmation, not physical write authority.

use super::super::complete_log_range::ordinary;
use super::*;
use crate::node_runtime::pg_store::{PgStoreRecoveryContext, StagingWindowConfirmationError as E};
use crate::{LocalClusterMap, StorageClusterRouteHandle};
use std::sync::Arc;

#[path = "payload_staging_window_confirmation_remote_tests.rs"]
mod remote;

#[path = "payload_staging_stream_window_allocation_tests.rs"]
mod allocation;
#[path = "payload_staging_direct_file_tests.rs"]
mod files;

#[path = "payload_staging_embedded_file_dispatch_tests.rs"]
mod embedded_dispatch;
#[path = "payload_staging_window_handoff_tests.rs"]
mod handoff;
#[path = "payload_staging_window_confirmation_primary_tests.rs"]
mod primary;
#[path = "payload_staging_window_receiver_tests.rs"]
mod receiver;
#[path = "payload_staging_direct_file_receiver_tests.rs"]
mod receiver_files;
#[path = "payload_staging_stream_file_tests.rs"]
mod stream_files;

#[path = "payload_staging_acknowledgement_tests.rs"]
mod acknowledgements;

#[path = "payload_staging_direct_introduction_tests.rs"]
mod direct_introduction;

struct Setup {
    _dir: test_util::TempDir,
    map: Arc<LocalClusterMap>,
    handle: StorageClusterRouteHandle,
    context: ContextualCommand,
}

fn setup(target: StagingTarget, append_window: bool) -> Setup {
    let (f, creation_proof) = unsettled_setup(target, append_window);
    let Setup {
        map,
        handle,
        context,
        ..
    } = &f;
    let pg_id = context.id().pg_id();
    {
        if let Some(proof) = &creation_proof {
            let node = map.node(NodeId::new(4)).unwrap().test_node();
            let pg = node
                .get_pg(topology().bucket_pg_for(&proof.bucket))
                .unwrap();
            terminal_release::seed_pg_reservations(&pg, proof, None);
        }
        let admission = handle.admit_current_route().unwrap();
        let mut fanout = fanout_embedded::sender(map, context, "installed", deadline())
            .into_fanout(
                admission
                    .complete_fanout_route(pg_id, context.id().cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let route = admission
            .embedded_publication_route(NodeId::new(4), pg_id, context.id().cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        fanout.publish_embedded(&section, deadline()).unwrap();
        for member in [0, 4, 2, 3, 1] {
            fanout
                .apply_embedded(&section, NodeId::new(member), deadline())
                .unwrap();
        }
        let mut terminal = fanout
            .into_terminal()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        if creation_proof.is_some() {
            terminal.release_reservation().unwrap();
        }
        terminal.retire_embedded(&section, deadline()).unwrap();
    }
    f
}

fn unsettled_setup(
    target: StagingTarget,
    append_window: bool,
) -> (Setup, Option<BucketWriteReservationProof>) {
    let release_creation = matches!(target, StagingTarget::UploadPart { .. }) && !append_window;
    let seed_dir = test_util::tempdir();
    let mut template = fresh_capture::template(target);
    let pg_id = template.subject.object_pg;
    let seed = PgStore::open(seed_dir.path(), pg_id.get()).unwrap();
    let digest = seed.metadata_command_replica_state().unwrap().state_digest;
    seed.initialize_metadata_transfer_empty_state(4, config(1).cluster_epoch, digest)
        .unwrap();
    let command = fresh_capture::candidate(&seed, &mut template);
    let creation_proof = command
        .payload()
        .primary_bucket_write_reservation_proof()
        .cloned();
    let intro =
        ContextualCommand::from_described(candidate::capture(&seed, &template, command, false));
    complete_application::anchor(&seed, intro.id());
    let context = if append_window {
        apply(&seed, &intro);
        let snapshot = seed.command_build_snapshot().unwrap();
        let owner = append::load(&seed, &template);
        snapshot.finish().unwrap();
        fresh_append_capture::prepare(&seed, &owner);
        ContextualCommand::from_described(candidate::capture(
            &seed,
            &owner,
            append::command(&owner),
            true,
        ))
    } else {
        intro
    };
    let dir = test_util::tempdir();
    let (map, handle, context) = application_embedded::from_seed_without_pending(
        &dir,
        &seed,
        context,
        1,
        RouteMapValidity::Forever,
    );
    for member in 0..5 {
        map.node(NodeId::new(member))
            .unwrap()
            .test_node()
            .get_pg(pg_id.get())
            .unwrap()
            .recover_complete_command_state(
                PgStoreRecoveryContext::for_node(NodeId::new(member)),
                &topology(),
            )
            .unwrap();
    }
    (
        Setup {
            _dir: dir,
            map,
            handle,
            context,
        },
        creation_proof.filter(|_| release_creation),
    )
}

#[test]
fn staging_window_confirmation_all_targets_is_read_only_and_holds_primary_serialization() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };
    let _clock = crate::clock::test_time_override_guard(2_000);
    for (target, append_window) in candidate::cases() {
        let f = setup(target, append_window);
        let id = f.context.id();
        let writes = (0..5)
            .map(|node| {
                f.map
                    .node(NodeId::new(node))
                    .unwrap()
                    .test_node()
                    .get_pg(id.pg_id().get())
                    .unwrap()
                    .conn
                    .total_changes()
            })
            .collect::<Vec<_>>();
        let commits = Arc::new(AtomicUsize::new(0));
        let statements = Arc::new(Mutex::new(Vec::new()));
        for node in [0, 4] {
            let commits = Arc::clone(&commits);
            let statements = Arc::clone(&statements);
            f.map
                .node(NodeId::new(node))
                .unwrap()
                .test_node()
                .get_pg(id.pg_id().get())
                .unwrap()
                .conn
                .trace_v2(
                    TraceEventCodes::SQLITE_TRACE_STMT,
                    Some(move |event: TraceEvent<'_>| {
                        if let TraceEvent::Stmt(_, sql) = event {
                            if sql == "COMMIT" {
                                commits.fetch_add(1, Ordering::Relaxed);
                            }
                            statements.lock().unwrap().push(sql.to_owned());
                        }
                    }),
                )
                .unwrap();
        }
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        for _ in 0..2 {
            let observation = f
                .context
                .inspect_window_confirmation_embedded(&section, deadline(), |_| {
                    assert!(f
                        .map
                        .runtime_state()
                        .metadata_command_pg_lock(id.pg_id())
                        .try_lock()
                        .is_none());
                })
                .unwrap();
            assert_eq!(observation.witness, Some(NodeId::new(0)));
            assert_eq!(observation.prefix.applied_log_index, id.log_index().get());
            assert_eq!(observation.prefix.cluster_epoch, id.cluster_epoch());
        }
        assert_eq!(commits.load(Ordering::Relaxed), 0);
        let statements = statements.lock().unwrap();
        assert!(statements
            .iter()
            .any(|sql| sql.contains("payload_staging_windows") && sql.contains("LIMIT 1")));
        assert!(!statements
            .iter()
            .any(|sql| sql.contains("WHERE pg_id <>") || sql.contains("COUNT(*)")));
        for node in 0..5 {
            assert_eq!(
                f.map
                    .node(NodeId::new(node))
                    .unwrap()
                    .test_node()
                    .get_pg(id.pg_id().get())
                    .unwrap()
                    .conn
                    .total_changes(),
                writes[node as usize]
            );
        }
    }
}

#[test]
fn staging_window_confirmation_uses_designated_witness_not_an_agreeing_peer() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let f = setup(StagingTarget::DirectPut, false);
    let id = f.context.id();
    // All owner/window rows stay identical. Advance ONLY the designated witness
    // with a real unrelated command, preserving its warm validated prefix.
    let next = MetadataCommandId::new(
        id.cluster_epoch(),
        id.pg_id(),
        MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
    );
    let unrelated =
        ContextualCommand::decode_field(ordinary(next), &[1, 0, 0], &topology()).unwrap();
    apply(
        &f.map
            .node(NodeId::new(0))
            .unwrap()
            .test_node()
            .get_pg(id.pg_id().get())
            .unwrap(),
        &unrelated,
    );
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let error = f
        .context
        .inspect_window_confirmation_embedded(&section, deadline(), |_| {})
        .unwrap_err();
    assert!(matches!(error, E::PrefixDisagreement), "{error}");
    // Singleton semantics are deliberate, not a fallback after that failure.
    drop(section);
    drop(route);
    drop(admission);
    let routes = config(1).pg_routes.into_iter().map(|route| {
        crate::control_plane::PgRouteSnapshot::reconstructed(
            route.cluster_epoch,
            PgId::new(route.pg_id),
            NodeId::new(4),
            vec![NodeId::new(4)],
            PgState::Active,
        )
    });
    let map = Arc::new(
        f.map
            .test_clone_with_pg_routes(id.cluster_epoch(), routes, [])
            .unwrap(),
    );
    let handle = StorageClusterRouteHandle::from_authorized_cluster(
        crate::StorageCluster::test_from_local_map_with_epoch(map, id.cluster_epoch()).unwrap(),
    );
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    assert!(f
        .context
        .inspect_window_confirmation_embedded(&section, deadline(), |_| {})
        .unwrap()
        .witness
        .is_none());
}

#[test]
fn staging_window_confirmation_rechecks_window_and_work_cutoffs_after_witness() {
    for stage in ["before witness", "after witness"] {
        for bound in ["window", "work"] {
            let clock = crate::clock::test_time_override_guard(2_000);
            let f = setup(StagingTarget::DirectPut, false);
            let id = f.context.id();
            let admission = f.handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let cutoff = Instant::now() + Duration::from_secs(1);
            let mut reached = false;
            let error = f
                .context
                .inspect_window_confirmation_embedded(&section, cutoff, |point| {
                    if point == stage {
                        assert!(
                            Instant::now() < cutoff,
                            "missed intended expiry interleaving"
                        );
                        reached = true;
                        if bound == "window" {
                            clock.set(1_000_000);
                        } else {
                            std::thread::sleep(cutoff.saturating_duration_since(Instant::now()));
                        }
                    }
                })
                .unwrap_err();
            assert!(reached, "{stage}/{bound}: {error}");
            assert!(
                match bound {
                    "window" => matches!(error, E::Store(StoreError::RouteMapExpired { .. })),
                    "work" => matches!(
                        error,
                        E::Store(StoreError::OperationDeadlineExceeded { .. })
                    ),
                    _ => unreachable!(),
                },
                "{stage}/{bound}: {error}"
            );
        }
    }
}

#[test]
fn staging_window_confirmation_witness_lock_wait_is_bounded_without_mutation() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let f = setup(StagingTarget::DirectPut, false);
    let id = f.context.id();
    let node = f.map.node(NodeId::new(0)).unwrap().test_node();
    let held = node.get_pg(id.pg_id().get()).unwrap();
    let before = snapshot(&held);
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let started = Instant::now();
    let error = f
        .context
        .inspect_window_confirmation_embedded(&section, started + Duration::from_millis(20), |_| {})
        .unwrap_err();
    assert!(error.to_string().contains("deadline"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(snapshot(&held), before);
}

#[test]
fn staging_window_confirmation_rejects_pending_or_unvalidated_member() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    for member in [0, 4] {
        for pending_slot in [false, true] {
            let f = setup(StagingTarget::DirectPut, false);
            let id = f.context.id();
            let node = f.map.node(NodeId::new(member)).unwrap().test_node();
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            if pending_slot {
                let next = MetadataCommandId::new(
                    id.cluster_epoch(),
                    id.pg_id(),
                    MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
                );
                let other =
                    ContextualCommand::decode_field(ordinary(next), &[1, 0, 0], &topology())
                        .unwrap();
                // Closed insertion preserves validation. Rejection must be due
                // to the actual pending barrier, not an invalidated test cache.
                install(&pg, &other);
            } else {
                pg.complete_prefix_validation.invalidate();
            }
            let before = snapshot(&pg);
            let writes = pg.conn.total_changes();
            drop(pg);
            let admission = f.handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let error = f
                .context
                .inspect_window_confirmation_embedded(&section, deadline(), |_| {})
                .unwrap_err();
            if pending_slot {
                assert!(error.to_string().contains("pending command"), "{error}");
            } else {
                assert!(
                    error
                        .to_string()
                        .contains("requires explicit local validation"),
                    "{error}"
                );
            }
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            assert_eq!(snapshot(&pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
        }
    }
}

#[test]
fn staging_window_confirmation_matches_each_immutable_descriptor_field() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    for (target, append_window) in candidate::cases() {
        let f = setup(target, append_window);
        let id = f.context.id();
        let pg = f
            .map
            .node(NodeId::new(4))
            .unwrap()
            .test_node()
            .get_pg(id.pg_id().get())
            .unwrap();
        let read = pg.command_build_snapshot().unwrap();
        let attempt: crate::SessionId = pg
            .conn
            .query_row("SELECT attempt FROM payload_staging_owners", [], |row| {
                row.get(0)
            })
            .unwrap();
        let owner = sql::load(&pg.conn, id.pg_id(), &attempt).unwrap().unwrap();
        let window = owner.windows.last().unwrap();
        let creation = match &owner.resources.creation {
            ResourceResponsibility::Live(proof) | ResourceResponsibility::ReleaseDue(proof) => {
                proof
            }
            ResourceResponsibility::NoResource => panic!("fixture must own creation"),
        };
        sql::require_current_window(
            &pg.conn,
            &owner.subject,
            window,
            Some(creation),
            &topology(),
        )
        .unwrap();
        let writes = pg.conn.total_changes();
        // These call the exact matcher under its read snapshot independently
        // of cache invalidation, so a removed equality check is observable.
        for field in 0..10 {
            let mut subject = owner.subject.clone();
            match field {
                0 => subject.bucket = crate::tests::bucket_name("crossed-bucket"),
                1 => subject.key = crate::tests::object_key("crossed-key"),
                2 => subject.bucket_incarnation += 1,
                3 => {
                    subject.payload_generation =
                        GenerationId::new(subject.payload_generation.get() + 1).unwrap()
                }
                4 => {
                    subject.originating_epoch =
                        ClusterEpoch::new(subject.originating_epoch.get() + 1).unwrap()
                }
                5 => subject.issuer.lifetime[0] ^= 1,
                6 => subject.issuer.node_id = NodeId::new(99),
                7 => subject.id.attempt = crate::tests::stream_session_id("crossed-attempt"),
                8 => {
                    subject.id.introduction = MetadataCommandId::new(
                        id.cluster_epoch(),
                        id.pg_id(),
                        MetadataCommandLogIndex::new(99).unwrap(),
                    )
                }
                9 => {
                    subject.target = if matches!(subject.target, StagingTarget::DirectPut) {
                        StagingTarget::StreamPut
                    } else {
                        StagingTarget::DirectPut
                    }
                }
                _ => unreachable!(),
            }
            assert!(
                sql::require_current_window(
                    &pg.conn,
                    &subject,
                    window,
                    Some(creation),
                    &topology()
                )
                .is_err(),
                "subject {field}"
            );
        }
        for field in 0..14 {
            let mut crossed = window.clone();
            match field {
                0 => crossed.index += 1,
                1 => crossed.segment_hash[0] ^= 1,
                2 => crossed.start = GenerationId::new(crossed.start.get() + 1).unwrap(),
                3 => crossed.end = GenerationId::new(crossed.end.get() + 1).unwrap(),
                4 => crossed.issued_at_ms += 1,
                5 => crossed.cutoff_ms += 1,
                6 => crossed.geometry.logical_bound -= 1,
                7 => crossed.geometry.stored_bound += 1,
                8 => crossed.geometry.shard_bound += 1,
                9 => crossed.geometry.ec.k += 1,
                10 => {
                    crossed.participants.data_pg = PgId::new(crossed.participants.data_pg.get() + 1)
                }
                11 => {
                    crossed.participants.placement_epoch =
                        ClusterEpoch::new(crossed.participants.placement_epoch.get() + 1).unwrap()
                }
                12 => crossed.participants.files[0] = NodeId::new(99),
                13 => crossed.participants.acknowledgement_primary = NodeId::new(99),
                _ => unreachable!(),
            }
            assert!(
                sql::require_current_window(
                    &pg.conn,
                    &owner.subject,
                    &crossed,
                    Some(creation),
                    &topology()
                )
                .is_err(),
                "window {field}"
            );
        }
        for field in 0..3 {
            let mut crossed = creation.clone();
            match field {
                0 => crossed.identity.owner_token.push('x'),
                1 => crossed.observed_lease_deadline += 1,
                2 => crossed.release_pg = PgId::new(crossed.release_pg.get() + 1),
                _ => unreachable!(),
            }
            assert!(sql::require_current_window(
                &pg.conn,
                &owner.subject,
                window,
                Some(&crossed),
                &topology()
            )
            .is_err());
        }
        assert_eq!(pg.conn.total_changes(), writes);
        read.finish().unwrap();
    }
}

#[test]
fn staging_window_confirmation_requires_active_unselected_frontier_and_resource_fence() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let f = setup(StagingTarget::DirectPut, false);
    let id = f.context.id();
    let pg = f
        .map
        .node(NodeId::new(4))
        .unwrap()
        .test_node()
        .get_pg(id.pg_id().get())
        .unwrap();
    for sql_mutation in [
        "UPDATE payload_staging_owners SET lifecycle = 2",
        "UPDATE payload_staging_owners SET generation_state = 2",
        "UPDATE payload_staging_owners SET reserved_end = reserved_end + 1",
        "UPDATE payload_staging_owners SET maximum_cutoff_ms = 1",
        "DELETE FROM payload_staging_windows",
        "DELETE FROM payload_staging_participants",
        "DELETE FROM payload_staging_resources",
        "UPDATE payload_staging_resources SET owner_token = 'different-owner'",
        "DELETE FROM object_write_counters",
        "INSERT INTO payload_staging_references SELECT attempt, window_index, segment_hash, start_vid, logical_bound, stored_bound, 0, NULL, NULL, NULL FROM payload_staging_windows",
    ] {
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let attempt: crate::SessionId = pg.conn.query_row("SELECT attempt FROM payload_staging_owners", [], |row| row.get(0)).unwrap();
        let owner = sql::load(&pg.conn, id.pg_id(), &attempt).unwrap().unwrap();
        let window = &owner.windows[0];
        let ResourceResponsibility::Live(creation) = &owner.resources.creation else { panic!("direct PUT creation"); };
        sql::require_current_window(&pg.conn, &owner.subject, window, Some(creation), &topology()).unwrap();
        pg.conn.execute_batch(sql_mutation).unwrap();
        let writes = pg.conn.total_changes();
        assert!(sql::require_current_window(&pg.conn, &owner.subject, window, Some(creation), &topology()).is_err(), "{sql_mutation}");
        assert_eq!(pg.conn.total_changes(), writes);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_window_confirmation_does_not_renew_admission_after_witness() {
    let clock = crate::clock::test_time_override_guard(2_000);
    let f = setup(StagingTarget::DirectPut, false);
    let id = f.context.id();
    f.map
        .test_store_route_map_validity(RouteMapValidity::until_ms(3_000).unwrap());
    let handle = StorageClusterRouteHandle::from_authorized_cluster(
        crate::StorageCluster::test_from_local_map_with_epoch(
            Arc::clone(&f.map),
            id.cluster_epoch(),
        )
        .unwrap(),
    );
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let mut reached = false;
    let error = f
        .context
        .inspect_window_confirmation_embedded(&section, deadline(), |point| {
            if point == "after witness" {
                reached = true;
                f.map
                    .test_store_route_map_validity(RouteMapValidity::until_ms(100_000).unwrap());
                clock.set(4_000);
                f.map.require_route_map_valid_now().unwrap();
            }
        })
        .unwrap_err();
    assert!(reached);
    assert!(
        matches!(
            error,
            E::Store(StoreError::RouteMapExpired {
                valid_until_ms: 3_000,
                ..
            })
        ),
        "{error}"
    );
}

#[test]
fn staging_window_confirmation_unconfirmed_snapshot_teardown_retires_member() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    for member in [0, 4] {
        let f = setup(StagingTarget::DirectPut, false);
        let id = f.context.id();
        let node = f.map.node(NodeId::new(member)).unwrap().test_node();
        node.get_pg(id.pg_id().get())
            .unwrap()
            .fail_next_snapshot_rollback
            .set(true);
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let error = f
            .context
            .inspect_window_confirmation_embedded(&section, deadline(), |_| {})
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected read-snapshot rollback failure"),
            "{error}"
        );
        assert!(node.get_pg(id.pg_id().get()).is_err());
        assert!(f
            .context
            .inspect_window_confirmation_embedded(&section, deadline(), |_| {})
            .is_err());
    }
}
