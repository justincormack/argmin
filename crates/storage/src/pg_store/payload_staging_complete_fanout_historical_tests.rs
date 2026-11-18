// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete historical cohort/ordering, not live dispatch or terminal cleanup.

use super::*;
use crate::node_runtime::pg_store::{ApplicationObservation as Applied, CompleteFanoutSender};

fn observations(
    fixture: &Fixture,
) -> Vec<(super::super::super::complete_node_startup::Snapshot, u64)> {
    (0..5)
        .map(|node| {
            let pg = fixture
                .map
                .node(NodeId::new(node))
                .unwrap()
                .test_node()
                .get_pg(fixture.context.id().pg_id().get())
                .unwrap();
            (snapshot(&pg), pg.conn.total_changes())
        })
        .collect()
}

pub(super) fn check_terminal_handoff(fanout: CompleteFanoutSender<'_>) {
    let original = fanout.installation_observation();
    let id = fanout.id();
    match fanout.into_terminal() {
        Ok(terminal) => {
            assert!(terminal.work().is_quiescent() || terminal.work().has_reservation_release());
            assert_eq!(terminal.id(), id);
            assert_eq!(terminal.installation_observation(), original);
        }
        Err(failure) => {
            assert!(failure.error().to_string().contains(
                "historical terminal handoff still requires non-quiescent work integration"
            ));
            let owner = failure.into_fanout();
            assert_eq!(owner.installation_observation(), original);
            assert!(owner.completion().is_ok());
        }
    }
}

#[test]
fn staging_complete_fanout_historical_all_contexts_and_origins_keep_cohort_order_and_custody() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    for case in 0..4 {
        for origin in ["inspected", "installed", "uncertain"] {
            let dir = test_util::tempdir();
            // Acting-set order is deliberately not canonical witness order.
            let fixture = Fixture::with_members(&dir, case, &[4, 2, 0], None);
            let id = fixture.context.id();
            let admission = fixture.handle.admit_current_route().unwrap();
            let mut owner =
                fanout_embedded::sender(&fixture.map, &fixture.context, origin, deadline())
                    .into_fanout(
                        admission
                            .complete_historical_fanout_route(&fixture.runtime_map, id.pg_id())
                            .unwrap(),
                    )
                    .unwrap_or_else(|(_, e)| panic!("{e}"));
            let installed = owner.installation_observation();
            let route = admission
                .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let lock = fixture
                .map
                .runtime_state()
                .metadata_command_pg_lock(id.pg_id());
            assert!(lock.try_lock().is_none());
            let before = observations(&fixture);
            assert!(owner
                .apply_embedded(&section, NodeId::new(0), deadline())
                .unwrap_err()
                .to_string()
                .contains("publication marker is not confirmed"));
            assert_eq!(observations(&fixture), before);
            owner.publish_embedded(&section, deadline()).unwrap();
            let before = observations(&fixture);
            assert!(owner
                .apply_embedded(&section, NodeId::new(4), deadline())
                .unwrap_err()
                .to_string()
                .contains("witness confirmation"));
            assert!(owner
                .apply_embedded(&section, NodeId::new(2), deadline())
                .unwrap_err()
                .to_string()
                .contains("primary confirmation"));
            assert!(owner
                .apply_embedded(&section, NodeId::new(1), deadline())
                .is_err());
            assert_eq!(observations(&fixture), before);

            let error = owner
                .test_apply_embedded_with_hook(&section, NodeId::new(0), deadline(), |at| {
                    if at == "after-commit" {
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "lost historical witness confirmation",
                        });
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("lost historical witness confirmation"));
            assert_eq!(
                owner.application_observation(NodeId::new(0)),
                Applied::Unconfirmed
            );
            let applied = observations(&fixture);
            assert_ne!(applied[0].0, before[0].0);
            assert!(owner
                .apply_embedded(&section, NodeId::new(4), deadline())
                .unwrap_err()
                .to_string()
                .contains("witness confirmation"));
            assert!(owner.completion().is_err());
            owner = owner.into_terminal().err().unwrap().into_fanout();
            assert_eq!(owner.installation_observation(), installed);
            owner
                .apply_embedded(&section, NodeId::new(0), deadline())
                .unwrap();
            assert_eq!(
                observations(&fixture),
                applied,
                "exact lost-reply retry does no writes"
            );
            let mut expected = None;
            for (position, member) in [0, 4, 2].into_iter().enumerate() {
                owner
                    .apply_embedded(&section, NodeId::new(member), deadline())
                    .unwrap();
                let node = fixture.map.node(NodeId::new(member)).unwrap().test_node();
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                let state = (
                    introduction::state(&pg),
                    complete_maintenance::log_state(&pg),
                    pg.metadata_command_replica_state().unwrap(),
                );
                if let Some(expected) = &expected {
                    assert_eq!(&state, expected);
                } else {
                    expected = Some(state);
                }
                drop(pg);
                if position < 2 {
                    assert!(owner.completion().is_err());
                }
                assert_eq!(owner.installation_observation(), installed);
                assert!(lock.try_lock().is_none());
            }
            assert_eq!(owner.completion().unwrap().confirmed_peers(), 3);
            let complete = observations(&fixture);
            check_terminal_handoff(owner);
            assert_eq!(observations(&fixture), complete, "no cleanup on Drop");
            assert!(started(
                &fixture
                    .map
                    .node(NodeId::new(4))
                    .unwrap()
                    .test_node()
                    .get_pg(id.pg_id().get())
                    .unwrap()
            ));
            for member in [1, 3] {
                assert_eq!(complete[member], before[member]);
            }
        }
    }
}

#[test]
fn staging_complete_fanout_historical_never_shrinks_to_eligible_or_current_members() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    for retained in [false, true] {
        let dir = test_util::tempdir();
        let fixture = Fixture::with_members(&dir, 0, &[4, 2, 0], Some((&[4, 1], retained)));
        let id = fixture.context.id();
        let before = observations(&fixture);
        let admission = fixture.handle.admit_current_route().unwrap();
        let route = admission.complete_historical_fanout_route(&fixture.runtime_map, id.pg_id());
        if !retained {
            assert!(route
                .err()
                .unwrap()
                .to_string()
                .contains("complete Active cohort"));
            assert_eq!(observations(&fixture), before);
            continue;
        }
        let mut owner = pending(&fixture.context)
            .into_publication_sender(NodeId::new(4), deadline())
            .into_fanout(route.unwrap())
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let route = admission
            .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        owner.publish_embedded(&section, deadline()).unwrap();
        assert!(owner
            .apply_embedded(&section, NodeId::new(1), deadline())
            .is_err());
        for member in [0, 4] {
            owner
                .apply_embedded(&section, NodeId::new(member), deadline())
                .unwrap();
        }
        assert!(owner
            .completion()
            .err()
            .unwrap()
            .to_string()
            .contains("2 of 3"));
        owner
            .apply_embedded(&section, NodeId::new(2), deadline())
            .unwrap();
        assert_eq!(owner.completion().unwrap().confirmed_peers(), 3);
        let after = observations(&fixture);
        assert_eq!(after[1], before[1]);
        check_terminal_handoff(owner);
        assert_eq!(observations(&fixture), after);
    }
}

#[test]
fn staging_complete_fanout_historical_report_and_admission_failures_keep_original_owner() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    for field in ["index", "crc", "sha"] {
        let dir = test_util::tempdir();
        let mut fixture = Fixture::with_members(&dir, 0, &[4, 2, 0], None);
        let id = fixture.context.id();
        let report = application::observation(&fixture.context);
        let mut sha = report.command_sha256();
        sha[0] ^= u8::from(field == "sha");
        let crossed = fixture.report(Some(PendingMetadataCommandObservation::new(
            report.cluster_epoch(),
            NonZeroU64::new(report.log_index() + u64::from(field == "index")).unwrap(),
            report.command_checksum() ^ u64::from(field == "crc"),
            sha,
        )));
        let crossed_handle = fixture.handle_for(&crossed);
        let crossed_admission = crossed_handle.admit_current_route().unwrap();
        let owner =
            fanout_embedded::sender(&fixture.map, &fixture.context, "uncertain", deadline());
        let installed = owner.installation_observation();
        let before = observations(&fixture);
        let (owner, error) = owner
            .into_fanout(
                crossed_admission
                    .complete_historical_fanout_route(&crossed, id.pg_id())
                    .unwrap(),
            )
            .err()
            .unwrap();
        assert!(error
            .to_string()
            .contains("command differs from admitted recovery report"));
        assert_eq!(owner.installation_observation(), installed);
        assert_eq!(observations(&fixture), before);

        let admission = fixture.handle.admit_current_route().unwrap();
        assert!(
            admission
                .complete_historical_fanout_route(&crossed, id.pg_id())
                .is_err(),
            "foreign report content cannot replace the pinned map"
        );
        let mut owner = owner
            .into_fanout(
                admission
                    .complete_historical_fanout_route(&fixture.runtime_map, id.pg_id())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        // Identical map/runtime/command, different admission domain/permit.
        let other_handle = fixture.handle_for(&fixture.runtime_map);
        let other = other_handle.admit_current_route().unwrap();
        let other_route = other
            .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
            .unwrap();
        let other_section = other_route.lock_until(deadline()).unwrap();
        assert!(owner
            .publish_embedded(&other_section, deadline())
            .unwrap_err()
            .to_string()
            .contains("original admitted primary section"));
        assert_eq!(observations(&fixture), before);
        drop(other_section);
        let route = admission
            .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        owner.publish_embedded(&section, deadline()).unwrap();
        for member in [0, 4, 2] {
            owner
                .apply_embedded(&section, NodeId::new(member), deadline())
                .unwrap();
        }
        assert_eq!(owner.completion().unwrap().confirmed_peers(), 3);
        assert_eq!(owner.installation_observation(), installed);
    }
}

fn reopen(dir: &test_util::TempDir, fixture: Fixture, case: usize) -> Fixture {
    let Fixture {
        authority,
        heartbeat_sequence,
        runtime_map,
        context,
        map,
        handle,
    } = fixture;
    drop(handle);
    drop(map);
    let map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_runtime_map(
            NodeId::new(4),
            (0..5).map(|node| {
                LocalNodeStoreConfig::new(NodeId::new(node), dir.path().join(node.to_string()))
            }),
            config(case).default_ec_shape,
            &runtime_map,
        )
        .unwrap(),
    );
    let handle = StorageClusterRouteHandle::from_authorized_cluster(
        StorageCluster::from_runtime_local_map(Arc::clone(&map), &runtime_map).unwrap(),
    );
    Fixture {
        authority,
        heartbeat_sequence,
        runtime_map,
        context,
        map,
        handle,
    }
}

#[test]
fn staging_complete_fanout_historical_partial_apply_restart_and_single_member() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    for case in 0..4 {
        let dir = test_util::tempdir();
        let fixture = Fixture::with_members(&dir, case, &[4, 2, 0], None);
        let id = fixture.context.id();
        {
            let admission = fixture.handle.admit_current_route().unwrap();
            let mut owner = pending(&fixture.context)
                .into_publication_sender(NodeId::new(4), deadline())
                .into_fanout(
                    admission
                        .complete_historical_fanout_route(&fixture.runtime_map, id.pg_id())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let route = admission
                .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            owner.publish_embedded(&section, deadline()).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                owner.test_apply_embedded_with_hook(&section, NodeId::new(0), deadline(), |at| {
                    if at == "after-commit" {
                        panic!("restart after historical witness apply");
                    }
                    Ok(())
                })
            }));
            assert!(result.is_err());
            assert_eq!(
                owner.application_observation(NodeId::new(0)),
                Applied::Unconfirmed
            );
        }
        let fixture = reopen(&dir, fixture, case);
        let admission = fixture.handle.admit_current_route().unwrap();
        let mut owner = pending(&fixture.context)
            .into_publication_sender(NodeId::new(4), deadline())
            .into_fanout(
                admission
                    .complete_historical_fanout_route(&fixture.runtime_map, id.pg_id())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let route = admission
            .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let before_republication = observations(&fixture);
        owner.publish_embedded(&section, deadline()).unwrap();
        let before = observations(&fixture);
        // Reconfirming publication trials then rolls back primary effects.
        // SQLite counts those temporary writes; durable state must not change.
        for (node, ((state, _), (previous, _))) in
            before.iter().zip(&before_republication).enumerate()
        {
            assert_eq!(state, previous, "republication changed node {node}");
        }
        assert!(
            owner
                .apply_embedded(&section, NodeId::new(4), deadline())
                .is_err(),
            "reinspection must confirm the witness after restart"
        );
        owner
            .apply_embedded(&section, NodeId::new(0), deadline())
            .unwrap();
        assert_eq!(observations(&fixture), before);
        for member in [4, 2] {
            owner
                .apply_embedded(&section, NodeId::new(member), deadline())
                .unwrap();
        }
        assert_eq!(owner.completion().unwrap().confirmed_peers(), 3);
        check_terminal_handoff(owner);
    }
    let dir = test_util::tempdir();
    let fixture = Fixture::new(&dir, 0);
    let id = fixture.context.id();
    let admission = fixture.handle.admit_current_route().unwrap();
    let mut owner = pending(&fixture.context)
        .into_publication_sender(NodeId::new(4), deadline())
        .into_fanout(
            admission
                .complete_historical_fanout_route(&fixture.runtime_map, id.pg_id())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    let route = admission
        .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    owner.publish_embedded(&section, deadline()).unwrap();
    owner
        .apply_embedded(&section, NodeId::new(4), deadline())
        .unwrap();
    assert_eq!(owner.completion().unwrap().confirmed_peers(), 1);
    check_terminal_handoff(owner);
}

#[test]
fn staging_complete_fanout_historical_frozen_authority_fences_apply_and_completion() {
    for cap in ["route", "work"] {
        for boundary in ["before-effect", "before-commit", "completion"] {
            let clock = crate::clock::test_time_override_guard(10_000);
            let dir = test_util::tempdir();
            let fixture = Fixture::with_members(&dir, 0, &[4, 2, 0], None);
            let id = fixture.context.id();
            let admission = fixture.handle.admit_current_route().unwrap();
            let cutoff = Instant::now() + Duration::from_secs(1);
            let mut owner = fanout_embedded::sender(
                &fixture.map,
                &fixture.context,
                "uncertain",
                if cap == "work" { cutoff } else { deadline() },
            )
            .into_fanout(
                admission
                    .complete_historical_fanout_route(&fixture.runtime_map, id.pg_id())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
            let installed = owner.installation_observation();
            let route = admission
                .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            owner.publish_embedded(&section, deadline()).unwrap();
            let expire = || {
                if cap == "route" {
                    fixture
                        .map
                        .test_store_route_map_validity(RouteMapValidity::until_ms(40_000).unwrap());
                    clock.set(21_000);
                    fixture.map.require_route_map_valid_now().unwrap();
                    admission.require_valid_now().unwrap();
                } else {
                    assert!(Instant::now() < cutoff);
                    while let Some(left) = cutoff.checked_duration_since(Instant::now()) {
                        std::thread::sleep(left);
                    }
                }
            };
            let before;
            if boundary == "completion" {
                for member in [0, 4, 2] {
                    owner
                        .apply_embedded(&section, NodeId::new(member), deadline())
                        .unwrap();
                }
                assert_eq!(owner.completion().unwrap().confirmed_peers(), 3);
                before = observations(&fixture);
                expire();
            } else {
                before = observations(&fixture);
                let mut reached = false;
                let error = owner
                    .test_apply_embedded_with_hook(&section, NodeId::new(0), deadline(), |at| {
                        if at == boundary {
                            reached = true;
                            expire();
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(reached, "{cap}/{boundary}: {error}");
                assert_eq!(
                    owner.application_observation(NodeId::new(0)),
                    Applied::Unconfirmed
                );
            }
            assert!(owner.completion().is_err());
            assert!(owner.publish_embedded(&section, deadline()).is_err());
            assert!(owner
                .apply_embedded(&section, NodeId::new(0), deadline())
                .is_err());
            owner = owner.into_terminal().err().unwrap().into_fanout();
            assert_eq!(owner.installation_observation(), installed);
            assert_eq!(
                observations(&fixture)
                    .iter()
                    .map(|(s, _)| s)
                    .collect::<Vec<_>>(),
                before.iter().map(|(s, _)| s).collect::<Vec<_>>()
            );
        }
    }
}
