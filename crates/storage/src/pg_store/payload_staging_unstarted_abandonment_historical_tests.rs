// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::UnstartedAbandonmentObservation as O;

#[test]
fn staging_replica_abandonment_historical_uses_exact_report_cohort_and_admission_domain() {
    use crate::node_runtime::pg_store::ReplicaAbandonmentObservation as R;
    let _clock = crate::clock::test_time_override_guard(10_000);
    for case in 0..4 {
        let dir = test_util::tempdir();
        let f = Fixture::with_members(&dir, case, &[4, 2, 0], None);
        let id = f.context.id();
        let admission = f.handle.admit_current_route().unwrap();
        let mut owner = super::super::unstarted_abandonment::sender(
            &f.map,
            &f.context,
            "uncertain",
            deadline(),
        )
        .into_fanout(
            admission
                .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"))
        .into_unstarted_abandonment()
        .unwrap_or_else(|(_, e)| panic!("{e}"));
        let observe = || {
            (0..5)
                .map(|n| {
                    let node = f.map.node(NodeId::new(n)).unwrap().test_node();
                    let pg = node.get_pg(id.pg_id().get()).unwrap();
                    (snapshot(&pg), pg.conn.total_changes())
                })
                .collect::<Vec<_>>()
        };
        {
            let route = admission
                .embedded_historical_publication_route(&f.runtime_map, id.pg_id())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            owner.record_primary_embedded(&section, deadline()).unwrap();
        }
        let before = observe();
        // Equal topology/command/stores but a different admission gate.
        let foreign = f.handle_for(&f.runtime_map);
        let foreign_admission = foreign.admit_current_route().unwrap();
        {
            let route = foreign_admission
                .embedded_historical_publication_route(&f.runtime_map, id.pg_id())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            assert!(owner
                .record_replica_embedded(&section, NodeId::new(0), deadline())
                .is_err());
            assert_eq!(
                owner.replica_observation(NodeId::new(0)),
                R::NoAttemptObserved
            );
        }
        assert_eq!(observe(), before);
        let route = admission
            .embedded_historical_publication_route(&f.runtime_map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        for n in [1, 3, 4] {
            assert!(owner
                .record_replica_embedded(&section, NodeId::new(n), deadline())
                .is_err());
        }
        assert_eq!(observe(), before);
        for n in [2, 0] {
            owner
                .record_replica_embedded(&section, NodeId::new(n), deadline())
                .unwrap();
        }
        let after = observe();
        for n in [1, 3, 4] {
            assert_eq!(after[n], before[n]);
        }
        for n in [0, 2] {
            assert_ne!(after[n], before[n]);
        }
        owner
            .record_replica_embedded(&section, NodeId::new(0), deadline())
            .unwrap();
        assert_eq!(observe(), after);
    }
}

#[test]
fn staging_unstarted_abandonment_historical_exact_report_keeps_source_and_all_replicas() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    for case in 0..4 {
        for origin in ["inspected", "installed", "uncertain"] {
            let dir = test_util::tempdir();
            let fixture = Fixture::with_members(&dir, case, &[4, 2, 0], None);
            let id = fixture.context.id();
            let admission = fixture.handle.admit_current_route().unwrap();
            let fanout = super::super::unstarted_abandonment::sender(
                &fixture.map,
                &fixture.context,
                origin,
                deadline(),
            )
            .into_fanout(
                admission
                    .complete_historical_fanout_route(&fixture.runtime_map, id.pg_id())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
            let original = fanout.installation_observation();
            let mut owner = fanout
                .into_unstarted_abandonment()
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let route = admission
                .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let node = fixture.map.node(NodeId::new(4)).unwrap().test_node();
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            let retained = (introduction::state(&pg), complete_pending::observe(&pg));
            let old_log = complete_maintenance::log_state(&pg);
            drop(pg);
            let peers = [0, 1, 2, 3].map(|n| {
                snapshot(
                    &fixture
                        .map
                        .node(NodeId::new(n))
                        .unwrap()
                        .test_node()
                        .get_pg(id.pg_id().get())
                        .unwrap(),
                )
            });
            let lock = fixture
                .map
                .runtime_state()
                .metadata_command_pg_lock(id.pg_id());
            assert!(lock.try_lock().is_none());
            // Lost reply after durable success, then a no-write exact retry.
            let mut committed = false;
            owner
                .test_record_primary_embedded_with_hook(&section, deadline(), |stage| {
                    if stage == "after-commit" {
                        committed = true;
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "historical abandonment lost confirmation",
                        });
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(committed);
            assert_eq!(owner.observation(), O::Unconfirmed);
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            let after = (snapshot(&pg), pg.conn.total_changes());
            assert_ne!(complete_maintenance::log_state(&pg), old_log);
            assert_eq!(
                (introduction::state(&pg), complete_pending::observe(&pg)),
                retained
            );
            drop(pg);
            owner.record_primary_embedded(&section, deadline()).unwrap();
            assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
            assert_eq!(owner.installation_observation(), original);
            drop(owner);
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            assert_eq!((snapshot(&pg), pg.conn.total_changes()), after);
            assert_eq!(
                [0, 1, 2, 3].map(|n| snapshot(
                    &fixture
                        .map
                        .node(NodeId::new(n))
                        .unwrap()
                        .test_node()
                        .get_pg(id.pg_id().get())
                        .unwrap()
                )),
                peers
            );
            assert!(lock.try_lock().is_none());
        }
    }
}

#[test]
fn staging_unstarted_abandonment_historical_rejects_foreign_admission_domain() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    let dir = test_util::tempdir();
    let fixture = Fixture::with_members(&dir, 2, &[4, 2, 0], None);
    let id = fixture.context.id();
    let admission = fixture.handle.admit_current_route().unwrap();
    let mut owner = super::super::unstarted_abandonment::sender(
        &fixture.map,
        &fixture.context,
        "uncertain",
        deadline(),
    )
    .into_fanout(
        admission
            .complete_historical_fanout_route(&fixture.runtime_map, id.pg_id())
            .unwrap(),
    )
    .unwrap_or_else(|(_, e)| panic!("{e}"))
    .into_unstarted_abandonment()
    .unwrap_or_else(|(_, e)| panic!("{e}"));
    // Same stores, topology and exact command, but another route-admission gate.
    let foreign = fixture.handle_for(&fixture.runtime_map);
    let foreign_admission = foreign.admit_current_route().unwrap();
    let route = foreign_admission
        .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let node = fixture.map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(id.pg_id().get()).unwrap();
    let before = (snapshot(&pg), pg.conn.total_changes());
    drop(pg);
    assert!(owner.record_primary_embedded(&section, deadline()).is_err());
    assert_eq!(owner.observation(), O::NoAttemptObserved);
    let pg = node.get_pg(id.pg_id().get()).unwrap();
    assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
}
