// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::{
    CompleteAbandonmentSendError as E, ReplicaAbandonmentObservation as R,
};

#[test]
fn staging_replica_abandonment_sender_historical_unix_contexts_origins() {
    matrix(false);
}
#[test]
fn staging_replica_abandonment_sender_historical_tls_contexts_origins() {
    matrix(true);
}

fn matrix(tls: bool) {
    for case in 0..4 {
        for origin in ["inspected", "installed", "uncertain"] {
            let mixed = Mixed::new(case, &[(4, tls), (0, !tls)]);
            let shadows = mixed.shadows();
            abandonment_historical::with_owner(&mixed, origin, |owner| {
                let primary = mixed.peer(4);
                let replica = mixed.peer(0);
                let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
                let session = owner
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                owner
                    .record_primary_remote(&primary.client, &session, deadline())
                    .unwrap();
                let before = mixed.observe();
                let history = owner.installation_observation();
                let count = replica.count(Kind::MetadataCommandRecordAbandoned);
                owner
                    .record_remote_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        &replica.client,
                        deadline(),
                    )
                    .unwrap();
                assert_eq!(owner.replica_observation(replica.id), R::TombstoneConfirmed);
                assert_eq!(owner.installation_observation(), history);
                let after = mixed.observe();
                assert_ne!(after[0], before[0]);
                for n in 1..5 {
                    assert_eq!(after[n], before[n]);
                }
                owner
                    .record_remote_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        &replica.client,
                        deadline(),
                    )
                    .unwrap();
                assert_eq!(count.load(Ordering::SeqCst), 1);
                assert_eq!(mixed.observe(), after);
                owner
                    .record_embedded_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        NodeId::new(2),
                        deadline(),
                    )
                    .unwrap();
                assert_eq!(
                    owner.replica_observation(NodeId::new(2)),
                    R::TombstoneConfirmed
                );
            });
            assert_eq!(mixed.shadows(), shadows);
        }
    }
}

#[test]
fn staging_replica_abandonment_sender_historical_embedded_primary_uses_exact_section() {
    for tls in [false, true] {
        for case in 0..4 {
            let mixed = Mixed::new(case, &[(0, tls)]);
            let f = &mixed.fixture;
            let id = f.context.id();
            let admission = f.handle.admit_current_route().unwrap();
            let source = super::super::super::abandonment_remote::sender(
                &mixed.node(4).get_pg(id.pg_id().get()).unwrap(),
                &f.context,
                "uncertain",
                deadline(),
            );
            let mut owner = source
                .into_fanout(
                    admission
                        .complete_historical_fanout_route(&f.runtime_map, id.pg_id())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"))
                .into_unstarted_abandonment()
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let route = admission
                .embedded_historical_publication_route(&f.runtime_map, id.pg_id())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            owner.record_primary_embedded(&section, deadline()).unwrap();
            let peer = mixed.peer(0);
            let _running = peer.run();
            owner
                .record_remote_replica_from_embedded_primary(&section, &peer.client, deadline())
                .unwrap();
            assert_eq!(owner.replica_observation(peer.id), R::TombstoneConfirmed);
        }
    }
}

#[test]
fn staging_replica_abandonment_sender_historical_restart_reconfirms_partial_lost_reply() {
    for tls in [false, true] {
        let mixed = Mixed::new(2, &[(4, tls), (0, !tls), (2, tls)]);
        abandonment_historical::with_owner(&mixed, "uncertain", |owner| {
            let primary = mixed.peer(4);
            let _primary_running = primary.run();
            let second = mixed.peer(2);
            let _second_running = second.run();
            let session = owner
                .open_primary_session(&primary.client, deadline())
                .unwrap();
            owner
                .record_primary_remote(&primary.client, &session, deadline())
                .unwrap();
            owner
                .record_remote_replica_from_remote_primary(
                    &primary.client,
                    &session,
                    &second.client,
                    deadline(),
                )
                .unwrap();
            let replica = mixed.peer(0);
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    replica
                        .server
                        .test_accept_one_abandonment_fault(false, true)
                        .unwrap()
                });
                let wake = WakeAcceptOnDrop(Some(replica));
                assert!(matches!(
                    owner.record_remote_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        &replica.client,
                        deadline()
                    ),
                    Err(E::Unconfirmed(_))
                ));
                wake.disarm();
                worker.join().unwrap();
            });
            assert_eq!(owner.replica_observation(replica.id), R::Unconfirmed);
            assert_eq!(owner.replica_observation(second.id), R::TombstoneConfirmed);
        });
        let before = mixed.observe();
        let mixed = lifetime::reopen(mixed);
        for ((after, _), (before, _)) in mixed.observe().iter().zip(&before) {
            assert_eq!(after, before);
        }
        abandonment_historical::with_owner(&mixed, "inspected", |owner| {
            let primary = mixed.peer(4);
            let _running: Vec<_> = mixed.peers.iter().map(Remote::run).collect();
            let session = owner
                .open_primary_session(&primary.client, deadline())
                .unwrap();
            let before = mixed.observe();
            owner
                .record_primary_remote(&primary.client, &session, deadline())
                .unwrap();
            for replica in [mixed.peer(0), mixed.peer(2)] {
                assert_eq!(owner.replica_observation(replica.id), R::NoAttemptObserved);
                owner
                    .record_remote_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        &replica.client,
                        deadline(),
                    )
                    .unwrap();
                assert_eq!(owner.replica_observation(replica.id), R::TombstoneConfirmed);
            }
            assert_eq!(
                mixed.observe(),
                before,
                "restart reconfirms each exact durable tombstone without writes"
            );
        });
    }
}
