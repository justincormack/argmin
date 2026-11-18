// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact finalizer release from the original source/cleanup terminal owner.
//! Metadata-only fixtures; release does not retire durable owner obligations.

use super::fanout_mixed::Mixed;
use super::*;
use crate::node_runtime::pg_store::TerminalReservationObservation as R;
use crate::node_runtime::traits::PgMetadataStore;
use crate::storage_rpc::StorageRpcMessageKind as Kind;
use crate::{StorageCluster, StorageClusterRouteHandle};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    Embedded,
    Unix,
    Tls,
}

#[test]
fn staging_abandoned_part_release_embedded_preserves_pair_and_exact_proof() {
    matrix(Endpoint::Embedded);
}

#[test]
fn staging_abandoned_part_release_unix_retains_uncertainty_and_immutable_cutoff() {
    matrix(Endpoint::Unix);
}

#[test]
fn staging_abandoned_part_release_tls_retains_uncertainty_and_immutable_cutoff() {
    matrix(Endpoint::Tls);
}

fn matrix(endpoint: Endpoint) {
    for count in [0, 2] {
        for origin in ["inspected", "installed", "uncertain"] {
            let dir = test_util::tempdir();
            let (map, handle, source, cleanup, proof) =
                abandonment_preparation::setup_with_seed(&dir, count, |pg, owner| {
                    part::command(pg, owner)
                        .payload()
                        .primary_bucket_write_reservation_proof()
                        .unwrap()
                        .clone()
                });
            let bucket_pg = topology().bucket_pg_for(&proof.bucket);
            assert_ne!(bucket_pg, source.id().pg_id().get());
            let bucket_primary = NodeId::new(if endpoint == Endpoint::Embedded { 4 } else { 2 });
            let mut config = config(2);
            config
                .pg_routes
                .iter_mut()
                .find(|r| r.pg_id == bucket_pg)
                .unwrap()
                .primary_node_id = bucket_primary;
            let routes = config.pg_routes.iter().map(|r| {
                crate::control_plane::PgRouteSnapshot::reconstructed(
                    r.cluster_epoch,
                    PgId::new(r.pg_id),
                    r.primary_node_id,
                    r.acting_set.clone(),
                    PgState::Active,
                )
            });
            // Change fixture topology before minting any final admission. Keep
            // the same open stores; remote replicas get independent snapshots.
            let selected = Arc::new(
                map.test_clone_with_pg_routes(source.id().cluster_epoch(), routes, [])
                    .unwrap(),
            );
            drop(handle);
            drop(map);
            let handle = StorageClusterRouteHandle::from_authorized_cluster(
                StorageCluster::test_from_local_map_with_epoch(
                    Arc::clone(&selected),
                    source.id().cluster_epoch(),
                )
                .unwrap(),
            );
            let mixed = Mixed::from_fixture_with_config(
                selected,
                handle,
                source,
                &config,
                [
                    (0, endpoint == Endpoint::Unix),
                    (2, endpoint == Endpoint::Tls),
                ],
            );
            for node in (0..5).map(NodeId::new) {
                terminal_release::seed_pg_reservations(
                    &mixed.node(node).get_pg(bucket_pg).unwrap(),
                    &proof,
                    None,
                );
                if mixed.remote(node).is_some() {
                    terminal_release::seed_pg_reservations(
                        &mixed
                            .map
                            .node(node)
                            .unwrap()
                            .test_node()
                            .get_pg(bucket_pg)
                            .unwrap(),
                        &proof,
                        None,
                    );
                }
            }
            let now = crate::clock::current_time_millis();
            let monotonic = crate::clock::monotonic_time_millis();
            mixed
                .map
                .test_store_route_map_validity(RouteMapValidity::until_ms(now + 60_000).unwrap());
            let admission = mixed.handle.admit_current_route().unwrap();
            let id = mixed.context.id();
            let route = admission
                .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let mut abandoned =
                unstarted_abandonment::sender(&mixed.map, &mixed.context, origin, deadline())
                    .into_fanout(
                        admission
                            .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                            .unwrap(),
                    )
                    .unwrap_or_else(|(_, e)| panic!("{e}"))
                    .into_unstarted_abandonment()
                    .unwrap_or_else(|(_, e)| panic!("{e}"));
            let running: Vec<_> = mixed.remote.iter().map(|peer| peer.run()).collect();
            abandoned
                .record_primary_embedded(&section, deadline())
                .unwrap();
            for node in [2, 0, 3, 1].map(NodeId::new) {
                if let Some(peer) = mixed.remote(node) {
                    abandoned
                        .record_remote_replica_from_embedded_primary(
                            &section,
                            &peer.client,
                            deadline(),
                        )
                        .unwrap();
                } else {
                    abandoned
                        .record_replica_embedded(&section, node, deadline())
                        .unwrap();
                }
            }
            let mut prepared = abandoned
                .into_abandoned()
                .unwrap_or_else(|e| panic!("{}", e.error()))
                .prepare_part_cleanup(&section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let history = prepared.installation_observation();
            prepared.install_cleanup(&section).unwrap();
            prepared.publish_cleanup(&section).unwrap();
            for node in [0, 4, 2, 3, 1].map(NodeId::new) {
                if let Some(peer) = mixed.remote(node) {
                    prepared
                        .apply_cleanup_remote(&section, &peer.client)
                        .unwrap();
                } else {
                    prepared.apply_cleanup_embedded(&section, node).unwrap();
                }
            }
            let mut terminal = prepared
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let custody = terminal.inspect_retained_custody(&section).unwrap();
            let objects = mixed.observe();
            let shadows = mixed.shadows();
            let bucket_shadows: Vec<_> = mixed
                .remote
                .iter()
                .map(|peer| {
                    let pg = mixed
                        .map
                        .node(peer.node)
                        .unwrap()
                        .test_node()
                        .get_pg(bucket_pg)
                        .unwrap();
                    (
                        pg.conn.total_changes(),
                        pg.durable_bucket_write_reservations(&proof.bucket).unwrap(),
                    )
                })
                .collect();
            let observe = || {
                (0..5)
                    .map(|id| {
                        let node = mixed.node(NodeId::new(id));
                        let pg = node.get_pg(bucket_pg).unwrap();
                        (
                            pg.conn.total_changes(),
                            pg.durable_bucket_write_reservations(&proof.bucket).unwrap(),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            let before = observe();
            assert_eq!(
                terminal.retained_finalizer_reservation_observation(),
                R::NoAttemptObserved
            );
            // Raw renewal cannot revive the pair's captured route/work cutoff.
            crate::clock::with_time_and_monotonic_override(
                now + 120_000,
                monotonic + 120_000,
                || {
                    mixed.map.test_store_route_map_validity(
                        RouteMapValidity::until_ms(now + 240_000).unwrap(),
                    );
                    assert!(terminal.release_retained_finalizer_reservation().is_err());
                    assert_eq!(
                        terminal.retained_finalizer_reservation_observation(),
                        R::NoAttemptObserved
                    );
                },
            );
            assert_eq!(observe(), before);
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                terminal.test_interrupt_retained_finalizer_release()
            }))
            .is_err());
            assert_eq!(
                terminal.retained_finalizer_reservation_observation(),
                R::Unconfirmed
            );
            assert_eq!(observe(), before);

            // Conflict must not release another owner even with the same ID.
            let mutate_token = |token: &str| {
                let node = mixed.node(bucket_primary);
                let pg = node.get_pg(bucket_pg).unwrap();
                pg.conn.execute("UPDATE bucket_write_reservations SET owner_token = ?1 WHERE reservation_id = ?2",
                    rusqlite::params![token, proof.reservation_id]).unwrap();
            };
            mutate_token("crossed-owner");
            let conflict = observe();
            let error = terminal
                .release_retained_finalizer_reservation()
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    crate::BucketSnapshotLoadError::Metadata(
                        crate::MetadataError::BucketWriteReservationConflict { .. }
                    )
                ),
                "{error:?}"
            );
            assert_eq!(
                terminal.retained_finalizer_reservation_observation(),
                R::Unconfirmed
            );
            assert_eq!(observe(), conflict);
            mutate_token(&proof.owner_token);
            let before = observe();
            let replies = Arc::new(AtomicUsize::new(0));
            if endpoint != Endpoint::Embedded {
                let count = Arc::clone(&replies);
                mixed
                    .remote(bucket_primary)
                    .unwrap()
                    .server
                    .set_response_envelope_test_hook(Arc::new(move |kind, bytes| {
                        if kind == Kind::ProofRelease && count.fetch_add(1, Ordering::SeqCst) == 0 {
                            *bytes.last_mut().unwrap() ^= 1;
                        }
                    }));
            }
            let (result, cutoffs) =
                crate::node_client::UnixStorageNodeClient::test_observe_proof_release_deadlines(
                    || terminal.release_retained_finalizer_reservation(),
                );
            if endpoint == Endpoint::Embedded {
                result.unwrap();
                assert!(cutoffs.is_empty());
            } else {
                assert!(result.is_err());
                assert_eq!(cutoffs.len(), 1);
                assert_eq!(
                    replies.load(Ordering::SeqCst),
                    1,
                    "no hidden retry after lost acknowledgement"
                );
                assert_eq!(
                    terminal.retained_finalizer_reservation_observation(),
                    R::Unconfirmed
                );
            }
            let after = observe();
            for id in 0..5 {
                if NodeId::new(id as u32) == bucket_primary {
                    assert_eq!(after[id].0, before[id].0 + 1);
                    let expected: Vec<_> = before[id]
                        .1
                        .iter()
                        .filter(|r| r.reservation_id != proof.reservation_id)
                        .cloned()
                        .collect();
                    assert!(!expected.is_empty(), "independent reservation canary");
                    assert_eq!(after[id].1, expected);
                } else {
                    assert_eq!(
                        after[id], before[id],
                        "only the admitted bucket primary may release"
                    );
                }
            }
            if endpoint != Endpoint::Embedded {
                crate::clock::with_time_and_monotonic_override(
                    now + 120_000,
                    monotonic + 120_000,
                    || {
                        assert!(terminal.release_retained_finalizer_reservation().is_err());
                        assert_eq!(
                            terminal.retained_finalizer_reservation_observation(),
                            R::Unconfirmed
                        );
                    },
                );
                assert_eq!(replies.load(Ordering::SeqCst), 1);
                let (result, retried) =
                    crate::node_client::UnixStorageNodeClient::test_observe_proof_release_deadlines(
                        || {
                            crate::clock::with_time_and_monotonic_override(
                                crate::clock::current_time_millis() - 100,
                                crate::clock::monotonic_time_millis(),
                                || terminal.release_retained_finalizer_reservation(),
                            )
                        },
                    );
                result.unwrap();
                assert_eq!(retried, cutoffs, "retry must not reproject the wire cutoff");
                assert_eq!(replies.load(Ordering::SeqCst), 2);
            }
            assert_eq!(
                terminal.retained_finalizer_reservation_observation(),
                R::Released
            );
            terminal.release_retained_finalizer_reservation().unwrap();
            assert_eq!(
                replies.load(Ordering::SeqCst),
                if endpoint == Endpoint::Embedded { 0 } else { 2 }
            );
            // Confirmed history cannot regress even when authority subsequently expires.
            crate::clock::with_time_and_monotonic_override(
                now + 120_000,
                monotonic + 120_000,
                || {
                    assert!(terminal.release_retained_finalizer_reservation().is_err());
                    assert_eq!(
                        terminal.retained_finalizer_reservation_observation(),
                        R::Released
                    );
                },
            );
            terminal.assert_custody_for_test(&mixed.context, &cleanup, &topology(), history);
            assert_eq!(
                terminal.inspect_retained_custody(&section).unwrap(),
                custody,
                "bucket release is not owner-resource retirement"
            );
            for node in (0..5).map(NodeId::new) {
                assert_eq!(
                    terminal.cleanup_application(node),
                    crate::node_runtime::pg_store::ApplicationObservation::Applied
                );
            }
            assert!(!terminal.work().is_quiescent());
            assert_eq!(
                observe(),
                after,
                "exact absence/confirmed retries perform no writes"
            );
            drop(terminal);
            assert_eq!(observe(), after, "Drop has no release effects");
            assert_eq!(
                mixed.observe(),
                objects,
                "release cannot retire owner resources or pending slot"
            );
            assert_eq!(mixed.shadows(), shadows);
            for (peer, before) in mixed.remote.iter().zip(bucket_shadows) {
                let pg = mixed
                    .map
                    .node(peer.node)
                    .unwrap()
                    .test_node()
                    .get_pg(bucket_pg)
                    .unwrap();
                assert_eq!(
                    (
                        pg.conn.total_changes(),
                        pg.durable_bucket_write_reservations(&proof.bucket).unwrap()
                    ),
                    before
                );
            }
            for peer in &mixed.remote {
                peer.client
                    .test_execution_endpoint()
                    .close_idle_requests_for_test();
            }
            drop(running);
        }
    }
}
