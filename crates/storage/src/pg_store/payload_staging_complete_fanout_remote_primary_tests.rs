// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real authenticated remote primary, embedded replicas and mixed peer ordering.
//! No live publisher selection or non-quiescent terminal effects.

use super::fanout_mixed::{Mixed, Remote};
use super::*;
use crate::node_client::UnixStorageNodeMetadataCommandSession;
use crate::node_runtime::pg_store::{
    ApplicationObservation as Applied, CompleteFanoutSender, CompletePublicationSender,
};

pub(super) fn owner(mixed: &Mixed, history: &str, cutoff: Instant) -> CompletePublicationSender {
    let context = &mixed.context;
    if history == "inspected" {
        return pending(context).into_publication_sender(NodeId::new(4), cutoff);
    }
    let encoded = context.encode_command().unwrap();
    let mut submission =
        ContextualCommand::decode_command(encoded.bytes(), encoded.checksum_crc64(), &topology())
            .unwrap()
            .into_submission();
    let node = mixed.node(NodeId::new(4));
    let pg = node.get_pg(context.id().pg_id().get()).unwrap();
    if history == "uncertain" {
        pg.conn
            .execute_batch("DELETE FROM metadata_command_pending_slot")
            .unwrap();
        handoff::uncertain_install(&mut submission, &pg);
        install(&pg, context);
    } else {
        complete_pending::install(&mut submission, &pg).unwrap();
    }
    submission.into_publication_sender(NodeId::new(4), cutoff)
}

pub(super) fn apply_member(
    mixed: &Mixed,
    fanout: &mut CompleteFanoutSender<'_>,
    session: &UnixStorageNodeMetadataCommandSession,
    node: u32,
) -> Result<(), String> {
    let primary = mixed.remote(NodeId::new(4)).unwrap();
    let result = if node == 4 {
        fanout.apply_primary(&primary.client, session, deadline())
    } else if let Some(peer) = mixed.remote(NodeId::new(node)) {
        fanout.apply_replica(&peer.client, deadline())
    } else {
        fanout.apply_embedded_replica(&primary.client, session, NodeId::new(node), deadline())
    };
    result.map(|_| ()).map_err(|e| e.to_string())
}

#[test]
fn staging_complete_fanout_remote_primary_unix_all_contexts_and_origins() {
    matrix(false);
}
#[test]
fn staging_complete_fanout_remote_primary_tls_all_contexts_and_origins() {
    matrix(true);
}

fn matrix(tls: bool) {
    for remote_witness in [false, true] {
        for case in 0..4 {
            for history in ["inspected", "installed", "uncertain"] {
                let dir = test_util::tempdir();
                let mixed = Mixed::with_endpoints(
                    &dir,
                    case,
                    [(4, tls), (if remote_witness { 0 } else { 2 }, !tls)],
                );
                let primary = mixed.remote(NodeId::new(4)).unwrap();
                let id = mixed.context.id();
                let admission = mixed.handle.admit_current_route().unwrap();
                assert!(admission
                    .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                    .is_err());
                let mut fanout = owner(&mixed, history, deadline())
                    .into_fanout(
                        admission
                            .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                            .unwrap(),
                    )
                    .unwrap_or_else(|(_, e)| panic!("{e}"));
                let original = fanout.installation_observation();
                let shadows = mixed.shadows();
                let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
                let session = primary
                    .client
                    .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                    .unwrap();
                assert!(apply_member(&mixed, &mut fanout, &session, 0)
                    .unwrap_err()
                    .contains("publication marker is not confirmed"));
                fanout
                    .publish(&primary.client, &session, deadline())
                    .unwrap();
                let before = mixed.observe();
                assert!(apply_member(&mixed, &mut fanout, &session, 4)
                    .unwrap_err()
                    .contains("witness confirmation"));
                for node in [1, 2, 3] {
                    assert!(apply_member(&mixed, &mut fanout, &session, node)
                        .unwrap_err()
                        .contains("primary confirmation"));
                }
                assert_eq!(mixed.observe(), before);
                let mut expected = None;
                for (position, node) in [0, 4, 2, 3, 1].into_iter().enumerate() {
                    apply_member(&mixed, &mut fanout, &session, node).unwrap();
                    let node_store = mixed.node(NodeId::new(node));
                    let pg = node_store.get_pg(id.pg_id().get()).unwrap();
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
                    let applied = mixed.observe();
                    apply_member(&mixed, &mut fanout, &session, node).unwrap();
                    assert_eq!(mixed.observe(), applied, "exact replay must not mutate");
                    if position < 4 {
                        assert!(fanout.completion().is_err());
                        fanout = fanout.into_terminal().err().unwrap().into_fanout();
                        assert_eq!(fanout.installation_observation(), original);
                    }
                }
                assert_eq!(fanout.completion().unwrap().confirmed_peers(), 5);
                let mut terminal = fanout
                    .into_terminal()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                if case == 2 {
                    let before = mixed.observe();
                    assert!(terminal
                        .retire_remote(&primary.client, &session, deadline())
                        .unwrap_err()
                        .to_string()
                        .contains("terminal work integration"));
                    assert_eq!(mixed.observe(), before);
                } else {
                    terminal
                        .retire_remote(&primary.client, &session, deadline())
                        .unwrap();
                    assert!(complete_pending::observe(
                        &mixed.node(NodeId::new(4)).get_pg(id.pg_id().get()).unwrap()
                    )
                    .iter()
                    .all(Vec::is_empty));
                }
                assert_eq!(terminal.installation_observation(), original);
                let before = mixed.observe();
                drop(terminal);
                assert_eq!(mixed.observe(), before);
                assert_eq!(mixed.shadows(), shadows);
                drop(session);
            }
        }
    }
}

#[test]
fn staging_complete_fanout_remote_primary_embedded_failure_and_unwind_keep_exact_history() {
    for tls in [false, true] {
        for boundary in [
            "before-effect",
            "before-commit",
            "after-commit",
            "unwind-after-commit",
        ] {
            let dir = test_util::tempdir();
            let mixed = Mixed::with_endpoints(&dir, 3, [(4, tls), (2, !tls)]);
            let primary = mixed.remote(NodeId::new(4)).unwrap();
            let id = mixed.context.id();
            let admission = mixed.handle.admit_current_route().unwrap();
            let mut fanout = owner(&mixed, "uncertain", deadline())
                .into_fanout(
                    admission
                        .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                        .unwrap(),
                )
                .unwrap_or_else(|(_, e)| panic!("{e}"));
            let original = fanout.installation_observation();
            let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
            let session = primary
                .client
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            fanout
                .publish(&primary.client, &session, deadline())
                .unwrap();
            let before = mixed.observe();
            let mut reached = false;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                fanout.test_apply_embedded_replica_with_hook(
                    &primary.client,
                    &session,
                    NodeId::new(0),
                    deadline(),
                    |point| {
                        if point == boundary
                            || (point == "after-commit" && boundary == "unwind-after-commit")
                        {
                            reached = true;
                            assert!(primary.client.node_id() == NodeId::new(4));
                            if boundary == "unwind-after-commit" {
                                panic!("lost embedded confirmation");
                            }
                            return Err(StoreError::OperationDeadlineExceeded {
                                context: "remote-primary embedded fault",
                            });
                        }
                        Ok(())
                    },
                )
            }));
            assert!(reached);
            if boundary == "unwind-after-commit" {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert_eq!(
                fanout.application_observation(NodeId::new(0)),
                Applied::Unconfirmed
            );
            assert_eq!(fanout.installation_observation(), original);
            if boundary == "before-effect" || boundary == "before-commit" {
                let after = mixed.observe();
                assert_eq!(
                    after.iter().map(|(s, _)| s).collect::<Vec<_>>(),
                    before.iter().map(|(s, _)| s).collect::<Vec<_>>()
                );
            }
            assert!(apply_member(&mixed, &mut fanout, &session, 4)
                .unwrap_err()
                .contains("witness confirmation"));
            let applied = mixed.observe();
            apply_member(&mixed, &mut fanout, &session, 0).unwrap();
            if boundary.ends_with("after-commit") {
                assert_eq!(mixed.observe(), applied);
            }
            for node in [4, 2, 3, 1] {
                apply_member(&mixed, &mut fanout, &session, node).unwrap();
            }
            assert_eq!(fanout.completion().unwrap().confirmed_peers(), 5);
            drop(session);
        }
    }
}

#[test]
fn staging_complete_fanout_remote_primary_rejects_crossed_sessions_and_targets_before_effects() {
    use crate::node_client::{LocalUnixStorageNodeClientAdmissionSettings, UnixStorageNodeClient};
    use crate::storage_node_server::PreparedStorageNodeServer;
    use std::sync::Arc;
    for tls in [false, true] {
        let dir = test_util::tempdir();
        let mixed = Mixed::with_endpoints(&dir, 0, [(4, tls), (2, !tls)]);
        let primary = mixed.remote(NodeId::new(4)).unwrap();
        let id = mixed.context.id();
        let admission = mixed.handle.admit_current_route().unwrap();
        let route = admission
            .complete_fanout_route(id.pg_id(), id.cluster_epoch())
            .unwrap();
        let (_, auth) = super::super::complete_log_rpc::authenticated_server(
            PreparedStorageNodeServer::new(config(0)),
        );
        let foreign = UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
            NodeId::new(4),
            id.cluster_epoch(),
            primary.client.test_execution_endpoint().clone(),
            LocalUnixStorageNodeClientAdmissionSettings::DEFAULT,
            Some(auth),
        )
        .with_pg_topology(Arc::new(topology()));
        let _running = primary.run();
        let before = mixed.observe();
        for (client, pg) in [
            (&foreign, id.pg_id()),
            (
                primary.client.as_ref(),
                PgId::new((id.pg_id().get() + 1) % 8),
            ),
        ] {
            let session = client
                .open_metadata_command_critical_section_until(pg, deadline())
                .unwrap();
            let error = route
                .embedded_replica_target(&primary.client, &session, NodeId::new(0), deadline())
                .err()
                .unwrap();
            assert!(
                error.to_string().contains(if pg == id.pg_id() {
                    "another client instance"
                } else {
                    "exact open primary session"
                }),
                "{error}"
            );
            assert!(route
                .embedded_replica_target(&foreign, &session, NodeId::new(0), deadline())
                .err()
                .unwrap()
                .to_string()
                .contains("exact admitted peer client"));
            drop(session);
        }
        let session = primary
            .client
            .open_metadata_command_critical_section_until(id.pg_id(), deadline())
            .unwrap();
        for (node, diagnostic) in [
            (4, "admitted remote primary"),
            (99, "admitted cohort"),
            (2, "embedded execution endpoint"),
        ] {
            assert!(route
                .embedded_replica_target(&primary.client, &session, NodeId::new(node), deadline())
                .err()
                .unwrap()
                .to_string()
                .contains(diagnostic));
        }
        route
            .embedded_replica_target(&primary.client, &session, NodeId::new(0), deadline())
            .unwrap()
            .require_effect()
            .unwrap();
        assert_eq!(mixed.observe(), before);
        drop(session);
    }
}

#[test]
fn staging_complete_fanout_remote_primary_lost_exchange_cannot_authorize_embedded_retry() {
    for tls in [false, true] {
        let dir = test_util::tempdir();
        let mixed = Mixed::with_endpoints(&dir, 0, [(4, tls), (2, !tls)]);
        let primary = mixed.remote(NodeId::new(4)).unwrap();
        let id = mixed.context.id();
        let admission = mixed.handle.admit_current_route().unwrap();
        let mut fanout = owner(&mixed, "uncertain", deadline())
            .into_fanout(
                admission
                    .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let original = fanout.installation_observation();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                primary
                    .server
                    .test_accept_one_complete_application_fault(false, true)
                    .unwrap()
            });
            let session = primary
                .client
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            fanout
                .publish(&primary.client, &session, deadline())
                .unwrap();
            apply_member(&mixed, &mut fanout, &session, 0).unwrap();
            assert!(apply_member(&mixed, &mut fanout, &session, 4).is_err());
            worker.join().unwrap();
            assert_eq!(
                fanout.application_observation(NodeId::new(4)),
                Applied::Unconfirmed
            );
            let before = mixed.observe();
            assert!(apply_member(&mixed, &mut fanout, &session, 0)
                .unwrap_err()
                .contains("exact open primary session"));
            assert_eq!(
                fanout.application_observation(NodeId::new(0)),
                Applied::Applied
            );
            assert_eq!(mixed.observe(), before);
            drop(session);
            let _running: Vec<_> = mixed.remote.iter().map(Remote::run).collect();
            let session = primary
                .client
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            apply_member(&mixed, &mut fanout, &session, 4).unwrap();
            assert_eq!(
                mixed.observe(),
                before,
                "lost primary reply must converge without reapply"
            );
            for node in [2, 3, 1] {
                apply_member(&mixed, &mut fanout, &session, node).unwrap();
            }
            assert_eq!(fanout.installation_observation(), original);
            assert_eq!(fanout.completion().unwrap().confirmed_peers(), 5);
            drop(session);
        });
    }
}

#[test]
fn staging_complete_fanout_remote_primary_cutoffs_reach_embedded_effect_and_commit() {
    for work_cutoff in [false, true] {
        for boundary in ["before-effect", "before-commit"] {
            let dir = test_util::tempdir();
            let mixed = Mixed::with_endpoints(&dir, 0, [(4, work_cutoff), (2, !work_cutoff)]);
            let primary = mixed.remote(NodeId::new(4)).unwrap();
            let id = mixed.context.id();
            let _running = primary.run();
            let session = primary
                .client
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            let now = crate::clock::current_time_millis();
            let cutoff = Instant::now() + Duration::from_secs(if work_cutoff { 2 } else { 4 });
            mixed.map.test_store_route_map_validity(
                RouteMapValidity::until_ms(now + if work_cutoff { 60_000 } else { 4_000 }).unwrap(),
            );
            let admission = mixed.handle.admit_current_route().unwrap();
            let mut fanout = owner(
                &mixed,
                "uncertain",
                if work_cutoff { cutoff } else { deadline() },
            )
            .into_fanout(
                admission
                    .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
            fanout
                .publish(&primary.client, &session, deadline())
                .unwrap();
            let before = mixed.observe();
            let mut reached = false;
            let error = fanout
                .test_apply_embedded_replica_with_hook(
                    &primary.client,
                    &session,
                    NodeId::new(0),
                    deadline(),
                    |point| {
                        if point == boundary {
                            reached = true;
                            if !work_cutoff {
                                mixed.map.test_store_route_map_validity(
                                    RouteMapValidity::until_ms(now + 240_000).unwrap(),
                                );
                            }
                            assert!(
                                Instant::now() < cutoff,
                                "expiry must happen inside the transaction"
                            );
                            while let Some(remaining) =
                                cutoff.checked_duration_since(Instant::now())
                            {
                                std::thread::sleep(remaining);
                            }
                            mixed.map.require_route_map_valid_now().unwrap();
                        }
                        Ok(())
                    },
                )
                .unwrap_err();
            assert!(reached, "{error}");
            assert_eq!(
                fanout.application_observation(NodeId::new(0)),
                Applied::Unconfirmed
            );
            let after = mixed.observe();
            assert_eq!(
                after.iter().map(|(s, _)| s).collect::<Vec<_>>(),
                before.iter().map(|(s, _)| s).collect::<Vec<_>>()
            );
            // New session / later caller deadline must not renew the owner cutoff.
            drop(session);
            let session = primary
                .client
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            if work_cutoff {
                assert!(apply_member(&mixed, &mut fanout, &session, 0)
                    .unwrap_err()
                    .contains("original work cutoff"));
            } else {
                assert!(apply_member(&mixed, &mut fanout, &session, 0).is_err());
            }
            assert_eq!(mixed.observe(), after);
            drop(session);
        }
    }
}

#[test]
fn staging_complete_fanout_remote_primary_success_with_closed_connection_revokes_embedded_bridge() {
    use crate::storage_rpc::{set_storage_rpc_response_connection_reusable, StorageRpcMessageKind};
    use std::sync::Arc;
    for tls in [false, true] {
        let dir = test_util::tempdir();
        let mixed = Mixed::with_endpoints(&dir, 0, [(4, tls), (2, !tls)]);
        let primary = mixed.remote(NodeId::new(4)).unwrap();
        let id = mixed.context.id();
        primary
            .server
            .set_response_frame_test_hook(Arc::new(|kind, frame| {
                if kind == StorageRpcMessageKind::MetadataCommandPublicationStart {
                    // Authentication covers the server's disposition. Success is
                    // preserved, but it is not authority to reuse the held session.
                    set_storage_rpc_response_connection_reusable(&mut frame.payload, false)
                        .unwrap();
                }
            }));
        let admission = mixed.handle.admit_current_route().unwrap();
        let mut fanout = owner(&mixed, "uncertain", deadline())
            .into_fanout(
                admission
                    .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
        let _running = primary.run();
        let session = primary
            .client
            .open_metadata_command_critical_section_until(id.pg_id(), deadline())
            .unwrap();
        fanout
            .publish(&primary.client, &session, deadline())
            .unwrap();
        assert!(started(
            &mixed.node(NodeId::new(4)).get_pg(id.pg_id().get()).unwrap()
        ));
        let before = mixed.observe();
        assert!(apply_member(&mixed, &mut fanout, &session, 0)
            .unwrap_err()
            .contains("exact open primary session"));
        assert_eq!(
            fanout.application_observation(NodeId::new(0)),
            Applied::NoAttemptObserved
        );
        assert_eq!(mixed.observe(), before);
        drop(session);
    }
}

#[test]
fn staging_complete_fanout_remote_primary_session_wait_obeys_attempt_work_and_route_cutoffs() {
    use std::sync::mpsc;

    for tls in [false, true] {
        for bound in ["attempt", "work", "route"] {
            let dir = test_util::tempdir();
            let mixed = Mixed::with_endpoints(&dir, 0, [(4, tls), (2, !tls)]);
            let primary = mixed.remote(NodeId::new(4)).unwrap();
            let id = mixed.context.id();
            let _running = primary.run();
            let session = primary
                .client
                .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                .unwrap();
            // Establish transport before starting the short authority budget.
            // The caller's attempt deadline is deliberately longer in the work
            // and route cases: omitting either intersection must fail this test.
            let cutoff = Instant::now() + Duration::from_secs(2);
            if bound == "route" {
                mixed.map.test_store_route_map_validity(
                    RouteMapValidity::until_ms(crate::clock::current_time_millis() + 2_000)
                        .unwrap(),
                );
            }
            let admission = mixed.handle.admit_current_route().unwrap();
            let mut fanout = owner(
                &mixed,
                "uncertain",
                if bound == "work" { cutoff } else { deadline() },
            )
            .into_fanout(
                admission
                    .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                    .unwrap(),
            )
            .unwrap_or_else(|(_, e)| panic!("{e}"));
            fanout
                .publish(&primary.client, &session, deadline())
                .unwrap();
            // Pin both untouched and previously confirmed application history.
            if tls {
                apply_member(&mixed, &mut fanout, &session, 0).unwrap();
            }
            let installation = fanout.installation_observation();
            let application = fanout.application_observation(NodeId::new(0));
            let before = mixed.observe();
            std::thread::scope(|scope| {
                let (ready_tx, ready_rx) = mpsc::channel();
                let (release_tx, release_rx) = mpsc::channel();
                let (finished_tx, finished_rx) = mpsc::channel();
                let borrowed_session = &session;
                let worker = scope.spawn(move || {
                    // This genuine session borrow owns the very mutex used by
                    // RPC exchange, without introducing a raw lock test seam.
                    let held = borrowed_session
                        .borrow_complete_primary(
                            &primary.client,
                            id.pg_id(),
                            id.cluster_epoch(),
                            deadline(),
                        )
                        .unwrap();
                    ready_tx.send(()).unwrap();
                    // A finite fallback makes the old blocking implementation
                    // fail the assertions instead of hanging the test suite.
                    let _ = release_rx.recv_timeout(Duration::from_secs(8));
                    drop(held);
                    finished_tx.send(()).unwrap();
                });
                ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                assert!(
                    Instant::now() < cutoff,
                    "test authority expired before contention"
                );
                let attempt_cutoff = if bound == "attempt" {
                    Instant::now() + Duration::from_millis(50)
                } else {
                    deadline()
                };
                let start = Instant::now();
                let result = fanout.apply_embedded_replica(
                    &primary.client,
                    &session,
                    NodeId::new(0),
                    attempt_cutoff,
                );
                let elapsed = start.elapsed();
                let still_held = matches!(finished_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
                let _ = release_tx.send(());
                worker.join().unwrap();
                assert!(
                    still_held,
                    "{bound}: acquisition waited for the other holder"
                );
                assert!(
                    elapsed
                        < if bound == "attempt" {
                            Duration::from_secs(1)
                        } else {
                            Duration::from_secs(3)
                        },
                    "{bound}: session wait took {elapsed:?}"
                );
                let error = result.unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("borrow complete primary session"),
                    "{bound}: {error}"
                );
            });
            assert_eq!(fanout.installation_observation(), installation);
            assert_eq!(fanout.application_observation(NodeId::new(0)), application);
            assert_eq!(
                mixed.observe(),
                before,
                "{bound}: lock timeout changed a replica"
            );
            // A failed borrow must not close or poison the other RPC's session.
            drop(
                session
                    .borrow_complete_primary(
                        &primary.client,
                        id.pg_id(),
                        id.cluster_epoch(),
                        deadline(),
                    )
                    .unwrap(),
            );
            if bound == "attempt" {
                apply_member(&mixed, &mut fanout, &session, 0).unwrap();
                assert_eq!(
                    fanout.application_observation(NodeId::new(0)),
                    Applied::Applied
                );
                if tls {
                    assert_eq!(
                        mixed.observe(),
                        before,
                        "retry of confirmed application must not write"
                    );
                }
            }
            drop(session);
        }
    }
}
