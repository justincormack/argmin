// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::payload_staging::submission::InstallationObservation as Installed;
use crate::node_runtime::pg_store::PublicationObservation as Observation;
use crate::storage_rpc::{self as rpc, StorageRpcErrorCode, StorageRpcMessageKind};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const KIND: StorageRpcMessageKind = StorageRpcMessageKind::MetadataCommandPublicationStart;

pub(super) fn remote_history(tls: bool, submitted: Option<Installed>) {
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (server, client, config) = if tls {
            super::tls::installed_tls(&dir, case)
        } else {
            installed_config(&dir, config(case), true)
        };
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), case);
        let mut sender = if let Some(history) = submitted {
            let encoded = context.encode_command().unwrap();
            let copy = ContextualCommand::decode_command(
                encoded.bytes(),
                encoded.checksum_crc64(),
                &topology(),
            )
            .unwrap();
            let mut holder = copy.into_submission();
            if history == Installed::Unconfirmed {
                handoff::uncertain_install(&mut holder, &node.get_pg(pg_id.get()).unwrap());
                // Exact recovery by another installer leaves this caller's
                // installation result uncertain, even when the slot now exists.
                install(&node.get_pg(pg_id.get()).unwrap(), &context);
            } else {
                assert_eq!(history, Installed::Installed);
                complete_pending::install(&mut holder, &node.get_pg(pg_id.get()).unwrap()).unwrap();
            }
            holder.into_publication_sender(config.node_id, deadline())
        } else {
            install(&node.get_pg(pg_id.get()).unwrap(), &context);
            pending(&context).into_publication_sender(config.node_id, deadline())
        };
        let installation = sender.installation_observation();
        assert_eq!(installation.map(|value| value.installation), submitted);
        let before = without_marker(&node.get_pg(pg_id.get()).unwrap());
        assert_eq!(sender.id(), Some(context.id()));
        assert_eq!(sender.observation(), Observation::NoAttemptObserved);
        let replies = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&replies);
        server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
            if kind != KIND {
                return;
            }
            let retained = rpc::decode_storage_rpc_response_payload_with_connection_disposition(
                &frame.payload,
            )
            .unwrap()
            .connection_reusable;
            match observed.fetch_add(1, Ordering::SeqCst) {
                1 => {
                    frame.payload =
                        rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                            code: StorageRpcErrorCode::ResourceExhausted,
                            message: "injected after committed publication".into(),
                        })
                        .unwrap()
                }
                2 => {
                    let mut body = rpc::decode_storage_rpc_response_payload(&frame.payload)
                        .unwrap()
                        .unwrap();
                    body[26] ^= 1;
                    frame.payload = rpc::encode_storage_rpc_success_response(&body);
                }
                _ => {}
            }
            // Inject only the semantic error / crossed identity, not a second
            // connection-close fault. The held-session retries below depend on
            // the installed server retaining this same critical section.
            rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, retained)
                .unwrap();
        }));
        // First attempt commits the marker but loses the actual connection.
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.test_accept_one_losing_publication_reply().unwrap());
            let session = client
                .open_metadata_command_critical_section_until(pg_id, deadline())
                .unwrap();
            // An unsent first attempt is not mistaken for an uncertain dispatch.
            assert!(sender.send_remote(&session, Instant::now()).is_err());
            assert_eq!(sender.observation(), Observation::NoAttemptObserved);
            assert_eq!(sender.installation_observation(), installation);
            assert_eq!(replies.load(Ordering::SeqCst), 0);
            assert!(!started(&node.get_pg(pg_id.get()).unwrap()));
            assert!(sender.send_remote(&session, deadline()).is_err());
            assert_eq!(sender.observation(), Observation::Unconfirmed);
            assert_eq!(sender.installation_observation(), installation);
            drop(session);
            worker.join().unwrap();
        });
        assert_eq!(replies.load(Ordering::SeqCst), 1);
        assert!(started(&node.get_pg(pg_id.get()).unwrap()));
        assert_eq!(without_marker(&node.get_pg(pg_id.get()).unwrap()), before);
        // Later NotSent, generic authenticated error and crossed confirmation
        // all preserve the same owning history. No hidden transport retries.
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.accept_one().unwrap());
            let session = client
                .open_metadata_command_critical_section_until(pg_id, deadline())
                .unwrap();
            assert!(sender.send_remote(&session, Instant::now()).is_err());
            assert_eq!(sender.observation(), Observation::Unconfirmed);
            assert_eq!(sender.installation_observation(), installation);
            assert_eq!(replies.load(Ordering::SeqCst), 1);
            for expected in [2, 3] {
                assert!(sender.send_remote(&session, deadline()).is_err());
                assert_eq!(sender.observation(), Observation::Unconfirmed);
                assert_eq!(sender.installation_observation(), installation);
                assert_eq!(replies.load(Ordering::SeqCst), expected);
                assert_eq!(without_marker(&node.get_pg(pg_id.get()).unwrap()), before);
            }
            sender.send_remote(&session, deadline()).unwrap();
            assert_eq!(sender.observation(), Observation::MarkerConfirmed);
            assert_eq!(sender.installation_observation(), installation);
            assert_eq!(replies.load(Ordering::SeqCst), 4);
            assert!(sender.send_remote(&session, Instant::now()).is_err());
            assert_eq!(sender.observation(), Observation::MarkerConfirmed);
            assert_eq!(sender.installation_observation(), installation);
            assert_eq!(replies.load(Ordering::SeqCst), 4);
            sender
                .apply_remote_in_session(&session, deadline())
                .unwrap();
            assert_eq!(sender.installation_observation(), installation);
            assert_eq!(sender.observation(), Observation::MarkerConfirmed);
            assert_eq!(
                sender.application_observation(config.node_id),
                crate::node_runtime::pg_store::ApplicationObservation::Applied
            );
            let applied = snapshot(&node.get_pg(pg_id.get()).unwrap());
            let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
            sender.send_remote(&session, deadline()).unwrap();
            assert_eq!(sender.observation(), Observation::MarkerConfirmed);
            assert_eq!(sender.installation_observation(), installation);
            assert_eq!(replies.load(Ordering::SeqCst), 5);
            assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), applied);
            assert_eq!(
                node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
                writes
            );
            drop(session);
            worker.join().unwrap();
        });
    }
}

#[test]
fn staging_complete_publication_sender_unix_preserves_history_across_attempts() {
    remote_history(false, None);
}

#[test]
fn staging_complete_publication_sender_tls_preserves_history_across_attempts() {
    remote_history(true, None);
}

#[test]
fn staging_complete_publication_sender_rejects_crossed_session_before_transport() {
    for crossing in ["node", "epoch", "pg"] {
        let dir = test_util::tempdir();
        let (server, client, config) = installed_config(&dir, config(0), true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(
            &node.get_pg(pg_id.get()).unwrap(),
            if crossing == "epoch" { 2 } else { 0 },
        );
        let node_id = if crossing == "node" {
            NodeId::new(2)
        } else {
            config.node_id
        };
        let mut sender = pending(&context).into_publication_sender(node_id, deadline());
        let replies = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&replies);
        server.set_response_frame_test_hook(Arc::new(move |kind, _| {
            if kind == KIND {
                observed.fetch_add(1, Ordering::SeqCst);
            }
        }));
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.accept_one().unwrap());
            let session = client
                .open_metadata_command_critical_section_until(
                    if crossing == "pg" {
                        PgId::new((pg_id.get() + 1) % 8)
                    } else {
                        pg_id
                    },
                    deadline(),
                )
                .unwrap();
            let error = sender.send_remote(&session, deadline()).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("complete publication session subject"),
                "{error}"
            );
            assert_eq!(sender.observation(), Observation::NoAttemptObserved);
            assert_eq!(replies.load(Ordering::SeqCst), 0, "{crossing}");
            assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
            drop(session);
            worker.join().unwrap();
        });
    }
}

#[test]
fn staging_complete_publication_sender_embedded_error_and_unwind_retain_history() {
    for case in 0..4 {
        for (stage, panic) in [
            ("before-commit", false),
            ("after-commit", false),
            ("after-commit", true),
        ] {
            let dir = test_util::tempdir();
            let (map, handle) =
                embedded::embedded(&dir, case, PgState::Active, RouteMapValidity::Forever);
            let node = map.node(NodeId::new(4)).unwrap().test_node();
            let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
            let context = context(&node.get_pg(pg_id.get()).unwrap(), case);
            install(&node.get_pg(pg_id.get()).unwrap(), &context);
            let mut sender = pending(&context).into_publication_sender(NodeId::new(4), deadline());
            let before = without_marker(&node.get_pg(pg_id.get()).unwrap());
            let admission = handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(NodeId::new(4), pg_id, context.id().cluster_epoch())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let mut reached = false;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                sender.test_send_embedded_with_hook(&section, deadline(), |at| {
                    if at == stage {
                        reached = true;
                        assert!(!panic, "injected after committed marker");
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "injected publication interruption",
                        });
                    }
                    Ok(())
                })
            }));
            assert!(reached);
            if panic {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert_eq!(sender.observation(), Observation::Unconfirmed);
            assert_eq!(
                started(&node.get_pg(pg_id.get()).unwrap()),
                stage == "after-commit"
            );
            assert_eq!(without_marker(&node.get_pg(pg_id.get()).unwrap()), before);
            assert!(sender.send_embedded(&section, Instant::now()).is_err());
            assert_eq!(sender.observation(), Observation::Unconfirmed);
            sender.send_embedded(&section, deadline()).unwrap();
            assert_eq!(sender.observation(), Observation::MarkerConfirmed);
            assert!(started(&node.get_pg(pg_id.get()).unwrap()));
            assert_eq!(without_marker(&node.get_pg(pg_id.get()).unwrap()), before);
        }
    }
}

#[test]
fn staging_complete_publication_sender_cannot_renew_original_expired_budget() {
    let dir = test_util::tempdir();
    let (server, client, config) = installed_config(&dir, config(0), true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    install(&node.get_pg(pg_id.get()).unwrap(), &context);
    let mut sender = pending(&context).into_publication_sender(config.node_id, Instant::now());
    let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
    let replies = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&replies);
    server.set_response_frame_test_hook(Arc::new(move |kind, _| {
        if kind == KIND {
            observed.fetch_add(1, Ordering::SeqCst);
        }
    }));
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| server.accept_one().unwrap());
        let session = client
            .open_metadata_command_critical_section_until(pg_id, deadline())
            .unwrap();
        for _ in 0..2 {
            assert!(sender.send_remote(&session, deadline()).is_err());
            assert_eq!(sender.observation(), Observation::NoAttemptObserved);
            assert_eq!(replies.load(Ordering::SeqCst), 0);
            assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
        }
        drop(session);
        worker.join().unwrap();
    });
}
