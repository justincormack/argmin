// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::{ApplicationObservation as Applied, PublicationObservation};
use crate::storage_rpc::{self as wire, StorageRpcMessageKind as Kind};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const KIND: Kind = Kind::MetadataCommandApplyAndRecord;

pub(super) struct RunningReplica {
    stop: Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl RunningReplica {
    pub(super) fn start(
        server: Arc<crate::storage_node_server::StorageNodeServer>,
        _tls: bool,
    ) -> Self {
        Self::start_inner(server, false)
    }

    pub(super) fn start_concurrent(
        server: Arc<crate::storage_node_server::StorageNodeServer>,
        _tls: bool,
    ) -> Self {
        Self::start_inner(server, true)
    }

    fn start_inner(
        server: Arc<crate::storage_node_server::StorageNodeServer>,
        concurrent: bool,
    ) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_server = Arc::clone(&server);
        let worker = std::thread::spawn(move || {
            worker_server
                .test_serve_candidate_until_stop(&worker_stop, concurrent)
                .unwrap();
        });
        Self {
            stop,
            worker: Some(worker),
        }
    }
}

impl Drop for RunningReplica {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Err(panic) = self.worker.take().unwrap().join() {
            if !std::thread::panicking() {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

pub(super) fn replica_from_primary(
    tls: bool,
    primary: &crate::storage_node_server::StorageNodeServer,
    primary_config: &crate::storage_node_server::StorageNodeProcessConfig,
    pg_id: PgId,
    dir: &test_util::TempDir,
    node_id: NodeId,
) -> (
    crate::storage_node_server::StorageNodeServer,
    crate::node_runtime::clients::UnixStorageNodeClient,
) {
    replica_from_pg(
        tls,
        &primary.test_storage_node().get_pg(pg_id.get()).unwrap(),
        primary_config,
        dir,
        node_id,
    )
}

pub(super) fn replica_from_pg(
    tls: bool,
    primary: &PgStore,
    primary_config: &crate::storage_node_server::StorageNodeProcessConfig,
    dir: &test_util::TempDir,
    node_id: NodeId,
) -> (
    crate::storage_node_server::StorageNodeServer,
    crate::node_runtime::clients::UnixStorageNodeClient,
) {
    use std::os::unix::fs::PermissionsExt;
    let mut config = primary_config.clone();
    config.node_id = node_id;
    config.data_dir = dir.path().join("node");
    config.socket_path = dir.path().join("socket/node.sock");
    let pg_dir = config.data_dir.join(format!("pg-{:04}", primary.pg_id()));
    std::fs::create_dir_all(&pg_dir).unwrap();
    let path = pg_dir.join("metadata.db");
    // Metadata-only owner fixture: take a coherent SQLite image including the
    // exact preceding command/receipt history. Independently building the same
    // logical scenario mints different random allocation identities. This is
    // fixture setup, not a new production checkpoint-transfer protocol.
    primary
        .conn
        .execute("VACUUM INTO ?1", [path.to_str().unwrap()])
        .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let copy = rusqlite::Connection::open(&path).unwrap();
    copy.execute_batch("PRAGMA foreign_keys = ON; DELETE FROM metadata_command_pending_slot;")
        .unwrap();
    drop(copy); // The replica has the same prefix/materialized state, no primary slot.
    if tls {
        super::tls::tls_server_at(config.clone(), true, config.cluster_epoch)
    } else {
        std::fs::create_dir_all(config.socket_path.parent().unwrap()).unwrap();
        std::fs::set_permissions(
            config.socket_path.parent().unwrap(),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let (prepared, client) = prepared(&config);
        (prepared.bind_complete_for_test().unwrap(), client)
    }
}

#[test]
fn staging_complete_application_sender_reuses_publication_cutoff_and_survives_unwind() {
    let dir = test_util::tempdir();
    let (server, client, config, context) = application_rpc::setup(false, 0, false, &dir);
    let cutoff = deadline();
    let mut holder = pending(&context).into_publication_sender(config.node_id, cutoff);
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| server.accept_one().unwrap());
        let section = client
            .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
            .unwrap();
        holder.send_remote(&section, deadline()).unwrap();
        let clock = crate::clock::test_time_override_guard(1_000);
        let later = cutoff + Duration::from_secs(60);
        let mut frozen = None;
        for now in [1_000, 500, 200_000] {
            clock.set(now);
            let error = holder.test_application_attempt(config.node_id, later, |bytes, local| {
                assert_eq!(local, cutoff);
                let wire = wire::complete_application::decode_request(bytes).unwrap();
                assert_eq!(
                    wire.command.command,
                    context.encode_command().unwrap().bytes()
                );
                let portable = wire.deadline.portable_wall_valid_until_ms;
                // The publication was captured before the artificial wall
                // clock, not freshly rebound on first application preparation.
                assert!(portable > 200_000);
                if let Some(expected) = frozen {
                    assert_eq!(portable, expected);
                }
                frozen = Some(portable);
            });
            assert!(error.is_err());
            assert_eq!(
                holder.application_observation(config.node_id),
                Applied::Unconfirmed
            );
        }
        holder
            .test_application_attempt(
                config.node_id,
                cutoff - Duration::from_millis(1),
                |bytes, local| {
                    assert_eq!(local, cutoff - Duration::from_millis(1));
                    assert!(
                        wire::complete_application::decode_request(bytes)
                            .unwrap()
                            .deadline
                            .portable_wall_valid_until_ms
                            < frozen.unwrap()
                    );
                },
            )
            .unwrap_err();
        drop(clock);
        for confirmed in [false, true] {
            if confirmed {
                holder
                    .apply_remote_in_session(&section, deadline())
                    .unwrap();
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                holder.test_application_attempt(config.node_id, later, |_, _| {
                    panic!("interrupt borrowed application attempt")
                })
            }));
            assert!(result.is_err());
            assert_eq!(
                holder.application_observation(config.node_id),
                if confirmed {
                    Applied::Applied
                } else {
                    Applied::Unconfirmed
                }
            );
            assert_eq!(
                holder.application_observation(NodeId::new(2)),
                Applied::NoAttemptObserved
            );
            assert_eq!(
                holder.observation(),
                PublicationObservation::MarkerConfirmed
            );
            assert!(holder
                .apply_remote_in_session(&section, Instant::now())
                .is_err());
            assert_eq!(
                holder.application_observation(config.node_id),
                if confirmed {
                    Applied::Applied
                } else {
                    Applied::Unconfirmed
                }
            );
        }
        drop(section);
        worker.join().unwrap();
    });
}

#[test]
fn staging_complete_application_sender_attempt_is_noncloneable() {
    trait NotClone<A> {
        fn check() {}
    }
    impl<T: ?Sized> NotClone<()> for T {}
    impl<T: Clone> NotClone<u8> for T {}
    let _ = <crate::node_runtime::pg_store::CompleteApplicationAttempt<'_> as NotClone<_>>::check;
    let _ = <crate::node_runtime::pg_store::CompletePublicationSender as NotClone<_>>::check;
}

#[test]
fn staging_complete_application_sender_unix_tls_contexts_retain_independent_peer_history() {
    for tls in [false, true] {
        for case in 0..4 {
            let primary_dir = test_util::tempdir();
            let replica_dir = test_util::tempdir();
            let (primary, primary_client, primary_config, context) =
                application_rpc::setup(tls, case, false, &primary_dir);
            let (replica, replica_client) = replica_from_primary(
                tls,
                &primary,
                &primary_config,
                context.id().pg_id(),
                &replica_dir,
                NodeId::new(2),
            );
            let replica_id = replica_client.node_id();
            let node = replica.test_storage_node();
            let primary_node = primary.test_storage_node();
            let pg_id = context.id().pg_id();
            let mut holder =
                pending(&context).into_publication_sender(primary_config.node_id, deadline());
            let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
            // Having inspected an already-started slot does not itself grant
            // this holder publication confirmation or an application attempt.
            assert!(holder
                .apply_remote(&replica_client, deadline())
                .unwrap_err()
                .to_string()
                .contains("marker is not confirmed"));
            assert_eq!(
                holder.application_observation(replica_id),
                Applied::NoAttemptObserved
            );
            let replies = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&replies);
            replica.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                if kind != KIND {
                    return;
                }
                match observed.fetch_add(1, Ordering::SeqCst) {
                    1 | 4 => {
                        frame.payload = wire::encode_storage_rpc_error_response(
                            &wire::StorageRpcErrorResponse {
                                code: wire::StorageRpcErrorCode::ResourceExhausted,
                                message: "injected after complete application".into(),
                            },
                        )
                        .unwrap()
                    }
                    2 => {
                        let mut response =
                            wire::decode_storage_rpc_response_payload(&frame.payload)
                                .unwrap()
                                .unwrap();
                        response[30] ^= 1;
                        frame.payload = wire::encode_storage_rpc_success_response(&response);
                    }
                    _ => {}
                }
            }));
            let replica = Arc::new(replica);
            std::thread::scope(|scope| {
                let primary_worker = scope.spawn(|| primary.accept_one().unwrap());
                let section = primary_client
                    .open_metadata_command_critical_section_until(pg_id, deadline())
                    .unwrap();
                holder.send_remote(&section, deadline()).unwrap();
                assert!(holder
                    .apply_remote(&replica_client, Instant::now())
                    .is_err());
                assert_eq!(
                    holder.application_observation(replica_id),
                    Applied::NoAttemptObserved
                );
                assert_eq!(replies.load(Ordering::SeqCst), 0);
                let worker = scope.spawn(|| {
                    replica
                        .test_accept_one_complete_application_fault(false, true)
                        .unwrap()
                });
                assert!(holder.apply_remote(&replica_client, deadline()).is_err());
                worker.join().unwrap();
                assert_eq!(
                    holder.application_observation(replica_id),
                    Applied::Unconfirmed
                );
                assert_eq!(
                    holder.application_observation(primary_config.node_id),
                    Applied::NoAttemptObserved
                );
                assert_eq!(replies.load(Ordering::SeqCst), 1);
                let applied = snapshot(&node.get_pg(pg_id.get()).unwrap());
                assert_ne!(before, applied);
                let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
                // Completing another peer cannot erase this peer's ambiguity.
                holder
                    .apply_remote_in_session(&section, deadline())
                    .unwrap();
                assert_eq!(
                    holder.application_observation(primary_config.node_id),
                    Applied::Applied
                );
                assert_eq!(
                    holder.application_observation(replica_id),
                    Applied::Unconfirmed
                );
                assert!(holder
                    .apply_remote(&replica_client, Instant::now())
                    .is_err());
                assert_eq!(
                    holder.application_observation(replica_id),
                    Applied::Unconfirmed
                );
                assert_eq!(replies.load(Ordering::SeqCst), 1);
                let _running = RunningReplica::start(Arc::clone(&replica), tls);
                for expected in 2..=5 {
                    let result = holder.apply_remote(&replica_client, deadline());
                    if expected == 4 {
                        assert_eq!(
                            result.unwrap().applied_log_index,
                            context.id().log_index().get()
                        );
                    } else {
                        assert!(result.is_err());
                    }
                    assert_eq!(
                        holder.application_observation(replica_id),
                        if expected < 4 {
                            Applied::Unconfirmed
                        } else {
                            Applied::Applied
                        }
                    );
                    assert_eq!(
                        holder.application_observation(primary_config.node_id),
                        Applied::Applied
                    );
                    assert_eq!(
                        holder.observation(),
                        PublicationObservation::MarkerConfirmed
                    );
                    assert_eq!(holder.installation_observation(), None);
                    assert_eq!(holder.id(), Some(context.id()));
                    assert_eq!(
                        replies.load(Ordering::SeqCst),
                        expected,
                        "no implicit transport retry"
                    );
                    assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), applied);
                    assert_eq!(
                        node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
                        writes
                    );
                }
                assert!(
                    started(&primary_node.get_pg(pg_id.get()).unwrap()),
                    "application is not slot retirement"
                );
                drop(section);
                primary_worker.join().unwrap();
            });
            let applied = snapshot(&primary_node.get_pg(pg_id.get()).unwrap());
            drop(holder);
            assert_eq!(
                snapshot(&primary_node.get_pg(pg_id.get()).unwrap()),
                applied,
                "drop cannot perform cleanup"
            );
        }
    }
}

#[test]
fn staging_complete_application_sender_rejects_crossed_primary_sections_before_transport() {
    for crossing in ["node", "pg", "epoch"] {
        let dir = test_util::tempdir();
        let (server, client, config, context) = application_rpc::setup(false, 0, false, &dir);
        let mut holder = pending(&context).into_publication_sender(config.node_id, deadline());
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| server.accept_one().unwrap());
            let section = client
                .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
                .unwrap();
            holder.send_remote(&section, deadline()).unwrap();
            drop(section);
            worker.join().unwrap();
        });
        let other_dir = test_util::tempdir();
        let mut other_config = super::config(if crossing == "epoch" { 2 } else { 0 });
        if crossing == "node" {
            other_config.node_id = NodeId::new(2);
            for route in &mut other_config.pg_routes {
                route.primary_node_id = other_config.node_id;
            }
        }
        let (other, other_client, _) = installed_config(&other_dir, other_config, true);
        let replies = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&replies);
        other.set_response_frame_test_hook(Arc::new(move |kind, _| {
            if kind == KIND {
                observed.fetch_add(1, Ordering::SeqCst);
            }
        }));
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| other.accept_one().unwrap());
            let pg = if crossing == "pg" {
                PgId::new((context.id().pg_id().get() + 1) % 8)
            } else {
                context.id().pg_id()
            };
            let section = other_client
                .open_metadata_command_critical_section_until(pg, deadline())
                .unwrap();
            assert!(holder
                .apply_remote_in_session(&section, deadline())
                .unwrap_err()
                .to_string()
                .contains("complete application session subject"));
            assert_eq!(
                holder.application_observation(config.node_id),
                Applied::NoAttemptObserved
            );
            assert_eq!(replies.load(Ordering::SeqCst), 0);
            drop(section);
            worker.join().unwrap();
        });
    }
}

#[test]
fn staging_complete_application_sender_preserves_bad_authentication_uncertainty() {
    for tls in [false, true] {
        let primary_dir = test_util::tempdir();
        let replica_dir = test_util::tempdir();
        let (primary, primary_client, config, context) =
            application_rpc::setup(tls, 0, false, &primary_dir);
        let (replica, replica_client) = replica_from_primary(
            tls,
            &primary,
            &config,
            context.id().pg_id(),
            &replica_dir,
            NodeId::new(2),
        );
        let replica_id = replica_client.node_id();
        let mut holder = pending(&context).into_publication_sender(config.node_id, deadline());
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| primary.accept_one().unwrap());
            let section = primary_client
                .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
                .unwrap();
            holder.send_remote(&section, deadline()).unwrap();
            drop(section);
            worker.join().unwrap();
        });
        replica.set_response_envelope_test_hook(Arc::new(|kind, bytes| {
            if kind == KIND {
                *bytes.last_mut().unwrap() ^= 1;
            }
        }));
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| replica.accept_one().unwrap());
            assert!(holder.apply_remote(&replica_client, deadline()).is_err());
            worker.join().unwrap();
        });
        assert_eq!(
            holder.application_observation(replica_id),
            Applied::Unconfirmed
        );
        assert!(holder
            .apply_remote(&replica_client, Instant::now())
            .is_err());
        assert_eq!(
            holder.application_observation(replica_id),
            Applied::Unconfirmed
        );
        assert_eq!(
            holder.observation(),
            PublicationObservation::MarkerConfirmed
        );
    }
}
