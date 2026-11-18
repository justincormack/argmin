// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed authenticated primary only. No live selection or payload cleanup.

use super::fanout_mixed::{Mixed, Remote};
use super::*;
use crate::node_runtime::pg_store::{
    CompleteAbandonmentQuery, CompleteAbandonmentSendError as E, CompletePendingRequest,
    CompletePublicationSender, CompleteUnstartedAbandonment, UnstartedAbandonmentObservation as O,
};
use crate::storage_rpc::{
    self as rpc, complete_abandonment as wire, StorageRpcMessageKind as Kind,
    StorageRpcOperationDeadline,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const KIND: Kind = Kind::MetadataCommandRecordAbandoned;

#[test]
fn staging_replica_abandonment_remote_primary_unix_tls_retains_session_for_embedded_target() {
    use crate::node_runtime::pg_store::ReplicaAbandonmentObservation as R;
    for tls in [false, true] {
        for case in 0..4 {
            with_owner(tls, case, "uncertain", |mixed, owner| {
                let primary = mixed.remote(NodeId::new(4)).unwrap();
                let _running = primary.run();
                let session = owner
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                let before = mixed.observe();
                assert!(owner
                    .record_embedded_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        NodeId::new(2),
                        deadline()
                    )
                    .is_err());
                assert_eq!(mixed.observe(), before);
                owner
                    .record_primary_remote(&primary.client, &session, deadline())
                    .unwrap();
                let before = mixed.observe();
                assert!(owner
                    .record_embedded_replica_from_remote_primary(
                        &mixed.remote(NodeId::new(0)).unwrap().client,
                        &session,
                        NodeId::new(2),
                        deadline()
                    )
                    .is_err());
                assert_eq!(mixed.observe(), before);
                let node = mixed.node(NodeId::new(2));
                let id = mixed.context.id();
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                let unchanged = retained(&pg);
                drop(pg);
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
                let after = mixed.observe();
                assert_ne!(after[2], before[2]);
                for n in [0, 1, 3, 4] {
                    assert_eq!(after[n], before[n]);
                }
                assert_eq!(retained(&node.get_pg(id.pg_id().get()).unwrap()), unchanged);
                owner
                    .record_embedded_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        NodeId::new(2),
                        deadline(),
                    )
                    .unwrap();
                assert_eq!(mixed.observe(), after);
                // This slice cannot select a remotely installed replica.
                assert!(owner
                    .record_embedded_replica_from_remote_primary(
                        &primary.client,
                        &session,
                        NodeId::new(0),
                        deadline()
                    )
                    .is_err());
                assert_eq!(
                    owner.replica_observation(NodeId::new(0)),
                    R::NoAttemptObserved
                );
            });
        }
    }
}

pub(super) fn encode(context: &ContextualCommand, node: NodeId, cutoff: u64) -> Vec<u8> {
    let command = context.encode_command().unwrap();
    wire::encode_request(wire::BorrowedRequest {
        command: rpc::complete_applied::BorrowedRequest {
            node_id: node,
            epoch: context.id().cluster_epoch(),
            pg_id: context.id().pg_id(),
            checksum: command.checksum_crc64(),
            command: command.bytes(),
        },
        deadline: StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: cutoff,
        },
    })
    .unwrap()
}

pub(super) fn sender(
    pg: &PgStore,
    context: &ContextualCommand,
    origin: &str,
    cutoff: Instant,
) -> CompletePublicationSender {
    if origin == "inspected" {
        return CompletePendingRequest::new(
            context.id().pg_id(),
            context.id().cluster_epoch(),
            &topology(),
        )
        .unwrap()
        .read(pg, 4)
        .unwrap()
        .into_publication_sender(NodeId::new(4), cutoff);
    }
    let encoded = context.encode_command().unwrap();
    let mut submission =
        ContextualCommand::decode_command(encoded.bytes(), encoded.checksum_crc64(), &topology())
            .unwrap()
            .into_submission();
    if origin == "uncertain" {
        pg.conn
            .execute_batch("DELETE FROM metadata_command_pending_slot")
            .unwrap();
        handoff::uncertain_install(&mut submission, pg);
        install(pg, context);
    } else {
        assert_eq!(origin, "installed");
        complete_pending::install(&mut submission, pg).unwrap();
    }
    submission.into_publication_sender(NodeId::new(4), cutoff)
}

pub(super) fn with_owner(
    tls: bool,
    case: usize,
    origin: &str,
    action: impl FnOnce(&Mixed, &mut CompleteUnstartedAbandonment<'_>),
) {
    let dir = test_util::tempdir();
    let mixed = Mixed::with_endpoints(&dir, case, [(4, tls), (0, !tls)]);
    let id = mixed.context.id();
    let admission = mixed.handle.admit_current_route().unwrap();
    let node = mixed.node(NodeId::new(4));
    let source = sender(
        &node.get_pg(id.pg_id().get()).unwrap(),
        &mixed.context,
        origin,
        deadline(),
    );
    let mut owner = source
        .into_fanout(
            admission
                .complete_fanout_route(id.pg_id(), id.cluster_epoch())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"))
        .into_unstarted_abandonment()
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    action(&mixed, &mut owner);
}

fn retained(
    pg: &PgStore,
) -> (
    Vec<crate::node_runtime::pg_store::MetadataCheckpointTableBlock>,
    Vec<Vec<String>>,
) {
    (introduction::state(pg), complete_pending::observe(pg))
}

struct Wake<'a>(Option<&'a Remote>);
impl Drop for Wake<'_> {
    fn drop(&mut self) {
        if let Some(remote) = self.0 {
            if remote.tls {
                let _ = std::net::TcpStream::connect(remote.server.tcp_listener_addr_for_test());
            } else {
                let _ =
                    std::os::unix::net::UnixStream::connect(remote.server.socket_path_for_test());
            }
        }
    }
}

#[test]
fn staging_abandonment_remote_unix_tls_contexts_origins_and_primary_only_confirmation() {
    for tls in [false, true] {
        for case in 0..4 {
            for origin in ["inspected", "installed", "uncertain"] {
                with_owner(tls, case, origin, |mixed, owner| {
                    let primary = mixed.remote(NodeId::new(4)).unwrap();
                    let node = mixed.node(NodeId::new(4));
                    let id = mixed.context.id();
                    let before = retained(&node.get_pg(id.pg_id().get()).unwrap());
                    let peers = mixed.observe();
                    let original = owner.installation_observation();
                    let seen = Arc::new(AtomicUsize::new(0));
                    let count = Arc::clone(&seen);
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, _| {
                            if kind == KIND {
                                count.fetch_add(1, Ordering::SeqCst);
                            }
                        }));
                    let _running = primary.run();
                    let session = owner
                        .open_primary_session(&primary.client, deadline())
                        .unwrap();
                    owner
                        .record_primary_remote(&primary.client, &session, deadline())
                        .unwrap();
                    assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
                    assert_eq!(owner.installation_observation(), original);
                    assert_eq!(retained(&node.get_pg(id.pg_id().get()).unwrap()), before);
                    let after = mixed.observe();
                    assert_ne!(after[4], peers[4]);
                    assert_eq!(&after[..4], &peers[..4]);
                    owner
                        .record_primary_remote(&primary.client, &session, deadline())
                        .unwrap();
                    assert_eq!(
                        seen.load(Ordering::SeqCst),
                        1,
                        "confirmed owner never redispatches"
                    );
                    assert_eq!(mixed.observe(), after);
                });
            }
        }
    }
}

#[test]
fn staging_abandonment_remote_lost_reply_preserves_uncertainty_and_no_write_retry() {
    for tls in [false, true] {
        with_owner(tls, 2, "uncertain", |mixed, owner| {
            let primary = mixed.remote(NodeId::new(4)).unwrap();
            let node = mixed.node(NodeId::new(4));
            let id = mixed.context.id();
            let before = retained(&node.get_pg(id.pg_id().get()).unwrap());
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    primary
                        .server
                        .test_accept_one_abandonment_fault(false, true)
                        .unwrap()
                });
                let mut wake = Wake(Some(primary));
                let session = owner
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                assert!(matches!(
                    owner.record_primary_remote(&primary.client, &session, deadline()),
                    Err(E::Unconfirmed(_))
                ));
                wake.0 = None;
                drop(session);
                worker.join().unwrap();
            });
            assert_eq!(owner.observation(), O::Unconfirmed);
            assert_eq!(retained(&node.get_pg(id.pg_id().get()).unwrap()), before);
            let after = mixed.observe();
            assert_ne!(
                node.get_pg(id.pg_id().get())
                    .unwrap()
                    .metadata_command_replica_state()
                    .unwrap()
                    .applied_log_index,
                0
            );
            let _running = primary.run();
            let session = owner
                .open_primary_session(&primary.client, deadline())
                .unwrap();
            assert!(matches!(
                owner.record_primary_remote(&primary.client, &session, Instant::now()),
                Err(E::NotSent(_))
            ));
            assert_eq!(owner.observation(), O::Unconfirmed);
            owner
                .record_primary_remote(&primary.client, &session, deadline())
                .unwrap();
            assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
            assert_eq!(
                mixed.observe(),
                after,
                "exact tombstone retry does no writes"
            );
        });
    }
}

#[test]
fn staging_abandonment_remote_receiver_rejects_started_applied_and_damaged_pending() {
    for tls in [false, true] {
        for damage in [
            "UPDATE metadata_command_pending_slot SET publication_started=1",
            "UPDATE metadata_command_pending_slot SET scope_bucket=NULL",
            "DELETE FROM metadata_command_pending_staging_protection",
            "applied",
        ] {
            with_owner(tls, 1, "uncertain", |mixed, owner| {
                let primary = mixed.remote(NodeId::new(4)).unwrap();
                let node = mixed.node(NodeId::new(4));
                let id = mixed.context.id();
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                if damage == "applied" {
                    apply(&pg, &mixed.context);
                } else {
                    pg.conn.execute_batch(damage).unwrap();
                }
                drop(pg);
                let before = mixed.observe();
                let _running = primary.run();
                let session = owner
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                assert!(
                    matches!(
                        owner.record_primary_remote(&primary.client, &session, deadline()),
                        Err(E::Unconfirmed(_))
                    ),
                    "{damage}"
                );
                assert_eq!(owner.observation(), O::Unconfirmed);
                assert_eq!(mixed.observe(), before, "{damage}");
            });
        }
    }
}

#[test]
fn staging_abandonment_remote_receiver_expires_inside_sql_and_rejects_crossed_admission() {
    for tls in [false, true] {
        with_owner(tls, 2, "uncertain", |mixed, owner| {
            let primary = mixed.remote(NodeId::new(4)).unwrap();
            let other = mixed.remote(NodeId::new(0)).unwrap();
            let id = mixed.context.id();
            let query = CompleteAbandonmentQuery::decode(
                &encode(
                    &mixed.context,
                    primary.node,
                    crate::clock::current_time_millis() + 60_000,
                ),
                &topology(),
            )
            .unwrap();
            let before = mixed.observe();
            primary
                .server
                .test_abandonment_crossed_admission(&other.server, &query.request().unwrap());
            assert_eq!(mixed.observe(), before);
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    primary
                        .server
                        .test_accept_one_abandonment_fault(true, false)
                        .unwrap()
                });
                let mut wake = Wake(Some(primary));
                let session = owner
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                let payload = encode(
                    &mixed.context,
                    primary.node,
                    crate::clock::current_time_millis() + 1_000,
                );
                let error = session
                    .test_abandonment_payload(payload, deadline())
                    .unwrap_err();
                assert!(
                    matches!(
                        error,
                        StoreError::StorageRpc {
                            failure: rpc::StorageRpcErrorCode::TransportTimeout,
                            ..
                        }
                    ),
                    "{error:?}"
                );
                wake.0 = None;
                drop(session);
                worker.join().unwrap();
            });
            assert_eq!(mixed.observe(), before);
            assert_eq!(
                owner.observation(),
                O::NoAttemptObserved,
                "raw receiver test does not forge sender history"
            );
            assert!(!started(
                &mixed.node(NodeId::new(4)).get_pg(id.pg_id().get()).unwrap()
            ));
        });
    }
}

#[test]
fn staging_abandonment_remote_transport_freezes_cutoff_and_validates_every_confirmation_field() {
    with_owner(false, 2, "uncertain", |mixed, owner| {
        let before = mixed.observe();
        let encoded = mixed.context.encode_command().unwrap();
        let reply = wire::encode_confirmation(wire::Confirmation {
            node_id: NodeId::new(4),
            id: mixed.context.id(),
            command_sha256: encoded.command_sha256(),
        });
        let failure = || StoreError::OperationDeadlineExceeded {
            context: "injected abandonment send failure",
        };
        let mut frozen = None;
        assert!(matches!(
            owner.test_remote_attempt(deadline(), |a| {
                frozen = Some((a.encode_request().unwrap(), a.deadline()));
                Err(a.not_sent(failure()))
            }),
            Err(E::NotSent(_))
        ));
        assert_eq!(owner.observation(), O::NoAttemptObserved);
        let (frozen, cutoff) = frozen.unwrap();
        assert!(matches!(
            owner.test_remote_attempt(deadline() + Duration::from_secs(600), |a| {
                assert_eq!(a.encode_request().unwrap(), frozen);
                assert_eq!(a.deadline(), cutoff);
                Err(a.unconfirmed(failure()))
            }),
            Err(E::Unconfirmed(_))
        ));
        assert!(matches!(
            owner.test_remote_attempt(deadline(), |a| Err(a.not_sent(failure()))),
            Err(E::NotSent(_))
        ));
        assert_eq!(owner.observation(), O::Unconfirmed);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ =
                owner.test_remote_attempt(deadline(), |_| panic!("interrupt abandonment dispatch"));
        }))
        .is_err());
        for offset in [0, 4, 6, 10, 18, 22, 30] {
            let mut wrong = reply.clone();
            wrong[offset] ^= 1;
            assert!(matches!(
                owner.test_remote_attempt(deadline(), |a| a.confirm(&wrong)),
                Err(E::Unconfirmed(_))
            ));
            assert_eq!(owner.observation(), O::Unconfirmed);
        }
        owner
            .test_remote_attempt(deadline(), |a| a.confirm(&reply))
            .unwrap();
        assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
        owner
            .test_remote_attempt(deadline(), |_| panic!("confirmed redispatch"))
            .unwrap();
        assert_eq!(
            mixed.observe(),
            before,
            "confirmation-parser test grants no store effects"
        );
    });
}

#[test]
fn staging_abandonment_remote_signed_crossed_confirmation_never_resolves_custody() {
    for tls in [false, true] {
        with_owner(tls, 2, "uncertain", |mixed, owner| {
            let primary = mixed.remote(NodeId::new(4)).unwrap();
            let seen = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&seen);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind == KIND && count.fetch_add(1, Ordering::SeqCst) == 0 {
                        rpc::decode_storage_rpc_response_payload(&frame.payload)
                            .unwrap()
                            .expect("commit must succeed before corrupting its confirmation");
                        *frame.payload.last_mut().unwrap() ^= 1; // authenticated wrong complete SHA
                    }
                }));
            let _running = primary.run();
            let session = owner
                .open_primary_session(&primary.client, deadline())
                .unwrap();
            assert!(matches!(
                owner.record_primary_remote(&primary.client, &session, deadline()),
                Err(E::Unconfirmed(StoreError::StorageRpc {
                    failure: rpc::StorageRpcErrorCode::PayloadDecode,
                    ..
                }))
            ));
            assert_eq!(owner.observation(), O::Unconfirmed);
            assert_eq!(seen.load(Ordering::SeqCst), 1);
            let after = mixed.observe();
            owner
                .record_primary_remote(&primary.client, &session, deadline())
                .unwrap();
            assert_eq!(owner.observation(), O::PrimaryTombstoneConfirmed);
            assert_eq!(seen.load(Ordering::SeqCst), 2);
            assert_eq!(mixed.observe(), after);
        });
    }
}

#[test]
fn staging_abandonment_remote_live_dispatch_rejects_candidate_purpose_without_mutation() {
    let dir = test_util::tempdir();
    let (server, client, config) = installed_config(&dir, config(0), false);
    let node = server.test_storage_node();
    let id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(id.get()).unwrap(), 0);
    let before = snapshot(&node.get_pg(id.get()).unwrap());
    let payload = encode(
        &context,
        config.node_id,
        crate::clock::current_time_millis() + 60_000,
    );
    let running =
        super::super::complete_log_rpc::Running::start(server, config.socket_path.clone());
    let session = client
        .open_metadata_command_critical_section_until(id, deadline())
        .unwrap();
    let error = session
        .test_abandonment_payload(payload, deadline())
        .unwrap_err();
    assert!(
        matches!(
            error,
            StoreError::StorageRpc {
                failure: rpc::StorageRpcErrorCode::PayloadDecode,
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(snapshot(&node.get_pg(id.get()).unwrap()), before);
    drop(session);
    drop(running);
}

#[test]
fn staging_abandonment_remote_receiver_unconfirmed_rollback_retires_connection() {
    with_owner(false, 2, "uncertain", |mixed, _| {
        let primary = mixed.remote(NodeId::new(4)).unwrap();
        let query = CompleteAbandonmentQuery::decode(
            &encode(
                &mixed.context,
                primary.node,
                crate::clock::current_time_millis() + 60_000,
            ),
            &topology(),
        )
        .unwrap();
        let request = query.request().unwrap();
        let mut reached = false;
        let error = primary
            .server
            .test_with_abandonment_authority(&request, deadline(), |pg, authority| {
                pg.test_fail_metadata_command_rollbacks(1);
                request.test_abandon_with_hook(pg, authority, |stage| {
                    if stage == "before-commit" {
                        reached = true;
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "injected remote abandonment rollback failure",
                        });
                    }
                    Ok(())
                })
            })
            .unwrap()
            .unwrap_err();
        assert!(reached);
        assert!(error.to_string().contains("rollback failed"), "{error}");
        assert!(mixed
            .node(NodeId::new(4))
            .get_pg(mixed.context.id().pg_id().get())
            .is_err());
    });
}
