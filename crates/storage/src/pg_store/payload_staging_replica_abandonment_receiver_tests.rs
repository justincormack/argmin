// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Receiver slice only: raw frames below are owner-test seams, not a production
//! sender. The primary is confirmed under its still-held embedded section.

use super::fanout_mixed::{Mixed, Remote};
use super::*;
use crate::node_runtime::pg_store::CompleteReplicaAbandonmentQuery;
use crate::storage_rpc::{
    self as rpc, complete_replica_abandonment as wire, StorageRpcOperationDeadline,
};
use std::sync::Arc;

pub(super) fn encode(
    context: &ContextualCommand,
    primary: NodeId,
    node: NodeId,
    cutoff: u64,
) -> Vec<u8> {
    let command = context.encode_command().unwrap();
    wire::encode_request(wire::BorrowedRequest {
        primary,
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

fn payload(mixed: &Mixed, node: NodeId) -> Vec<u8> {
    encode(
        &mixed.context,
        NodeId::new(4),
        node,
        crate::clock::current_time_millis() + 60_000,
    )
}

fn with_primary(tls: bool, case: usize, action: impl FnOnce(&Mixed)) {
    let dir = test_util::tempdir();
    let mixed = Mixed::with_endpoints(&dir, case, [(0, tls), (2, !tls)]);
    let id = mixed.context.id();
    let admission = mixed.handle.admit_current_route().unwrap();
    let node = mixed.node(NodeId::new(4));
    let source = abandonment_remote::sender(
        &node.get_pg(id.pg_id().get()).unwrap(),
        &mixed.context,
        "inspected",
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
    let route = admission
        .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    owner.record_primary_embedded(&section, deadline()).unwrap();
    action(&mixed);
}

pub(super) fn confirm(bytes: &[u8], context: &ContextualCommand, node: NodeId) {
    wire::decode_confirmation_for(
        bytes,
        NodeId::new(4),
        node,
        context.id(),
        context.encode_command().unwrap().command_sha256(),
    )
    .unwrap();
}

#[test]
fn staging_replica_abandonment_receiver_unix_tls_all_contexts_preserve_custody_and_replay() {
    for tls in [false, true] {
        for case in 0..4 {
            with_primary(tls, case, |mixed| {
                for existing_slot in [false, true] {
                    let n = NodeId::new(if existing_slot { 2 } else { 0 });
                    let peer = mixed.remote(n).unwrap();
                    let node = mixed.node(n);
                    let pg = node.get_pg(mixed.context.id().pg_id().get()).unwrap();
                    if existing_slot {
                        install(&pg, &mixed.context);
                    }
                    let retained = (introduction::state(&pg), complete_pending::observe(&pg));
                    drop(pg);
                    let before = mixed.observe();
                    let _running = peer.run();
                    let reply = peer
                        .client
                        .test_replica_abandonment_request_until(payload(mixed, n), deadline())
                        .unwrap()
                        .unwrap();
                    confirm(&reply, &mixed.context, n);
                    let after = mixed.observe();
                    assert_ne!(before[n.as_u32() as usize], after[n.as_u32() as usize]);
                    for other in (0..5).filter(|id| *id != n.as_u32() as usize) {
                        assert_eq!(before[other], after[other]);
                    }
                    let pg = node.get_pg(mixed.context.id().pg_id().get()).unwrap();
                    assert_eq!(
                        (introduction::state(&pg), complete_pending::observe(&pg)),
                        retained
                    );
                    drop(pg);
                    let reply = peer
                        .client
                        .test_replica_abandonment_request_until(payload(mixed, n), deadline())
                        .unwrap()
                        .unwrap();
                    confirm(&reply, &mixed.context, n);
                    assert_eq!(
                        mixed.observe(),
                        after,
                        "exact tombstone replay must not write"
                    );
                }
            });
        }
    }
}

#[test]
fn staging_replica_abandonment_receiver_rejects_started_applied_crossed_and_orphan_custody() {
    for tls in [false, true] {
        for damage in [
            "started",
            "applied",
            "crossed-command",
            "null-scope",
            "crossed-scope",
            "routes",
            "staging",
            "orphan",
        ] {
            with_primary(tls, 1, |mixed| {
                let peer = mixed.remote(NodeId::new(0)).unwrap();
                let node = mixed.node(peer.node);
                let pg = node.get_pg(mixed.context.id().pg_id().get()).unwrap();
                install(&pg, &mixed.context);
                match damage {
                    "applied" => apply(&pg, &mixed.context),
                    "crossed-command" => {
                        let id = mixed.context.id();
                        // Wrong exact slot position is rejected before the recorder.
                        pg.conn
                            .execute(
                                "UPDATE metadata_command_pending_slot SET log_index=?1",
                                [id.log_index().get() + 1],
                            )
                            .unwrap();
                    }
                    mode => {
                        pg.conn.execute_batch(match mode {
                        "started" => "UPDATE metadata_command_pending_slot SET publication_started=1",
                        "null-scope" => "UPDATE metadata_command_pending_slot SET scope_bucket=NULL",
                        "crossed-scope" => "UPDATE metadata_command_pending_slot SET scope_bucket='other-bucket'",
                        "routes" => "DELETE FROM metadata_command_pending_route_dependencies",
                        "staging" => "DELETE FROM metadata_command_pending_staging_protection",
                            "orphan" => "PRAGMA foreign_keys=OFF; DELETE FROM metadata_command_pending_slot; PRAGMA foreign_keys=ON;",
                        _ => unreachable!(),
                    }).unwrap();
                    }
                }
                if damage == "orphan" {
                    let present: bool = pg.conn.query_row("SELECT EXISTS(SELECT 1 FROM metadata_command_pending_staging_protection)", [], |row| row.get(0)).unwrap();
                    assert!(present, "orphan canary must retain the sidecar");
                }
                drop(pg);
                let before = mixed.observe();
                let _running = peer.run();
                let error = peer
                    .client
                    .test_replica_abandonment_request_until(payload(mixed, peer.node), deadline())
                    .unwrap()
                    .expect_err(damage);
                assert_eq!(
                    error.code,
                    rpc::StorageRpcErrorCode::Internal,
                    "{damage}: {error:?}"
                );
                assert_eq!(mixed.observe(), before, "{damage}");
            });
        }
    }
}

#[test]
fn staging_replica_abandonment_receiver_rejects_crossed_role_subject_and_admission() {
    for tls in [false, true] {
        with_primary(tls, 2, |mixed| {
            let peer = mixed.remote(NodeId::new(0)).unwrap();
            let query =
                CompleteReplicaAbandonmentQuery::decode(&payload(mixed, peer.node), &topology())
                    .unwrap();
            peer.server.test_replica_abandonment_crossed_admission(
                &mixed.remote(NodeId::new(2)).unwrap().server,
                &query.request().unwrap(),
            );
            let before = mixed.observe();
            {
                let _running = peer.run();
                for mode in [
                    "primary",
                    "node",
                    "epoch",
                    "pg",
                    "context",
                    "primary-purpose",
                    "expired",
                ] {
                    let mut bytes = payload(mixed, peer.node);
                    match mode {
                        "primary" => bytes[14..18].copy_from_slice(&2_u32.to_le_bytes()),
                        "node" => bytes[18..22].copy_from_slice(&2_u32.to_le_bytes()),
                        "epoch" => bytes[22] ^= 1,
                        "pg" => bytes[30] ^= 1,
                        "context" => *bytes.last_mut().unwrap() ^= 1,
                        "primary-purpose" => bytes[..4].copy_from_slice(b"ABND"),
                        "expired" => bytes[6..14].copy_from_slice(&1_u64.to_le_bytes()),
                        _ => unreachable!(),
                    }
                    assert!(
                        peer.client
                            .test_replica_abandonment_request_until(bytes, deadline())
                            .unwrap()
                            .is_err(),
                        "{mode}"
                    );
                    assert_eq!(mixed.observe(), before, "{mode}");
                }
            }
        });
    }
}

#[test]
fn staging_replica_abandonment_receiver_live_dispatch_rejects_candidate_grammar() {
    let dir = test_util::tempdir();
    let (server, client, config) = installed_config(&dir, config(0), false);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
    let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
    let bytes = encode(
        &context,
        NodeId::new(0),
        config.node_id,
        crate::clock::current_time_millis() + 60_000,
    );
    let _running =
        super::super::complete_log_rpc::Running::start(server, config.socket_path.clone());
    let error = client
        .test_replica_abandonment_request_until(bytes, deadline())
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, rpc::StorageRpcErrorCode::PayloadDecode);
    assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
}

struct Wake<'a>(Option<&'a Remote>);
impl Drop for Wake<'_> {
    fn drop(&mut self) {
        if let Some(peer) = self.0 {
            if peer.tls {
                let _ = std::net::TcpStream::connect(peer.server.tcp_listener_addr_for_test());
            } else {
                let _ = std::os::unix::net::UnixStream::connect(peer.server.socket_path_for_test());
            }
        }
    }
}

#[test]
fn staging_replica_abandonment_receiver_lost_reply_replays_without_mutation() {
    for tls in [false, true] {
        with_primary(tls, 3, |mixed| {
            let peer = mixed.remote(NodeId::new(0)).unwrap();
            let before = mixed.observe();
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    peer.server
                        .test_accept_one_replica_abandonment_hook(|_| (), true)
                        .unwrap()
                });
                let mut wake = Wake(Some(peer));
                assert!(peer
                    .client
                    .test_replica_abandonment_request_until(payload(mixed, peer.node), deadline())
                    .is_err());
                wake.0 = None;
                worker.join().unwrap();
            });
            let after = mixed.observe();
            assert_ne!(after[0], before[0]);
            let _running = peer.run();
            let reply = peer
                .client
                .test_replica_abandonment_request_until(payload(mixed, peer.node), deadline())
                .unwrap()
                .unwrap();
            confirm(&reply, &mixed.context, peer.node);
            assert_eq!(mixed.observe(), after);
        });
    }
}

#[test]
fn staging_replica_abandonment_receiver_wire_expiry_is_checked_at_insert_and_commit() {
    for tls in [false, true] {
        for boundary in ["before-tombstone", "before-commit"] {
            with_primary(tls, 1, |mixed| {
                let peer = mixed.remote(NodeId::new(0)).unwrap();
                let before = mixed.observe();
                let cutoff = crate::clock::current_time_millis() + 2_000;
                let bytes = encode(&mixed.context, NodeId::new(4), peer.node, cutoff);
                let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let seen = Arc::clone(&reached);
                std::thread::scope(|scope| {
                    let worker = scope.spawn(|| {
                        peer.server
                            .test_accept_one_replica_abandonment_hook(
                                move |at| {
                                    if at == boundary {
                                        seen.store(true, std::sync::atomic::Ordering::SeqCst);
                                        assert!(crate::clock::current_time_millis() < cutoff);
                                        while crate::clock::current_time_millis() <= cutoff {
                                            std::thread::sleep(Duration::from_millis(1));
                                        }
                                    }
                                },
                                false,
                            )
                            .unwrap()
                    });
                    let mut wake = Wake(Some(peer));
                    let error = peer
                        .client
                        .test_replica_abandonment_request_until(bytes, deadline())
                        .unwrap()
                        .unwrap_err();
                    wake.0 = None;
                    worker.join().unwrap();
                    assert_eq!(
                        error.code,
                        rpc::StorageRpcErrorCode::TransportTimeout,
                        "{error:?}"
                    );
                });
                assert!(reached.load(std::sync::atomic::Ordering::SeqCst));
                for ((after, changes), (before, old_changes)) in mixed.observe().iter().zip(&before)
                {
                    assert_eq!(after, before);
                    if boundary == "before-tombstone" {
                        assert_eq!(changes, old_changes);
                    }
                }
            });
        }
    }
}
