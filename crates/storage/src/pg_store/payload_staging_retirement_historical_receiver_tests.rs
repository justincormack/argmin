// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::CompleteRetirementQuery;
use crate::storage_rpc::{
    self as rpc, complete_retirement as wire, StorageRpcErrorCode as Code,
    StorageRpcOperationDeadline,
};

fn encode(context: &ContextualCommand, node: NodeId, cutoff: u64) -> Vec<u8> {
    let encoded = context.encode_command().unwrap();
    wire::encode_request(wire::BorrowedRequest {
        command: rpc::complete_applied::BorrowedRequest {
            node_id: node,
            epoch: context.id().cluster_epoch(),
            pg_id: context.id().pg_id(),
            checksum: encoded.checksum_crc64(),
            command: encoded.bytes(),
        },
        deadline: StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: cutoff,
        },
    })
    .unwrap()
}

#[test]
fn staging_retirement_historical_receiver_exact_report_and_section_domains() {
    for tls in [false, true] {
        let mixed = Mixed::new(1, &[(4, tls), (0, !tls)]);
        with_terminal(&mixed, "uncertain", deadline(), |_, terminal| {
            let primary = mixed.peer(4);
            let context = &mixed.fixture.context;
            let payload = encode(
                context,
                primary.id,
                crate::clock::current_time_millis() + 60_000,
            );
            let query = CompleteRetirementQuery::decode(&payload, &topology()).unwrap();
            let before = mixed.observe();
            primary.server.test_retirement_crossed_admission(
                &mixed.peer(0).server,
                &query.request().unwrap(),
            );
            assert_eq!(mixed.observe(), before);

            // Keep command ID and CRC unchanged. Only the primary's reported
            // complete SHA differs; local pending/Applied evidence still matches.
            let mut crossed = primary.config.clone();
            let p = crossed.pending_metadata_command_recoveries[0].1.pending();
            let mut sha = p.command_sha256();
            sha[0] ^= 1;
            crossed.pending_metadata_command_recoveries[0].1 =
                crate::control_plane::PendingMetadataCommandRecovery::new(
                    primary.id,
                    PendingMetadataCommandObservation::new(
                        p.cluster_epoch(),
                        NonZeroU64::new(p.log_index()).unwrap(),
                        p.command_checksum(),
                        sha,
                    ),
                );
            primary
                .server
                .install_control_plane_runtime_config(crossed)
                .unwrap();
            let seen = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&seen);
            primary
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind == Kind::MetadataCommandPendingSlotRemove {
                        let error = rpc::decode_storage_rpc_response_payload(&frame.payload)
                            .unwrap()
                            .unwrap_err();
                        assert_eq!(error.code, Code::StaleShardLocation);
                        assert!(error
                            .message
                            .contains("exact historical pending-command authorization"));
                        observed.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            {
                let _running = primary.run();
                let session = terminal
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                assert!(matches!(
                    terminal.retire_remote(&primary.client, &session, deadline()),
                    Err(CompleteRetirementSendError::Unconfirmed(_))
                ));
            }
            assert_eq!(seen.load(Ordering::SeqCst), 1);
            assert_eq!(terminal.slot_retirement_observation(), O::Unconfirmed);
            assert_eq!(mixed.observe(), before);
            primary
                .server
                .install_control_plane_runtime_config(primary.config.clone())
                .unwrap();
            let confirmed = primary.count(Kind::MetadataCommandPendingSlotRemove);
            let _running = primary.run();
            let session = terminal
                .open_primary_session(&primary.client, deadline())
                .unwrap();
            terminal
                .retire_remote(&primary.client, &session, deadline())
                .unwrap();
            assert_eq!(terminal.slot_retirement_observation(), O::Retired);
            assert_eq!(confirmed.load(Ordering::SeqCst), 1);
        });
    }
}

#[test]
fn staging_retirement_historical_receiver_captured_server_and_wire_fences_at_delete_and_commit() {
    for kind in ["wire", "server"] {
        for boundary in ["before-delete", "before-commit"] {
            let mixed = Mixed::new(1, &[(4, false)]);
            with_terminal(&mixed, "uncertain", deadline(), |_, _| {
                let primary = mixed.peer(4);
                let mut config = primary.config.clone();
                let now = crate::clock::current_time_millis();
                let expires = now + if kind == "server" { 3_000 } else { 1_000 };
                if kind == "server" {
                    config.route_map_validity = RouteMapValidity::until_ms(expires).unwrap();
                    primary
                        .server
                        .install_control_plane_runtime_config(config.clone())
                        .unwrap();
                }
                let payload = encode(
                    &mixed.fixture.context,
                    primary.id,
                    now + if kind == "wire" { 1_000 } else { 60_000 },
                );
                let query = CompleteRetirementQuery::decode(&payload, &topology()).unwrap();
                let request = query.request().unwrap();
                let before = mixed.observe();
                let mut reached = false;
                let result = primary
                    .server
                    .test_with_retirement_authority(&request, deadline(), |pg, authority| {
                        request.test_retire_with_hook(pg, authority, |at| {
                            if at == boundary {
                                reached = true;
                                assert!(crate::clock::current_time_millis() < expires);
                                if kind == "server" {
                                    // Validity-only renewal does not wait on the old
                                    // admission, and must not replace its captured fence.
                                    config.route_map_validity =
                                        RouteMapValidity::until_ms(now + 60_000).unwrap();
                                    primary
                                        .server
                                        .install_control_plane_runtime_config(config.clone())
                                        .unwrap();
                                }
                                while crate::clock::current_time_millis() <= expires {
                                    std::thread::sleep(Duration::from_millis(1));
                                }
                            }
                            Ok(())
                        })
                    })
                    .unwrap();
                assert!(reached);
                assert!(result.is_err(), "{kind}/{boundary}");
                for ((after, _), (before, _)) in mixed.observe().iter().zip(&before) {
                    assert_eq!(after, before);
                }
            });
        }
    }
}

#[test]
fn staging_retirement_historical_receiver_unix_tls_expiry_inside_transaction() {
    for tls in [false, true] {
        let mixed = Mixed::new(1, &[(4, tls)]);
        with_terminal(&mixed, "uncertain", deadline(), |_, terminal| {
            let primary = mixed.peer(4);
            let before = mixed.observe();
            let count = primary.count(Kind::MetadataCommandPendingSlotRemove);
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    primary
                        .server
                        .test_accept_one_retirement_fault(true, false)
                        .unwrap()
                });
                let wake = WakeAcceptOnDrop(Some(primary));
                let session = terminal
                    .open_primary_session(&primary.client, deadline())
                    .unwrap();
                let payload = encode(
                    &mixed.fixture.context,
                    primary.id,
                    crate::clock::current_time_millis() + 1_000,
                );
                // Keep transport alive long enough to observe receiver expiry,
                // rather than winning with a client-side socket timeout.
                assert!(session
                    .test_retirement_payload(payload, deadline())
                    .is_err());
                assert_eq!(count.load(Ordering::SeqCst), 1);
                wake.disarm();
                drop(session);
                worker.join().unwrap();
            });
            assert_eq!(mixed.observe(), before);
        });
    }
}

#[test]
fn staging_retirement_historical_receiver_restart_and_compaction_preserve_absence_confirmation() {
    for tls in [false, true] {
        let mixed = Mixed::new(1, &[(4, tls)]);
        let context = &mixed.fixture.context;
        let payload = encode(
            context,
            NodeId::new(4),
            crate::clock::current_time_millis() + 60_000,
        );
        with_terminal(&mixed, "inspected", deadline(), |_, terminal| {
            let primary = mixed.peer(4);
            let _running = primary.run();
            let session = terminal
                .open_primary_session(&primary.client, deadline())
                .unwrap();
            terminal
                .retire_remote(&primary.client, &session, deadline())
                .unwrap();
        });
        let id = mixed.fixture.context.id();
        let before = mixed.observe();
        let mixed = super::super::lifetime::reopen(mixed);
        for ((after, _), (before, _)) in mixed.observe().iter().zip(&before) {
            assert_eq!(after, before);
        }
        let primary = mixed.peer(4);
        let node = mixed.node(4);
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        pg.record_complete_metadata_command_checkpoint(4, &topology())
            .unwrap();
        pg.compact_complete_metadata_command_log(&topology())
            .unwrap();
        assert!(complete_maintenance::log_state(&pg)[0].is_empty());
        let before = (snapshot(&pg), pg.conn.total_changes());
        drop(pg);
        // Receiver-only replay evidence: do not manufacture a new all-peer
        // completion from the now-absent primary slot.
        let admission = mixed.fixture.handle.admit_current_route().unwrap();
        let route = admission
            .complete_historical_fanout_route(&mixed.fixture.runtime_map, id.pg_id())
            .unwrap();
        let _running = primary.run();
        let session = primary
            .client
            .open_complete_fanout_primary(&route, deadline())
            .unwrap();
        let reply = session
            .test_retirement_payload(payload, deadline())
            .unwrap();
        wire::decode_confirmation_for(
            &reply,
            primary.id,
            id,
            mixed
                .fixture
                .context
                .encode_command()
                .unwrap()
                .command_sha256(),
        )
        .unwrap();
        let pg = node.get_pg(id.pg_id().get()).unwrap();
        assert_eq!((snapshot(&pg), pg.conn.total_changes()), before);
    }
}
