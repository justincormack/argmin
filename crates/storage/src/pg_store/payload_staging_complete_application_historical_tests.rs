// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed historical receiver, with retained complete custody on the primary.
//! No historical fanout completion, reissue, or terminal retirement is implied.

use super::*;
use crate::control_plane::{PendingMetadataCommandObservation, PendingMetadataCommandRecovery};
use crate::node_runtime::clients::UnixStorageNodeClient;
use crate::node_runtime::pg_store::{
    ApplicationObservation, CompleteApplicationRequest, PublicationObservation,
};
use crate::storage_node_server::{StorageNodeProcessConfig, StorageNodeServer};
use crate::storage_rpc::{self as rpc, StorageRpcErrorCode as Code, StorageRpcMessageKind as Kind};
use std::num::NonZeroU64;

fn setup(
    dir: &test_util::TempDir,
    case: usize,
    replica: bool,
) -> (StorageNodeProcessConfig, ContextualCommand) {
    let (server, client, config, context) = application_rpc::setup(false, case, replica, dir);
    drop(client);
    drop(server);
    let mut config = historical::historical_config(config, &context);
    let pending = config.pending_metadata_command_recoveries[0].1.pending();
    config.pending_metadata_command_recoveries[0].1 =
        PendingMetadataCommandRecovery::new(NodeId::new(4), pending);
    (config, context)
}

fn exchange(
    server: &StorageNodeServer,
    client: &UnixStorageNodeClient,
    context: &ContextualCommand,
    lose: bool,
) -> Result<Result<Vec<u8>, rpc::StorageRpcErrorResponse>, StoreError> {
    let payload = application_rpc::encode_at(
        context,
        client.node_id(),
        crate::clock::current_time_millis() + 30_000,
    );
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            server
                .test_accept_one_complete_application_fault(false, lose)
                .unwrap()
        });
        let result = client.rpc_request_result(Kind::MetadataCommandApplyAndRecord, payload);
        worker.join().unwrap();
        result
    })
}

#[test]
fn staging_complete_application_historical_equal_crc_substitution_rejected_unix_tls() {
    for tls in [false, true] {
        let dir = test_util::tempdir();
        let mut config = config(2);
        config.node_id = NodeId::new(2);
        let (server, client, config) = installed_config(&dir, config, true);
        let node = server.test_storage_node();
        let (reported, substituted) = {
            let pg = node
                .get_pg(fixture(multipart_target()).subject.object_pg.get())
                .unwrap();
            let owner = part::initialize(&pg, 2);
            let source = part::command(&pg, &owner);
            let other = abandonment::source_carrier::crossed_proof_with_equal_crc(&source, "etag");
            let reported =
                ContextualCommand::decode_field(source, &[1, 0, 0], &topology()).unwrap();
            let substituted =
                ContextualCommand::decode_field(other, &[1, 0, 0], &topology()).unwrap();
            assert_eq!(reported.id(), substituted.id());
            let a = reported.encode_command().unwrap();
            let b = substituted.encode_command().unwrap();
            assert_eq!(a.checksum_crc64(), b.checksum_crc64());
            assert_ne!(a.command_sha256(), b.command_sha256());
            complete_application::anchor(&pg, substituted.id());
            // Local exact-command checking must succeed for B; only the report
            // from the historical primary authorizes A instead.
            install(&pg, &substituted);
            (reported, substituted)
        };
        drop(node);
        drop(client);
        drop(server);
        let mut historical = historical::historical_config(config, &reported);
        let pending = historical.pending_metadata_command_recoveries[0]
            .1
            .pending();
        historical.pending_metadata_command_recoveries[0].1 =
            PendingMetadataCommandRecovery::new(NodeId::new(4), pending);
        for authorized in [false, true] {
            if authorized {
                let encoded = substituted.encode_command().unwrap();
                historical.pending_metadata_command_recoveries[0].1 =
                    PendingMetadataCommandRecovery::new(
                        NodeId::new(4),
                        PendingMetadataCommandObservation::new(
                            substituted.id().cluster_epoch(),
                            NonZeroU64::new(substituted.id().log_index().get()).unwrap(),
                            encoded.checksum_crc64(),
                            encoded.command_sha256(),
                        ),
                    );
            }
            let (server, client) =
                historical::reopen(&historical, substituted.id().cluster_epoch(), tls);
            let node = server.test_storage_node();
            let pg_id = substituted.id().pg_id().get();
            let before = snapshot(&node.get_pg(pg_id).unwrap());
            let writes = node.get_pg(pg_id).unwrap().conn.total_changes();
            let result = exchange(&server, &client, &substituted, false).unwrap();
            if authorized {
                result.unwrap();
                assert_ne!(
                    snapshot(&node.get_pg(pg_id).unwrap()),
                    before,
                    "B is otherwise valid and applies with its own authenticated report"
                );
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.code, Code::StaleShardLocation);
                assert!(error
                    .message
                    .contains("exact historical pending-command authorization"));
                assert_eq!(snapshot(&node.get_pg(pg_id).unwrap()), before);
                assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), writes);
            }
            drop(node);
            drop(client);
            drop(server);
        }
    }
}

#[test]
fn staging_complete_application_historical_primary_unix_tls_lost_reply_custody_and_restart() {
    for tls in [false, true] {
        for case in 0..4 {
            let dir = test_util::tempdir();
            let (config, context) = setup(&dir, case, false);
            let id = context.id();
            let mut holder = pending(&context).into_publication_sender(config.node_id, deadline());
            let mut expected = None;
            for attempt in 0..3 {
                let (server, client) = historical::reopen(&config, id.cluster_epoch(), tls);
                let node = server.test_storage_node();
                let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
                let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
                if let Some(expected) = &expected {
                    assert_eq!(&before, expected);
                }
                std::thread::scope(|scope| {
                    let worker = scope.spawn(|| {
                        server
                            .test_accept_one_complete_application_fault(false, attempt == 0)
                            .unwrap()
                    });
                    let session = client
                        .open_metadata_command_critical_section_until(id.pg_id(), deadline())
                        .unwrap();
                    holder.send_remote(&session, deadline()).unwrap();
                    let result = holder.apply_remote_in_session(&session, deadline());
                    if attempt == 0 {
                        assert!(result.is_err(), "lose the committed application reply");
                        assert_eq!(
                            holder.application_observation(config.node_id),
                            ApplicationObservation::Unconfirmed
                        );
                    } else {
                        assert_eq!(result.unwrap().applied_log_index, id.log_index().get());
                        assert_eq!(
                            holder.application_observation(config.node_id),
                            ApplicationObservation::Applied
                        );
                    }
                    assert_eq!(
                        holder.observation(),
                        PublicationObservation::MarkerConfirmed
                    );
                    assert_eq!(
                        holder.application_observation(NodeId::new(2)),
                        ApplicationObservation::NoAttemptObserved
                    );
                    drop(session);
                    worker.join().unwrap();
                });
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                assert!(started(&pg), "application does not retire pending custody");
                if attempt == 0 {
                    assert_ne!(snapshot(&pg), before);
                    expected = Some(snapshot(&pg));
                } else {
                    assert_eq!(snapshot(&pg), before);
                    assert_eq!(
                        pg.conn.total_changes(),
                        writes,
                        "exact replay must not write"
                    );
                }
                drop(pg);
                drop(node);
                drop(client);
                drop(server);
            }
        }
    }
}

#[test]
fn staging_complete_application_historical_replica_unix_tls_contexts_replay_and_restart() {
    for tls in [false, true] {
        for case in 0..4 {
            let dir = test_util::tempdir();
            let (config, context) = setup(&dir, case, true);
            let id = context.id();
            let mut expected = None;
            for attempt in 0..3 {
                let (server, client) = historical::reopen(&config, id.cluster_epoch(), tls);
                let node = server.test_storage_node();
                let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
                let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
                if let Some(expected) = &expected {
                    assert_eq!(&before, expected);
                }
                let response = exchange(&server, &client, &context, attempt == 0);
                if attempt == 0 {
                    assert!(response.is_err());
                    let applied = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
                    assert_ne!(applied, before);
                    expected = Some(applied);
                } else {
                    let state = rpc::complete_application::decode_confirmation_for(
                        &response.unwrap().unwrap(),
                        config.node_id,
                        id,
                        context.encode_command().unwrap().command_sha256(),
                    )
                    .unwrap();
                    assert_eq!(state.applied_log_index, id.log_index().get());
                    assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
                    assert_eq!(
                        node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
                        writes
                    );
                }
                drop(node);
                drop(client);
                drop(server);
            }
        }
    }
}

#[test]
fn staging_complete_application_historical_rejects_inexact_report_and_placement_without_mutation() {
    for tls in [false, true] {
        for mode in [
            "checksum",
            "sha256",
            "index",
            "epoch",
            "pg",
            "reporter",
            "missing",
            "route",
            "inactive",
            "nonmember",
            "peering",
            "unbounded",
        ] {
            let dir = test_util::tempdir();
            let (mut config, context) = setup(&dir, 1, true);
            let id = context.id();
            let (pg_id, recovery) = config.pending_metadata_command_recoveries[0];
            config.pending_metadata_command_recoveries[0] = (
                if mode == "pg" {
                    PgId::new((pg_id.get() + 1) % 8)
                } else {
                    pg_id
                },
                PendingMetadataCommandRecovery::new(
                    if mode == "reporter" {
                        NodeId::new(2)
                    } else {
                        recovery.reporting_node_id()
                    },
                    PendingMetadataCommandObservation::new(
                        if mode == "epoch" {
                            config.cluster_epoch
                        } else {
                            id.cluster_epoch()
                        },
                        NonZeroU64::new(id.log_index().get() + u64::from(mode == "index")).unwrap(),
                        recovery.pending().command_checksum() ^ u64::from(mode == "checksum"),
                        {
                            let mut sha = recovery.pending().command_sha256();
                            sha[0] ^= u8::from(mode == "sha256");
                            sha
                        },
                    ),
                ),
            );
            match mode {
                "missing" => config.pending_metadata_command_recoveries.clear(),
                "route" => config.historical_pg_routes.clear(),
                "inactive" => config
                    .historical_pg_routes
                    .iter_mut()
                    .for_each(|r| r.state = PgState::Peering),
                "nonmember" => config
                    .historical_pg_routes
                    .iter_mut()
                    .for_each(|r| r.acting_set.retain(|n| *n != config.node_id)),
                "peering" => config
                    .pg_routes
                    .iter_mut()
                    .for_each(|r| r.state = PgState::Active),
                "unbounded" => config.route_map_validity = RouteMapValidity::Forever,
                _ => (),
            }
            if matches!(
                mode,
                "epoch" | "reporter" | "route" | "inactive" | "peering" | "unbounded"
            ) {
                // These cannot reach dispatch: startup already rejects an
                // internally inconsistent recovery report. Test that exact
                // boundary, rather than bypassing config validation in a fake
                // server. The remaining cases reach authenticated dispatch.
                let (prepared, _) = super::super::complete_log_rpc::authenticated_server(
                    crate::storage_node_server::PreparedStorageNodeServer::new(config),
                );
                let error = match prepared.bind_complete_for_test() {
                    Ok(_) => panic!("{mode}: invalid recovery configuration started"),
                    Err(error) => error,
                };
                assert!(matches!(error, crate::storage_node_server::StorageNodeServerError::InvalidPendingMetadataCommandRecovery { .. }), "{mode}: {error:?}");
                continue;
            }
            let (server, client) = historical::reopen(&config, id.cluster_epoch(), tls);
            let node = server.test_storage_node();
            let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
            let writes = node.get_pg(pg_id.get()).unwrap().conn.total_changes();
            let error = exchange(&server, &client, &context, false)
                .unwrap()
                .unwrap_err();
            assert_eq!(
                error.code,
                match mode {
                    "inactive" | "peering" => Code::InactivePgRoute,
                    "nonmember" => Code::NonActingSetAccess,
                    _ => Code::StaleShardLocation,
                },
                "{mode}: {error:?}"
            );
            if matches!(
                mode,
                "checksum" | "index" | "epoch" | "pg" | "reporter" | "missing"
            ) {
                assert!(
                    error
                        .message
                        .contains("exact historical pending-command authorization"),
                    "{mode}: {error:?}"
                );
            }
            assert_eq!(
                snapshot(&node.get_pg(pg_id.get()).unwrap()),
                before,
                "{mode}"
            );
            assert_eq!(
                node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
                writes,
                "{mode}"
            );
        }
    }
}

#[test]
fn staging_complete_application_historical_primary_requires_held_recovery_section() {
    for tls in [false, true] {
        let dir = test_util::tempdir();
        let (config, context) = setup(&dir, 0, false);
        let (server, client) = historical::reopen(&config, context.id().cluster_epoch(), tls);
        let node = server.test_storage_node();
        let pg_id = context.id().pg_id().get();
        let before = snapshot(&node.get_pg(pg_id).unwrap());
        let writes = node.get_pg(pg_id).unwrap().conn.total_changes();
        let error = exchange(&server, &client, &context, false)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, Code::StaleShardLocation);
        assert!(error
            .message
            .contains("exact held historical-primary section"));
        let encoded = context.encode_command().unwrap();
        let request = CompleteApplicationRequest::new(config.node_id, &encoded, deadline());
        server.test_complete_application_historical_sections(&request);
        assert_eq!(snapshot(&node.get_pg(pg_id).unwrap()), before);
        assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_application_historical_fence_rechecked_at_effect_and_commit() {
    for replica in [false, true] {
        for stage in ["before-effect", "before-commit"] {
            for replay in [false, true] {
                if replay && stage == "before-effect" {
                    continue;
                }
                let clock = crate::clock::test_time_override_guard(1_000);
                let dir = test_util::tempdir();
                let (mut config, context) = setup(&dir, 3, replica);
                config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
                let (server, _) = historical::reopen(&config, context.id().cluster_epoch(), false);
                let encoded = context.encode_command().unwrap();
                let request = CompleteApplicationRequest::new(config.node_id, &encoded, deadline());
                server
                    .test_with_complete_application_authority(&request, |pg, authority| {
                        if replay {
                            pg.apply_admitted_complete_metadata_command(authority)
                                .unwrap();
                        }
                        let before = snapshot(pg);
                        let mut reached = false;
                        let error = pg
                            .test_apply_admitted_complete_command_with_hook(authority, |at| {
                                if at == stage {
                                    reached = true;
                                    clock.set(6_000);
                                }
                                Ok(())
                            })
                            .unwrap_err()
                            .into_store_error()
                            .unwrap();
                        assert!(reached);
                        assert!(
                            matches!(error, StoreError::RouteMapExpired { .. }),
                            "{error:?}"
                        );
                        assert_eq!(snapshot(pg), before);
                        assert!(pg.conn.is_autocommit());
                    })
                    .unwrap();
            }
        }
    }
}

#[test]
fn staging_complete_application_historical_wire_cutoff_expires_after_decode() {
    for tls in [false, true] {
        let _clock = crate::clock::test_time_override_guard(9_000);
        let dir = test_util::tempdir();
        let (config, context) = setup(&dir, 1, true);
        let (server, client) = historical::reopen(&config, context.id().cluster_epoch(), tls);
        let node = server.test_storage_node();
        let pg_id = context.id().pg_id().get();
        let before = snapshot(&node.get_pg(pg_id).unwrap());
        let writes = node.get_pg(pg_id).unwrap().conn.total_changes();
        let bytes = application_rpc::encode_at(&context, config.node_id, 10_500);
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let _clock = crate::clock::test_time_override_guard(10_000);
                server
                    .test_accept_one_complete_application_fault(true, false)
                    .unwrap();
            });
            let error = client
                .test_complete_application_request_until(bytes, deadline())
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, Code::TransportTimeout, "{error:?}");
            worker.join().unwrap();
        });
        assert_eq!(snapshot(&node.get_pg(pg_id).unwrap()), before);
        assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_application_historical_work_cutoff_rechecked_at_effect_and_commit() {
    for replica in [false, true] {
        for stage in ["before-effect", "before-commit"] {
            let dir = test_util::tempdir();
            let (config, context) = setup(&dir, 1, replica);
            let (server, _) = historical::reopen(&config, context.id().cluster_epoch(), false);
            let encoded = context.encode_command().unwrap();
            let cutoff = Instant::now() + Duration::from_secs(1);
            let request = CompleteApplicationRequest::new(config.node_id, &encoded, cutoff);
            server
                .test_with_complete_application_authority(&request, |pg, authority| {
                    let before = snapshot(pg);
                    let mut reached = false;
                    let error = pg
                        .test_apply_admitted_complete_command_with_hook(authority, |at| {
                            if at == stage {
                                reached = true;
                                assert!(Instant::now() < cutoff);
                                while let Some(remaining) =
                                    cutoff.checked_duration_since(Instant::now())
                                {
                                    std::thread::sleep(remaining);
                                }
                            }
                            Ok(())
                        })
                        .unwrap_err()
                        .into_store_error()
                        .unwrap();
                    assert!(reached);
                    assert!(
                        matches!(error, StoreError::OperationDeadlineExceeded { .. }),
                        "{error:?}"
                    );
                    let later =
                        CompleteApplicationRequest::new(config.node_id, &encoded, deadline());
                    assert!(matches!(
                        authority.require_effect(pg, &later),
                        Err(StoreError::OperationDeadlineExceeded { .. })
                    ));
                    assert_eq!(snapshot(pg), before);
                    assert!(pg.conn.is_autocommit());
                })
                .unwrap();
        }
    }
}
