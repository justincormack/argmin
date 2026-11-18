// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Receiver boundary only: complete apply/record under real route admission and
//! replica serialization. No live RPC selection or sender-custody handoff.

use super::*;
use crate::node_runtime::pg_store::CompleteApplicationRequest;
use crate::storage_node_server::{StorageNodeProcessConfig, StorageNodeServer};
use crate::storage_rpc::StorageRpcErrorCode;

// The production store boundary takes only the admitted authority, never an
// optional fence, independent command/topology, or caller mutation callback.
const _: fn(
    &PgStore,
    &crate::node_runtime::server::CompleteApplicationAuthority<'_>,
) -> Result<
    crate::metadata_command::MetadataCommandReplicaState,
    crate::node_runtime::pg_store::CompleteApplicationError,
> = |pg, authority| pg.apply_admitted_complete_metadata_command(authority);

#[test]
fn staging_complete_application_rejects_crossed_permits_and_serialization() {
    let dir = test_util::tempdir();
    let (server, config, context) = setup(&dir, 0, false);
    let foreign_dir = test_util::tempdir();
    let (foreign, _, _) = setup(&foreign_dir, 0, false);
    let encoded = context.encode_command().unwrap();
    let request = CompleteApplicationRequest::new(config.node_id, &encoded, deadline());
    let node = server.test_storage_node();
    let pg_id = context.id().pg_id().get();
    let before = snapshot(&node.get_pg(pg_id).unwrap());
    let writes = node.get_pg(pg_id).unwrap().conn.total_changes();
    server.test_complete_application_rejects_crossed_authority(&foreign, &request);
    assert_eq!(snapshot(&node.get_pg(pg_id).unwrap()), before);
    assert_eq!(node.get_pg(pg_id).unwrap().conn.total_changes(), writes);
}

#[test]
fn staging_complete_application_bounds_pg_serialization_and_store_waits() {
    let dir = test_util::tempdir();
    let (server, config, context) = setup(&dir, 0, true);
    let encoded = context.encode_command().unwrap();
    let request = CompleteApplicationRequest::new(
        config.node_id,
        &encoded,
        Instant::now() + Duration::from_millis(50),
    );
    server.test_complete_application_serialization_is_bounded(&request);
    let node = server.test_storage_node();
    let pg = node.get_pg(context.id().pg_id().get()).unwrap();
    let before = snapshot(&pg);
    let request = CompleteApplicationRequest::new(
        config.node_id,
        &encoded,
        Instant::now() + Duration::from_millis(50),
    );
    assert!(server
        .test_with_complete_application_authority(&request, |_, _| panic!("held store admitted"))
        .is_err());
    assert_eq!(snapshot(&pg), before);
}

fn setup(
    dir: &test_util::TempDir,
    case: usize,
    replica: bool,
) -> (
    StorageNodeServer,
    StorageNodeProcessConfig,
    ContextualCommand,
) {
    let mut config = config(case);
    if replica {
        config.node_id = NodeId::new(2);
    }
    let (server, _, config) = installed_config(dir, config, true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    if case == 1 {
        let pg = node.get_pg(pg_id.get()).unwrap();
        let digest = pg.metadata_command_replica_state().unwrap().state_digest;
        pg.initialize_metadata_transfer_empty_state(
            config.node_id.as_u32(),
            config.cluster_epoch,
            digest,
        )
        .unwrap();
    }
    let context = context(&node.get_pg(pg_id.get()).unwrap(), case);
    if !replica {
        install(&node.get_pg(pg_id.get()).unwrap(), &context);
        let evidence = pending(&context);
        let request = evidence
            .publication_request(config.node_id, &topology())
            .unwrap();
        server
            .test_with_complete_publication_authority(&request, deadline(), |pg, authority| {
                request.start(pg, authority)
            })
            .unwrap()
            .unwrap();
    }
    (server, config, context)
}

#[test]
fn staging_complete_application_admitted_primary_and_replica_contexts_exact_replay() {
    for case in 0..4 {
        for replica in [false, true] {
            let dir = test_util::tempdir();
            let (server, config, context) = setup(&dir, case, replica);
            let encoded = context.encode_command().unwrap();
            let request = CompleteApplicationRequest::new(config.node_id, &encoded, deadline());
            let node = server.test_storage_node();
            let pg_id = context.id().pg_id().get();
            let before = snapshot(&node.get_pg(pg_id).unwrap());
            let result = server
                .test_apply_complete_command(&request)
                .unwrap()
                .unwrap();
            assert_eq!(result.cluster_epoch, context.id().cluster_epoch());
            assert_eq!(result.applied_log_index, context.id().log_index().get());
            assert_ne!(snapshot(&node.get_pg(pg_id).unwrap()), before);
            server
                .test_with_complete_application_authority(&request, |pg, authority| {
                    let after = snapshot(pg);
                    let writes = pg.conn.total_changes();
                    pg.test_apply_admitted_complete_command_with_hook(authority, |stage| {
                        assert_ne!(
                            stage, "before-effect",
                            "replay must not reapply owner transfers"
                        );
                        assert_ne!(stage, "after-effect");
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(snapshot(pg), after);
                    assert_eq!(pg.conn.total_changes(), writes);
                    if !replica {
                        assert!(started(pg), "apply does not remove the pending owner");
                    }
                })
                .unwrap();
        }
    }
}

#[test]
fn staging_complete_application_admitted_failure_and_unwind_keep_atomic_custody() {
    for stage in [
        "before-effect",
        "after-effect",
        "before-commit",
        "after-commit",
    ] {
        for unwind in [false, true] {
            let dir = test_util::tempdir();
            let (server, config, context) = setup(&dir, 3, true);
            let encoded = context.encode_command().unwrap();
            let request = CompleteApplicationRequest::new(config.node_id, &encoded, deadline());
            server
                .test_with_complete_application_authority(&request, |pg, authority| {
                    let before = snapshot(pg);
                    let mut reached = false;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        pg.test_apply_admitted_complete_command_with_hook(authority, |at| {
                            if at == stage {
                                reached = true;
                                if unwind {
                                    panic!("injected admitted application unwind");
                                }
                                return Err(StoreError::RouteCapabilitySubjectMismatch {
                                    operation: "injected admitted application interruption",
                                });
                            }
                            Ok(())
                        })
                    }));
                    assert!(reached, "{stage}");
                    if unwind {
                        assert!(result.is_err());
                    } else {
                        assert!(result
                            .unwrap()
                            .unwrap_err()
                            .to_string()
                            .contains("injected admitted application interruption"));
                    }
                    assert!(pg.conn.is_autocommit());
                    if stage == "after-commit" {
                        assert_ne!(snapshot(pg), before);
                    } else {
                        assert_eq!(snapshot(pg), before);
                    }
                    let committed =
                        (stage == "after-commit").then(|| (snapshot(pg), pg.conn.total_changes()));
                    pg.apply_admitted_complete_metadata_command(authority)
                        .unwrap();
                    if let Some((state, writes)) = committed {
                        assert_eq!(snapshot(pg), state);
                        assert_eq!(pg.conn.total_changes(), writes);
                    }
                })
                .unwrap();
        }
    }
}

#[test]
fn staging_complete_application_admission_binds_exact_store_node_and_complete_command() {
    let dir = test_util::tempdir();
    let (server, config, context) = setup(&dir, 0, true);
    let foreign_dir = test_util::tempdir();
    let (foreign, _, _) = setup(&foreign_dir, 0, true);
    let foreign_node = foreign.test_storage_node();
    let foreign_pg = foreign_node.get_pg(context.id().pg_id().get()).unwrap();
    let foreign_before = snapshot(&foreign_pg);
    let encoded = context.encode_command().unwrap();
    let request = CompleteApplicationRequest::new(config.node_id, &encoded, deadline());
    let owner = fixture(StagingTarget::DirectPut);
    let foreign_topology = crate::PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap();
    let foreign_context = ContextualCommand::decode_command(
        encoded.bytes(),
        encoded.checksum_crc64(),
        &foreign_topology,
    )
    .unwrap();
    let foreign_encoded = foreign_context.encode_command().unwrap();
    let crossed_topology =
        CompleteApplicationRequest::new(config.node_id, &foreign_encoded, deadline());
    assert_eq!(crossed_topology.id(), request.id());
    assert_eq!(crossed_topology.command_sha256(), request.command_sha256());
    assert_ne!(crossed_topology.topology(), request.topology());
    let crossed = crate::metadata_command::MetadataCommandEnvelope::new(
        context.id(),
        crate::metadata_command::MetadataCommandPayload::ReserveObjectVersion(
            crate::metadata_command::ReserveObjectVersionCommand::new(
                owner.subject.bucket,
                owner.subject.key,
                crate::VersionId::from_u64(99),
            ),
        ),
    );
    let crossed = ContextualCommand::decode_field(crossed, &[1, 0, 0], &topology()).unwrap();
    let crossed_encoded = crossed.encode_command().unwrap();
    let crossed = CompleteApplicationRequest::new(config.node_id, &crossed_encoded, deadline());
    assert_eq!(crossed.id(), request.id());
    assert_ne!(crossed.command_sha256(), request.command_sha256());
    server
        .test_with_complete_application_authority(&request, |pg, authority| {
            let before = snapshot(pg);
            assert!(matches!(
                authority.require_effect(pg, &crossed),
                Err(StoreError::RouteCapabilitySubjectMismatch { .. })
            ));
            assert!(matches!(
                authority.require_effect(pg, &crossed_topology),
                Err(StoreError::RouteCapabilitySubjectMismatch { .. })
            ));
            let wrong_node = CompleteApplicationRequest::new(NodeId::new(4), &encoded, deadline());
            assert!(matches!(
                authority.require_effect(pg, &wrong_node),
                Err(StoreError::RouteCapabilitySubjectMismatch { .. })
            ));
            assert!(matches!(
                foreign_pg
                    .apply_admitted_complete_metadata_command(authority)
                    .unwrap_err()
                    .into_store_error()
                    .unwrap(),
                StoreError::RouteCapabilitySubjectMismatch { .. }
            ));
            assert_eq!(snapshot(pg), before);
            assert_eq!(snapshot(&foreign_pg), foreign_before);
            pg.apply_admitted_complete_metadata_command(authority)
                .unwrap();
        })
        .unwrap();
}

#[test]
fn staging_complete_application_rejects_nonactive_foreign_and_historical_routes() {
    for mode in [
        "peering",
        "nonmember",
        "wrong-node",
        "old-epoch",
        "future-epoch",
    ] {
        let dir = test_util::tempdir();
        let mut config = config(0);
        config.node_id = NodeId::new(2);
        for route in &mut config.pg_routes {
            match mode {
                "peering" => route.state = PgState::Peering,
                "nonmember" => route.acting_set.retain(|id| *id != config.node_id),
                "old-epoch" => route.cluster_epoch = ClusterEpoch::new(6).unwrap(),
                "future-epoch" => route.cluster_epoch = ClusterEpoch::new(4).unwrap(),
                _ => {}
            }
        }
        if mode == "old-epoch" {
            config.cluster_epoch = ClusterEpoch::new(6).unwrap();
        }
        if mode == "future-epoch" {
            config.cluster_epoch = ClusterEpoch::new(4).unwrap();
        }
        let (server, _, config) = installed_config(&dir, config, true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let context = context(&node.get_pg(pg_id.get()).unwrap(), 0);
        let encoded = context.encode_command().unwrap();
        let target = if mode == "wrong-node" {
            NodeId::new(4)
        } else {
            config.node_id
        };
        let request = CompleteApplicationRequest::new(target, &encoded, deadline());
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        let error = server
            .test_with_complete_application_authority(&request, |_, _| panic!("{mode} admitted"))
            .unwrap_err();
        let expected = match mode {
            "peering" => StorageRpcErrorCode::InactivePgRoute,
            "nonmember" => StorageRpcErrorCode::NonActingSetAccess,
            "wrong-node" => StorageRpcErrorCode::UnknownNode,
            _ => StorageRpcErrorCode::StaleShardLocation,
        };
        assert_eq!(error.code, expected, "{mode}: {error:?}");
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
    }
}

#[test]
fn staging_complete_application_rechecks_route_at_effect_and_commit_including_replay() {
    for stage in ["before-effect", "before-commit"] {
        for replay in [false, true] {
            if replay && stage == "before-effect" {
                continue;
            }
            let clock = crate::clock::test_time_override_guard(1_000);
            let dir = test_util::tempdir();
            let mut config = config(3);
            config.route_map_validity = RouteMapValidity::until_ms(5_000).unwrap();
            config.node_id = NodeId::new(2);
            let (server, _, config) = installed_config(&dir, config, true);
            let node = server.test_storage_node();
            let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
            let context = context(&node.get_pg(pg_id.get()).unwrap(), 3);
            if replay {
                apply(&node.get_pg(pg_id.get()).unwrap(), &context);
            }
            let encoded = context.encode_command().unwrap();
            let request = CompleteApplicationRequest::new(config.node_id, &encoded, deadline());
            server
                .test_with_complete_application_authority(&request, |pg, authority| {
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

#[test]
fn staging_complete_application_rechecks_work_deadline_and_cannot_renew_authority() {
    for stage in ["before-effect", "before-commit"] {
        let dir = test_util::tempdir();
        let (server, config, context) = setup(&dir, 1, true);
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
                assert!(matches!(
                    error,
                    StoreError::OperationDeadlineExceeded { .. }
                ));
                let later = CompleteApplicationRequest::new(config.node_id, &encoded, deadline());
                assert!(matches!(
                    authority.require_effect(pg, &later),
                    Err(StoreError::OperationDeadlineExceeded { .. })
                ));
                assert_eq!(snapshot(pg), before);
            })
            .unwrap();
    }
}

#[test]
fn staging_complete_application_failed_rollback_retires_connection() {
    let dir = test_util::tempdir();
    let (server, config, context) = setup(&dir, 1, true);
    let encoded = context.encode_command().unwrap();
    let request = CompleteApplicationRequest::new(config.node_id, &encoded, deadline());
    server
        .test_with_complete_application_authority(&request, |pg, authority| {
            let before = snapshot(pg);
            let error = pg
                .test_apply_admitted_complete_command_with_hook(authority, |stage| {
                    if stage == "after-effect" {
                        pg.test_fail_metadata_command_rollbacks(1);
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "injected apply rollback failure",
                        });
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(error.to_string().contains("rollback failed"), "{error}");
            assert!(pg.require_usable_connection().is_err());
            pg.conn.execute_batch("ROLLBACK").unwrap();
            assert_eq!(snapshot(pg), before);
            assert!(pg
                .apply_admitted_complete_metadata_command(authority)
                .is_err());
        })
        .unwrap();
}
