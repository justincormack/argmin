// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate embedded receiver admission. Actual shared PG serialization and
//! coherent metadata replicas; no live sender selection or physical payload IO.

use super::*;
use crate::node_runtime::pg_store::{CompleteApplicationQuery, CompleteApplicationRequest};
use crate::storage_rpc::{self as wire, complete_application};
use crate::{LocalClusterMap, StorageClusterRouteHandle};
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

const _: fn(
    &PgStore,
    &crate::node_runtime::engine::EmbeddedApplicationAuthority<'_>,
) -> Result<
    crate::metadata_command::MetadataCommandReplicaState,
    crate::node_runtime::pg_store::CompleteApplicationError,
> = |pg, authority| pg.apply_embedded_complete_metadata_command(authority);

pub(super) fn setup(
    dir: &test_util::TempDir,
    case: usize,
    validity: RouteMapValidity,
) -> (
    Arc<LocalClusterMap>,
    StorageClusterRouteHandle,
    ContextualCommand,
) {
    let seed_dir = test_util::tempdir();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let seed = PgStore::open(seed_dir.path(), pg_id.get()).unwrap();
    if case == 1 {
        let digest = seed.metadata_command_replica_state().unwrap().state_digest;
        seed.initialize_metadata_transfer_empty_state(4, config(case).cluster_epoch, digest)
            .unwrap();
    }
    let context = context(&seed, case);
    from_seed(dir, &seed, context, case, validity)
}

pub(super) fn from_seed(
    dir: &test_util::TempDir,
    seed: &PgStore,
    context: ContextualCommand,
    case: usize,
    validity: RouteMapValidity,
) -> (
    Arc<LocalClusterMap>,
    StorageClusterRouteHandle,
    ContextualCommand,
) {
    let (map, handle, context) = from_seed_without_pending(dir, seed, context, case, validity);
    install(
        &map.node(NodeId::new(4))
            .unwrap()
            .test_node()
            .get_pg(context.id().pg_id().get())
            .unwrap(),
        &context,
    );
    (map, handle, context)
}

pub(super) fn from_seed_without_pending(
    dir: &test_util::TempDir,
    seed: &PgStore,
    context: ContextualCommand,
    case: usize,
    validity: RouteMapValidity,
) -> (
    Arc<LocalClusterMap>,
    StorageClusterRouteHandle,
    ContextualCommand,
) {
    let pg_id = context.id().pg_id();
    // Preserve identical random allocation identities, prefix and receipts.
    // This is metadata-only fixture setup, not a new transfer protocol. All
    // images are created before any runtime opens them.
    for id in 0..5 {
        let pg_dir = dir
            .path()
            .join(id.to_string())
            .join(format!("pg-{:04}", pg_id.get()));
        std::fs::create_dir_all(&pg_dir).unwrap();
        let db = pg_dir.join("metadata.db");
        seed.conn
            .execute("VACUUM INTO ?1", [db.to_str().unwrap()])
            .unwrap();
        std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let (map, handle) = embedded::embedded(dir, case, PgState::Active, validity);
    (map, handle, context)
}

fn query(context: &ContextualCommand, node: NodeId) -> CompleteApplicationQuery {
    let encoded = context.encode_command().unwrap();
    let bytes = complete_application::encode_request(complete_application::BorrowedRequest {
        command: wire::complete_applied::BorrowedRequest {
            node_id: node,
            epoch: context.id().cluster_epoch(),
            pg_id: context.id().pg_id(),
            checksum: encoded.checksum_crc64(),
            command: encoded.bytes(),
        },
        deadline: wire::StorageRpcOperationDeadline::from_instant(deadline()),
    })
    .unwrap();
    CompleteApplicationQuery::decode(&bytes, &topology()).unwrap()
}

#[test]
fn staging_complete_application_embedded_members_share_serialization_and_exact_replay() {
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (map, handle, context) = setup(&dir, case, RouteMapValidity::Forever);
        let pg_id = context.id().pg_id();
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(NodeId::new(4), pg_id, context.id().cluster_epoch())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let pending = pending(&context);
        section
            .start_complete_publication(
                &pending
                    .publication_request(NodeId::new(4), &topology())
                    .unwrap(),
                deadline(),
            )
            .unwrap();
        let shared = map.runtime_state().metadata_command_pg_lock(pg_id);
        assert!(shared.try_lock().is_none());
        let mut expected = None;
        // Witness, primary, then trailing members. Receiver authority does not
        // claim to enforce this sender ordering; it must work under the one
        // genuine primary guard, without recursively acquiring its mutex.
        for id in [0, 4, 1, 2, 3] {
            let node = map.node(NodeId::new(id)).unwrap().test_node();
            let target = section.application_target(NodeId::new(id)).unwrap();
            let query = query(&context, NodeId::new(id));
            query
                .with_request(deadline(), |request| {
                    let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
                    let result = target.apply_complete_command(request).unwrap();
                    assert_eq!(result.cluster_epoch, context.id().cluster_epoch());
                    assert_eq!(result.applied_log_index, context.id().log_index().get());
                    assert_ne!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
                    let pg = node.get_pg(pg_id.get()).unwrap();
                    let materialized = (
                        introduction::state(&pg),
                        complete_maintenance::log_state(&pg),
                        result,
                    );
                    if let Some(expected) = &expected {
                        assert_eq!(&materialized, expected);
                    } else {
                        expected = Some(materialized);
                    }
                    let after = snapshot(&pg);
                    let writes = pg.conn.total_changes();
                    drop(pg);
                    target
                        .test_with_application_authority(request, |pg, authority| {
                            pg.test_apply_embedded_complete_command_with_hook(authority, |stage| {
                                assert_ne!(stage, "before-effect", "replay must not reapply");
                                Ok(())
                            })
                            .unwrap();
                            assert_eq!(snapshot(pg), after);
                            assert_eq!(pg.conn.total_changes(), writes);
                            Ok(())
                        })
                        .unwrap();
                })
                .unwrap();
            assert!(shared.try_lock().is_none());
        }
        assert!(section.application_target(NodeId::new(99)).is_err());
        drop(section);
        assert!(shared.try_lock().is_some());
    }
}

#[test]
fn staging_complete_application_embedded_rejects_existing_nonmember_and_nonactive_routes() {
    let dir = test_util::tempdir();
    let (map, _, context) = setup(&dir, 0, RouteMapValidity::Forever);
    let pg_id = context.id().pg_id();
    for state in [PgState::Active, PgState::Peering] {
        let routes = config(0).pg_routes.into_iter().map(|route| {
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch,
                PgId::new(route.pg_id),
                route.primary_node_id,
                vec![NodeId::new(4), NodeId::new(2), NodeId::new(3)],
                state,
            )
        });
        let selected = Arc::new(
            map.test_clone_with_pg_routes(context.id().cluster_epoch(), routes, [])
                .unwrap(),
        );
        assert!(
            selected.node(NodeId::new(0)).is_some(),
            "negative must isolate membership, not node absence"
        );
        let handle = StorageClusterRouteHandle::from_authorized_cluster(
            crate::StorageCluster::test_from_local_map_with_epoch(
                Arc::clone(&selected),
                context.id().cluster_epoch(),
            )
            .unwrap(),
        );
        let admission = handle.admit_current_route().unwrap();
        let node = selected.node(NodeId::new(0)).unwrap().test_node();
        let before = snapshot(&node.get_pg(pg_id.get()).unwrap());
        let route = admission.embedded_publication_route(
            NodeId::new(4),
            pg_id,
            context.id().cluster_epoch(),
        );
        if state == PgState::Peering {
            assert!(matches!(route, Err(StoreError::PgNotActive { .. })));
        } else {
            let route = route.unwrap();
            let section = route.lock_until(deadline()).unwrap();
            assert!(section.application_target(NodeId::new(2)).is_ok());
            assert!(matches!(
                section.application_target(NodeId::new(0)),
                Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "embedded application target is not in the admitted acting set"
                })
            ));
        }
        assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
    }
}

#[test]
fn staging_complete_application_embedded_binds_exact_member_store_command_and_topology() {
    let dir = test_util::tempdir();
    let (_, handle, context) = setup(&dir, 0, RouteMapValidity::Forever);
    let foreign_dir = test_util::tempdir();
    let (foreign, _, _) = setup(&foreign_dir, 0, RouteMapValidity::Forever);
    let pg_id = context.id().pg_id();
    let foreign_pg = foreign
        .node(NodeId::new(2))
        .unwrap()
        .test_node()
        .get_pg(pg_id.get())
        .unwrap();
    let foreign_before = snapshot(&foreign_pg);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(NodeId::new(4), pg_id, context.id().cluster_epoch())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let target = section.application_target(NodeId::new(2)).unwrap();
    let encoded = context.encode_command().unwrap();
    let request = CompleteApplicationRequest::new(NodeId::new(2), &encoded, deadline());
    let foreign_topology = crate::PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap();
    let crossed_topology = ContextualCommand::decode_command(
        encoded.bytes(),
        encoded.checksum_crc64(),
        &foreign_topology,
    )
    .unwrap();
    let crossed_topology = crossed_topology.encode_command().unwrap();
    assert_eq!(crossed_topology.command_sha256(), encoded.command_sha256());
    target
        .test_with_application_authority(&request, |pg, authority| {
            let before = snapshot(pg);
            let writes = pg.conn.total_changes();
            for crossed in [
                CompleteApplicationRequest::new(NodeId::new(4), &encoded, deadline()),
                CompleteApplicationRequest::new(NodeId::new(2), &crossed_topology, deadline()),
            ] {
                assert!(matches!(
                    authority.require_effect(pg, &crossed),
                    Err(StoreError::RouteCapabilitySubjectMismatch { .. })
                ));
            }
            for (epoch, pg_id, index) in [
                (5, pg_id, 9),
                (6, pg_id, context.id().log_index().get()),
                (
                    5,
                    PgId::new((pg_id.get() + 1) % 8),
                    context.id().log_index().get(),
                ),
            ] {
                let crossed = super::super::complete_log_range::ordinary(MetadataCommandId::new(
                    ClusterEpoch::new(epoch).unwrap(),
                    pg_id,
                    MetadataCommandLogIndex::new(index).unwrap(),
                ));
                let crossed =
                    ContextualCommand::decode_field(crossed, &[1, 0, 0], &topology()).unwrap();
                let crossed = crossed.encode_command().unwrap();
                let request = CompleteApplicationRequest::new(NodeId::new(2), &crossed, deadline());
                assert!(matches!(
                    authority.require_effect(pg, &request),
                    Err(StoreError::RouteCapabilitySubjectMismatch { .. })
                ));
            }
            // Isolate SHA binding: same node, ID, topology, different payload.
            let owner = fixture(StagingTarget::DirectPut);
            let crossed = crate::metadata_command::MetadataCommandEnvelope::new(
                context.id(),
                crate::metadata_command::MetadataCommandPayload::ReserveObjectVersion(
                    crate::metadata_command::ReserveObjectVersionCommand::new(
                        owner.subject.bucket,
                        owner.subject.key,
                        crate::VersionId::from_u64(999),
                    ),
                ),
            );
            let crossed =
                ContextualCommand::decode_field(crossed, &[1, 0, 0], &topology()).unwrap();
            let crossed = crossed.encode_command().unwrap();
            assert_ne!(crossed.command_sha256(), request.command_sha256());
            assert!(matches!(
                authority.require_effect(
                    pg,
                    &CompleteApplicationRequest::new(NodeId::new(2), &crossed, deadline())
                ),
                Err(StoreError::RouteCapabilitySubjectMismatch { .. })
            ));
            let error = foreign_pg
                .apply_embedded_complete_metadata_command(authority)
                .unwrap_err()
                .into_store_error()
                .unwrap();
            assert!(matches!(
                error,
                StoreError::RouteCapabilitySubjectMismatch { .. }
            ));
            assert_eq!(snapshot(&foreign_pg), foreign_before);
            assert_eq!(snapshot(pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
            pg.apply_embedded_complete_metadata_command(authority)?;
            Ok(())
        })
        .unwrap();
}

#[test]
fn staging_complete_application_embedded_fence_cannot_be_renewed_in_transaction_or_replay() {
    for (stage, replay) in [
        ("before-effect", false),
        ("before-commit", false),
        ("before-commit", true),
    ] {
        let clock = crate::clock::test_time_override_guard(10_000);
        let dir = test_util::tempdir();
        let (map, handle, context) = setup(&dir, 0, RouteMapValidity::until_ms(20_000).unwrap());
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                context.id().pg_id(),
                context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let target = section.application_target(NodeId::new(2)).unwrap();
        let encoded = context.encode_command().unwrap();
        let request = CompleteApplicationRequest::new(NodeId::new(2), &encoded, deadline());
        if replay {
            target.apply_complete_command(&request).unwrap();
        }
        target
            .test_with_application_authority(&request, |pg, authority| {
                let before = snapshot(pg);
                let mut reached = false;
                let error = pg
                    .test_apply_embedded_complete_command_with_hook(authority, |at| {
                        if at == stage {
                            reached = true;
                            map.test_store_route_map_validity(
                                RouteMapValidity::until_ms(40_000).unwrap(),
                            );
                            clock.set(21_000);
                            map.require_route_map_valid_now().unwrap();
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(reached);
                assert!(matches!(
                    error.into_store_error().unwrap(),
                    StoreError::RouteMapExpired { .. }
                ));
                assert!(pg.conn.is_autocommit());
                assert_eq!(snapshot(pg), before);
                Ok(())
            })
            .unwrap();
        assert!(target.apply_complete_command(&request).is_err());
    }
}

fn expire(cutoff: Instant) {
    assert!(
        Instant::now() < cutoff,
        "test did not reach the intended expiry gap"
    );
    while let Some(remaining) = cutoff.checked_duration_since(Instant::now()) {
        std::thread::sleep(remaining);
    }
}

#[test]
fn staging_complete_application_embedded_work_and_section_deadlines_reach_transaction() {
    for section_expires in [false, true] {
        for (stage, replay) in [
            ("before-effect", false),
            ("before-commit", false),
            ("before-commit", true),
        ] {
            let dir = test_util::tempdir();
            let (map, handle, context) = setup(&dir, 0, RouteMapValidity::Forever);
            let admission = handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    context.id().pg_id(),
                    context.id().cluster_epoch(),
                )
                .unwrap();
            let cutoff = Instant::now() + Duration::from_secs(1);
            let section = route
                .lock_until(if section_expires { cutoff } else { deadline() })
                .unwrap();
            let target = section.application_target(NodeId::new(2)).unwrap();
            let encoded = context.encode_command().unwrap();
            let request = CompleteApplicationRequest::new(
                NodeId::new(2),
                &encoded,
                if section_expires { deadline() } else { cutoff },
            );
            if replay {
                target.apply_complete_command(&request).unwrap();
            }
            target
                .test_with_application_authority(&request, |pg, authority| {
                    let before = snapshot(pg);
                    let mut reached = false;
                    let error = pg
                        .test_apply_embedded_complete_command_with_hook(authority, |at| {
                            if at == stage {
                                reached = true;
                                expire(cutoff);
                            }
                            Ok(())
                        })
                        .unwrap_err();
                    assert!(reached);
                    assert!(matches!(
                        error.into_store_error().unwrap(),
                        StoreError::OperationDeadlineExceeded { .. }
                    ));
                    let later =
                        CompleteApplicationRequest::new(NodeId::new(2), &encoded, deadline());
                    assert!(matches!(
                        authority.require_effect(pg, &later),
                        Err(StoreError::OperationDeadlineExceeded { .. })
                    ));
                    assert_eq!(snapshot(pg), before);
                    assert!(pg.conn.is_autocommit());
                    Ok(())
                })
                .unwrap();
            assert!(target.apply_complete_command(&request).is_err());
            // The original PG guard remains held even after the budget expires.
            assert!(map
                .runtime_state()
                .metadata_command_pg_lock(context.id().pg_id())
                .try_lock()
                .is_none());
        }
    }
}

#[test]
fn staging_complete_application_embedded_bounds_store_wait_and_rejects_expired_arrival() {
    let dir = test_util::tempdir();
    let (map, handle, context) = setup(&dir, 0, RouteMapValidity::Forever);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            context.id().pg_id(),
            context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let target = section.application_target(NodeId::new(2)).unwrap();
    let pg = map
        .node(NodeId::new(2))
        .unwrap()
        .test_node()
        .get_pg(context.id().pg_id().get())
        .unwrap();
    let before = snapshot(&pg);
    let writes = pg.conn.total_changes();
    let encoded = context.encode_command().unwrap();
    let request = CompleteApplicationRequest::new(
        NodeId::new(2),
        &encoded,
        Instant::now() + Duration::from_millis(30),
    );
    assert!(matches!(
        target
            .apply_complete_command(&request)
            .unwrap_err()
            .into_store_error()
            .unwrap(),
        StoreError::OperationDeadlineExceeded { .. }
    ));
    assert_eq!(snapshot(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
    drop(pg);
    // Once the lock is available, arrival is still expired, not renewed.
    assert!(matches!(
        target
            .apply_complete_command(&request)
            .unwrap_err()
            .into_store_error()
            .unwrap(),
        StoreError::OperationDeadlineExceeded { .. }
    ));
}

#[test]
fn staging_complete_application_embedded_failure_unwind_and_lost_confirmation_are_atomic() {
    for stage in [
        "before-effect",
        "after-effect",
        "before-commit",
        "after-commit",
    ] {
        for unwind in [false, true] {
            let dir = test_util::tempdir();
            let (_, handle, context) = setup(&dir, 3, RouteMapValidity::Forever);
            let admission = handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    context.id().pg_id(),
                    context.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let target = section.application_target(NodeId::new(2)).unwrap();
            let encoded = context.encode_command().unwrap();
            let request = CompleteApplicationRequest::new(NodeId::new(2), &encoded, deadline());
            target
                .test_with_application_authority(&request, |pg, authority| {
                    let before = snapshot(pg);
                    let mut reached = false;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        pg.test_apply_embedded_complete_command_with_hook(authority, |at| {
                            if at == stage {
                                reached = true;
                                if unwind {
                                    panic!("injected embedded application unwind");
                                }
                                return Err(StoreError::RouteCapabilitySubjectMismatch {
                                    operation: "injected embedded application interruption",
                                });
                            }
                            Ok(())
                        })
                    }));
                    assert!(reached);
                    if unwind {
                        assert!(result.is_err());
                    } else {
                        assert!(result
                            .unwrap()
                            .unwrap_err()
                            .to_string()
                            .contains("injected embedded application interruption"));
                    }
                    assert!(pg.conn.is_autocommit());
                    let committed = if stage == "after-commit" {
                        assert_ne!(snapshot(pg), before);
                        Some((snapshot(pg), pg.conn.total_changes()))
                    } else {
                        assert_eq!(snapshot(pg), before);
                        None
                    };
                    pg.apply_embedded_complete_metadata_command(authority)?;
                    if let Some((state, writes)) = committed {
                        assert_eq!(snapshot(pg), state);
                        assert_eq!(pg.conn.total_changes(), writes);
                    }
                    Ok(())
                })
                .unwrap();
        }
    }
}

#[test]
fn staging_complete_application_embedded_unconfirmed_rollback_retires_connection() {
    let dir = test_util::tempdir();
    let (_, handle, context) = setup(&dir, 1, RouteMapValidity::Forever);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            context.id().pg_id(),
            context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let target = section.application_target(NodeId::new(2)).unwrap();
    let encoded = context.encode_command().unwrap();
    let request = CompleteApplicationRequest::new(NodeId::new(2), &encoded, deadline());
    target
        .test_with_application_authority(&request, |pg, authority| {
            let before = snapshot(pg);
            let mut reached = false;
            let error = pg
                .test_apply_embedded_complete_command_with_hook(authority, |stage| {
                    if stage == "after-effect" {
                        reached = true;
                        pg.test_fail_metadata_command_rollbacks(1);
                        return Err(StoreError::OperationDeadlineExceeded {
                            context: "injected embedded rollback failure",
                        });
                    }
                    Ok(())
                })
                .unwrap_err();
            assert!(reached);
            assert!(error.to_string().contains("rollback failed"), "{error}");
            assert!(pg.require_usable_connection().is_err());
            assert!(pg
                .apply_embedded_complete_metadata_command(authority)
                .is_err());
            // Explicit owner-test inspection, not a production unretire operation.
            pg.conn.execute_batch("ROLLBACK").unwrap();
            assert_eq!(snapshot(pg), before);
            assert!(pg.require_usable_connection().is_err());
            Ok(())
        })
        .unwrap();
}
