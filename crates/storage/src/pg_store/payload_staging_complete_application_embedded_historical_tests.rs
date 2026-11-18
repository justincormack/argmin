// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Historical embedded receiver admission, not an all-member recovery owner.

use super::*;
use crate::node_runtime::pg_store::{ApplicationObservation, CompleteApplicationRequest};

pub(super) fn observation(context: &ContextualCommand) -> PendingMetadataCommandObservation {
    let encoded = context.encode_command().unwrap();
    PendingMetadataCommandObservation::new(
        context.id().cluster_epoch(),
        NonZeroU64::new(context.id().log_index().get()).unwrap(),
        encoded.checksum_crc64(),
        encoded.command_sha256(),
    )
}

#[test]
fn staging_complete_application_embedded_historical_contexts_lost_confirmation_retry_and_restart() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    for case in 0..4 {
        let dir = test_util::tempdir();
        let fixture = Fixture::with_members(&dir, case, &[4, 2], None);
        let id = fixture.context.id();
        {
            let admission = fixture.handle.admit_current_route().unwrap();
            let route = admission
                .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let shared = fixture
                .map
                .runtime_state()
                .metadata_command_pg_lock(id.pg_id());
            assert!(shared.try_lock().is_none());
            let mut sender =
                pending(&fixture.context).into_publication_sender(NodeId::new(4), deadline());
            sender.send_embedded(&section, deadline()).unwrap();
            let mut expected = None;
            for member in [2, 4] {
                let node_id = NodeId::new(member);
                let target = section.application_target(node_id).unwrap();
                let node = fixture.map.node(node_id).unwrap().test_node();
                let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
                let error = sender
                    .test_apply_embedded_with_hook(&target, deadline(), |stage| {
                        if stage == "after-commit" {
                            return Err(StoreError::RouteCapabilitySubjectMismatch {
                                operation: "lost historical embedded application confirmation",
                            });
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(error
                    .to_string()
                    .contains("lost historical embedded application confirmation"));
                assert_eq!(
                    sender.application_observation(node_id),
                    ApplicationObservation::Unconfirmed
                );
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                let after = snapshot(&pg);
                assert_ne!(before, after);
                let writes = pg.conn.total_changes();
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
                sender.apply_embedded(&target, deadline()).unwrap();
                assert_eq!(
                    sender.application_observation(node_id),
                    ApplicationObservation::Applied
                );
                let pg = node.get_pg(id.pg_id().get()).unwrap();
                assert_eq!(snapshot(&pg), after);
                assert_eq!(pg.conn.total_changes(), writes);
                assert!(shared.try_lock().is_none());
            }
            assert!(
                started(
                    &fixture
                        .map
                        .node(NodeId::new(4))
                        .unwrap()
                        .test_node()
                        .get_pg(id.pg_id().get())
                        .unwrap()
                ),
                "application must not retire custody"
            );
        }
        // Every original runtime and admission is dropped before reopening stores.
        let Fixture {
            runtime_map,
            context,
            map: old_map,
            handle: old_handle,
            authority,
            heartbeat_sequence: _,
        } = fixture;
        drop(old_handle);
        drop(old_map);
        drop(authority);
        let map = Arc::new(
            LocalClusterMap::open_frontend_with_configs_and_runtime_map(
                NodeId::new(4),
                (0..5).map(|member| {
                    LocalNodeStoreConfig::new(
                        NodeId::new(member),
                        dir.path().join(member.to_string()),
                    )
                }),
                config(case).default_ec_shape,
                &runtime_map,
            )
            .unwrap(),
        );
        let handle = StorageClusterRouteHandle::from_authorized_cluster(
            StorageCluster::from_runtime_local_map(Arc::clone(&map), &runtime_map).unwrap(),
        );
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_historical_publication_route(&runtime_map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let mut sender = pending(&context).into_publication_sender(NodeId::new(4), deadline());
        sender.send_embedded(&section, deadline()).unwrap();
        for member in [2, 4] {
            let node = map.node(NodeId::new(member)).unwrap().test_node();
            let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
            let writes = node.get_pg(id.pg_id().get()).unwrap().conn.total_changes();
            sender
                .apply_embedded(
                    &section.application_target(NodeId::new(member)).unwrap(),
                    deadline(),
                )
                .unwrap();
            assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
            assert_eq!(
                node.get_pg(id.pg_id().get()).unwrap().conn.total_changes(),
                writes
            );
        }
    }
}

#[test]
fn staging_complete_application_embedded_historical_requires_both_active_and_peering_membership() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    for retained_peering in [false, true] {
        let dir = test_util::tempdir();
        let fixture = Fixture::with_members(&dir, 0, &[4, 2], Some((&[4, 0], retained_peering)));
        let id = fixture.context.id();
        let map = &fixture.runtime_map;
        let current = map
            .pg_routes()
            .iter()
            .find(|r| r.pg_id() == id.pg_id())
            .unwrap();
        assert_eq!(current.acting_set(), &[NodeId::new(4), NodeId::new(0)]);
        let handle = fixture.handle_for(map);
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_historical_publication_route(map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        // Node 0 has current Peering placement but was not an old Active member.
        // Node 2 was Active; only the second case retains its Peering placement.
        for member in [0, 2] {
            let node = fixture.map.node(NodeId::new(member)).unwrap().test_node();
            let before = snapshot(&node.get_pg(id.pg_id().get()).unwrap());
            let target = section.application_target(NodeId::new(member));
            if member == 2 && retained_peering {
                let encoded = fixture.context.encode_command().unwrap();
                target
                    .unwrap()
                    .apply_complete_command(&CompleteApplicationRequest::new(
                        NodeId::new(member),
                        &encoded,
                        deadline(),
                    ))
                    .unwrap();
                assert_ne!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
            } else {
                assert!(matches!(
                    target,
                    Err(StoreError::RouteCapabilitySubjectMismatch { .. })
                ));
                assert_eq!(snapshot(&node.get_pg(id.pg_id().get()).unwrap()), before);
            }
        }
        assert!(section.application_target(NodeId::new(4)).is_ok());
        assert!(section.application_target(NodeId::new(99)).is_err());
    }
}

#[test]
fn staging_complete_application_embedded_historical_report_binds_index_crc_and_sha_before_access() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    for field in ["index", "crc", "sha"] {
        let dir = test_util::tempdir();
        let mut fixture = Fixture::with_members(&dir, 0, &[4, 2], None);
        let id = fixture.context.id();
        let expected = observation(&fixture.context);
        let mut sha = expected.command_sha256();
        sha[0] ^= u8::from(field == "sha");
        let crossed = PendingMetadataCommandObservation::new(
            id.cluster_epoch(),
            NonZeroU64::new(id.log_index().get() + u64::from(field == "index")).unwrap(),
            expected.command_checksum() ^ u64::from(field == "crc"),
            sha,
        );
        let map = fixture.report(Some(crossed));
        let handle = fixture.handle_for(&map);
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_historical_publication_route(&map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let encoded = fixture.context.encode_command().unwrap();
        for member in [2, 4] {
            let node = fixture.map.node(NodeId::new(member)).unwrap().test_node();
            let target = section.application_target(NodeId::new(member)).unwrap();
            // Hold the PG mutex: wrong reported identity must fail even before
            // trying to acquire this store, not merely fail during local apply.
            let pg = node.get_pg(id.pg_id().get()).unwrap();
            let before = snapshot(&pg);
            let writes = pg.conn.total_changes();
            let error = target
                .apply_complete_command(&CompleteApplicationRequest::new(
                    NodeId::new(member),
                    &encoded,
                    Instant::now() + Duration::from_millis(30),
                ))
                .unwrap_err()
                .into_store_error()
                .unwrap();
            assert!(
                matches!(
                    error,
                    StoreError::RouteCapabilitySubjectMismatch {
                        operation: "embedded publication command differs from admitted subject",
                    }
                ),
                "{field}: {error:?}"
            );
            assert_eq!(snapshot(&pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
        }
        drop(section);
        let original = fixture.handle.admit_current_route().unwrap();
        let route = original
            .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let mut sender =
            pending(&fixture.context).into_publication_sender(NodeId::new(4), deadline());
        sender.send_embedded(&section, deadline()).unwrap();
        for member in [2, 4] {
            sender
                .apply_embedded(
                    &section.application_target(NodeId::new(member)).unwrap(),
                    deadline(),
                )
                .unwrap();
        }
    }
}

#[test]
fn staging_complete_application_embedded_historical_fences_effect_commit_and_replay() {
    for cutoff_kind in ["route", "work", "section"] {
        for (stage, replay) in [
            ("before-effect", false),
            ("before-commit", false),
            ("before-commit", true),
        ] {
            let clock = crate::clock::test_time_override_guard(10_000);
            let dir = test_util::tempdir();
            let fixture = Fixture::with_members(&dir, 0, &[4, 2], None);
            let id = fixture.context.id();
            let admission = fixture.handle.admit_current_route().unwrap();
            let route = admission
                .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
                .unwrap();
            let cutoff = Instant::now() + Duration::from_secs(1);
            let section = route
                .lock_until(if cutoff_kind == "section" {
                    cutoff
                } else {
                    deadline()
                })
                .unwrap();
            let target = section.application_target(NodeId::new(2)).unwrap();
            let encoded = fixture.context.encode_command().unwrap();
            let request = CompleteApplicationRequest::new(
                NodeId::new(2),
                &encoded,
                if cutoff_kind == "work" {
                    cutoff
                } else {
                    deadline()
                },
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
                                if cutoff_kind == "route" {
                                    fixture.map.test_store_route_map_validity(
                                        RouteMapValidity::until_ms(40_000).unwrap(),
                                    );
                                    clock.set(21_000);
                                    fixture.map.require_route_map_valid_now().unwrap();
                                    admission.require_valid_now().unwrap();
                                } else {
                                    assert!(Instant::now() < cutoff);
                                    while let Some(remaining) =
                                        cutoff.checked_duration_since(Instant::now())
                                    {
                                        std::thread::sleep(remaining);
                                    }
                                }
                            }
                            Ok(())
                        })
                        .unwrap_err()
                        .into_store_error()
                        .unwrap();
                    assert!(reached);
                    if cutoff_kind == "route" {
                        assert!(
                            matches!(error, StoreError::RouteMapExpired { .. }),
                            "{error:?}"
                        );
                    } else {
                        assert!(
                            matches!(error, StoreError::OperationDeadlineExceeded { .. }),
                            "{error:?}"
                        );
                    }
                    assert!(pg.conn.is_autocommit());
                    assert_eq!(snapshot(pg), before);
                    assert!(
                        authority
                            .require_effect(
                                pg,
                                &CompleteApplicationRequest::new(
                                    NodeId::new(2),
                                    &encoded,
                                    deadline(),
                                )
                            )
                            .is_err(),
                        "a later attempt must not renew captured authority"
                    );
                    Ok(())
                })
                .unwrap();
            assert!(fixture
                .map
                .runtime_state()
                .metadata_command_pg_lock(id.pg_id())
                .try_lock()
                .is_none());
        }
    }
}

#[test]
fn staging_complete_application_embedded_historical_store_wait_is_bounded_without_mutation() {
    let _clock = crate::clock::test_time_override_guard(10_000);
    let dir = test_util::tempdir();
    let fixture = Fixture::with_members(&dir, 0, &[4, 2], None);
    let id = fixture.context.id();
    let admission = fixture.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_historical_publication_route(&fixture.runtime_map, id.pg_id())
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let target = section.application_target(NodeId::new(2)).unwrap();
    let node = fixture.map.node(NodeId::new(2)).unwrap().test_node();
    let pg = node.get_pg(id.pg_id().get()).unwrap();
    let before = snapshot(&pg);
    let writes = pg.conn.total_changes();
    let encoded = fixture.context.encode_command().unwrap();
    let request = CompleteApplicationRequest::new(
        NodeId::new(2),
        &encoded,
        Instant::now() + Duration::from_millis(30),
    );
    let error = target
        .apply_complete_command(&request)
        .unwrap_err()
        .into_store_error()
        .unwrap();
    assert!(
        matches!(error, StoreError::OperationDeadlineExceeded { .. }),
        "{error:?}"
    );
    assert_eq!(snapshot(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
    drop(pg);
    assert!(target.apply_complete_command(&request).is_err());
    target
        .apply_complete_command(&CompleteApplicationRequest::new(
            NodeId::new(2),
            &encoded,
            deadline(),
        ))
        .unwrap();
}
