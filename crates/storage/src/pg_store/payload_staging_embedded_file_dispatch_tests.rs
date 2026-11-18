// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming embedded serialization into dispatch-ready custody, without IO.
use super::*;

fn state(
    f: &Setup,
) -> Vec<Vec<crate::node_runtime::pg_store::command_log::MetadataCheckpointTableBlock>> {
    (0..5)
        .map(|node| {
            introduction::state(
                &f.map
                    .node(NodeId::new(node))
                    .unwrap()
                    .test_node()
                    .get_pg(f.context.id().pg_id().get())
                    .unwrap(),
            )
        })
        .collect()
}

#[test]
fn staging_embedded_file_dispatch_consumes_guard_for_all_targets_windows_and_histories() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for (target, append) in candidate::cases() {
        for history in ["installed", "inspected", "uncertain"] {
            let f = allocation::setup_live(target.clone(), append);
            allocation::with_confirmed_owned(
                &f,
                history,
                1,
                Duration::from_secs(10),
                |section, mut windows| {
                    let window = windows.pop().unwrap();
                    let observed = window.installation_observation();
                    let lock = f
                        .map
                        .runtime_state()
                        .metadata_command_pg_lock(f.context.id().pg_id());
                    assert!(lock.try_lock().is_none());
                    if target == StagingTarget::DirectPut {
                        let before = state(&f);
                        let writer = window
                            .into_direct_file_dispatch_embedded(
                                section,
                                &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                            )
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        assert_eq!(writer.id(), f.context.id());
                        assert_eq!(writer.test_binding_state().1, observed);
                        assert!(
                            lock.try_lock().is_some(),
                            "guard must be absent immediately, without a waiting acquisition"
                        );
                        assert_eq!(state(&f), before);
                        drop(writer);
                        assert_eq!(state(&f), before);
                    } else {
                        let (_, request, expected) = allocation::inputs(&f, target.clone());
                        let allocated = window
                            .allocate_stream_embedded(&section, &request, &expected, |_| Ok(()))
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        let original = allocated.test_binding_state();
                        let before = state(&f);
                        let writer = allocated
                            .into_stream_file_dispatch_embedded(section)
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        assert_eq!(writer.id(), f.context.id());
                        assert_eq!(writer.test_binding_state(), original);
                        assert!(lock.try_lock().is_some());
                        assert_eq!(state(&f), before);
                        drop(writer);
                        assert_eq!(state(&f), before);
                    }
                },
            );
        }
    }
}

#[test]
fn staging_embedded_file_dispatch_rejects_foreign_section_without_detaching_custody() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for (target, append) in candidate::cases() {
        let f = allocation::setup_live(target.clone(), append);
        let foreign = allocation::setup_live(target.clone(), append);
        allocation::with_confirmed_owned(
            &f,
            "uncertain",
            1,
            Duration::from_secs(10),
            |section, mut windows| {
                let window = windows.pop().unwrap();
                let observed = window.installation_observation();
                let id = f.context.id();
                let admission = foreign.handle.admit_current_route().unwrap();
                let route = admission
                    .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
                    .unwrap();
                let crossed = route.lock_until(deadline()).unwrap();
                let foreign_before = state(&foreign);
                let lock = f.map.runtime_state().metadata_command_pg_lock(id.pg_id());
                let foreign_lock = foreign
                    .map
                    .runtime_state()
                    .metadata_command_pg_lock(id.pg_id());
                assert!(lock.try_lock().is_none());
                assert!(foreign_lock.try_lock().is_none());
                if target == StagingTarget::DirectPut {
                    let before = state(&f);
                    let failure = window
                        .into_direct_file_dispatch_embedded(
                            crossed,
                            &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                        )
                        .err()
                        .expect("foreign section");
                    assert_eq!(failure.id(), id);
                    assert_eq!(failure.test_binding_state().1, observed);
                    assert!(matches!(
                        failure.error(),
                        E::Store(StoreError::RouteCapabilitySubjectMismatch { .. })
                    ));
                    drop(failure);
                    assert_eq!(state(&f), before);
                } else {
                    let (_, request, expected) = allocation::inputs(&f, target.clone());
                    let allocated = window
                        .allocate_stream_embedded(&section, &request, &expected, |_| Ok(()))
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let original = allocated.test_binding_state();
                    let before = state(&f);
                    let failure = allocated
                        .into_stream_file_dispatch_embedded(crossed)
                        .err()
                        .expect("foreign section");
                    assert_eq!(failure.id(), id);
                    assert_eq!(failure.test_binding_state(), original);
                    assert!(matches!(
                        failure.error(),
                        E::Store(StoreError::RouteCapabilitySubjectMismatch { .. })
                    ));
                    drop(failure);
                    assert_eq!(state(&f), before);
                }
                assert!(
                    foreign_lock.try_lock().is_some(),
                    "consumed foreign guard released on failure"
                );
                assert!(
                    lock.try_lock().is_none(),
                    "unconsumed original section remains held"
                );
                drop(section);
                assert!(lock.try_lock().is_some());
                assert_eq!(state(&foreign), foreign_before);
            },
        );
    }
}

#[test]
fn staging_embedded_file_dispatch_expiry_releases_guard_but_keeps_owner_and_allocation() {
    let _clock = crate::clock::test_time_override_guard(crate::clock::current_time_millis());
    for (target, append) in candidate::cases() {
        let f = allocation::setup_live(target.clone(), append);
        allocation::with_confirmed_owned(
            &f,
            "uncertain",
            1,
            Duration::from_secs(10),
            |section, mut windows| {
                let window = windows.pop().unwrap();
                let observed = window.installation_observation();
                let now = crate::clock::current_time_millis();
                if target == StagingTarget::DirectPut {
                    let before = state(&f);
                    let failure = crate::clock::with_time_and_monotonic_override(
                        now + 60_000,
                        crate::clock::monotonic_time_millis(),
                        || {
                            window.into_direct_file_dispatch_embedded(
                                section,
                                &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                            )
                        },
                    )
                    .err()
                    .expect("expired authority");
                    assert_eq!(failure.test_binding_state().1, observed);
                    assert!(matches!(
                        failure.error(),
                        E::Store(
                            StoreError::OperationDeadlineExceeded { .. }
                                | StoreError::RouteMapExpired { .. }
                        )
                    ));
                    drop(failure);
                    assert_eq!(state(&f), before);
                } else {
                    let (_, request, expected) = allocation::inputs(&f, target.clone());
                    let allocated = window
                        .allocate_stream_embedded(&section, &request, &expected, |_| Ok(()))
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let original = allocated.test_binding_state();
                    let before = state(&f);
                    let failure = crate::clock::with_time_and_monotonic_override(
                        now + 60_000,
                        crate::clock::monotonic_time_millis(),
                        || allocated.into_stream_file_dispatch_embedded(section),
                    )
                    .err()
                    .expect("expired authority");
                    assert_eq!(failure.test_binding_state(), original);
                    assert!(matches!(
                        failure.error(),
                        E::Store(
                            StoreError::OperationDeadlineExceeded { .. }
                                | StoreError::RouteMapExpired { .. }
                        )
                    ));
                    drop(failure);
                    assert_eq!(state(&f), before);
                }
                assert!(f
                    .map
                    .runtime_state()
                    .metadata_command_pg_lock(f.context.id().pg_id())
                    .try_lock()
                    .is_some());
            },
        );
    }
}
