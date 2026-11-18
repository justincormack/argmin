// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real bucket snapshot scope and durable reservations; candidate object-PG
//! introduction only. No live command-format selector is changed.
use super::*;

#[path = "payload_staging_direct_introduction_request_encryption_tests.rs"]
mod encryption;

fn prepare_bucket(f: &Setup, proof: &BucketWriteReservationProof) {
    for member in 0..5 {
        let node = f.map.node(NodeId::new(member)).unwrap().test_node();
        let pg = node
            .get_pg(topology().bucket_pg_for(&proof.bucket))
            .unwrap();
        terminal_release::seed_pg_reservations(&pg, proof, None);
    }
}

fn reservations(f: &Setup, bucket: &BucketName) -> Vec<crate::BucketWriteReservationRecord> {
    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(topology().bucket_pg_for(bucket)).unwrap();
    pg.durable_bucket_write_reservations(bucket).unwrap()
}

#[test]
fn staging_direct_introduction_request_definitive_rejection_releases_only_its_proof() {
    for install in [false, true] {
        let clock = crate::clock::test_time_override_guard(2_000);
        let (f, owner, proof) = inputs();
        prepare_bucket(&f, &proof);
        let before = reservations(&f, &proof.bucket);
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .active_put_object_route(&owner.subject.bucket, &owner.subject.key)
            .unwrap();
        let result = route
            .with_direct_put_scope(Default::default(), |_, scope| {
                let primary = admission
                    .embedded_publication_route(
                        NodeId::new(4),
                        f.context.id().pg_id(),
                        f.context.id().cluster_epoch(),
                    )
                    .unwrap();
                let section = primary.lock_until(deadline()).unwrap();
                let mut captured = section
                    .prepare_request_direct_introduction(scope, &owner.subject.id.attempt)
                    .unwrap();
                assert!(
                    scope.reserve_generation().is_err(),
                    "legacy generation entry is blocked"
                );
                assert!(section
                    .prepare_request_direct_introduction(
                        scope,
                        &crate::tests::stream_session_id("second-attempt")
                    )
                    .is_err());
                if install {
                    let control = clock.control();
                    PgStore::test_with_before_pending_slot_effect_hook(
                        move |_| control.set(12_001),
                        || captured.install(),
                    )
                    .unwrap_err();
                    assert_eq!(captured.installation(), Installed::NoAcceptanceObserved);
                    clock.set(2_000);
                }
                let failure = captured
                    .into_publication()
                    .err()
                    .expect("no acceptance cannot escape request custody");
                assert!(failure
                    .error()
                    .to_string()
                    .contains("confirmed installation"));
                drop(failure.into_introduction());
                Err::<(), _>("request rejected")
            })
            .unwrap();
        assert_eq!(result, Err("request rejected"));
        assert_eq!(reservations(&f, &proof.bucket), before);
        let pg = f.map.node(NodeId::new(4)).unwrap().test_node();
        assert!(pg
            .get_pg(f.context.id().pg_id().get())
            .unwrap()
            .pending_staging_protection(&topology())
            .unwrap()
            .is_none());
    }
}

#[test]
fn staging_direct_introduction_request_installed_return_and_unwind_keep_proof() {
    for unwind in [false, true] {
        let _clock = crate::clock::test_time_override_guard(2_000);
        let (f, owner, proof) = inputs();
        prepare_bucket(&f, &proof);
        let before = reservations(&f, &proof.bucket);
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .active_put_object_route(&owner.subject.bucket, &owner.subject.key)
            .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            route
                .with_direct_put_scope(Default::default(), |_, scope| {
                    let primary = admission
                        .embedded_publication_route(
                            NodeId::new(4),
                            f.context.id().pg_id(),
                            f.context.id().cluster_epoch(),
                        )
                        .unwrap();
                    let section = primary.lock_until(deadline()).unwrap();
                    let mut captured = section
                        .prepare_request_direct_introduction(scope, &owner.subject.id.attempt)
                        .unwrap();
                    captured.install().unwrap();
                    assert_eq!(captured.installation(), Installed::Installed);
                    // An expired retry cannot erase a positively confirmed owner.
                    crate::clock::with_time_override(100_000, || {
                        assert!(captured.install().is_err())
                    });
                    assert_eq!(captured.installation(), Installed::Installed);
                    let mut publication = captured
                        .into_publication()
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    publication.publish().unwrap();
                    publication.apply(NodeId::new(0)).unwrap();
                    // Partial fanout remains command-owned, even if the frontend dies.
                    if unwind {
                        panic!("frontend interrupted after witness apply");
                    }
                    Err::<(), _>("frontend stopped after witness apply")
                })
                .unwrap()
        }));
        assert_eq!(result.is_err(), unwind);
        let retained = reservations(&f, &proof.bucket);
        assert_eq!(retained.len(), before.len() + 1);
        for record in before {
            assert!(retained.contains(&record));
        }
    }
}

#[test]
fn staging_direct_introduction_request_crossed_admission_does_not_claim_scope() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    prepare_bucket(&f, &proof);
    let before = reservations(&f, &proof.bucket);
    let admission = f.handle.admit_current_route().unwrap();
    let other = f.handle.admit_current_route().unwrap();
    let route = admission
        .active_put_object_route(&owner.subject.bucket, &owner.subject.key)
        .unwrap();
    route
        .with_direct_put_scope(Default::default(), |_, scope| {
            let primary = other
                .embedded_publication_route(
                    NodeId::new(4),
                    f.context.id().pg_id(),
                    f.context.id().cluster_epoch(),
                )
                .unwrap();
            let section = primary.lock_until(deadline()).unwrap();
            let error = section
                .prepare_request_direct_introduction(scope, &owner.subject.id.attempt)
                .err()
                .expect("same cluster/PG is not the same admission");
            assert!(error
                .to_string()
                .contains("original admitted primary section"));
            drop(section);
            let wrong_pg = PgId::new((f.context.id().pg_id().get() + 1) % 8);
            let crossed = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    wrong_pg,
                    f.context.id().cluster_epoch(),
                )
                .unwrap();
            let crossed_section = crossed.lock_until(deadline()).unwrap();
            let error = crossed_section
                .prepare_request_direct_introduction(scope, &owner.subject.id.attempt)
                .err()
                .expect("same admission cannot substitute another PG");
            assert!(error
                .to_string()
                .contains("original admitted primary section"));
            drop(crossed_section);
            let primary = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    f.context.id().pg_id(),
                    f.context.id().cluster_epoch(),
                )
                .unwrap();
            let section = primary.lock_until(deadline()).unwrap();
            let captured = section
                .prepare_request_direct_introduction(scope, &owner.subject.id.attempt)
                .unwrap();
            assert_eq!(captured.installation(), Installed::NoAcceptanceObserved);
            drop(captured);
            Ok::<_, ()>(())
        })
        .unwrap()
        .unwrap();
    assert_eq!(reservations(&f, &proof.bucket), before);
}

#[test]
fn staging_direct_introduction_request_unwind_releases_only_before_acceptance_attempt() {
    for during_install in [false, true] {
        let _clock = crate::clock::test_time_override_guard(2_000);
        let (f, owner, proof) = inputs();
        prepare_bucket(&f, &proof);
        let before = reservations(&f, &proof.bucket);
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .active_put_object_route(&owner.subject.bucket, &owner.subject.key)
            .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<Result<(), ()>, _> =
                route.with_direct_put_scope(Default::default(), |_, scope| {
                    let primary = admission
                        .embedded_publication_route(
                            NodeId::new(4),
                            f.context.id().pg_id(),
                            f.context.id().cluster_epoch(),
                        )
                        .unwrap();
                    let section = primary.lock_until(deadline()).unwrap();
                    let mut captured = section
                        .prepare_request_direct_introduction(scope, &owner.subject.id.attempt)
                        .unwrap();
                    if during_install {
                        PgStore::test_with_before_pending_slot_finish_hook(
                            |_| panic!("request unwind during installation"),
                            || captured.install(),
                        )
                        .unwrap();
                        unreachable!("hook must unwind through the request scope");
                    }
                    panic!("request unwind before installation");
                });
        }));
        assert!(result.is_err());
        let retained = reservations(&f, &proof.bucket);
        if during_install {
            assert_eq!(retained.len(), before.len() + 1);
        } else {
            assert_eq!(retained, before);
        }
    }
}

#[test]
fn staging_direct_introduction_request_interrupted_install_keeps_uncertainty_on_rejection() {
    for retry in ["none", "reject", "converge"] {
        let clock = crate::clock::test_time_override_guard(2_000);
        let (f, owner, proof) = inputs();
        prepare_bucket(&f, &proof);
        let before = reservations(&f, &proof.bucket);
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .active_put_object_route(&owner.subject.bucket, &owner.subject.key)
            .unwrap();
        route
            .with_direct_put_scope(Default::default(), |_, scope| {
                let primary = admission
                    .embedded_publication_route(
                        NodeId::new(4),
                        f.context.id().pg_id(),
                        f.context.id().cluster_epoch(),
                    )
                    .unwrap();
                let section = primary.lock_until(deadline()).unwrap();
                let mut captured = section
                    .prepare_request_direct_introduction(scope, &owner.subject.id.attempt)
                    .unwrap();
                assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    PgStore::test_with_before_pending_slot_finish_hook(
                        |_| panic!("lost installation confirmation"),
                        || captured.install(),
                    )
                }))
                .is_err());
                assert_eq!(captured.installation(), Installed::Unconfirmed);
                if retry == "reject" {
                    clock.set(100_000);
                    assert!(captured.install().is_err());
                    clock.set(2_000);
                }
                assert_eq!(captured.installation(), Installed::Unconfirmed);
                if retry == "converge" {
                    let id = captured.id();
                    captured.install().unwrap();
                    assert_eq!(captured.id(), id);
                    assert_eq!(captured.installation(), Installed::Installed);
                }
                drop(captured);
                Err::<(), _>("uncertain introduction")
            })
            .unwrap()
            .unwrap_err();
        assert_eq!(reservations(&f, &proof.bucket).len(), before.len() + 1);
    }
}
