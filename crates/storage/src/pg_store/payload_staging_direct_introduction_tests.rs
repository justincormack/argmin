// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Admitted primary construction, not live publisher activation.
use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectGenerationCommand,
    PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node_runtime::pg_store::payload_staging::submission::InstallationObservation;
use crate::node_runtime::pg_store::DirectIntroductionInstallation as Installed;
use crate::node_runtime::traits::PgMetadataStore;

#[path = "payload_staging_direct_introduction_request_tests.rs"]
mod request;

#[path = "payload_staging_direct_introduction_parked_tests.rs"]
mod parked;
#[path = "payload_staging_direct_introduction_publication_tests.rs"]
mod publication;
#[path = "payload_staging_direct_introduction_receiver_tests.rs"]
mod receiver;
#[path = "payload_staging_direct_introduction_transport_tests.rs"]
mod transport;

#[test]
fn staging_direct_introduction_installation_is_owned_and_confirmation_never_regresses() {
    let clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let mut captured = section
        .prepare_direct_introduction(
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        )
        .unwrap();
    let before = observe(&f);
    captured.install().unwrap();
    assert_eq!(captured.installation(), Installed::Installed);
    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
    {
        let pg = node.get_pg(section.pg_id().get()).unwrap();
        let index = pg.pending_staging_protection(&topology()).unwrap().unwrap();
        assert!(index.window.is_some());
        assert_eq!(
            pg.pending_metadata_command_route_references(4)
                .unwrap()
                .unwrap()
                .id,
            captured.id()
        );
        assert_eq!(
            introduction::state(&pg),
            before.0,
            "installation applied the owner"
        );
        pg.conn
            .execute(
                "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
                [],
            )
            .unwrap();
    }
    let damaged = observe(&f);
    // Historical confirmation does not claim the damaged slot is still valid.
    captured.install().unwrap();
    assert_eq!(captured.installation(), Installed::Installed);
    assert_eq!(observe(&f), damaged);
    clock.set(100_000);
    assert!(captured.install().is_err());
    assert_eq!(captured.installation(), Installed::Installed);
    drop(captured);
    assert_eq!(observe(&f), damaged, "Drop performed hidden cleanup");
}

#[test]
fn staging_direct_introduction_installation_checks_original_bounds_at_sqlite_effect() {
    let clock = crate::clock::test_time_override_guard(2_000);
    for work_deadline in [false, true] {
        clock.set(2_000);
        let (f, owner, proof) = inputs();
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                f.context.id().pg_id(),
                f.context.id().cluster_epoch(),
            )
            .unwrap();
        let until = if work_deadline {
            Instant::now() + Duration::from_secs(3)
        } else {
            deadline()
        };
        let section = route.lock_until(until).unwrap();
        let mut captured = section
            .prepare_direct_introduction(
                &owner.subject.bucket,
                &owner.subject.key,
                &owner.subject.id.attempt,
                &proof,
            )
            .unwrap();
        let before = observe(&f);
        let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hit = Arc::clone(&reached);
        let control = clock.control();
        let error = PgStore::test_with_before_pending_slot_effect_hook(
            move |transaction| {
                assert!(transaction);
                hit.store(true, std::sync::atomic::Ordering::SeqCst);
                if work_deadline {
                    std::thread::sleep(until.saturating_duration_since(Instant::now()));
                } else {
                    // The original finite staging cutoff, not just the route cutoff.
                    control.set(12_001);
                }
            },
            || captured.install(),
        )
        .unwrap_err();
        assert!(
            reached.load(std::sync::atomic::Ordering::SeqCst),
            "{error:?}"
        );
        assert_eq!(captured.installation(), Installed::NoAcceptanceObserved);
        assert_eq!(observe(&f), before);
        assert!(captured.install().is_err(), "retry renewed the cutoff");
        assert_eq!(observe(&f), before);
    }
}

#[test]
fn staging_direct_introduction_installation_unwind_preserves_history_and_retries_exactly() {
    let clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let mut captured = section
        .prepare_direct_introduction(
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        )
        .unwrap();
    let before = observe(&f);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        PgStore::test_with_before_pending_slot_finish_hook(
            |pg| {
                assert!(pg
                    .pending_staging_protection(&topology())
                    .unwrap()
                    .is_some());
                panic!("interrupted complete introduction installation");
            },
            || captured.install(),
        )
    }));
    assert!(panic.is_err());
    assert_eq!(captured.installation(), Installed::Unconfirmed);
    let rolled_back = observe(&f);
    assert_eq!(rolled_back.0, before.0);
    assert_eq!(rolled_back.1, before.1);
    clock.set(100_000);
    assert!(captured.install().is_err());
    assert_eq!(captured.installation(), Installed::Unconfirmed);
    assert_eq!(observe(&f), rolled_back);
    // Test-only clock rewind to exercise retry of the very same retained bytes.
    clock.set(2_000);
    captured.install().unwrap();
    assert_eq!(captured.installation(), Installed::Installed);
    let installed = observe(&f);
    captured.install().unwrap();
    drop(captured);
    assert_eq!(observe(&f), installed);
}

fn inputs() -> (Setup, StagingOwnerRecord, BucketWriteReservationProof) {
    let (f, _) = unsettled_setup(StagingTarget::DirectPut, false);
    let owner = fresh_capture::template(StagingTarget::DirectPut);
    let proof = proof(
        &owner.subject,
        PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
    );
    (f, owner, proof)
}

#[test]
fn staging_direct_introduction_receiver_capture_uses_actual_primary_and_is_read_only() {
    receiver_capture("success");
}

#[test]
fn staging_direct_introduction_receiver_capture_rejects_crossed_admission_route_and_proof() {
    receiver_capture("admission");
}

#[test]
fn staging_direct_introduction_receiver_capture_rechecks_wire_and_server_expiry() {
    receiver_capture("expiry");
}

#[test]
fn staging_direct_introduction_receiver_response_binds_original_preparation_and_section() {
    receiver_capture("response");
}

fn receiver_capture(case: &str) {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    let Setup {
        _dir,
        map,
        handle,
        context,
    } = f;
    let f = fanout_mixed::Mixed::from_settled_fixture(map, handle, context, [(4, false)]);
    let server = &f.remote(NodeId::new(4)).unwrap().server;
    let expected = {
        let node = server.test_storage_node();
        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
        let generation = pg
            .next_generation_id(&owner.subject.bucket, &owner.subject.key)
            .unwrap();
        let command = MetadataCommandEnvelope::new(
            f.context.id(),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                owner.subject.bucket.clone(),
                owner.subject.key.clone(),
                owner.subject.id.attempt.clone(),
                generation,
                2_000,
            )),
        );
        let context =
            ContextualCommand::from_described(candidate::capture(&pg, &owner, command, false));
        (
            context.id(),
            context.encode_field().unwrap(),
            context.encode_command().unwrap().bytes().to_vec(),
        )
    };
    let before = f.observe();
    let shadows = f.shadows();
    if case.starts_with("park-") {
        let materialized = {
            let node = f.node(NodeId::new(4));
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            introduction::state(&pg)
        };
        let after = std::cell::RefCell::new(None);
        server.test_direct_introduction_parked(
            &expected,
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
            (
                if case == "park-work" {
                    Duration::from_secs(1)
                } else {
                    Duration::from_secs(30)
                },
                |owner| {
                    parked::before(case, owner);
                    if case == "park-expired" {
                        _clock.set(owner.test_bounds().1);
                    }
                },
                |retained| {
                    parked::after(case, &f, &expected.2, retained);
                    *after.borrow_mut() = Some(f.observe());
                },
            ),
        );
        assert_eq!(
            f.observe(),
            after.into_inner().unwrap(),
            "connection custody Drop changed storage"
        );
        let now = f.observe();
        assert_eq!(
            &now[..4],
            &before[..4],
            "installation touched another participant"
        );
        let node = f.node(NodeId::new(4));
        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
        assert_eq!(
            introduction::state(&pg),
            materialized,
            "installation applied the owner"
        );
        assert_eq!(
            f.shadows(),
            shadows,
            "receiver used frontend shadow storage"
        );
        return;
    }
    if case.starts_with("install-") {
        let materialized = {
            let node = f.node(NodeId::new(4));
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            introduction::state(&pg)
        };
        let after = std::cell::RefCell::new(None);
        server.test_direct_introduction_receiver_installation(
            &expected,
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
            (
                if case == "install-work" {
                    Duration::from_secs(1)
                } else {
                    Duration::from_secs(30)
                },
                |owner| {
                    receiver::exercise(case, &f, &expected.2, owner);
                    *after.borrow_mut() = Some(f.observe());
                },
            ),
        );
        assert_eq!(
            f.observe(),
            after.into_inner().unwrap(),
            "receiver holder Drop changed storage"
        );
        let now = f.observe();
        assert_eq!(
            &now[..4],
            &before[..4],
            "installation touched another participant"
        );
        let node = f.node(NodeId::new(4));
        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
        assert_eq!(
            introduction::state(&pg),
            materialized,
            "installation applied the owner"
        );
        assert_eq!(
            f.shadows(),
            shadows,
            "receiver used frontend shadow storage"
        );
        return;
    }
    match case {
        "response" => server.test_direct_introduction_receiver_response(
            &expected,
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        ),
        "success" => server.test_direct_introduction_receiver_success(
            &expected,
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        ),
        "admission" => server.test_direct_introduction_receiver_rejections(
            &expected,
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        ),
        "expiry" => server.test_direct_introduction_receiver_expiry(
            &expected,
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        ),
        _ => unreachable!(),
    }
    assert_eq!(
        f.observe(),
        before,
        "receiver capture mutated actual participant state"
    );
    assert_eq!(
        f.shadows(),
        shadows,
        "receiver capture used frontend shadow stores"
    );
}

#[test]
fn staging_direct_introduction_installation_rejects_foreign_section_and_classifies_commit_failure()
{
    let _clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    let (foreign, _, _) = inputs();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let foreign_admission = foreign.handle.admit_current_route().unwrap();
    let foreign_route = foreign_admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let foreign_section = foreign_route.lock_until(deadline()).unwrap();
    let mut captured = section
        .prepare_direct_introduction(
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        )
        .unwrap();
    let before = observe(&f);
    let foreign_before = observe(&foreign);
    let error = foreign_section
        .install_direct_introduction(&mut captured)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("direct introduction original section"),
        "{error:?}"
    );
    assert_eq!(captured.installation(), Installed::NoAcceptanceObserved);
    assert_eq!(observe(&f), before);
    assert_eq!(observe(&foreign), foreign_before);
    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
    {
        let pg = node.get_pg(section.pg_id().get()).unwrap();
        pg.conn.execute_batch("CREATE TEMP TABLE deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_failure(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER fail_pending AFTER INSERT ON metadata_command_pending_staging_protection BEGIN INSERT INTO deferred_failure VALUES (1, 2); END;").unwrap();
    }
    let error = captured.install().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("commit pending metadata command slot transaction"),
        "{error:?}"
    );
    assert_eq!(captured.installation(), Installed::Unconfirmed);
    let after = observe(&f);
    assert_eq!(after.0, before.0);
    assert_eq!(after.1, before.1);
    assert!(after.2 > before.2, "did not reach sidecar writes");
    {
        let pg = node.get_pg(section.pg_id().get()).unwrap();
        pg.conn.execute_batch("DROP TRIGGER fail_pending; CREATE TEMP TRIGGER fail_pending BEFORE INSERT ON metadata_command_pending_slot BEGIN SELECT RAISE(ABORT, 'definitive subsequent failure'); END;").unwrap();
    }
    let error = captured.install().unwrap_err();
    assert!(
        error.to_string().contains("definitive subsequent failure"),
        "{error:?}"
    );
    assert_eq!(captured.installation(), Installed::Unconfirmed);
    drop(captured);
    assert_eq!(observe(&f), after, "Drop retried or released custody");
}

fn observe(
    f: &Setup,
) -> (
    Vec<crate::node_runtime::pg_store::command_log::MetadataCheckpointTableBlock>,
    Vec<Vec<String>>,
    u64,
) {
    let node = f.map.node(NodeId::new(4)).unwrap().test_node();
    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
    (
        introduction::state(&pg),
        complete_pending::observe(&pg),
        pg.conn.total_changes(),
    )
}

#[test]
fn staging_direct_introduction_admitted_capture_is_exact_and_read_only() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let before = observe(&f);
    let expected = {
        let node = f.map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
        let generation = pg
            .next_generation_id(&owner.subject.bucket, &owner.subject.key)
            .unwrap();
        let command = MetadataCommandEnvelope::new(
            f.context.id(),
            MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
                owner.subject.bucket.clone(),
                owner.subject.key.clone(),
                owner.subject.id.attempt.clone(),
                generation,
                2_000,
            )),
        );
        ContextualCommand::from_described(candidate::capture(&pg, &owner, command, false))
    };
    let transactions = Arc::new(std::sync::Mutex::new(Vec::new()));
    {
        use rusqlite::trace::{TraceEvent, TraceEventCodes};
        let node = f.map.node(NodeId::new(4)).unwrap().test_node();
        let mut pg = node.get_pg(section.pg_id().get()).unwrap();
        let observed = Arc::clone(&transactions);
        pg.conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        if ["BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE"]
                            .iter()
                            .any(|prefix| sql.starts_with(prefix))
                        {
                            observed.lock().unwrap().push(sql.to_owned());
                        }
                    }
                }),
            )
            .unwrap();
    }
    let captured = section
        .prepare_direct_introduction(
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt,
            &proof,
        )
        .unwrap();
    assert_eq!(captured.id(), expected.id());
    assert_eq!(
        *transactions.lock().unwrap(),
        ["BEGIN DEFERRED", "ROLLBACK"]
    );
    captured
        .test_submission()
        .assert_context_for_test(&expected, &topology());
    assert_eq!(
        captured.test_submission().observation().installation,
        InstallationObservation::NoAcceptanceObserved
    );
    assert_eq!(observe(&f), before);
    drop(captured);
    assert_eq!(
        observe(&f),
        before,
        "Drop cannot release or install anything"
    );
}

#[test]
fn staging_direct_introduction_crossed_proofs_and_pg_are_nonmutating() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let before = observe(&f);
    for field in ["epoch", "bucket", "operation", "key"] {
        let mut crossed = proof.clone();
        match field {
            "epoch" => {
                crossed.cluster_epoch = ClusterEpoch::new(proof.cluster_epoch.get() + 1).unwrap()
            }
            "bucket" => crossed.bucket = crate::tests::bucket_name("crossed-introduction"),
            "operation" => crossed.operation_kind = "put-object-stream-create".into(),
            "key" => crossed.target_context = Some("crossed-key".into()),
            _ => unreachable!(),
        }
        let error = section
            .prepare_direct_introduction(
                &owner.subject.bucket,
                &owner.subject.key,
                &owner.subject.id.attempt,
                &crossed,
            )
            .err()
            .expect(field);
        assert!(
            error.to_string().contains("scoped subject/proof"),
            "{field}: {error}"
        );
        assert_eq!(observe(&f), before);
    }
    let key = (0..1000)
        .map(|i| crate::tests::object_key(format!("crossed-{i}")))
        .find(|key| topology().object_pg_for(&owner.subject.bucket, key) != section.pg_id().get())
        .unwrap();
    let mut crossed = proof.clone();
    crossed.target_context = Some(key.as_str().into());
    assert!(section
        .prepare_direct_introduction(
            &owner.subject.bucket,
            &key,
            &owner.subject.id.attempt,
            &crossed
        )
        .is_err());
    assert_eq!(observe(&f), before);
}

#[test]
fn staging_direct_introduction_derives_next_generation_and_command_from_applied_prefix() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let (f, owner, proof) = inputs();
    {
        let node = f.map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
        pg.apply_complete_metadata_command_and_record(
            4,
            &f.context.encode_command().unwrap(),
            &topology(),
        )
        .unwrap();
    }
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let before = observe(&f);
    let attempt = crate::tests::stream_session_id("fresh-attempt");
    let captured = section
        .prepare_direct_introduction(&owner.subject.bucket, &owner.subject.key, &attempt, &proof)
        .unwrap();
    let expected = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            section.epoch(),
            section.pg_id(),
            crate::metadata_command::MetadataCommandLogIndex::new(2).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            owner.subject.bucket.clone(),
            owner.subject.key.clone(),
            attempt,
            GenerationId::new(2).unwrap(),
            2_000,
        )),
    );
    assert_eq!(captured.id(), expected.id());
    captured
        .test_submission()
        .assert_command_for_test(&expected);
    assert_eq!(observe(&f), before);
}

#[test]
fn staging_direct_introduction_refuses_existing_attempt_pending_and_retired_issuer() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    for fault in ["reservation", "pending", "issuer"] {
        let (f, owner, proof) = inputs();
        {
            let node = f.map.node(NodeId::new(4)).unwrap().test_node();
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            match fault {
                "reservation" => pg
                    .apply_reserve_object_generation_command(&ReserveObjectGenerationCommand::new(
                        owner.subject.bucket.clone(),
                        owner.subject.key.clone(),
                        owner.subject.id.attempt.clone(),
                        GenerationId::MIN,
                        2_000,
                    ))
                    .unwrap(),
                "pending" => {
                    let encoded = f.context.encode_command().unwrap();
                    let context = ContextualCommand::decode_command(
                        encoded.bytes(),
                        encoded.checksum_crc64(),
                        &topology(),
                    )
                    .unwrap();
                    context
                        .into_submission()
                        .install(
                            &pg,
                            4,
                            &topology(),
                            crate::types::AdmittedRouteEffectFence::unbounded(
                                f.context.id().cluster_epoch(),
                            ),
                            None,
                        )
                        .unwrap();
                }
                "issuer" => {
                    drop(pg.begin_allocation_lifetime_replacement().unwrap());
                }
                _ => unreachable!(),
            }
            if fault != "issuer" {
                pg.refresh_metadata_command_state_digest().unwrap();
                pg.recover_complete_command_state(
                    PgStoreRecoveryContext::for_node(NodeId::new(4)),
                    &topology(),
                )
                .unwrap();
            }
        }
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                f.context.id().pg_id(),
                f.context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let before = observe(&f);
        let error = section
            .prepare_direct_introduction(
                &owner.subject.bucket,
                &owner.subject.key,
                &owner.subject.id.attempt,
                &proof,
            )
            .err()
            .expect(fault);
        assert!(
            match fault {
                "reservation" =>
                    error.to_string().contains("independently")
                        || error.to_string().contains("existing attempt"),
                "pending" => error.to_string().contains("pending"),
                "issuer" => matches!(
                    error,
                    E::Store(StoreError::PgAllocationLifetimeUnavailable { .. })
                ),
                _ => false,
            },
            "{fault}: {error}"
        );
        assert_eq!(observe(&f), before);
    }
}

#[test]
fn staging_direct_introduction_authority_rejects_a_foreign_same_pg_store() {
    let (f, _, _) = inputs();
    let admission = f.handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            f.context.id().pg_id(),
            f.context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let dir = test_util::tempdir();
    let foreign = PgStore::open(dir.path(), section.pg_id().get()).unwrap();
    let foreign_changes = foreign.conn.total_changes();
    let before = observe(&f);
    let error = section
        .test_direct_introduction_store_binding(&foreign)
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::RouteCapabilitySubjectMismatch {
            operation: "direct introduction admitted primary store"
        }
    ));
    assert_eq!(foreign.conn.total_changes(), foreign_changes);
    assert_eq!(observe(&f), before);
}

#[test]
fn staging_direct_introduction_capture_and_authority_are_noncloneable() {
    trait NotClone<A> {
        fn check() {}
    }
    impl<T: ?Sized> NotClone<()> for T {}
    impl<T: Clone> NotClone<u8> for T {}
    let _ =
        <crate::node_runtime::pg_store::CapturedDirectIntroduction<'_, '_> as NotClone<_>>::check;
    let _ = <crate::node_runtime::engine::DirectIntroductionAuthority<'_, '_, '_> as NotClone<_>>::check;
}

#[test]
fn staging_direct_introduction_expiry_during_capture_or_teardown_exports_nothing() {
    for boundary in ["before capture", "after snapshot"] {
        let clock = crate::clock::test_time_override_guard(2_000);
        let (f, owner, proof) = inputs();
        let admission = f.handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                f.context.id().pg_id(),
                f.context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route
            .lock_until(Instant::now() + std::time::Duration::from_secs(5))
            .unwrap();
        let before = observe(&f);
        let mut reached = false;
        let error = section
            .test_prepare_direct_introduction(
                &owner.subject.bucket,
                &owner.subject.key,
                &owner.subject.id.attempt,
                &proof,
                |point| {
                    if point == boundary {
                        reached = true;
                        clock.set(100_000);
                    }
                },
            )
            .err()
            .expect("expired capture");
        assert!(reached);
        assert!(
            error.to_string().contains("expired") || error.to_string().contains("deadline"),
            "{error}"
        );
        assert_eq!(observe(&f), before);
    }
}
