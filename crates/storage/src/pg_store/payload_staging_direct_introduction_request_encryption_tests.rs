// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Request scope → introduction → original-section release → real crypto.
//! No synthetic wrapped DEKs or metadata-only payloads are used here.
use super::*;
use crate::node_runtime::traits::ShardStore;
use crate::StoredObject;

fn members(f: &Setup) -> impl PartialEq + std::fmt::Debug {
    (0..5)
        .map(|id| {
            let pg = f
                .map
                .node(NodeId::new(id))
                .unwrap()
                .test_node()
                .get_pg(f.context.id().pg_id().get())
                .unwrap();
            (introduction::state(&pg), pg.conn.total_changes())
        })
        .collect::<Vec<_>>()
}

macro_rules! with_request_writer {
    ($size:expr, |$f:ident, $writer:ident, $fixture:ident| $body:block) => {
        with_request_writer!($size, |$f, $writer, $fixture, admission| $body)
    };
    ($size:expr, |$f:ident, $writer:ident, $fixture:ident, $admission:ident| $body:block) => {{
        with_request_writer!(@released false, $size, |$f, $writer, $fixture, $admission| $body)
    }};
    (@released $released:expr, $size:expr, |$f:ident, $writer:ident, $fixture:ident, $admission:ident| $body:block) => {{
        let _clock = crate::clock::test_time_override_guard(2_000);
        let ($f, owner, proof) = inputs();
        prepare_bucket(&$f, &proof);
        let before = reservations(&$f, &proof.bucket);
        let $admission = $f.handle.admit_current_route().unwrap();
        let route = $admission.active_put_object_route(&proof.bucket, &owner.subject.key).unwrap();
        let result = route.with_direct_put_scope(Default::default(), |_, scope| {
            let primary = $admission.embedded_publication_route(NodeId::new(4), $f.context.id().pg_id(), $f.context.id().cluster_epoch()).unwrap();
            let section = primary.lock_until(deadline()).unwrap();
            let mut introduction = section.prepare_request_direct_introduction(scope, &owner.subject.id.attempt).unwrap();
            introduction.install().unwrap();
            let mut publication = introduction.into_publication().unwrap_or_else(|e| panic!("{}", e.error()));
            publication.publish().unwrap();
            for id in [0, 4, 2, 3, 1] { publication.apply(NodeId::new(id)).unwrap(); }
            let mut terminal = publication.into_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
            terminal.retire().unwrap();
            let window = terminal.into_confirmed_window().unwrap_or_else(|e| panic!("{}", e.error()));
            let mut $fixture = crate::tests::sse_s3_segment_test_fixture(&vec![0x61; $size], 0);
            let files = window.prepare_file_dispatch(&$fixture.encryption).unwrap_or_else(|e| panic!("{}", e.error()));
            let $writer = files.release_primary(section).unwrap_or_else(|e| panic!("{}", e.error()));
            assert!($f.map.runtime_state().metadata_command_pg_lock($f.context.id().pg_id()).try_lock().is_some());
            $body
            if $released { Ok(()) } else { Err("frontend stopped with durable cleanup owner") }
        }).unwrap();
        assert_eq!(result.is_ok(), $released);
        let retained = reservations(&$f, &proof.bucket);
        assert_eq!(retained.len(), before.len() + usize::from(!$released));
        for record in before { assert!(retained.contains(&record)); }
    }};
}

#[path = "payload_staging_direct_contender_drain_tests.rs"]
mod contender_drain;

#[test]
fn staging_encrypted_request_contender_capture_retains_exact_command_payload_and_budget() {
    for case in [
        "clear",
        "unstarted",
        "started",
        "damaged",
        "expired",
        "budget",
    ] {
        with_request_writer!(4096, |f, writer, fixture, admission| {
            let prepared = writer
                .prepare_encryption(4096)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            fixture.encrypt_for_subject(prepared.encryption_subject(), &vec![0x61; 4096]);
            let evidence =
                fixture.committed_encryption_evidence(prepared.object_encryption_subject());
            let mut payload = prepared
                .bind_ciphertext(fixture.storage_bytes.clone(), evidence)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let owner = files::load_owner(&f);
            payload
                .write_files(&vec![None; owner.windows[0].participants.files.len()])
                .unwrap();
            payload.persist_acknowledgements(None).unwrap();
            let mut ready = payload
                .into_acknowledged()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let bounds = ready.test_terminal_bounds();
            let primary = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    ready.id().pg_id(),
                    ready.id().cluster_epoch(),
                )
                .unwrap();
            let section = primary.lock_until(deadline()).unwrap();
            let expected = if matches!(case, "unstarted" | "started" | "damaged") {
                let pg = f
                    .map
                    .node(NodeId::new(4))
                    .unwrap()
                    .test_node()
                    .get_pg(ready.id().pg_id().get())
                    .unwrap();
                let command = complete_node_startup::next(&pg, ready.id().cluster_epoch());
                let encoded = command.encode_command().unwrap();
                let expected = (encoded.id(), encoded.bytes().to_vec());
                complete_pending::install(&mut command.into_submission(), &pg).unwrap();
                if case == "started" {
                    pg.conn
                        .execute(
                            "UPDATE metadata_command_pending_slot SET publication_started = 1",
                            [],
                        )
                        .unwrap();
                }
                if case == "damaged" {
                    pg.conn
                        .execute(
                            "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
                            [],
                        )
                        .unwrap();
                }
                Some(expected)
            } else {
                None
            };
            if case == "budget" {
                ready.test_limit_attempts(0);
            }
            let before = members(&f);
            let result = if case == "expired" {
                crate::clock::with_time_override(bounds.1, || {
                    ready.inspect_contender_embedded(&section)
                })
            } else {
                ready.inspect_contender_embedded(&section)
            };
            if matches!(case, "damaged" | "expired" | "budget") {
                let failure = result
                    .err()
                    .expect("invalid capture must return the original payload");
                let ready = failure.into_writer();
                assert_eq!(ready.test_terminal_bounds(), bounds);
                assert_eq!(ready.id(), f.context.id());
                drop(ready);
            } else {
                let captured = result.unwrap_or_else(|e| panic!("{case}: {}", e.error()));
                assert_eq!(captured.test_attempts(), 1);
                assert_eq!(
                    captured.contender_id(),
                    expected.as_ref().map(|(id, _)| *id)
                );
                assert_eq!(
                    captured.test_contender_bytes(),
                    expected.as_ref().map(|(_, bytes)| bytes.clone())
                );
                assert_eq!(captured.publication_started(), case == "started");
                if case == "clear" {
                    let ready = captured
                        .into_clear_payload()
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_eq!(ready.test_terminal_bounds(), bounds);
                    assert_eq!(ready.test_attempts(), 1);
                    let inspected = ready
                        .inspect_embedded(&section)
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_eq!(
                        inspected.test_attempts(),
                        2,
                        "clear capture does not reset work accounting"
                    );
                } else {
                    let failure = captured
                        .into_clear_payload()
                        .err()
                        .expect("cannot discard an undrained contender");
                    assert!(failure.error().to_string().contains("undrained contender"));
                    let captured = failure.into_writer();
                    assert_eq!(
                        captured.contender_id(),
                        expected.as_ref().map(|(id, _)| *id)
                    );
                    assert_eq!(
                        captured.test_contender_bytes(),
                        expected.map(|(_, bytes)| bytes)
                    );
                    assert_eq!(captured.test_attempts(), 1);
                    drop(captured);
                }
            }
            assert_eq!(
                members(&f),
                before,
                "capture/failure/Drop must perform no effects: {case}"
            );
        });
    }
}

#[test]
fn staging_encrypted_request_embedded_publishes_readable_payload_and_retires_exact_slot() {
    const WALL: u64 = 2_000;
    const MONOTONIC: u64 = 100_000;
    let _clock = crate::clock::test_time_and_monotonic_override_guard(WALL, MONOTONIC);
    for size in [0, 4096, 16_385] {
        with_request_writer!(@released true, size, |f, writer, fixture, admission| {
            let plaintext = vec![0x61; size];
            let prepared = writer.prepare_encryption(size as u64)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            fixture.encrypt_for_subject(prepared.encryption_subject(), &plaintext);
            let wrap_context = prepared.object_encryption_subject().dek_wrap_subject().aead_context().to_vec();
            let evidence = fixture.committed_encryption_evidence(prepared.object_encryption_subject());
            let input = terminal_input(evidence.encryption().clone(), fixture.payload_crc64);
            let mut payload = prepared.bind_ciphertext(fixture.storage_bytes.clone(), evidence)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let owner = files::load_owner(&f);
            let window = &owner.windows[0];
            payload.write_files(&vec![None; window.participants.files.len()]).unwrap();
            payload.persist_acknowledgements(None).unwrap();
            let original_fence = payload.test_effect_fence();
            let ready = payload.into_acknowledged().unwrap_or_else(|e| panic!("{}", e.error()));
            let bounds = ready.test_terminal_bounds();
            let expired_wall = bounds.1.checked_add(1).unwrap();
            let expired_monotonic = MONOTONIC.checked_add(expired_wall.checked_sub(WALL).unwrap()).unwrap();
            assert!(expired_wall < original_fence.deadline().expect("original bounded window fence").authority_valid_until_ms());
            let primary = admission.embedded_publication_route(NodeId::new(4), ready.id().pg_id(), ready.id().cluster_epoch()).unwrap();
            let section = primary.lock_until(deadline()).unwrap();
            let inspected = ready.inspect_embedded(&section).unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(inspected.test_attempts(), 1);
            let decisions = std::cell::Cell::new(0);
            let prepared = inspected.prepare_terminal_embedded_if(&section, &input, |snapshot| {
                decisions.set(decisions.get() + 1);
                assert!(snapshot.existing_etag.is_none());
                Ok::<(), &'static str>(())
            }).unwrap_or_else(|e| panic!("{:?}", e.error()));
            let command = prepared.test_command();
            let mut submission = prepared.into_submission();
            let before = members(&f);
            let clock = std::rc::Rc::new(std::cell::RefCell::new(None));
            let expired = std::rc::Rc::clone(&clock);
            PgStore::test_with_before_pending_slot_effect_hook(
                move |_| {
                    expired.replace(Some(crate::clock::test_time_and_monotonic_override_guard(expired_wall, expired_monotonic)));
                    original_fence.require_valid_for(original_fence.cluster_epoch())
                        .expect("original authority must still be valid at the SQL boundary");
                },
                || submission.install_embedded(&section),
            ).expect_err("terminal budget expired immediately before slot effect");
            section.require_effect().expect("original section remains valid after terminal cutoff");
            drop(clock.take());
            assert_eq!(members(&f), before);
            assert_eq!(submission.installation(), crate::node_runtime::pg_store::DirectTerminalInstallationObservation::NoAcceptanceObserved);
            let ready = submission.into_reinspection().unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(ready.test_terminal_bounds(), bounds);
            assert_eq!(ready.test_attempts(), 2, "SQL rejection retains inspection/install accounting");
            // Reinspection validates the rolled-back prefix through the admitted
            // read path: no test-only startup recovery or durable cache repair.
            let inspected = ready.inspect_embedded(&section).unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(inspected.test_attempts(), 3);
            assert_eq!(members(&f), before, "prefix revalidation must not write SQL");
            let prepared = inspected.prepare_terminal_embedded_if(&section, &input, |snapshot| {
                decisions.set(decisions.get() + 1);
                assert!(snapshot.existing_etag.is_none());
                Ok::<(), &'static str>(())
            }).unwrap_or_else(|e| panic!("{:?}", e.error()));
            assert_eq!(decisions.get(), 2, "a new preparation must reevaluate request facts");
            assert_eq!(prepared.test_command(), command);
            let mut submission = prepared.into_submission();
            submission.install_embedded(&section).unwrap();
            assert_eq!(submission.test_attempts(), 4);
            let accepted = members(&f);
            let failure = submission.into_reinspection().err().expect("installed command cannot be discarded");
            let mut submission = failure.into_submission();
            assert_eq!(members(&f), accepted);
            assert_eq!(submission.id(), command.id());
            assert_eq!(submission.test_attempts(), 4);
            assert_eq!(submission.installation(), crate::node_runtime::pg_store::DirectTerminalInstallationObservation::Installed);
            let installed = members(&f);
            submission.install_embedded(&section).unwrap();
            assert_eq!(members(&f), installed, "exact installation retry mutated state");
            let mut publication = submission.into_publication().unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(publication.test_attempts(), 5);
            assert_eq!(publication.test_cutoff().0, bounds.0);
            assert!(publication.test_cutoff().1 <= bounds.1);
            let progress = publication.advance_embedded(&section).unwrap();
            assert!(!progress.is_ready());
            assert_eq!(progress.applied_node(), None);
            assert_eq!(publication.test_attempts(), 6);
            for id in 0..5 { assert_ne!(publication.application(NodeId::new(id)), crate::node_runtime::pg_store::ApplicationObservation::Applied, "marker step cannot also apply a replica"); }
            let progress = publication.advance_embedded(&section).unwrap();
            assert_eq!(progress.applied_node(), Some(NodeId::new(0)));
            let failure = publication.into_terminal().err().expect("partial apply cannot release custody");
            let mut publication = failure.into_publication();
            assert_eq!(publication.test_attempts(), 7, "partial handoff retains publication/apply accounting");
            for (index, id) in [4, 1, 2, 3].into_iter().enumerate() {
                let before = publication.test_attempts();
                let progress = publication.advance_embedded(&section).unwrap();
                assert_eq!(progress.applied_node(), Some(NodeId::new(id)));
                assert_eq!(publication.test_attempts(), before + 1);
                assert_eq!((0..5).filter(|node| publication.application(NodeId::new(*node)) == crate::node_runtime::pg_store::ApplicationObservation::Applied).count(), index + 2);
            }
            let applied = members(&f);
            for _ in 0..2 { assert!(publication.advance_embedded(&section).unwrap().is_ready()); }
            assert_eq!(publication.test_attempts(), 11, "ready observation performs no effect");
            assert_eq!(members(&f), applied);
            crate::clock::with_time_override(bounds.1, || publication.advance_embedded(&section)).expect_err("readiness must still check the original deadline");
            let mut terminal = publication.into_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(terminal.test_attempts(), 11);
            terminal.release_reservation().unwrap();
            assert_eq!(terminal.test_attempts(), 12);
            terminal.retire_embedded(&section).unwrap();
            assert_eq!(terminal.test_attempts(), 13);
            assert_eq!(terminal.retirement(), crate::node_runtime::pg_store::TerminalSlotRetirementObservation::Retired);
            let mut stored = Vec::new();
            for (index, node) in window.participants.files.iter().enumerate().take(usize::from(window.geometry.ec.k)) {
                let key = crate::ShardKey::new(&window.segment_hash, window.start.get(), index as u8);
                stored.extend(f.map.node(*node).unwrap().test_node().read_shard_file(window.participants.data_pg.get(), &key).unwrap());
            }
            stored.truncate(fixture.storage_bytes.len());
            assert_eq!(stored, fixture.storage_bytes);
            for id in 0..5 {
                let pg = f.map.node(NodeId::new(id)).unwrap().test_node().get_pg(owner.subject.object_pg.get()).unwrap();
                object::assert_object(&pg, &command);
                let StoredObject::Live(row) = pg.get_object_version(&owner.subject.bucket, &owner.subject.key, crate::VersionId::Null).unwrap() else { panic!("live object") };
                assert_eq!(fixture.open_committed_payload(&row.encryption, &wrap_context, &stored), plaintext);
                let snapshot = pg.command_build_snapshot().unwrap();
                assert_eq!(append::load(&pg, &owner), object::expected(&owner, &command));
                snapshot.finish().unwrap();
                if id == 4 { assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty)); }
            }
            let after = members(&f);
            drop(terminal);
            assert_eq!(members(&f), after, "terminal Drop must not perform cleanup");
        });
    }
}

fn terminal_input(encryption: crate::ObjectEncryption, crc: u64) -> crate::StreamPutCommitInput {
    crate::StreamPutCommitInput {
        versioning: crate::BucketVersioningState::Disabled,
        version_id: crate::VersionId::Null,
        owner: crate::OwnerIdentity::from_principal("encrypted-request-owner"),
        acl_grants: Default::default(),
        public_read: false,
        etag_crc64: crc,
        tags: None,
        metadata_blob: Default::default(),
        system_metadata_blob: Default::default(),
        object_lock: Default::default(),
        encryption,
    }
}

#[test]
fn staging_encrypted_terminal_embedded_retains_evidence_through_rejection_and_reinspection() {
    with_request_writer!(4096, |f, writer, fixture, admission| {
        let prepared = writer
            .prepare_encryption(4096)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        fixture.encrypt_for_subject(prepared.encryption_subject(), &vec![0x61; 4096]);
        let evidence = fixture.committed_encryption_evidence(prepared.object_encryption_subject());
        let input = terminal_input(evidence.encryption().clone(), fixture.payload_crc64);
        let payload = prepared
            .bind_ciphertext(fixture.storage_bytes.clone(), evidence)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let failure = payload
            .into_acknowledged()
            .err()
            .expect("unconfirmed payload cannot close writing");
        let mut payload = failure.into_writer();
        let owner = files::load_owner(&f);
        let window = &owner.windows[0];
        payload
            .write_files(&vec![None; window.participants.files.len()])
            .unwrap();
        payload.persist_acknowledgements(None).unwrap();
        let ready = payload
            .into_acknowledged()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let bounds = ready.test_terminal_bounds();
        assert!(
            bounds.1 < owner.maximum_cutoff_ms,
            "terminal work must shorten the window cutoff"
        );
        let id = ready.id();
        let primary = admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap();
        let section = primary.lock_until(deadline()).unwrap();
        let before = members(&f);
        let failure =
            crate::clock::with_time_override(bounds.1, || ready.inspect_embedded(&section))
                .err()
                .expect("expired inspection");
        assert_eq!(members(&f), before);
        let ready = failure.into_writer();
        assert_eq!(ready.test_terminal_bounds(), bounds);
        assert_eq!(ready.id(), id);
        let mut inspected = ready
            .inspect_embedded(&section)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert!(inspected.auth_snapshot().existing_etag.is_none());
        let calls = std::cell::Cell::new(0);
        let failure = crate::clock::with_time_override(bounds.1, || {
            inspected.prepare_terminal_embedded_if(&section, &input, |_| {
                calls.set(calls.get() + 1);
                Ok::<(), &'static str>(())
            })
        })
        .err()
        .expect("expired decision");
        assert!(failure.storage_error().is_some());
        assert!(failure.request_error().is_none());
        assert_eq!(
            calls.get(),
            0,
            "expired authority must not run request logic"
        );
        inspected = failure.into_retry();
        let failure = inspected
            .prepare_terminal_embedded_if(&section, &input, |snapshot| {
                calls.set(calls.get() + 1);
                assert!(snapshot.existing_etag.is_none());
                Err("exact conditional rejection")
            })
            .err()
            .expect("request rejection");
        assert_eq!(
            failure.request_error(),
            Some(&"exact conditional rejection")
        );
        assert!(failure.storage_error().is_none());
        assert_eq!(calls.get(), 1);
        assert_eq!(members(&f), before);
        inspected = failure.into_retry();
        let clock = crate::clock::test_time_override_guard(2_000);
        let control = clock.control();
        let failure = inspected
            .prepare_terminal_embedded_if(&section, &input, |_| {
                control.set(bounds.1);
                Ok::<(), &'static str>(())
            })
            .err()
            .expect("authority expired during request evaluation");
        assert!(failure.storage_error().is_some());
        assert!(failure.request_error().is_none());
        assert_eq!(members(&f), before);
        clock.set(2_000);
        inspected = failure.into_retry();
        let mut crossed = input.clone();
        crossed.encryption = fixture.encryption.clone();
        let failure = inspected
            .prepare_terminal_embedded(&section, &crossed)
            .err()
            .expect("provisional encryption cannot replace committed evidence");
        assert!(failure
            .error()
            .to_string()
            .contains("committed encryption subject"));
        inspected = failure.into_writer();
        assert_eq!(inspected.id(), id);
        inspected.test_cross_snapshot();
        let failure = inspected
            .prepare_terminal_embedded_if(&section, &input, |snapshot| {
                calls.set(calls.get() + 1);
                assert_eq!(snapshot.existing_etag.as_deref(), Some("crossed snapshot"));
                Ok::<(), &'static str>(())
            })
            .err()
            .expect("crossed snapshot");
        assert!(failure
            .storage_error()
            .unwrap()
            .to_string()
            .contains("snapshot changed"));
        assert!(failure.request_error().is_none());
        assert_eq!(calls.get(), 2);
        assert_eq!(members(&f), before);
        let inspected = failure
            .into_retry()
            .reinspect_embedded(&section)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(
            inspected.test_terminal_bounds(),
            bounds,
            "reinspection must not renew terminal work"
        );
        assert!(inspected.auth_snapshot().existing_etag.is_none());
        let terminal = inspected
            .prepare_terminal_embedded_if(&section, &input, |snapshot| {
                calls.set(calls.get() + 1);
                assert!(snapshot.existing_etag.is_none());
                Ok::<(), &'static str>(())
            })
            .unwrap_or_else(|e| panic!("{:?}", e.error()));
        assert_eq!(
            calls.get(),
            3,
            "reinspection must reevaluate fresh request facts"
        );
        let command = terminal.test_command();
        assert_eq!(terminal.test_binding_state().0, bounds.0);
        let crate::metadata_command::MetadataCommandPayload::CommitDirectPutObject(commit) =
            command.payload()
        else {
            panic!("terminal payload")
        };
        assert_eq!(commit.object.encryption, input.encryption);
        assert_eq!(commit.object.size, 4096);
        assert_eq!(
            commit.object.generation_id,
            owner.subject.payload_generation
        );
        assert_eq!(commit.segments[0].segment_crc64, fixture.storage_crc64);
        assert!(commit.segments[0].shard_integrity.is_some());
        assert_eq!(command.id().log_index().get(), id.log_index().get() + 1);
        assert_eq!(members(&f), before);
        drop(terminal);
        assert_eq!(members(&f), before);
        assert_eq!(files::load_owner(&f), owner);
    });
}

#[test]
fn staging_encrypted_publication_budget_exhaustion_preserves_each_effect_owner() {
    for stage in ["publish", "partial", "release", "retire"] {
        with_request_writer!(@released stage == "retire", 4096, |f, writer, fixture, admission| {
            let prepared = writer.prepare_encryption(4096).unwrap_or_else(|e| panic!("{}", e.error()));
            fixture.encrypt_for_subject(prepared.encryption_subject(), &vec![0x61; 4096]);
            let evidence = fixture.committed_encryption_evidence(prepared.object_encryption_subject());
            let input = terminal_input(evidence.encryption().clone(), fixture.payload_crc64);
            let mut payload = prepared.bind_ciphertext(fixture.storage_bytes.clone(), evidence).unwrap_or_else(|e| panic!("{}", e.error()));
            let owner = files::load_owner(&f);
            payload.write_files(&vec![None; owner.windows[0].participants.files.len()]).unwrap();
            payload.persist_acknowledgements(None).unwrap();
            let ready = payload.into_acknowledged().unwrap_or_else(|e| panic!("{}", e.error()));
            let primary = admission.embedded_publication_route(NodeId::new(4), ready.id().pg_id(), ready.id().cluster_epoch()).unwrap();
            let section = primary.lock_until(deadline()).unwrap();
            let inspected = ready.inspect_embedded(&section).unwrap_or_else(|e| panic!("{}", e.error()));
            let prepared = inspected.prepare_terminal_embedded_if(&section, &input, |_| Ok::<(), ()>(())).unwrap_or_else(|e| panic!("{:?}", e.error()));
            let mut submission = prepared.into_submission();
            submission.install_embedded(&section).unwrap();
            let mut publication = submission.into_publication().unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(publication.test_attempts(), 2);
            if stage != "publish" {
                publication.publish_embedded(&section).unwrap();
                publication.apply_embedded(&section, NodeId::new(0)).unwrap();
            }
            if matches!(stage, "publish" | "partial") {
                let count = publication.test_attempts();
                publication.test_limit_attempts(count);
                let before = members(&f);
                let marker = publication.publication();
                let applied = publication.application(NodeId::new(4));
                let error = publication.advance_embedded(&section).unwrap_err();
                assert!(error.to_string().contains("encrypted direct terminal attempt budget"), "{stage}: {error}");
                assert_eq!(publication.publication(), marker);
                assert_eq!(publication.application(NodeId::new(4)), applied);
                let failure = publication.into_terminal().err().expect("incomplete apply must retain custody");
                let publication = failure.into_publication();
                assert_eq!(publication.test_attempts(), count);
                drop(publication);
                assert_eq!(members(&f), before, "{stage}: no effects or hidden Drop cleanup");
            } else {
                for id in [4, 2, 3, 1] { publication.apply_embedded(&section, NodeId::new(id)).unwrap(); }
                let mut terminal = publication.into_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
                if stage == "retire" { terminal.release_reservation().unwrap(); }
                let count = terminal.test_attempts();
                terminal.test_limit_attempts(count);
                let before = members(&f);
                let proof = terminal.reservation();
                let slot = terminal.retirement();
                if stage == "release" {
                    assert!(matches!(terminal.release_reservation().unwrap_err(),
                        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandContention {
                            context: "encrypted direct terminal attempt budget"
                        })));
                } else {
                    assert!(matches!(terminal.retire_embedded(&section).unwrap_err(),
                        crate::node_runtime::pg_store::CompleteRetirementSendError::NotSent(StoreError::MetadataCommandContention {
                            context: "encrypted direct terminal attempt budget"
                        })));
                }
                assert_eq!(terminal.test_attempts(), count);
                assert_eq!(terminal.reservation(), proof);
                assert_eq!(terminal.retirement(), slot);
                drop(terminal);
                assert_eq!(members(&f), before, "{stage}: no effects or hidden Drop cleanup");
            }
        });
    }
}

#[test]
fn staging_encrypted_terminal_attempt_budget_survives_decision_and_installation_handoffs() {
    with_request_writer!(4096, |f, writer, fixture, admission| {
        let prepared = writer
            .prepare_encryption(4096)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        fixture.encrypt_for_subject(prepared.encryption_subject(), &vec![0x61; 4096]);
        let evidence = fixture.committed_encryption_evidence(prepared.object_encryption_subject());
        let input = terminal_input(evidence.encryption().clone(), fixture.payload_crc64);
        let mut payload = prepared
            .bind_ciphertext(fixture.storage_bytes.clone(), evidence)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let owner = files::load_owner(&f);
        payload
            .write_files(&vec![None; owner.windows[0].participants.files.len()])
            .unwrap();
        payload.persist_acknowledgements(None).unwrap();
        let mut ready = payload
            .into_acknowledged()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        ready.test_limit_attempts(2);
        assert_eq!(ready.test_attempts(), 0);
        let bounds = ready.test_terminal_bounds();
        let id = ready.id();
        let primary = admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap();
        let section = primary.lock_until(deadline()).unwrap();
        let before = members(&f);
        let inspected = ready
            .inspect_embedded(&section)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(inspected.test_attempts(), 1);
        let failure = inspected
            .prepare_terminal_embedded_if(&section, &input, |_| Err("request condition"))
            .err()
            .expect("request rejection");
        assert_eq!(failure.request_error(), Some(&"request condition"));
        let inspected = failure.into_retry();
        assert_eq!(
            inspected.test_attempts(),
            1,
            "decision neither charges nor resets inspection"
        );
        let inspected = inspected
            .reinspect_embedded(&section)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(inspected.test_attempts(), 2);
        let prepared = inspected
            .prepare_terminal_embedded_if(&section, &input, |_| Ok::<(), ()>(()))
            .unwrap_or_else(|e| panic!("{:?}", e.error()));
        let mut submission = prepared.into_submission();
        let command = submission.id();
        assert_eq!(submission.test_attempts(), 2);
        let error = submission.install_embedded(&section).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("encrypted direct terminal attempt budget"),
            "{error}"
        );
        assert_eq!(submission.test_attempts(), 2);
        assert_eq!(submission.id(), command);
        assert_eq!(submission.installation(), crate::node_runtime::pg_store::DirectTerminalInstallationObservation::NoAcceptanceObserved);
        let ready = submission
            .into_reinspection()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(ready.test_terminal_bounds(), bounds);
        let failure = ready
            .inspect_embedded(&section)
            .err()
            .expect("reinspection cannot reset exhausted budget");
        assert!(failure
            .error()
            .to_string()
            .contains("encrypted direct terminal attempt budget"));
        let ready = failure.into_writer();
        assert_eq!(ready.id(), id);
        assert_eq!(ready.test_attempts(), 2);
        assert_eq!(
            members(&f),
            before,
            "no preparation/install failure may release custody or mutate SQL"
        );
        drop(ready);
        assert_eq!(
            members(&f),
            before,
            "budget failure must not trigger hidden cleanup"
        );
    });
}

#[test]
fn staging_direct_encrypted_dispatch_uses_retained_geometry_and_ack_primary() {
    for size in [0, 4096, 16_385] {
        with_request_writer!(size, |f, writer, fixture| {
            let prepared = writer
                .prepare_encryption(size as u64)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            fixture.encrypt_for_subject(prepared.encryption_subject(), &vec![0x61; size]);
            let evidence =
                fixture.committed_encryption_evidence(prepared.object_encryption_subject());
            let mut payload = prepared
                .bind_ciphertext(fixture.storage_bytes.clone(), evidence)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let owner = files::load_owner(&f);
            let window = &owner.windows[0];
            let count = usize::from(window.geometry.ec.k + window.geometry.ec.m);
            let dispatch = vec![None; count];
            assert!(payload.persist_acknowledgements(None).is_err());
            assert!(payload.write_files(&dispatch[..count - 1]).is_err());
            payload.write_files(&dispatch).unwrap();
            let mut systematic = Vec::new();
            let mut shards = Vec::new();
            for (index, node) in window.participants.files.iter().enumerate() {
                let key =
                    crate::ShardKey::new(&window.segment_hash, window.start.get(), index as u8);
                let bytes = f
                    .map
                    .node(*node)
                    .unwrap()
                    .test_node()
                    .read_shard_file(window.participants.data_pg.get(), &key)
                    .unwrap();
                assert_eq!(
                    bytes.len(),
                    fixture
                        .storage_bytes
                        .len()
                        .div_ceil(usize::from(window.geometry.ec.k))
                );
                if index < usize::from(window.geometry.ec.k) {
                    systematic.extend_from_slice(&bytes);
                }
                shards.push((key, bytes));
            }
            assert_eq!(
                &systematic[..fixture.storage_bytes.len()],
                fixture.storage_bytes
            );
            assert!(systematic[fixture.storage_bytes.len()..]
                .iter()
                .all(|b| *b == 0));
            // Parity agrees with an independent invocation of the EC library.
            let ec = ec::ErasureCodec::new(
                ec::EcConfig::new(window.geometry.ec.k, window.geometry.ec.m).unwrap(),
            )
            .unwrap();
            let data: Vec<_> = shards[..usize::from(window.geometry.ec.k)]
                .iter()
                .map(|(_, b)| b.as_slice())
                .collect();
            let mut parity = vec![vec![0; shards[0].1.len()]; usize::from(window.geometry.ec.m)];
            ec.encode(
                &data,
                &mut parity.iter_mut().map(Vec::as_mut_slice).collect::<Vec<_>>(),
            )
            .unwrap();
            for (actual, expected) in shards[usize::from(window.geometry.ec.k)..]
                .iter()
                .zip(parity)
            {
                assert_eq!(actual.1, expected);
            }
            payload.persist_acknowledgements(None).unwrap();
            payload.write_files(&dispatch).unwrap();
            payload.persist_acknowledgements(None).unwrap();
            for node in 0..5 {
                let node_id = NodeId::new(node);
                let node = f.map.node(node_id).unwrap().test_node();
                let pg = node.get_pg(window.participants.data_pg.get()).unwrap();
                for (key, bytes) in &shards {
                    if node_id == window.participants.acknowledgement_primary {
                        let ack = pg.stat_shard(key).unwrap();
                        assert_eq!(ack.size, bytes.len() as u64);
                        assert_eq!(ack.crc64, checksum::crc64::checksum(bytes));
                    } else {
                        assert!(matches!(
                            pg.stat_shard(key),
                            Err(crate::StoreError::NotFound)
                        ));
                    }
                }
            }
            assert_eq!(files::load_owner(&f), owner);
            let before_drop = members(&f);
            drop(payload);
            assert_eq!(members(&f), before_drop);
        });
    }
}

#[test]
fn staging_direct_encrypted_dispatch_unwind_and_expiry_keep_retry_custody() {
    with_request_writer!(4096, |f, writer, fixture| {
        let prepared = writer
            .prepare_encryption(4096)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        fixture.encrypt_for_subject(prepared.encryption_subject(), &vec![0x61; 4096]);
        let evidence = fixture.committed_encryption_evidence(prepared.object_encryption_subject());
        let mut payload = prepared
            .bind_ciphertext(fixture.storage_bytes.clone(), evidence)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let owner = files::load_owner(&f);
        let window = &owner.windows[0];
        let dispatch = vec![None; window.participants.files.len()];
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            PgStore::test_with_after_shard_publication_hook(
                || panic!("lost first file acknowledgement"),
                || payload.write_files(&dispatch),
            )
        }));
        assert!(result.is_err());
        let key = crate::ShardKey::new(&window.segment_hash, window.start.get(), 0);
        let bytes = f
            .map
            .node(window.participants.files[0])
            .unwrap()
            .test_node()
            .read_shard_file(window.participants.data_pg.get(), &key)
            .unwrap();
        assert!(
            !bytes.is_empty(),
            "first file survived the lost acknowledgement"
        );
        assert!(payload.persist_acknowledgements(None).is_err());
        let before = members(&f);
        crate::clock::with_time_override(owner.maximum_cutoff_ms, || {
            assert!(payload.write_files(&dispatch).is_err());
        });
        assert_eq!(members(&f), before);
        assert_eq!(files::load_owner(&f), owner);
        payload.write_files(&dispatch).unwrap();
        payload.persist_acknowledgements(None).unwrap();
        assert_eq!(payload.test_observe().1, fixture.storage_bytes);
        assert_eq!(files::load_owner(&f), owner);
    });
}

#[test]
fn staging_direct_request_encryption_binds_real_ciphertext_without_effects() {
    for size in [0, 4096, 16_385, crate::INTERNAL_SEGMENT_SIZE] {
        with_request_writer!(size, |f, writer, fixture| {
            let before = members(&f);
            let prepared = writer
                .prepare_encryption(size as u64)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(prepared.id(), f.context.id());
            assert!(prepared
                .encryption_subject()
                .matches_standard_object_segment(&fixture.encryption, 0, size as u64));
            assert!(!prepared
                .encryption_subject()
                .matches_standard_object_segment(&fixture.encryption, 1, size as u64));
            assert!(!prepared
                .encryption_subject()
                .matches_standard_object_segment(&fixture.encryption, 0, size as u64 + 1));
            let plaintext = vec![0x61; size];
            fixture.encrypt_for_subject(prepared.encryption_subject(), &plaintext);
            // This helper unwraps the actual rewrapped DEK and decrypts every
            // payload record, pinning coherence after terminal rewrap.
            let evidence =
                fixture.committed_encryption_evidence(prepared.object_encryption_subject());
            let expected_encryption = evidence.encryption().clone();
            let ciphertext = fixture.storage_bytes.clone();
            let pointer = ciphertext.as_ptr();
            let encrypted = prepared
                .bind_ciphertext(ciphertext, evidence)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let (id, bytes, encryption, crc) = encrypted.test_observe();
            assert_eq!(id, f.context.id());
            assert_eq!(bytes, fixture.storage_bytes);
            assert_eq!(
                bytes.as_ptr(),
                pointer,
                "binding must move the owned buffer without copying"
            );
            assert_eq!(encryption, &expected_encryption);
            assert_eq!(crc, fixture.storage_crc64);
            assert_eq!(members(&f), before);
            drop(encrypted);
            assert_eq!(members(&f), before, "Drop does not mutate custody");
        });
    }
}

#[test]
fn staging_direct_request_encryption_crossed_subjects_lengths_and_expiry_retain_owner() {
    with_request_writer!(4096, |f, writer, fixture| {
        let before = members(&f);
        let failure = writer
            .prepare_encryption(crate::INTERNAL_SEGMENT_SIZE as u64 + 1)
            .err()
            .expect("oversized request");
        assert!(failure
            .error()
            .to_string()
            .contains("logical size exceeds captured window"));
        let writer = failure.into_writer();
        let owner = files::load_owner(&f);
        let mut prepared = writer
            .prepare_encryption(4096)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        fixture.encrypt_for_subject(prepared.encryption_subject(), &vec![0x61; 4096]);
        for case in [
            "bucket",
            "key",
            "incarnation",
            "generation",
            "short",
            "long",
            "expired",
        ] {
            let subject = &owner.subject;
            let crossed = crate::ObjectEncryptionSubject::standard_object(
                subject.bucket_incarnation + u64::from(case == "incarnation"),
                &if case == "bucket" {
                    BucketName::new("other-bucket").unwrap()
                } else {
                    subject.bucket.clone()
                },
                &if case == "key" {
                    ObjectKey::new("other-key").unwrap()
                } else {
                    subject.key.clone()
                },
                GenerationId::new(
                    subject.payload_generation.get() + u64::from(case == "generation"),
                )
                .unwrap(),
                &fixture.encryption,
            );
            let evidence = fixture.committed_encryption_evidence(&crossed);
            let mut bytes = fixture.storage_bytes.clone();
            if case == "short" {
                bytes.pop();
            }
            if case == "long" {
                bytes.push(0);
            }
            let result = if case == "expired" {
                crate::clock::with_time_override(1_000_000, || {
                    prepared.bind_ciphertext(bytes, evidence)
                })
            } else {
                prepared.bind_ciphertext(bytes, evidence)
            };
            let failure = result.err().unwrap_or_else(|| panic!("accepted {case}"));
            if case != "expired" {
                let expected = if matches!(case, "short" | "long") {
                    "length mismatch"
                } else {
                    "committed subject mismatch"
                };
                assert!(
                    failure.error().to_string().contains(expected),
                    "{case}: {}",
                    failure.error()
                );
            }
            prepared = failure.into_writer();
            assert_eq!(prepared.id(), f.context.id());
            assert_eq!(members(&f), before, "{case}");
        }
        let evidence = fixture.committed_encryption_evidence(prepared.object_encryption_subject());
        let encrypted = prepared
            .bind_ciphertext(fixture.storage_bytes, evidence)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(encrypted.test_observe().0, f.context.id());
        assert_eq!(members(&f), before);
    });
}
