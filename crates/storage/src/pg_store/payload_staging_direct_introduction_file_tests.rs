// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Original introduction custody enters file dispatch without effects or renewal.
use super::*;

#[test]
fn staging_direct_introduction_files_release_original_section_before_dispatch() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    introduced!(f, section, terminal, deadline());
    terminal.retire().unwrap();
    let id = terminal.id();
    let cutoffs = terminal.test_original_deadlines();
    let window = terminal
        .into_confirmed_window()
        .unwrap_or_else(|e| panic!("{}", e.error()));
    let before = members(&f);
    let lock = f.map.runtime_state().metadata_command_pg_lock(id.pg_id());
    let fixture = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
    let prepared = window
        .prepare_file_dispatch(&fixture.encryption)
        .unwrap_or_else(|e| panic!("{}", e.error()));
    assert_eq!(prepared.id(), id);
    assert_eq!(prepared.test_original_deadlines(), cutoffs);
    for node in 0..5 {
        assert_eq!(
            prepared.test_application(NodeId::new(node)),
            Applied::Applied
        );
    }
    let binding = prepared.test_binding_state();
    assert!(
        lock.try_lock().is_none(),
        "preparation must keep the primary held"
    );
    assert_eq!(members(&f), before);
    let writer = prepared
        .release_primary(section)
        .unwrap_or_else(|e| panic!("{}", e.error()));
    assert_eq!(writer.id(), id);
    assert_eq!(writer.test_binding_state(), binding);
    assert!(
        lock.try_lock().is_some(),
        "release must be complete on return, without waiting"
    );
    assert_eq!(members(&f), before);
    drop(writer);
    assert_eq!(
        members(&f),
        before,
        "Drop must not release generation or cleanup custody"
    );
}

#[test]
fn staging_direct_introduction_files_reject_reopened_and_foreign_sections() {
    for reopened in [false, true] {
        let _clock = crate::clock::test_time_override_guard(2_000);
        introduced!(f, section, terminal, deadline());
        terminal.retire().unwrap();
        let id = terminal.id();
        let cutoffs = terminal.test_original_deadlines();
        let window = terminal
            .into_confirmed_window()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let fixture = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
        let prepared = window
            .prepare_file_dispatch(&fixture.encryption)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let before = members(&f);
        let (foreign, _, _) = inputs();
        let handle = if reopened { &f.handle } else { &foreign.handle };
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(NodeId::new(4), id.pg_id(), id.cluster_epoch())
            .unwrap();
        // For a reopen, drop the actual original guard first. The same domain,
        // node, PG and epoch must not substitute for this exact held section.
        let original = if reopened {
            drop(section);
            None
        } else {
            Some(section)
        };
        let other = route.lock_until(deadline()).unwrap();
        let foreign_before = members(&foreign);
        let failure = prepared
            .release_primary(other)
            .err()
            .expect("crossed section accepted");
        assert!(matches!(
            failure.error(),
            E::Store(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction requires its original primary section"
            })
        ));
        let (prepared, other) = failure.into_retry();
        assert_eq!(prepared.id(), id);
        assert_eq!(prepared.test_original_deadlines(), cutoffs);
        assert_eq!(members(&f), before);
        assert_eq!(members(&foreign), foreign_before);
        let selected = if reopened { &f } else { &foreign };
        assert!(selected
            .map
            .runtime_state()
            .metadata_command_pg_lock(id.pg_id())
            .try_lock()
            .is_none());
        drop(other);
        if let Some(original) = original {
            let writer = prepared
                .release_primary(original)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert!(f
                .map
                .runtime_state()
                .metadata_command_pg_lock(id.pg_id())
                .try_lock()
                .is_some());
            drop(writer);
        } else {
            drop(prepared);
        }
        assert_eq!(members(&f), before);
    }
}

#[test]
fn staging_direct_introduction_files_expiry_and_invalid_encryption_preserve_custody() {
    for stage in ["before binding", "before release", "invalid encryption"] {
        let clock = crate::clock::test_time_override_guard(2_000);
        introduced!(f, section, terminal, deadline());
        terminal.retire().unwrap();
        let id = terminal.id();
        let cutoffs = terminal.test_original_deadlines();
        let window = terminal
            .into_confirmed_window()
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let before = members(&f);
        let fixture = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
        if stage == "before binding" {
            clock.set(100_000);
        }
        let encryption = if stage == "invalid encryption" {
            // A committed state cannot serve as the provisional encryption input.
            let subject = crate::ObjectEncryptionSubject::standard_object(
                1,
                &BucketName::new("other").unwrap(),
                &ObjectKey::new("other").unwrap(),
                GenerationId::new(1).unwrap(),
                &fixture.encryption,
            );
            fixture.committed_encryption(&subject)
        } else {
            fixture.encryption.clone()
        };
        let result = window.prepare_file_dispatch(&encryption);
        if stage == "before release" {
            let prepared = result.unwrap_or_else(|e| panic!("{}", e.error()));
            clock.set(100_000);
            let failure = prepared
                .release_primary(section)
                .err()
                .expect("expired release accepted");
            assert!(matches!(
                failure.error(),
                E::Store(
                    StoreError::OperationDeadlineExceeded { .. }
                        | StoreError::RouteMapExpired { .. }
                )
            ));
            let (prepared, section) = failure.into_retry();
            assert_eq!(prepared.test_original_deadlines(), cutoffs);
            assert!(f
                .map
                .runtime_state()
                .metadata_command_pg_lock(id.pg_id())
                .try_lock()
                .is_none());
            drop((prepared, section));
        } else {
            let failure = result.err().expect("invalid binding accepted");
            if stage == "invalid encryption" {
                assert!(
                    matches!(failure.error(), E::Window(message) if message == "direct provisional encryption binding")
                );
            } else {
                assert!(matches!(
                    failure.error(),
                    E::Store(
                        StoreError::OperationDeadlineExceeded { .. }
                            | StoreError::RouteMapExpired { .. }
                    )
                ));
            }
            assert_eq!(failure.id(), id);
            drop(failure);
        }
        assert_eq!(members(&f), before);
    }
}
