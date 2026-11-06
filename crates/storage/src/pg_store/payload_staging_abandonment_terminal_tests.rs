// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only terminal-custody schedules. Source tombstones are real linked
//! records; terminal transitions use the components, not inferred session absence.

use super::*;

fn proof_only(template: &MetadataCommandEnvelope) -> MetadataCommandEnvelope {
    let MetadataCommandPayload::AbortStreamUpload(mut abort) = template.payload().clone() else {
        unreachable!()
    };
    abort.staged_segments.clear();
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            template.id().cluster_epoch(),
            template.id().pg_id(),
            MetadataCommandLogIndex::new(template.id().log_index().get() + 1).unwrap(),
        ),
        MetadataCommandPayload::AbortStreamUpload(abort),
    )
}

fn close_before_follow_up(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    source: &MetadataCommandEnvelope,
) -> MetadataCommandEnvelope {
    let ordinary = cleanup(pg, owner, source);
    terminal(pg, source, true);
    super::super::super::close::apply_in_open_transaction(pg, &topology(), &ordinary).unwrap();
    pg.record_metadata_command_applied_inner(0, &ordinary)
        .unwrap();
    proof_only(&ordinary)
}

#[test]
fn staging_abandonment_closed_owner_retains_proof_after_compaction_and_epoch_reset() {
    for count in [0, 2] {
        for next_epoch in [false, true] {
            let dir = test_util::tempdir();
            let pg_id = fixture(multipart_target()).subject.object_pg.get();
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            let owner = part::initialize(&pg, count);
            let source = part::command(&pg, &owner);
            let mut follow_up = close_before_follow_up(&pg, &owner, &source);
            pg.conn.execute_batch("COMMIT").unwrap();
            pg.record_current_metadata_command_checkpoint(0, source.id().cluster_epoch())
                .unwrap();
            pg.compact_metadata_command_log(source.id().cluster_epoch())
                .unwrap();
            assert_eq!(
                pg.metadata_command_log_stats(source.id().cluster_epoch())
                    .unwrap()
                    .retained_entries,
                0
            );
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            if next_epoch {
                follow_up =
                    epoch_order::at_epoch_start(&pg, &follow_up, ClusterEpoch::new(7).unwrap());
            }
            assert_eq!(
                pg.metadata_command_acceptance(0, &follow_up).unwrap(),
                MetadataCommandAcceptance::Apply
            );
            assert_eq!(
                apply(&pg, &source, &follow_up).unwrap(),
                AbandonmentOutcome::RetainedOnClosedOwner
            );
            assert_closed(&pg, &owner, &source);
            pg.record_metadata_command_applied_inner(0, &follow_up)
                .unwrap();
            pg.conn.execute_batch("COMMIT").unwrap();
            let replica = pg.metadata_command_replica_state().unwrap();
            assert_eq!(
                replica.state_digest.value(),
                pg.metadata_state_digest().unwrap()
            );
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert_closed(&pg, &owner, &source);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), replica);
            assert_eq!(
                pg.metadata_command_acceptance(0, &follow_up).unwrap(),
                MetadataCommandAcceptance::AlreadyApplied
            );
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            // A distinct later command can discover exact custody already
            // retained. That is not an AlreadyApplied verdict for this new ID.
            let later = proof_only(&follow_up);
            assert_eq!(
                pg.metadata_command_acceptance(0, &later).unwrap(),
                MetadataCommandAcceptance::Apply
            );
            assert_eq!(
                apply(&pg, &source, &later).unwrap(),
                AbandonmentOutcome::AlreadyRetained
            );
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
    }
}

#[test]
fn staging_abandonment_pruned_prefix_authenticates_source_against_later_checkpoint() {
    for count in [0, 2] {
        for next_epoch in [false, true] {
            let dir = test_util::tempdir();
            let pg_id = fixture(multipart_target()).subject.object_pg.get();
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            let owner = part::initialize(&pg, count);
            let source = part::command(&pg, &owner);
            let mut follow_up = close_before_follow_up(&pg, &owner, &source);
            pg.conn.execute_batch("COMMIT").unwrap();
            let checkpoint = pg
                .record_current_metadata_command_checkpoint(0, source.id().cluster_epoch())
                .unwrap();
            assert_eq!(
                checkpoint.applied_log_index,
                source.id().log_index().get() + 1,
                "the only checkpoint is N, and the abandoned source is N-1"
            );
            pg.compact_metadata_command_log_with_receipt_retention(source.id().cluster_epoch(), 2)
                .unwrap();
            assert_eq!(
                pg.metadata_command_log_stats(source.id().cluster_epoch())
                    .unwrap()
                    .retained_entries,
                0
            );
            let (count, first): (u64, u64) = pg
                .conn
                .query_row(
                    "SELECT count(*), min(log_index) FROM metadata_command_terminal_receipts",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!((count, first), (2, source.id().log_index().get()));
            assert!(
                first > 1,
                "the genesis-to-source prefix must actually be pruned"
            );
            assert!(pg
                .metadata_command_checkpoint_candidates(
                    source.id().cluster_epoch(),
                    source.id().log_index().get(),
                    8
                )
                .unwrap()
                .is_empty());
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            if next_epoch {
                follow_up =
                    epoch_order::at_epoch_start(&pg, &follow_up, ClusterEpoch::new(7).unwrap());
                assert_eq!(follow_up.id().log_index().get(), 1);
            }
            assert!(matches!(
                pg.metadata_command_startup_disposition(0, &source).unwrap(),
                crate::pg_store::MetadataCommandStartupDisposition::Abandoned { .. }
            ));
            assert_eq!(
                pg.metadata_command_acceptance(0, &follow_up).unwrap(),
                MetadataCommandAcceptance::Apply
            );
            assert_eq!(
                apply(&pg, &source, &follow_up).unwrap(),
                AbandonmentOutcome::RetainedOnClosedOwner
            );
            assert_closed(&pg, &owner, &source);
            pg.record_metadata_command_applied_inner(0, &follow_up)
                .unwrap();
            pg.conn.execute_batch("COMMIT").unwrap();
            let replica = pg.metadata_command_replica_state().unwrap();
            assert_eq!(
                replica.state_digest.value(),
                pg.metadata_state_digest().unwrap()
            );
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert_closed(&pg, &owner, &source);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), replica);
            assert_eq!(
                pg.metadata_command_acceptance(0, &follow_up).unwrap(),
                MetadataCommandAcceptance::AlreadyApplied
            );
        }
    }
}

#[test]
fn staging_abandonment_completion_closed_attempt_does_not_touch_completed_payload() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owners = completion::setup(&pg, count);
        let active = &owners[3];
        let source = part::command(&pg, active);
        let follow_up = proof_only(&cleanup(&pg, active, &source));
        terminal(&pg, &source, true);
        let complete = completion::command(&pg, &owners[0], VersionId::Null);
        completion::apply(&pg, &complete).unwrap();
        completion::assert_complete(&pg, &complete, &owners);
        let follow_up = epoch_order::at_epoch_start(&pg, &follow_up, ClusterEpoch::new(8).unwrap());
        let retained = owners
            .iter()
            .map(|owner| append::load(&pg, owner))
            .collect::<Vec<_>>();
        let before = introduction::state(&pg);
        let resources: u64 = pg
            .conn
            .query_row(
                "SELECT count(*) FROM payload_staging_resources",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            apply(&pg, &source, &follow_up).unwrap(),
            AbandonmentOutcome::RetainedOnClosedOwner
        );
        let after_resources: u64 = pg
            .conn
            .query_row(
                "SELECT count(*) FROM payload_staging_resources",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            after_resources,
            resources + 1,
            "only one release-resource row may be inserted"
        );
        assert_closed(&pg, active, &source);
        for (index, owner) in retained.iter().enumerate() {
            if index != 3 {
                assert_eq!(append::load(&pg, owner), *owner);
            }
        }
        // Compare all other checkpoint table blocks, including object/part rows.
        let after = introduction::state(&pg);
        let changed = before.iter().zip(&after).filter(|(a, b)| a != b).count();
        assert_eq!(changed, 1, "only the retained resource table changes");
        let pg = epoch_order::record_and_reopen(pg, dir.path(), &follow_up);
        assert_closed(&pg, active, &source);
    }
}

#[test]
fn staging_abandonment_terminal_custody_rejects_inconsistent_live_state_and_missing_fence() {
    for case in [
        "session",
        "reservation",
        "missing-fence",
        "lowered-fence",
        "payload-input",
        "absent-source",
        "substituted-source",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, 2);
        pg.conn
            .execute_batch("CREATE TEMP TABLE saved_session AS SELECT * FROM stream_uploads;
                CREATE TEMP TABLE saved_reservation AS SELECT * FROM object_generation_reservations")
            .unwrap();
        let source = part::command(&pg, &owner);
        let ordinary = cleanup(&pg, &owner, &source);
        let mut follow_up = close_before_follow_up(&pg, &owner, &source);
        let mut source = source;
        match case {
            "session" => {
                pg.conn
                    .execute_batch("INSERT INTO stream_uploads SELECT * FROM saved_session")
                    .unwrap();
            }
            "reservation" => {
                pg.conn.execute("INSERT INTO object_generation_reservations SELECT * FROM saved_reservation WHERE reservation_id = ?1", [&owner.subject.id.attempt]).unwrap();
            }
            "missing-fence" => {
                pg.conn
                    .execute(
                        "DELETE FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
                        rusqlite::params![owner.subject.bucket, owner.subject.key],
                    )
                    .unwrap();
            }
            "lowered-fence" => {
                pg.conn.execute("UPDATE object_write_counters SET max_committed_generation = ?1 WHERE bucket = ?2 AND key = ?3",
                rusqlite::params![owner.subject.payload_generation.get() - 1, owner.subject.bucket, owner.subject.key]).unwrap();
            }
            "payload-input" => {
                follow_up =
                    MetadataCommandEnvelope::new(follow_up.id(), ordinary.payload().clone());
            }
            "absent-source" => {
                source = MetadataCommandEnvelope::new(follow_up.id(), source.payload().clone());
            }
            "substituted-source" => {
                let mut bad = payload(&source).clone();
                bad.bucket_write_reservation.owner_token.push('x');
                source = MetadataCommandEnvelope::new(
                    source.id(),
                    MetadataCommandPayload::CommitStreamPart(Box::new(bad)),
                );
            }
            _ => unreachable!(),
        }
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(apply(&pg, &source, &follow_up).is_err(), "{case}");
        assert_eq!(pg.conn.total_changes(), changes, "{case}");
        assert_eq!(introduction::state(&pg), before, "{case}");
    }
}

#[test]
fn staging_abandonment_published_owner_retains_distinct_obligation_without_replacing_the_winner() {
    for count in [0, 2] {
        for complete_upload in [false, true] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(multipart_target()).subject.object_pg.get(),
            )
            .unwrap();
            let owner = part::initialize(&pg, count);
            let source = part::command(&pg, &owner);
            let follow_up = proof_only(&cleanup(&pg, &owner, &source));
            terminal(&pg, &source, true);
            let mut winning = payload(&source).clone();
            winning
                .bucket_write_reservation
                .reservation_id
                .push_str("-winner");
            winning
                .bucket_write_reservation
                .owner_token
                .push_str("-winner");
            let winner = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    source.id().cluster_epoch(),
                    source.id().pg_id(),
                    MetadataCommandLogIndex::new(source.id().log_index().get() + 1).unwrap(),
                ),
                MetadataCommandPayload::CommitStreamPart(Box::new(winning)),
            );
            part::apply(&pg, &winner).unwrap();
            if complete_upload {
                completion::apply(&pg, &completion::command(&pg, &owner, VersionId::Null)).unwrap();
            }
            let terminal_owner = append::load(&pg, &owner);
            assert_eq!(terminal_owner.lifecycle, StagingLifecycle::Published);
            let before = introduction::state(&pg);
            assert_eq!(
                apply(&pg, &source, &follow_up).unwrap(),
                AbandonmentOutcome::RetainedOnClosedOwner
            );
            let mut expected = terminal_owner;
            expected
                .resources
                .abandoned_finalizations
                .push(StagingBucketResource::capture(
                    &payload(&source).bucket_write_reservation,
                    PgId::new(topology().bucket_pg_for(&owner.subject.bucket)),
                ));
            assert_eq!(append::load(&pg, &owner), expected);
            let after = introduction::state(&pg);
            assert_eq!(
                before.iter().zip(&after).filter(|(a, b)| a != b).count(),
                1,
                "only the resource table changes"
            );
            let changes = pg.conn.total_changes();
            assert_eq!(
                apply(&pg, &source, &follow_up).unwrap(),
                AbandonmentOutcome::AlreadyRetained
            );
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), after);
        }
    }
}

#[test]
fn staging_abandonment_terminal_resource_insert_failure_preserves_outer_transaction() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let source = part::command(&pg, &owner);
    let follow_up = close_before_follow_up(&pg, &owner, &source);
    pg.conn.execute_batch("CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42);
        CREATE TEMP TRIGGER fail_retention BEFORE INSERT ON payload_staging_resources WHEN NEW.role = 1
        BEGIN SELECT RAISE(ABORT, 'terminal resource failure'); END").unwrap();
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    let error = apply(&pg, &source, &follow_up).unwrap_err();
    assert!(
        error.to_string().contains("terminal resource failure"),
        "{error}"
    );
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(introduction::state(&pg), before);
    assert!(!pg.conn.is_autocommit());
    assert_eq!(
        pg.conn
            .query_row("SELECT value FROM outer_work", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        42
    );
    pg.conn
        .execute_batch("DROP TRIGGER fail_retention")
        .unwrap();
    assert_eq!(
        apply(&pg, &source, &follow_up).unwrap(),
        AbandonmentOutcome::RetainedOnClosedOwner
    );
    assert_closed(&pg, &owner, &source);
}

#[test]
fn staging_abandonment_compacted_source_requires_contiguous_authenticated_receipts() {
    for case in [
        "missing-suffix",
        "missing-source",
        "checksum",
        "hash",
        "unlinked-rehashed",
        "source-sha",
        "relinked-suffix",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, 2);
        let source = part::command(&pg, &owner);
        let follow_up = close_before_follow_up(&pg, &owner, &source);
        pg.conn.execute_batch("COMMIT").unwrap();
        pg.record_current_metadata_command_checkpoint(0, source.id().cluster_epoch())
            .unwrap();
        pg.compact_metadata_command_log_with_receipt_retention(source.id().cluster_epoch(), 2)
            .unwrap();
        assert_eq!(
            pg.metadata_command_log_stats(source.id().cluster_epoch())
                .unwrap()
                .retained_entries,
            0
        );
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let suffix_index = source.id().log_index().get() + 1;
        // Positive canary before corrupting the chain: only source-to-checkpoint
        // receipts survive; all full entries and the older prefix are gone.
        assert!(matches!(
            pg.metadata_command_startup_disposition(0, &source).unwrap(),
            crate::pg_store::MetadataCommandStartupDisposition::Abandoned { .. }
        ));
        match case {
            "missing-suffix" => {
                pg.conn
                    .execute(
                        "DELETE FROM metadata_command_terminal_receipts WHERE log_index = ?1",
                        [suffix_index],
                    )
                    .unwrap();
            }
            "missing-source" => {
                pg.conn
                    .execute(
                        "DELETE FROM metadata_command_terminal_receipts WHERE log_index = ?1",
                        [source.id().log_index().get()],
                    )
                    .unwrap();
            }
            "checksum" => {
                pg.conn.execute("UPDATE metadata_command_terminal_receipts SET command_checksum = command_checksum + 1 WHERE log_index = ?1", [suffix_index]).unwrap();
            }
            "hash" => {
                pg.conn.execute("UPDATE metadata_command_terminal_receipts SET log_hash = 0 WHERE log_index = ?1", [suffix_index]).unwrap();
            }
            "unlinked-rehashed" => {
                let checksum: i64 = pg.conn.query_row("SELECT command_checksum FROM metadata_command_terminal_receipts WHERE log_index = ?1", [suffix_index], |row| row.get(0)).unwrap();
                let hash = crate::metadata_command::metadata_command_log_hash(
                    source.id().cluster_epoch(),
                    source.id().pg_id(),
                    MetadataCommandLogIndex::new(suffix_index).unwrap(),
                    123,
                    checksum as u64,
                );
                pg.conn.execute("UPDATE metadata_command_terminal_receipts SET previous_log_hash = 123, log_hash = ?1 WHERE log_index = ?2", rusqlite::params![hash.value() as i64, suffix_index]).unwrap();
            }
            "source-sha" => {
                pg.conn.execute("UPDATE metadata_command_terminal_receipts SET command_sha256 = zeroblob(32) WHERE log_index = ?1", [source.id().log_index().get()]).unwrap();
            }
            "relinked-suffix" => {
                // Every receipt is individually valid and each suffix link
                // agrees. Only the comparison to the checkpoint rejects this.
                let mut previous = 123;
                for index in source.id().log_index().get()..=suffix_index {
                    let checksum: i64 = pg.conn.query_row("SELECT command_checksum FROM metadata_command_terminal_receipts WHERE log_index = ?1", [index], |row| row.get(0)).unwrap();
                    let hash = crate::metadata_command::metadata_command_log_hash(
                        source.id().cluster_epoch(),
                        source.id().pg_id(),
                        MetadataCommandLogIndex::new(index).unwrap(),
                        previous,
                        checksum as u64,
                    )
                    .value();
                    pg.conn.execute("UPDATE metadata_command_terminal_receipts SET previous_log_hash = ?1, log_hash = ?2 WHERE log_index = ?3", rusqlite::params![previous as i64, hash as i64, index]).unwrap();
                    previous = hash;
                }
            }
            _ => unreachable!(),
        }
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(apply(&pg, &source, &follow_up).is_err(), "{case}");
        assert_eq!(pg.conn.total_changes(), changes, "{case}");
        assert_eq!(introduction::state(&pg), before, "{case}");
    }
}

#[test]
fn staging_abandonment_exact_published_custody_preserves_references_and_renewable_evidence() {
    for count in [0, 2] {
        for destination in ["part", "object", "replaced"] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(multipart_target()).subject.object_pg.get(),
            )
            .unwrap();
            let owner = part::initialize(&pg, count);
            let published = part::command(&pg, &owner);
            let abort = cleanup(&pg, &owner, &published);
            part::apply(&pg, &published).unwrap();
            terminal(&pg, &published, false);
            // A later rejected duplicate carries the same stable reservation
            // with renewed lease evidence, not the already-applied command ID.
            let mut duplicate = payload(&published).clone();
            duplicate.bucket_write_reservation.lease_deadline += 1_000;
            let source = MetadataCommandEnvelope::new(
                abort.id(),
                MetadataCommandPayload::CommitStreamPart(Box::new(duplicate)),
            );
            pg.conn.execute_batch("COMMIT").unwrap();
            pg.record_metadata_command_abandoned(0, &source).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            match destination {
                "part" => (),
                "object" => {
                    completion::apply(&pg, &completion::command(&pg, &owner, VersionId::Null))
                        .unwrap();
                }
                "replaced" => {
                    let replacement = part::next_attempt(&pg, &owner, count);
                    part::apply(&pg, &part::command(&pg, &replacement)).unwrap();
                }
                _ => unreachable!(),
            }
            let follow_up = epoch_order::at_epoch_start(
                &pg,
                &proof_only(&abort),
                ClusterEpoch::new(8).unwrap(),
            );
            let before = introduction::state(&pg);
            let retained = append::load(&pg, &owner);
            assert_ne!(retained.lifecycle, StagingLifecycle::Active);
            let ResourceResponsibility::ReleaseDue(proof) = &retained.resources.finalization else {
                unreachable!()
            };
            assert_ne!(
                proof.observed_lease_deadline,
                payload(&source).bucket_write_reservation.lease_deadline
            );
            let changes = pg.conn.total_changes();
            assert_eq!(
                apply(&pg, &source, &follow_up).unwrap(),
                AbandonmentOutcome::AlreadyRetained
            );
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(append::load(&pg, &owner), retained);
            let pg = epoch_order::record_and_reopen(pg, dir.path(), &follow_up);
            assert_eq!(append::load(&pg, &owner), retained);
        }
    }
}
