// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owner-local source/cleanup handoff, not an activated command carrier. No
//! current v13 envelope is recorded as authentication of its separate source.

use super::super::super::abandonment::source::AbandonedPartCleanupCommand;
use super::super::super::command_context::{Applied, ContextualCommand};
use super::*;

fn receive(
    source: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> Result<AbandonedPartCleanupCommand, AbandonmentError> {
    let bytes = PartFinalizationSource::capture_for_test(source)
        .unwrap()
        .encode();
    let cleanup = crate::metadata_command::decode_metadata_command_envelope(
        &cleanup.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    AbandonedPartCleanupCommand::decode_source_field(cleanup, &bytes, &topology())
}

#[test]
fn staging_abandonment_candidate_rejects_crossed_source_cleanup_before_storage_access() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let source = part::command(&pg, &owner);
    let cleanup = cleanup(&pg, &owner, &source);
    let crossed_key = (0..1_000)
        .map(|i| ObjectKey::try_from(format!("crossed-{i}")).unwrap())
        .find(|key| {
            topology().object_pg_for(&owner.subject.bucket, key) == owner.subject.object_pg.get()
        })
        .unwrap();
    assert_ne!(crossed_key, owner.subject.key);
    assert_eq!(
        topology().object_pg_for(&owner.subject.bucket, &crossed_key),
        owner.subject.object_pg.get()
    );
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    for field in [
        "key",
        "session",
        "pg",
        "position",
        "creation-proof",
        "source-operation",
        "source-target",
        "source-epoch",
        "source-bucket",
        "source-internal",
    ] {
        let MetadataCommandPayload::AbortStreamUpload(mut abort) = cleanup.payload().clone() else {
            unreachable!()
        };
        let mut finalizer = payload(&source).clone();
        let mut id = cleanup.id();
        match field {
            "key" => abort.key = crossed_key.clone(),
            "session" => abort.session_id = crate::tests::stream_session_id("other-session"),
            "pg" => {
                id = MetadataCommandId::new(
                    id.cluster_epoch(),
                    PgId::new((id.pg_id().get() + 1) % 8),
                    id.log_index(),
                )
            }
            "position" => id = source.id(),
            "creation-proof" => {
                abort.stream_create_bucket_write_reservation = Some(proof(
                    &owner.subject,
                    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                ))
            }
            "source-operation" => {
                finalizer.bucket_write_reservation.operation_kind =
                    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.into()
            }
            "source-target" => {
                finalizer.bucket_write_reservation.target_context =
                    Some(crossed_key.as_str().into())
            }
            "source-epoch" => {
                finalizer.bucket_write_reservation.cluster_epoch =
                    ClusterEpoch::new(source.id().cluster_epoch().get() + 1).unwrap()
            }
            "source-bucket" => {
                finalizer.bucket_write_reservation.bucket =
                    BucketName::try_from("other-bucket").unwrap()
            }
            "source-internal" => finalizer.part.part_number += 1,
            _ => unreachable!(),
        }
        let crossed_source = MetadataCommandEnvelope::new(
            source.id(),
            MetadataCommandPayload::CommitStreamPart(Box::new(finalizer)),
        );
        let crossed_cleanup =
            MetadataCommandEnvelope::new(id, MetadataCommandPayload::AbortStreamUpload(abort));
        assert!(
            matches!(
                receive(&crossed_source, &crossed_cleanup),
                Err(AbandonmentError::Subject)
            ),
            "{field}"
        );
        assert_eq!(pg.conn.total_changes(), writes, "{field}");
        assert_eq!(introduction::state(&pg), before, "{field}");
    }
    let received = receive(&source, &cleanup).unwrap();
    assert_eq!(received.command(), &cleanup);
    assert!(matches!(
        receive(&source, &source),
        Err(AbandonmentError::Subject)
    ));
    for bytes in [&[][..], &received.source_bytes()[..10]] {
        assert!(matches!(
            AbandonedPartCleanupCommand::decode_source_field(cleanup.clone(), bytes, &topology()),
            Err(AbandonmentError::Source(_))
        ));
    }
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_abandonment_candidate_reissue_keeps_original_source_through_compaction_and_restart() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let cleanup = cleanup(&pg, &owner, &source);
        let original_bytes = PartFinalizationSource::capture_for_test(&source)
            .unwrap()
            .encode();
        let candidate = PartFinalizationSource::capture_for_test(&source)
            .unwrap()
            .bind_cleanup(cleanup.clone(), &topology())
            .unwrap();
        terminal(&pg, &source, true);
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
        let same_epoch = candidate
            .reissued_at(MetadataCommandId::new(
                cleanup.id().cluster_epoch(),
                cleanup.id().pg_id(),
                MetadataCommandLogIndex::new(cleanup.id().log_index().get() + 1).unwrap(),
            ))
            .unwrap();
        assert_eq!(same_epoch.source_bytes(), original_bytes);
        let epoch = ClusterEpoch::new(source.id().cluster_epoch().get() + 1).unwrap();
        let digest = pg.metadata_command_replica_state().unwrap().state_digest;
        pg.initialize_metadata_transfer_matching_state(
            0,
            epoch,
            0,
            MetadataCommandLogHash::genesis(),
            digest,
        )
        .unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let reissued = same_epoch
            .reissued_at(MetadataCommandId::new(
                epoch,
                cleanup.id().pg_id(),
                MetadataCommandLogIndex::new(1).unwrap(),
            ))
            .unwrap();
        assert_eq!(reissued.command().payload(), cleanup.payload());
        assert_eq!(reissued.source_bytes(), original_bytes);
        assert_ne!(reissued.command().command_bytes(), source.command_bytes());
        let received = AbandonedPartCleanupCommand::decode_source_field(
            reissued.command().clone(),
            &reissued.source_bytes(),
            &topology(),
        )
        .unwrap();
        assert_eq!(received.source_bytes(), original_bytes);
        // Reissue never substitutes the previous cleanup for the original
        // finalizer. Only the original has an authenticated tombstone/receipt.
        let topology = topology();
        let context = ContextualCommand::abandoned_part(received, &topology).unwrap();
        assert!(matches!(
            context
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0),
            Err(super::super::super::command_context::ApplicationError::TransactionRequired)
        ));
        let prefix = pg.metadata_command_replica_state().unwrap();
        let before = introduction::state(&pg);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let result = crate::clock::with_time_and_monotonic_override(9_000_000, 90_000_000, || {
            context
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0)
        })
        .unwrap();
        assert_eq!(
            result,
            Applied::Abandonment(AbandonmentOutcome::ClosedAndRetained)
        );
        assert_closed(&pg, &owner, &source);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_abandonment_candidate_rejects_topology_and_nonadvancing_reissues() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let source = part::command(&pg, &owner);
    let cleanup = cleanup(&pg, &owner, &source);
    let candidate = receive(&source, &cleanup).unwrap();
    let topology = topology();
    let foreign = (1_u32..256)
        .find_map(|mask| {
            let ids: Vec<_> = (0..8).filter(|pg| mask & (1 << pg) != 0).collect();
            let other = PgTopology::new(&ids).unwrap();
            (other.object_pg_for(&owner.subject.bucket, &owner.subject.key)
                == owner.subject.object_pg.get()
                && other.bucket_pg_for(&owner.subject.bucket)
                    != topology.bucket_pg_for(&owner.subject.bucket))
            .then_some(other)
        })
        .expect("same object PG but different bucket release route");
    assert_eq!(
        foreign.object_pg_for(&owner.subject.bucket, &owner.subject.key),
        owner.subject.object_pg.get()
    );
    assert_ne!(
        foreign.bucket_pg_for(&owner.subject.bucket),
        topology.bucket_pg_for(&owner.subject.bucket)
    );
    assert!(matches!(
        candidate.bind(&foreign),
        Err(AbandonmentError::Topology)
    ));
    for id in [
        cleanup.id(),
        source.id(),
        MetadataCommandId::new(
            cleanup.id().cluster_epoch(),
            PgId::new((pg.pg_id + 1) % 8),
            MetadataCommandLogIndex::new(cleanup.id().log_index().get() + 1).unwrap(),
        ),
    ] {
        assert!(matches!(
            candidate.reissued_at(id),
            Err(AbandonmentError::ReissuePosition)
        ));
    }
    let context = ContextualCommand::abandoned_part(candidate, &topology).unwrap();
    assert!(matches!(
        context.application(&foreign),
        Err(
            super::super::super::command_context::ApplicationError::Candidate(
                super::super::super::candidate::CandidateBindingError::Topology
            )
        )
    ));
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_abandonment_candidate_binding_does_not_cache_terminal_authentication() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let cleanup = cleanup(&pg, &owner, &source);
        terminal(&pg, &source, true);
        let candidate = receive(&source, &cleanup).unwrap();
        let topology = topology();
        // Positive canary, followed by rollback, ensures only the later removal
        // of durable terminal evidence prevents the same candidate applying.
        pg.conn
            .execute_batch("SAVEPOINT positive_application")
            .unwrap();
        assert_eq!(
            candidate
                .bind(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0)
                .unwrap(),
            AbandonmentOutcome::ClosedAndRetained
        );
        pg.conn
            .execute_batch("ROLLBACK TO positive_application; RELEASE positive_application")
            .unwrap();
        let bound = candidate.bind(&topology).unwrap();
        assert_eq!(
            pg.conn
                .execute(
                    "DELETE FROM metadata_command_log WHERE cluster_epoch = ?1 AND log_index = ?2",
                    rusqlite::params![
                        source.id().cluster_epoch().get(),
                        source.id().log_index().get()
                    ]
                )
                .unwrap(),
            1
        );
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        assert!(matches!(
            bound.apply_in_open_transaction(&pg, 0),
            Err(AbandonmentError::NotAbandoned | AbandonmentError::Store(_))
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(append::load(&pg, &owner), owner);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}
