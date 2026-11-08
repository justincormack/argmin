// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only finalization with real closed owner/window records. No physical
//! deletion or retirement is implied by absence of the bucket/session/manifest.

use super::super::command_context::Applied;
use super::super::inventory::{protection_page, ProtectedWindow};
use super::*;
use crate::error::BucketSnapshotLoadError;
use crate::metadata_command::{
    CreateBucketCommand, DeleteFinalizedBucketCommand, MarkBucketDeletingCommand,
    MetadataCommandEnvelope, MetadataCommandPayload,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::{
    AclGrants, BucketObjectLockConfig, BucketObjectOwnership, BucketOwnershipControls,
    BucketVersioningState, CanonicalUserId, CreateBucketConfig, MetadataError,
};
use rusqlite::OptionalExtension;
use std::num::NonZeroU16;

fn envelope(pg: &PgStore, payload: MetadataCommandPayload) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(9).unwrap(),
            PgId::new(pg.pg_id),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        payload,
    )
}

fn create_bucket(pg: &PgStore, bucket: &BucketName, incarnation: u64) {
    let create = CreateBucketCommand::from_config_for_test(
        &CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &CanonicalUserId::from_principal("owner"),
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::ObjectWriter,
            },
        },
        1_000,
        incarnation,
    )
    .unwrap();
    pg.apply_metadata_command(&envelope(pg, MetadataCommandPayload::CreateBucket(create)))
        .unwrap();
}

fn mark_and_delete(pg: &PgStore, bucket: &BucketName) -> MetadataCommandEnvelope {
    let current = pg.head_bucket_record_raw(bucket).unwrap();
    let next = pg.next_bucket_execution_generation_candidate().unwrap();
    pg.apply_metadata_command(&envelope(
        pg,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            current.with_execution_generation(next),
        )),
    ))
    .unwrap();
    let deleting = pg.head_bucket_record_raw(bucket).unwrap();
    envelope(
        pg,
        MetadataCommandPayload::DeleteFinalizedBucket(DeleteFinalizedBucketCommand::new(
            bucket.clone(),
            deleting.bucket_execution_generation,
            deleting.bucket_incarnation_generation,
        )),
    )
}

struct Fixture {
    dir: test_util::TempDir,
    pg: PgStore,
    owner: StagingOwnerRecord,
    delete: MetadataCommandEnvelope,
    unowned_key: ObjectKey,
    other_bucket: BucketName,
}

fn setup(target: StagingTarget) -> Fixture {
    let template = fixture(target.clone());
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), template.subject.object_pg.get()).unwrap();
    create_bucket(
        &pg,
        &template.subject.bucket,
        template.subject.bucket_incarnation,
    );
    let owner = if matches!(target, StagingTarget::UploadPart { .. }) {
        let owner = part::initialize(&pg, 2);
        let abort = abort::command(&pg, &owner);
        application::ordinary_context(&abort, &topology())
            .unwrap()
            .application(&topology())
            .unwrap()
            .apply_in_open_transaction(&pg, 0)
            .unwrap();
        owner
    } else {
        let owner = object::initialize(&pg, target, 2);
        let close = close::command(&pg, &owner);
        application::ordinary_context(&close, &topology())
            .unwrap()
            .application(&topology())
            .unwrap()
            .apply_in_open_transaction(&pg, 0)
            .unwrap();
        owner
    };
    let owner = append::load(&pg, &owner);
    assert_eq!(
        owner.lifecycle,
        StagingLifecycle::Retiring(StagingRetirement::Unpublished)
    );
    for table in [
        "objects",
        "stream_uploads",
        "stream_upload_segments",
        "multipart_uploads",
        "object_generation_reservations",
    ] {
        assert_eq!(
            pg.conn
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            0,
            "{table} must not mask owner-only protection"
        );
    }
    let topology = topology();
    let unowned_key = (0..1_000)
        .map(|i| ObjectKey::new(format!("unowned-{i}")).unwrap())
        .find(|key| topology.object_pg_for(&owner.subject.bucket, key) == pg.pg_id)
        .unwrap();
    let other_bucket = (0..1_000)
        .map(|i| BucketName::new(format!("other-bucket-{i}")).unwrap())
        .find(|bucket| topology.object_pg_for(bucket, &owner.subject.key) == pg.pg_id)
        .unwrap();
    assert_ne!(unowned_key, owner.subject.key);
    assert_ne!(other_bucket, owner.subject.bucket);
    assert_eq!(
        topology.object_pg_for(&owner.subject.bucket, &unowned_key),
        pg.pg_id
    );
    assert_eq!(
        topology.object_pg_for(&other_bucket, &owner.subject.key),
        pg.pg_id
    );
    create_bucket(&pg, &other_bucket, 50);
    // Counter-only residue from unrelated completed cleanup. These canaries
    // must be pruned for the target bucket and untouched for a different one.
    pg.advance_object_generation_high_water_in_open_txn(
        &owner.subject.bucket,
        &unowned_key,
        GenerationId::new(71).unwrap(),
    )
    .unwrap();
    pg.advance_object_generation_high_water_in_open_txn(
        &other_bucket,
        &owner.subject.key,
        GenerationId::new(73).unwrap(),
    )
    .unwrap();
    let delete = mark_and_delete(&pg, &owner.subject.bucket);
    pg.refresh_metadata_command_state_digest().unwrap();
    pg.conn.execute_batch("COMMIT").unwrap();
    Fixture {
        dir,
        pg,
        owner,
        delete,
        unowned_key,
        other_bucket,
    }
}

fn counter(pg: &PgStore, bucket: &BucketName, key: &ObjectKey) -> Option<(u64, u64)> {
    pg.conn.query_row(
        "SELECT next_write_sequence, max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![bucket, key], |row| Ok((row.get(0)?, row.get(1)?)),
    ).optional().unwrap()
}

fn protection(pg: &PgStore, owner: &StagingOwnerRecord) -> Vec<ProtectedWindow> {
    let page = protection_page(
        &pg.conn,
        owner.subject.object_pg,
        &topology(),
        None,
        NonZeroU16::new(64).unwrap(),
    )
    .unwrap();
    assert!(page.complete);
    assert_eq!(page.windows.len(), owner.windows.len());
    assert!(!page.windows.is_empty(), "discovery cannot pass vacuously");
    assert_eq!(append::load(pg, owner), *owner);
    page.windows
}

#[test]
fn staging_bucket_finalization_retains_fences_and_discovery_after_reopen_and_import() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let Fixture {
            dir,
            pg,
            owner,
            delete,
            unowned_key,
            other_bucket,
        } = setup(target);
        let retained = counter(&pg, &owner.subject.bucket, &owner.subject.key).unwrap();
        let other = counter(&pg, &other_bucket, &owner.subject.key).unwrap();
        assert!(counter(&pg, &owner.subject.bucket, &unowned_key).is_some());
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let before = protection(&pg, &owner);
        assert_eq!(
            application::ordinary_context(&delete, &topology())
                .unwrap()
                .application(&topology())
                .unwrap()
                .apply_in_open_transaction(&pg, 0)
                .unwrap(),
            Applied::Existing
        );
        assert!(matches!(
            pg.head_bucket_record_raw(&owner.subject.bucket),
            Err(MetadataError::BucketNotFound { .. })
        ));
        assert_eq!(
            counter(&pg, &owner.subject.bucket, &owner.subject.key),
            Some(retained)
        );
        assert_eq!(counter(&pg, &owner.subject.bucket, &unowned_key), None);
        assert_eq!(counter(&pg, &other_bucket, &owner.subject.key), Some(other));
        assert_eq!(protection(&pg, &owner), before);
        // The same key in another bucket must NOT inherit the first bucket's
        // owner protection. This pins both sides of the retained-key query.
        let other_delete = mark_and_delete(&pg, &other_bucket);
        application::ordinary_context(&other_delete, &topology())
            .unwrap()
            .application(&topology())
            .unwrap()
            .apply_in_open_transaction(&pg, 0)
            .unwrap();
        assert_eq!(counter(&pg, &other_bucket, &owner.subject.key), None);
        assert_eq!(protection(&pg, &owner), before);
        pg.refresh_metadata_command_state_digest().unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        pg.conn.execute_batch("BEGIN").unwrap();
        assert_eq!(
            counter(&pg, &owner.subject.bucket, &owner.subject.key),
            Some(retained)
        );
        assert_eq!(protection(&pg, &owner), before);
        pg.conn.execute_batch("COMMIT").unwrap();
        let checkpoint = pg
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        let destination = test_util::tempdir();
        let imported = PgStore::open(destination.path(), owner.subject.object_pg.get()).unwrap();
        imported
            .install_metadata_transfer_checkpoint_base(
                0,
                ClusterEpoch::new(19).unwrap(),
                &checkpoint,
                &topology(),
            )
            .unwrap();
        imported.conn.execute_batch("BEGIN").unwrap();
        assert_eq!(
            counter(&imported, &owner.subject.bucket, &owner.subject.key),
            Some(retained)
        );
        assert_eq!(protection(&imported, &owner), before);
        imported.conn.execute_batch("COMMIT").unwrap();
    }
}

#[test]
fn staging_bucket_finalization_preserves_old_incarnation_fences_on_recreated_bucket_delete() {
    let Fixture {
        dir: _dir,
        pg,
        owner,
        delete,
        ..
    } = setup(StagingTarget::DirectPut);
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let retained = counter(&pg, &owner.subject.bucket, &owner.subject.key).unwrap();
    let before = protection(&pg, &owner);
    application::ordinary_context(&delete, &topology())
        .unwrap()
        .application(&topology())
        .unwrap()
        .apply_in_open_transaction(&pg, 0)
        .unwrap();
    create_bucket(&pg, &owner.subject.bucket, 100);
    assert_ne!(
        pg.head_bucket_record_raw(&owner.subject.bucket)
            .unwrap()
            .bucket_incarnation_generation,
        owner.subject.bucket_incarnation
    );
    assert!(
        pg.next_generation_id(&owner.subject.bucket, &owner.subject.key)
            .unwrap()
            .get()
            > owner.subject.payload_generation.get()
    );
    let recreated_delete = mark_and_delete(&pg, &owner.subject.bucket);
    let state = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    // The old finalization command is a no-op against the new incarnation.
    application::ordinary_context(&delete, &topology())
        .unwrap()
        .application(&topology())
        .unwrap()
        .apply_in_open_transaction(&pg, 0)
        .unwrap();
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), state);
    application::ordinary_context(&recreated_delete, &topology())
        .unwrap()
        .application(&topology())
        .unwrap()
        .apply_in_open_transaction(&pg, 0)
        .unwrap();
    assert!(matches!(
        pg.head_bucket_record_raw(&owner.subject.bucket),
        Err(MetadataError::BucketNotFound { .. })
    ));
    assert_eq!(
        counter(&pg, &owner.subject.bucket, &owner.subject.key),
        Some(retained)
    );
    assert_eq!(protection(&pg, &owner), before);
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_bucket_finalization_commit_failure_rolls_back_then_exact_retry_retains_owner() {
    let Fixture {
        dir: _dir,
        pg,
        owner,
        delete,
        unowned_key,
        ..
    } = setup(StagingTarget::DirectPut);
    let before = introduction::state(&pg);
    let retained = counter(&pg, &owner.subject.bucket, &owner.subject.key).unwrap();
    let prefix = pg.metadata_command_replica_state().unwrap();
    pg.fail_next_metadata_txn_commit();
    let error = pg
        .apply_metadata_command_and_record(0, &delete)
        .unwrap_err();
    assert!(
        matches!(
            error,
            BucketSnapshotLoadError::Metadata(MetadataError::Db { .. })
                | BucketSnapshotLoadError::Store(_)
        ),
        "{error:?}"
    );
    assert!(pg.conn.is_autocommit());
    assert_eq!(introduction::state(&pg), before);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    let applied = pg.apply_metadata_command_and_record(0, &delete).unwrap();
    assert_eq!(applied.applied_log_index, 1);
    assert_eq!(
        counter(&pg, &owner.subject.bucket, &owner.subject.key),
        Some(retained)
    );
    assert_eq!(counter(&pg, &owner.subject.bucket, &unowned_key), None);
    let after = introduction::state(&pg);
    assert_eq!(
        pg.apply_metadata_command_and_record(0, &delete).unwrap(),
        applied
    );
    assert_eq!(introduction::state(&pg), after);
    pg.conn.execute_batch("BEGIN").unwrap();
    protection(&pg, &owner);
    pg.conn.execute_batch("COMMIT").unwrap();
}
