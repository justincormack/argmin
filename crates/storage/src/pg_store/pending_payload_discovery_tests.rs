// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only pending discovery. Remove the component fixture's prospective
//! owners and live segment rows so neither can mask loss of command protection.

use super::pending_payload_transaction::{install, pages, setup};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::pg_store::PgStore;
use crate::types::{
    ShardScavengerPayloadReference as Reference, ShardScavengerPlacedShardSetReference,
    ShardScavengerProtectedShardSetReference,
};
use crate::{StoreError, StreamUploadSegmentRecord};

fn segments(command: &MetadataCommandEnvelope) -> &[StreamUploadSegmentRecord] {
    match command.payload() {
        MetadataCommandPayload::AppendStreamSegment(command) => {
            std::slice::from_ref(&command.segment)
        }
        MetadataCommandPayload::AbortStreamUpload(command) => &command.staged_segments,
        _ => unreachable!(),
    }
}

fn expected(command: &MetadataCommandEnvelope, placed: bool) -> Vec<Reference> {
    segments(command)
        .iter()
        .map(|segment| {
            let ec = EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            };
            if placed {
                assert!(segment.shard_integrity.is_some());
                Reference::Placed(ShardScavengerPlacedShardSetReference {
                    data_pg_id: segment.data_pg_id,
                    okh: segment.segment_okh,
                    generation_id: segment.segment_vid,
                    placement_cluster_epoch: segment.placement_cluster_epoch,
                    stored_size: crate::tests::metadata_only_test_encryption()
                        .payload_codec()
                        .stored_size(segment.size)
                        .unwrap(),
                    crc64: segment.segment_crc64,
                    ec,
                    shard_integrity: segment.shard_integrity.clone(),
                })
            } else {
                Reference::ProtectionOnly(ShardScavengerProtectedShardSetReference {
                    data_pg_id: segment.data_pg_id,
                    okh: segment.segment_okh,
                    generation_id: segment.segment_vid,
                    ec,
                })
            }
        })
        .collect()
}

fn isolate_pending(pg: &PgStore, owner: &StagingOwnerRecord) {
    pg.conn
        .execute(
            "DELETE FROM payload_staging_owners WHERE attempt = ?1",
            [&owner.subject.id.attempt],
        )
        .unwrap();
    pg.conn
        .execute(
            "DELETE FROM stream_upload_segments WHERE session_id = ?1",
            [&owner.subject.id.attempt],
        )
        .unwrap();
    assert!(pg
        .list_shard_scavenger_payload_references(&topology())
        .unwrap()
        .is_empty());
}

fn scan(pg: &PgStore, command: &MetadataCommandEnvelope, placed: bool) {
    let writes = pg.conn.total_changes();
    let indexed = pages(pg);
    let slot = pending_install::observe(pg);
    let prefix = pg.metadata_command_replica_state().unwrap();
    assert_eq!(
        pg.list_shard_scavenger_payload_references(&topology())
            .unwrap(),
        expected(command, placed)
    );
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(pages(pg), indexed);
    assert_eq!(pending_install::observe(pg), slot);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
}

#[test]
fn pending_payload_discovery_retains_identity_after_session_loss_and_reopen() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for append in [false, true] {
            let (dir, pg, owner, command) = setup(target.clone(), append);
            isolate_pending(&pg, &owner);
            install(&pg, &command, None).unwrap();
            scan(&pg, &command, true);
            // Same session ID, crossed subject: do not borrow its geometry.
            // All rows stay in this PG, so placement cannot mask the binding.
            for fault in ["missing", "bucket", "key", "session"] {
                pg.conn.execute_batch("SAVEPOINT missing_geometry").unwrap();
                let sql = match fault {
                    "missing" => "DELETE FROM stream_uploads WHERE session_id = ?1",
                    "bucket" => "UPDATE stream_uploads SET bucket = 'other-bucket' WHERE session_id = ?1",
                    "key" => "UPDATE stream_uploads SET key = 'other-key' WHERE session_id = ?1",
                    "session" => "UPDATE stream_uploads SET session_id = 'other-session' WHERE session_id = ?1",
                    _ => unreachable!(),
                };
                assert_eq!(
                    pg.conn.execute(sql, [&owner.subject.id.attempt]).unwrap(),
                    1
                );
                scan(&pg, &command, false);
                pg.conn
                    .execute_batch("ROLLBACK TO missing_geometry; RELEASE missing_geometry")
                    .unwrap();
                scan(&pg, &command, true);
            }
            pg.conn
                .execute(
                    "DELETE FROM stream_uploads WHERE session_id = ?1",
                    [&owner.subject.id.attempt],
                )
                .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
            scan(&pg, &command, false);
            drop(pg);
            let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
            scan(&pg, &command, false);
            // Negative canary: only the pending command protects these shards.
            assert!(pg.test_clear_pending_metadata_command_slot().unwrap());
            assert!(pg
                .list_shard_scavenger_payload_references(&topology())
                .unwrap()
                .is_empty());
        }
    }
}

#[test]
fn pending_payload_discovery_malformed_live_geometry_fails_closed_without_mutation() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for append in [false, true] {
            let (_dir, pg, owner, command) = setup(target.clone(), append);
            isolate_pending(&pg, &owner);
            install(&pg, &command, None).unwrap();
            pg.conn.execute("UPDATE stream_uploads SET encryption_state = zeroblob(length(encryption_state)) WHERE session_id = ?1", [&owner.subject.id.attempt]).unwrap();
            let writes = pg.conn.total_changes();
            let indexed = pages(&pg);
            let slot = pending_install::observe(&pg);
            let error = pg
                .list_shard_scavenger_payload_references(&topology())
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    StoreError::Db {
                        context: "load exact stream encryption for pending payload inventory",
                        ..
                    }
                ),
                "{error:?}"
            );
            assert!(pg.conn.is_autocommit());
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(pages(&pg), indexed);
            assert_eq!(pending_install::observe(&pg), slot);
        }
    }
}

#[test]
fn pending_payload_discovery_rejects_malformed_terminal_headers() {
    for abort in [false, true] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owners = completion::setup(&pg, 2);
        let command = if abort {
            abort::command(&pg, &owners[0])
        } else {
            completion::command(&pg, &owners[0], crate::VersionId::Null)
        };
        pg.conn.execute_batch("COMMIT").unwrap();
        for fault in ["missing", "bucket", "key", "duplicate"] {
            let mut payload = command.payload().clone();
            let headers = match &mut payload {
                MetadataCommandPayload::CommitMultipartObject(command) => {
                    &mut command.stream_uploads
                }
                MetadataCommandPayload::AbortMultipartUpload(command) => {
                    &mut command.cleanup.stream_uploads
                }
                _ => unreachable!(),
            };
            assert!(!headers.is_empty());
            match fault {
                "missing" => headers.clear(),
                "bucket" => headers[0].bucket = BucketName::new("other-bucket").unwrap(),
                "key" => headers[0].key = ObjectKey::new("other-key").unwrap(),
                "duplicate" => headers.push(headers[0].clone()),
                _ => unreachable!(),
            }
            let malformed = MetadataCommandEnvelope::new(command.id(), payload);
            // Durable malformed input bypasses the fresh installer deliberately.
            pg.test_insert_raw_pending_metadata_command_slot(
                malformed.id(),
                malformed.checksum_crc64(),
                &malformed.command_bytes(),
                Some(malformed.bucket_name()),
            )
            .unwrap();
            let writes = pg.conn.total_changes();
            let slot = pending_install::observe(&pg);
            let error = pg
                .list_shard_scavenger_payload_references(&topology())
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    StoreError::ShardScavengerScanIncomplete {
                        context: "derive pending command payload inventory",
                        ..
                    }
                ),
                "{error:?}"
            );
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(pending_install::observe(&pg), slot);
            assert!(pg.conn.is_autocommit());
            assert!(pg.test_clear_pending_metadata_command_slot().unwrap());
        }
    }
}
