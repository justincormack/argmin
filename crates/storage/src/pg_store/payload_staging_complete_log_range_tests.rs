// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete log observations, not production recovery authorization. Fixtures
//! use the real atomic apply/record path and deliberately contain no payload IO.

use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    metadata_command_log_hash, MetadataCommandEnvelope, MetadataCommandPayload,
    ReserveObjectVersionCommand,
};
use crate::node_runtime::pg_store::{
    command_log::{CompleteCommandLogEntryKind, CompleteLogReadError},
    PgStore,
};
use crate::{StoreError, VersionId};

pub(super) fn index(value: u64) -> MetadataCommandLogIndex {
    MetadataCommandLogIndex::new(value).unwrap()
}

pub(super) fn ordinary(id: MetadataCommandId) -> MetadataCommandEnvelope {
    let owner = fixture(StagingTarget::DirectPut);
    MetadataCommandEnvelope::new(
        id,
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            owner.subject.bucket,
            owner.subject.key,
            VersionId::from_u64(17),
        )),
    )
}

pub(super) fn apply(pg: &PgStore, context: &ContextualCommand) {
    pg.apply_complete_metadata_command_and_record(
        0,
        &context.encode_command().unwrap(),
        &topology(),
    )
    .unwrap();
}

#[test]
fn staging_complete_log_range_preserves_all_contexts_and_replays_after_reopen() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..18 {
        let (dir, pg, context) = complete_application::setup(case);
        let prefix = pg.metadata_command_replica_state().unwrap();
        apply(&pg, &context);
        let state = pg.metadata_command_replica_state().unwrap();
        let expected = introduction::state(&pg);
        let pg_id = pg.pg_id;
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let writes = pg.conn.total_changes();
        let mut entries = Vec::new();
        let limit = crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES;
        for first in (1..=context.id().log_index().get()).step_by(limit as usize) {
            let last = (first + limit - 1).min(context.id().log_index().get());
            entries.extend(
                pg.retained_complete_metadata_command_log_entries(
                    0,
                    context.id().cluster_epoch(),
                    index(first),
                    index(last),
                    &topology(),
                )
                .unwrap_or_else(|error| panic!("case {case}, range {first}..={last}: {error}")),
            );
        }
        assert_eq!(entries.len() as u64, context.id().log_index().get());
        let mut hashes = Vec::new();
        let hash_limit =
            crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES;
        for first in (1..=context.id().log_index().get()).step_by(hash_limit as usize) {
            let last = (first + hash_limit - 1).min(context.id().log_index().get());
            hashes.extend(
                pg.retained_complete_metadata_command_log_hashes(
                    0,
                    context.id().cluster_epoch(),
                    index(first),
                    index(last),
                    &topology(),
                )
                .unwrap_or_else(|error| {
                    panic!("case {case}, hash range {first}..={last}: {error}")
                }),
            );
        }
        assert_eq!(hashes.len(), entries.len(), "case {case}");
        for (hash, entry) in hashes.iter().zip(&entries) {
            assert_eq!(hash.log_index, entry.id().log_index().get());
            assert_eq!(hash.previous_log_hash, entry.previous_log_hash());
            assert_eq!(hash.log_hash, entry.log_hash());
        }
        for pair in entries.windows(2) {
            assert_eq!(
                pair[0].id().log_index().get() + 1,
                pair[1].id().log_index().get()
            );
            assert_eq!(pair[0].log_hash(), pair[1].previous_log_hash());
        }
        let last = entries.pop().unwrap();
        assert_eq!(last.id(), context.id());
        assert_eq!(last.previous_log_hash(), prefix.applied_log_hash);
        assert_eq!(last.log_hash(), state.applied_log_hash);
        assert_eq!(last.pre_state_digest(), Some(prefix.state_digest));
        assert_eq!(last.post_state_digest(), Some(state.state_digest));
        let CompleteCommandLogEntryKind::Applied(retained) = last.into_kind() else {
            panic!("case {case}")
        };
        assert_eq!(
            retained.encode_command().unwrap().bytes(),
            context.encode_command().unwrap().bytes()
        );
        assert_eq!(
            retained.encode_command().unwrap().command_sha256(),
            context.encode_command().unwrap().command_sha256()
        );
        apply(&pg, &retained);
        assert_eq!(pg.conn.total_changes(), writes, "case {case}");
        assert_eq!(introduction::state(&pg), expected);
        assert!(pg.conn.is_autocommit());
        assert!(!pg
            .conn
            .pragma_query_value(None, "query_only", |row| row.get::<_, bool>(0))
            .unwrap());
        assert!(
            pg.retained_metadata_command_log_entries(
                0,
                context.id().cluster_epoch(),
                context.id().log_index(),
                context.id().log_index()
            )
            .is_err(),
            "live V13 reader accepted V14"
        );
        assert!(
            pg.retained_metadata_command_log_hashes(
                0,
                context.id().cluster_epoch(),
                context.id().log_index(),
                context.id().log_index(),
            )
            .is_err(),
            "live V13 hash reader accepted V14"
        );
    }
}

#[test]
fn staging_complete_log_range_retains_complete_tombstone_identity_and_compacted_gaps() {
    let _clock = test_time_override_guard(2_000);
    for case in [0, 5, 6] {
        let (_dir, pg, context) = complete_application::setup(case);
        let encoded = context.encode_command().unwrap();
        pg.test_record_complete_command_abandoned(0, &encoded)
            .unwrap();
        let writes = pg.conn.total_changes();
        let entries = pg
            .retained_complete_metadata_command_log_entries(
                0,
                context.id().cluster_epoch(),
                context.id().log_index(),
                context.id().log_index(),
                &topology(),
            )
            .unwrap();
        assert_eq!(entries.len(), 1);
        let hashes = pg
            .retained_complete_metadata_command_log_hashes(
                0,
                context.id().cluster_epoch(),
                context.id().log_index(),
                context.id().log_index(),
                &topology(),
            )
            .unwrap();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].log_index, entries[0].id().log_index().get());
        assert_eq!(hashes[0].previous_log_hash, entries[0].previous_log_hash());
        assert_eq!(hashes[0].log_hash, entries[0].log_hash());
        let CompleteCommandLogEntryKind::Abandoned {
            original_command_checksum,
            original_command_sha256,
        } = entries.into_iter().next().unwrap().into_kind()
        else {
            panic!("expected tombstone")
        };
        assert_eq!(original_command_checksum, encoded.checksum_crc64());
        assert_eq!(original_command_sha256, encoded.command_sha256());
        assert_eq!(pg.conn.total_changes(), writes);
        pg.test_checkpoint_and_compact_complete_command(0, &encoded, 2)
            .unwrap();
        let writes = pg.conn.total_changes();
        assert!(
            pg.retained_complete_metadata_command_log_entries(
                0,
                context.id().cluster_epoch(),
                index(1),
                context.id().log_index(),
                &topology(),
            )
            .unwrap()
            .is_empty(),
            "receipts must not invent retained commands"
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert!(
            pg.retained_complete_metadata_command_log_hashes(
                0,
                context.id().cluster_epoch(),
                index(1),
                context.id().log_index(),
                &topology(),
            )
            .unwrap()
            .is_empty(),
            "receipts must not invent retained hashes"
        );
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

fn require_rejection_without_writes(pg: &PgStore, id: MetadataCommandId) -> CompleteLogReadError {
    let writes = pg.conn.total_changes();
    let before = introduction::state(pg);
    let prefix = pg.metadata_command_replica_state().unwrap();
    let error = pg
        .retained_complete_metadata_command_log_entries(
            0,
            id.cluster_epoch(),
            index(1),
            id.log_index(),
            &topology(),
        )
        .unwrap_err();
    let hash_error = pg
        .retained_complete_metadata_command_log_hashes(
            0,
            id.cluster_epoch(),
            index(1),
            id.log_index(),
            &topology(),
        )
        .unwrap_err();
    assert_eq!(hash_error.to_string(), error.to_string());
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(pg), before);
    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    assert!(pg.conn.is_autocommit());
    assert!(!pg
        .conn
        .pragma_query_value(None, "query_only", |row| row.get::<_, bool>(0))
        .unwrap());
    error
}

fn require_conflict(error: &CompleteLogReadError, id: MetadataCommandId) {
    assert!(
        matches!(error, CompleteLogReadError::Store(StoreError::MetadataCommandLogConflict {
        node_id: 0, pg_id, cluster_epoch, log_index,
    }) if *pg_id == id.pg_id().get() && *cluster_epoch == id.cluster_epoch() && *log_index == id.log_index().get()),
        "{error:?}"
    );
}

fn replace_bytes(pg: &PgStore, id: MetadataCommandId, bytes: &[u8]) {
    let previous: i64 = pg
        .conn
        .query_row(
            "SELECT previous_log_hash FROM metadata_command_log WHERE log_index = ?1",
            [id.log_index().get() as i64],
            |row| row.get(0),
        )
        .unwrap();
    let checksum = checksum::crc64::checksum(bytes);
    let hash = metadata_command_log_hash(
        id.cluster_epoch(),
        id.pg_id(),
        id.log_index(),
        previous as u64,
        checksum,
    );
    pg.conn.execute("UPDATE metadata_command_log SET command_bytes = ?1, command_checksum = ?2, log_hash = ?3 WHERE log_index = ?4",
        rusqlite::params![bytes, checksum as i64, hash.value() as i64, id.log_index().get() as i64]).unwrap();
}

#[test]
fn staging_complete_log_range_rejects_resealed_crossed_identity_format_and_context() {
    let _clock = test_time_override_guard(2_000);
    for change in ["pg", "epoch", "index", "v13", "context"] {
        let (_dir, pg, context) = complete_application::setup(5);
        apply(&pg, &context);
        let id = context.id();
        let bytes = match change {
            "v13" => ordinary(id).command_bytes(),
            "context" => {
                let mut bytes = context.encode_command().unwrap().bytes().to_vec();
                *bytes.last_mut().unwrap() = 42;
                bytes
            }
            _ => {
                let changed = MetadataCommandId::new(
                    if change == "epoch" {
                        ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap()
                    } else {
                        id.cluster_epoch()
                    },
                    if change == "pg" {
                        PgId::new(id.pg_id().get() ^ 1)
                    } else {
                        id.pg_id()
                    },
                    if change == "index" {
                        index(id.log_index().get() + 1)
                    } else {
                        id.log_index()
                    },
                );
                let crossed =
                    ContextualCommand::decode_field(ordinary(changed), &[1, 0, 0], &topology())
                        .unwrap();
                crossed.encode_command().unwrap().bytes().to_vec()
            }
        };
        replace_bytes(&pg, id, &bytes);
        let error = require_rejection_without_writes(&pg, id);
        require_conflict(&error, id);
    }
}

#[test]
fn staging_complete_log_range_rejects_damaged_rows_without_returning_a_prefix() {
    let _clock = test_time_override_guard(2_000);
    for damage in [
        "command_checksum = command_checksum + 1",
        "previous_log_hash = NULL",
        "log_hash = NULL",
        "log_hash = log_hash + 1",
        "abandoned = 1",
        "pre_state_digest = NULL",
        "post_state_digest = NULL",
    ] {
        let (_dir, pg, context) = complete_application::setup(5);
        apply(&pg, &context);
        assert!(
            context.id().log_index().get() > 1,
            "valid prefix precedes corrupt row"
        );
        pg.conn
            .execute(
                &format!("UPDATE metadata_command_log SET {damage} WHERE log_index = ?1"),
                [context.id().log_index().get() as i64],
            )
            .unwrap();
        let error = require_rejection_without_writes(&pg, context.id());
        if damage.starts_with("command_checksum") {
            assert!(
                matches!(
                    error,
                    CompleteLogReadError::Store(
                        StoreError::MetadataCommandLogChecksumMismatch { .. }
                    )
                ),
                "{damage}: {error}"
            );
        } else {
            require_conflict(&error, context.id());
        }
    }
    let (_dir, pg, context) = complete_application::setup(5);
    pg.test_record_complete_command_abandoned(0, &context.encode_command().unwrap())
        .unwrap();
    pg.conn
        .execute(
            "UPDATE metadata_command_log SET abandoned = 0 WHERE log_index = ?1",
            [context.id().log_index().get() as i64],
        )
        .unwrap();
    require_conflict(
        &require_rejection_without_writes(&pg, context.id()),
        context.id(),
    );
}

#[test]
fn staging_complete_log_range_checks_links_even_when_each_row_hash_is_valid() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = complete_application::setup(5);
    apply(&pg, &context);
    let id = context.id();
    let previous: i64 = pg
        .conn
        .query_row(
            "SELECT previous_log_hash FROM metadata_command_log WHERE log_index = ?1",
            [id.log_index().get() as i64],
            |row| row.get(0),
        )
        .unwrap();
    let previous = previous as u64 ^ 1;
    let hash = metadata_command_log_hash(
        id.cluster_epoch(),
        id.pg_id(),
        id.log_index(),
        previous,
        context.encode_command().unwrap().checksum_crc64(),
    );
    pg.conn.execute("UPDATE metadata_command_log SET previous_log_hash = ?1, log_hash = ?2 WHERE log_index = ?3", rusqlite::params![previous as i64, hash.value() as i64, id.log_index().get() as i64]).unwrap();
    // An isolated row is self-consistent; the range must still reject its link.
    assert_eq!(
        pg.retained_complete_metadata_command_log_entries(
            0,
            id.cluster_epoch(),
            id.log_index(),
            id.log_index(),
            &topology()
        )
        .unwrap()
        .len(),
        1
    );
    require_conflict(&require_rejection_without_writes(&pg, id), id);
}

#[test]
fn staging_complete_log_range_rejects_bounds_topology_and_caller_transaction() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = complete_application::setup(0);
    apply(&pg, &context);
    let id = context.id();
    let writes = pg.conn.total_changes();
    let before = introduction::state(&pg);
    let limit = crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES;
    assert!(pg
        .retained_complete_metadata_command_log_entries(
            0,
            id.cluster_epoch(),
            index(1),
            index(limit),
            &topology()
        )
        .is_ok());
    for (first, last) in [(2, 1), (1, limit + 1), (u64::MAX, u64::MAX)] {
        assert!(pg
            .retained_complete_metadata_command_log_entries(
                0,
                id.cluster_epoch(),
                index(first),
                index(last),
                &topology()
            )
            .is_err());
    }
    for foreign in [
        PgTopology::new(&[99]).unwrap(),
        PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap(),
    ] {
        assert!(pg
            .retained_complete_metadata_command_log_entries(
                0,
                id.cluster_epoch(),
                index(1),
                id.log_index(),
                &foreign
            )
            .is_err());
    }
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let error = pg
        .retained_complete_metadata_command_log_entries(
            0,
            id.cluster_epoch(),
            index(1),
            id.log_index(),
            &topology(),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("requires its own read snapshot"),
        "{error}"
    );
    assert!(!pg.conn.is_autocommit());
    pg.conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
}
