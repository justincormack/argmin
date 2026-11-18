// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Constructor-only node startup under the exclusive root lock. These fixtures
//! carry real complete metadata commands, but perform no physical payload IO.

use super::complete_log_range::{apply, index, ordinary};
use super::complete_log_rpc::{installed_at_epoch, prepared, seed_context, Running};
use super::complete_maintenance::log_state;
use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, MetadataCommandReplicaState,
    ReserveObjectVersionCommand,
};
use crate::node_runtime::pg_store::{command_log::MetadataCheckpointTableBlock, PgStore};
use crate::storage_node_server::{StorageNodeProcessConfig, StorageNodeServerError};
use crate::{PgState, StoreError};
use std::os::unix::net::UnixStream;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Snapshot {
    materialized: Vec<MetadataCheckpointTableBlock>,
    pending: Vec<Vec<String>>,
    log: Vec<Vec<String>>,
    prefix: MetadataCommandReplicaState,
}

pub(super) fn snapshot(pg: &PgStore) -> Snapshot {
    Snapshot {
        materialized: introduction::state(pg),
        pending: complete_pending::observe(pg),
        log: log_state(pg),
        prefix: pg.metadata_command_replica_state().unwrap(),
    }
}

pub(super) fn reopen_raw(config: &StorageNodeProcessConfig, pg_id: PgId) -> PgStore {
    PgStore::open(
        &config.data_dir.join(format!("pg-{:04}", pg_id.get())),
        pg_id.get(),
    )
    .unwrap()
}

pub(super) fn next(pg: &PgStore, epoch: ClusterEpoch) -> ContextualCommand {
    let state = pg.metadata_command_replica_state().unwrap();
    let position = if epoch == state.cluster_epoch {
        state.applied_log_index + 1
    } else {
        1
    };
    let owner = fixture(StagingTarget::DirectPut);
    ContextualCommand::decode_field(
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(epoch, PgId::new(pg.pg_id), index(position)),
            // The ordinary prefix fixture reserves version 17. A distinct
            // successor must be applicable too, not only abandonable.
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                owner.subject.bucket,
                owner.subject.key,
                crate::VersionId::from_u64(18),
            )),
        ),
        &[1, 0, 0],
        &topology(),
    )
    .unwrap()
}

pub(super) fn install(pg: &PgStore, context: &ContextualCommand) {
    let encoded = context.encode_command().unwrap();
    let copy =
        ContextualCommand::decode_command(encoded.bytes(), encoded.checksum_crc64(), &topology())
            .unwrap();
    complete_pending::install(&mut copy.into_submission(), pg).unwrap();
    pg.conn
        .execute(
            "UPDATE metadata_command_pending_slot SET publication_started = 1",
            [],
        )
        .unwrap();
}

#[test]
fn staging_complete_node_startup_reopens_all_context_kinds_and_compacted_prefixes_before_rpc() {
    for case in 0..3 {
        for compacted in [false, true] {
            let dir = test_util::tempdir();
            let epoch = ClusterEpoch::new(if case == 2 { 6 } else { 5 }).unwrap();
            let (server, _, config) = installed_at_epoch(&dir, PgState::Active, epoch, true);
            let node = server.test_storage_node();
            let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
            let (before, checkpoint) = {
                let pg = node.get_pg(pg_id.get()).unwrap();
                seed_context(&pg, case);
                let checkpoint = pg
                    .complete_metadata_command_checkpoint(4, &topology())
                    .unwrap();
                if compacted {
                    pg.record_complete_metadata_command_checkpoint(4, &topology())
                        .unwrap();
                    pg.compact_complete_metadata_command_log(&topology())
                        .unwrap();
                    assert!(log_state(&pg)[0].is_empty());
                }
                (snapshot(&pg), checkpoint)
            };
            drop(node);
            drop(server);
            let (prepared, client) = prepared(&config);
            let server = prepared.bind_complete_for_test().unwrap();
            let node = server.test_storage_node();
            assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
            // Every other PG was opened/recovered too, at its own stored epoch.
            for &other in &config.pg_ids {
                if other != pg_id.get() {
                    assert_eq!(
                        node.get_pg(other)
                            .unwrap()
                            .metadata_command_replica_state()
                            .unwrap()
                            .cluster_epoch,
                        ClusterEpoch::INITIAL
                    );
                }
            }
            let running = Running::start(server, config.socket_path.clone());
            assert_eq!(
                client.metadata_command_checkpoint(pg_id, epoch).unwrap(),
                checkpoint
            );
            drop(running);
            drop(node);
            assert_eq!(snapshot(&reopen_raw(&config, pg_id)), before);
        }
    }
}

#[test]
fn staging_complete_node_startup_preserves_pending_custody_at_current_and_historical_epochs() {
    for terminal in ["unresolved", "applied", "abandoned"] {
        for historical in [false, true] {
            let dir = test_util::tempdir();
            let epoch = ClusterEpoch::new(5).unwrap();
            let (server, _, mut config) = installed_at_epoch(&dir, PgState::Active, epoch, true);
            let node = server.test_storage_node();
            let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
            let before = {
                let pg = node.get_pg(pg_id.get()).unwrap();
                seed_context(&pg, 1);
                let context = next(&pg, epoch);
                install(&pg, &context);
                match terminal {
                    "applied" => apply(&pg, &context),
                    "abandoned" => {
                        pg.test_record_complete_command_abandoned(
                            4,
                            &context.encode_command().unwrap(),
                        )
                        .unwrap();
                    }
                    "unresolved" => {}
                    _ => unreachable!(),
                }
                if historical {
                    let successor = next(&pg, ClusterEpoch::new(6).unwrap());
                    pg.test_record_complete_command_abandoned(
                        4,
                        &successor.encode_command().unwrap(),
                    )
                    .unwrap();
                }
                snapshot(&pg)
            };
            drop(node);
            drop(server);
            // Startup must not substitute this config epoch for either the
            // stored prefix epoch or the retained pending command's epoch.
            config.cluster_epoch = ClusterEpoch::new(9).unwrap();
            for route in &mut config.pg_routes {
                route.cluster_epoch = config.cluster_epoch;
                route.state = PgState::Peering;
            }
            let server = prepared(&config).0.bind_complete_for_test().unwrap();
            assert_eq!(
                snapshot(&server.test_storage_node().get_pg(pg_id.get()).unwrap()),
                before,
                "{terminal}, historical={historical}"
            );
        }
    }
}

#[test]
fn staging_complete_node_startup_repairs_interrupted_tail_and_cache_without_discarding_custody() {
    let dir = test_util::tempdir();
    let epoch = ClusterEpoch::new(5).unwrap();
    let (server, _, config) = installed_at_epoch(&dir, PgState::Active, epoch, true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let expected = {
        let pg = node.get_pg(pg_id.get()).unwrap();
        seed_context(&pg, 1);
        let prior = pg.metadata_command_replica_state().unwrap();
        let context = next(&pg, epoch);
        install(&pg, &context);
        pg.test_record_complete_command_abandoned(4, &context.encode_command().unwrap())
            .unwrap();
        let expected = snapshot(&pg);
        pg.conn.execute("UPDATE metadata_command_replica_state SET applied_log_index = ?1, applied_log_hash = ?2", rusqlite::params![prior.applied_log_index as i64, prior.applied_log_hash.value() as i64]).unwrap();
        pg.conn.execute("UPDATE metadata_command_log SET previous_log_hash = NULL, log_hash = NULL WHERE cluster_epoch = ?1 AND log_index = ?2", rusqlite::params![epoch.get() as i64, context.id().log_index().get() as i64]).unwrap();
        pg.conn.execute("UPDATE metadata_table_digests SET table_digest = table_digest + 1, row_hash_sum = row_hash_sum + 1 WHERE table_name = 'buckets'", []).unwrap();
        assert!(!pg
            .test_metadata_digest_table_mismatches()
            .unwrap()
            .is_empty());
        expected
    };
    drop(node);
    drop(server);
    let server = prepared(&config).0.bind_complete_for_test().unwrap();
    {
        let node = server.test_storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert_eq!(snapshot(&pg), expected);
        assert!(pg
            .test_metadata_digest_table_mismatches()
            .unwrap()
            .is_empty());
    }
    drop(server);
    let server = prepared(&config).0.bind_complete_for_test().unwrap();
    assert_eq!(
        snapshot(&server.test_storage_node().get_pg(pg_id.get()).unwrap()),
        expected
    );
}

#[test]
fn staging_complete_node_startup_one_bad_pg_prevents_binding_and_preserves_failed_pg() {
    for damage in [
        "scope",
        "route_index",
        "staging_index",
        "log",
        "digest",
        "future_pending",
    ] {
        let dir = test_util::tempdir();
        let epoch = ClusterEpoch::new(5).unwrap();
        let (server, _, mut config) = installed_at_epoch(&dir, PgState::Active, epoch, true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let before = {
            let pg = node.get_pg(pg_id.get()).unwrap();
            seed_context(&pg, 1);
            let context = next(
                &pg,
                if damage == "future_pending" {
                    ClusterEpoch::new(6).unwrap()
                } else {
                    epoch
                },
            );
            install(&pg, &context);
            let sql = match damage {
                "scope" => "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
                "route_index" => "DELETE FROM metadata_command_pending_route_dependencies",
                "staging_index" => "DELETE FROM metadata_command_pending_staging_protection",
                "log" => "UPDATE metadata_command_log SET log_hash = log_hash + 1 WHERE log_index = 1",
                "digest" => "UPDATE object_write_counters SET max_committed_generation = max_committed_generation + 1",
                "future_pending" => "SELECT 1",
                _ => unreachable!(),
            };
            pg.conn.execute_batch(sql).unwrap();
            snapshot(&pg)
        };
        drop(node);
        drop(server);
        config.socket_path = config.socket_path.with_file_name("must-not-bind.sock");
        assert!(!config.socket_path.exists());
        let error = match prepared(&config).0.bind_complete_for_test() {
            Ok(_) => panic!("{damage} was served"),
            Err(error) => error,
        };
        let StorageNodeServerError::Store(failure) = &error else {
            panic!("{damage}: {error:?}");
        };
        assert!(
            matches!(
                failure.retained_store_error(),
                Some(
                    StoreError::MetadataCommandLogConflict { .. }
                        | StoreError::MetadataCommandLogHashMismatch { .. }
                        | StoreError::MetadataStateDigestMismatch { .. }
                        | StoreError::RouteCapabilitySubjectMismatch { .. }
                )
            ),
            "{damage}: {error:?}"
        );
        assert!(
            !config.socket_path.exists(),
            "{damage} reached listener binding"
        );
        assert!(UnixStream::connect(&config.socket_path).is_err());
        let pg = reopen_raw(&config, pg_id);
        assert_eq!(snapshot(&pg), before, "{damage}");
        drop(pg);
        // Failure also relinquishes the exclusive root lock, not a half-node.
        crate::node_runtime::server::StorageNodeDataDirGuard::acquire(&config.data_dir).unwrap();
    }
}

#[test]
fn staging_complete_node_startup_requires_exclusive_root_and_explicit_grammar() {
    let dir = test_util::tempdir();
    let epoch = ClusterEpoch::new(5).unwrap();
    let (server, _, config) = installed_at_epoch(&dir, PgState::Active, epoch, true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let before = {
        let pg = node.get_pg(pg_id.get()).unwrap();
        seed_context(&pg, 1);
        snapshot(&pg)
    };
    assert!(
        matches!(
            prepared(&config).0.bind_complete_for_test(),
            Err(StorageNodeServerError::DataDirAlreadyLocked { .. })
        ),
        "duplicate runtime was not rejected at exclusive-root admission"
    );
    assert_eq!(snapshot(&node.get_pg(pg_id.get()).unwrap()), before);
    drop(node);
    drop(server);
    assert!(
        prepared(&config).0.bind().is_err(),
        "current startup silently accepted the complete grammar"
    );
    assert_eq!(snapshot(&reopen_raw(&config, pg_id)), before);
    let server = prepared(&config).0.bind_complete_for_test().unwrap();
    assert_eq!(
        snapshot(&server.test_storage_node().get_pg(pg_id.get()).unwrap()),
        before
    );
}

#[test]
fn staging_complete_node_startup_rejects_current_applied_commands_without_format_fallback() {
    let dir = test_util::tempdir();
    let epoch = ClusterEpoch::new(5).unwrap();
    let (server, _, config) = installed_at_epoch(&dir, PgState::Active, epoch, false);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let before = {
        let pg = node.get_pg(pg_id.get()).unwrap();
        let command = ordinary(MetadataCommandId::new(epoch, pg_id, index(1)));
        pg.apply_metadata_command_and_record(4, &command).unwrap();
        snapshot(&pg)
    };
    drop(node);
    drop(server);
    assert!(prepared(&config).0.bind_complete_for_test().is_err());
    assert_eq!(snapshot(&reopen_raw(&config, pg_id)), before);
    let server = prepared(&config).0.bind().unwrap();
    assert_eq!(
        snapshot(&server.test_storage_node().get_pg(pg_id.get()).unwrap()),
        before
    );
}
