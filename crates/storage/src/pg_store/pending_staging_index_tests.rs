// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Index components only: real command/descriptor binding and slot transactions,
//! no shard I/O or frontend custody activation.

use super::super::descriptor::{AppendDescriptor, IntroductionDescriptor};
use super::*;
use crate::metadata_command::MetadataCommandEnvelope;
use crate::node_runtime::pg_store::{pending_staging_index::PendingStagingIndex, PgStore};
use rusqlite::params;

fn raw(pg: &PgStore) -> (Vec<u8>, u64) {
    pg.conn.query_row("SELECT encoded_protection, protection_checksum FROM metadata_command_pending_staging_protection", [], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64))).unwrap()
}

pub(super) fn install(pg: &PgStore, owner: &StagingOwnerRecord, command: &MetadataCommandEnvelope) {
    let topology = topology();
    let descriptor = IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
    let bound = descriptor.bind(&topology, command).unwrap();
    let protection = bound.pending_protection();
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    pg.test_replace_pending_metadata_command_slot(command, None)
        .unwrap();
    PendingStagingIndex::from_bound(&protection)
        .install_in_open_transaction(pg, &topology)
        .unwrap();
    pg.conn.execute_batch("COMMIT").unwrap();
}

fn assert_window(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: &MetadataCommandEnvelope,
    window: &StagingWindow,
) {
    let read = pg.pending_staging_protection(&topology()).unwrap().unwrap();
    assert_eq!(read.command, command.id());
    assert_eq!(read.command_checksum, command.checksum_crc64());
    let read = read.window.unwrap();
    assert_eq!(read.attempt, owner.subject.id.attempt);
    assert_eq!(read.introduction, owner.subject.id.introduction);
    assert_eq!(read.window_index, window.index);
    assert_eq!(
        read.protection.identity.data_pg_id,
        window.participants.data_pg.get()
    );
    assert_eq!(read.protection.identity.okh, window.segment_hash);
    assert_eq!(read.protection.identity.generation_id, window.start);
    assert_eq!(read.protection.identity.ec, window.geometry.ec);
    assert_eq!(
        read.protection.placement_epoch,
        window.participants.placement_epoch
    );
    assert_eq!(read.protection.file_nodes, window.participants.files);
    assert_eq!(
        read.protection.acknowledgement_primary,
        window.participants.acknowledgement_primary
    );
}

#[test]
fn staging_pending_index_introductions_reopen_and_reissue_preserve_exact_protection() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let owner = fixture(target);
        let command = introduction::command(&owner);
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        assert!(pg
            .pending_staging_protection(&topology())
            .unwrap()
            .is_none());
        install(&pg, &owner, &command);
        let original = raw(&pg);
        drop(pg);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        crate::clock::with_time_and_monotonic_override(9_000_000, 9_000_000, || {
            assert_window(&pg, &owner, &command, &owner.windows[0]);
        });
        assert_eq!(raw(&pg), original);
        // Even an opaque/corrupt envelope must not force a full decode just to
        // inspect the independently sealed pending protection index.
        pg.conn
            .execute(
                "UPDATE metadata_command_pending_slot SET command_bytes = X'00'",
                [],
            )
            .unwrap();
        assert_window(&pg, &owner, &command, &owner.windows[0]);
        let reissued = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(6).unwrap(),
                command.id().pg_id(),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            command.payload().clone(),
        );
        install(&pg, &owner, &reissued);
        assert_window(&pg, &owner, &reissued, &owner.windows[0]);
        assert_eq!(raw(&pg).0, original.0);
        assert_ne!(
            raw(&pg).1,
            original.1,
            "checksum binds the reissued slot identity"
        );
        assert!(pg.test_clear_pending_metadata_command_slot().unwrap());
        assert!(pg
            .pending_staging_protection(&topology())
            .unwrap()
            .is_none());
        assert_eq!(
            pg.conn
                .query_row(
                    "SELECT count(*) FROM metadata_command_pending_staging_protection",
                    [],
                    |row| row.get::<_, u32>(0)
                )
                .unwrap(),
            0
        );
    }
}

#[test]
fn staging_pending_index_append_targets_preserve_unwritten_lookahead() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = append::initialize(&pg, target);
        let window = append::next(&pg, &owner);
        let command = append::command(&owner);
        let topology = topology();
        let descriptor =
            AppendDescriptor::capture(&owner.validate(&topology).unwrap(), window.clone()).unwrap();
        let bound = descriptor.bind(&topology, &command).unwrap();
        let protection = bound.pending_protection();
        pg.test_replace_pending_metadata_command_slot(&command, None)
            .unwrap();
        PendingStagingIndex::from_bound(&protection)
            .install_in_open_transaction(&pg, &topology)
            .unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        assert_window(&pg, &owner, &command, &window);
        pg.conn.execute_batch("BEGIN").unwrap();
        assert!(
            sql::load(&pg.conn, owner.subject.object_pg, &owner.subject.id.attempt)
                .unwrap()
                .unwrap()
                .windows
                .iter()
                .all(|existing| existing.index < window.index)
        );
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_pending_index_install_rejects_crossed_command_topology_pg_and_transaction() {
    let owner = fixture(StagingTarget::DirectPut);
    let command = introduction::command(&owner);
    let topology = topology();
    let descriptor = IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
    let bound = descriptor.bind(&topology, &command).unwrap();
    let protection = bound.pending_protection();
    for fault in [
        "transaction",
        "topology",
        "PG",
        "command",
        "bytes",
        "checksum",
        "epoch",
        "index",
    ] {
        let dir = test_util::tempdir();
        let pg_id = if fault == "PG" {
            owner.subject.object_pg.get() + 1
        } else {
            owner.subject.object_pg.get()
        };
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        if fault != "PG" {
            pg.test_replace_pending_metadata_command_slot(&command, None)
                .unwrap();
        }
        match fault {
            "command" => {
                pg.test_clear_pending_metadata_command_slot().unwrap();
            }
            "bytes" => {
                pg.conn
                    .execute(
                        "UPDATE metadata_command_pending_slot SET command_bytes = X'00'",
                        [],
                    )
                    .unwrap();
            }
            "checksum" => {
                pg.conn.execute("UPDATE metadata_command_pending_slot SET command_checksum = command_checksum + 1", []).unwrap();
            }
            "epoch" => {
                pg.conn.execute("UPDATE metadata_command_pending_slot SET cluster_epoch = cluster_epoch + 1", []).unwrap();
            }
            "index" => {
                pg.conn
                    .execute(
                        "UPDATE metadata_command_pending_slot SET log_index = log_index + 1",
                        [],
                    )
                    .unwrap();
            }
            _ => {}
        }
        if fault != "transaction" {
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        }
        // Keep the captured object/data PGs present, but add an unrelated PG:
        // this must reject full topology identity, not only membership.
        let mut ids = vec![];
        topology
            .for_each_pg::<std::convert::Infallible>(|id| {
                ids.push(id);
                Ok(())
            })
            .unwrap();
        ids.push(1000);
        let foreign = PgTopology::new(&ids).unwrap();
        let before = pg.conn.total_changes();
        assert!(
            PendingStagingIndex::from_bound(&protection)
                .install_in_open_transaction(
                    &pg,
                    if fault == "topology" {
                        &foreign
                    } else {
                        &topology
                    }
                )
                .is_err(),
            "{fault}"
        );
        assert_eq!(pg.conn.total_changes(), before, "{fault}");
        if fault != "transaction" {
            pg.conn.execute_batch("ROLLBACK").unwrap();
        }
    }
}

fn reseal(command: &MetadataCommandEnvelope, bytes: &[u8]) -> u64 {
    let mut preimage = b"argmin.metadata.pending-staging-protection.v1".to_vec();
    for value in [
        command.id().cluster_epoch().get(),
        u64::from(command.id().pg_id().get()),
        command.id().log_index().get(),
        command.checksum_crc64(),
        bytes.len() as u64,
    ] {
        preimage.extend_from_slice(&value.to_be_bytes());
    }
    preimage.extend_from_slice(bytes);
    checksum::crc64::checksum(&preimage)
}

#[test]
fn staging_pending_index_rejects_missing_crossed_and_resealed_malformed_rows() {
    let owner = fixture(StagingTarget::DirectPut);
    let command = introduction::command(&owner);
    for fault in [
        "missing",
        "checksum",
        "command checksum",
        "slot epoch",
        "slot index",
        "topology",
        "attempt",
        "introduction epoch",
        "introduction index",
        "future introduction",
        "data PG",
        "zero VID",
        "large VID",
        "VID endpoint",
        "window index",
        "placement epoch",
        "future placement",
        "old placement",
        "EC",
        "duplicate files",
        "length",
        "oversized",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        install(&pg, &owner, &command);
        let (mut bytes, _) = raw(&pg);
        match fault {
            "topology" => bytes[0] ^= 1,
            "attempt" => bytes[32] = b'z',
            "introduction epoch" => bytes[64..72].fill(0),
            "introduction index" => bytes[72..80].fill(0),
            "future introduction" => bytes[72..80].copy_from_slice(&12_u64.to_be_bytes()),
            "data PG" => bytes[84..88].fill(255),
            "zero VID" => bytes[104..112].fill(0),
            "large VID" => bytes[104] = 0x80,
            "VID endpoint" => bytes[104..112].copy_from_slice(&(i64::MAX as u64).to_be_bytes()),
            "window index" => bytes[80..84].fill(255),
            "placement epoch" => bytes[112..120].fill(0),
            "future placement" => bytes[112..120].copy_from_slice(&6_u64.to_be_bytes()),
            "old placement" => bytes[112..120].copy_from_slice(&4_u64.to_be_bytes()),
            "EC" => bytes[120] = 0,
            "duplicate files" => {
                let node = bytes[126..130].to_vec();
                bytes[130..134].copy_from_slice(&node);
            }
            "length" => {
                bytes.pop();
            }
            "oversized" => {
                bytes.resize(1_048_576, 0);
                pg.conn
                    .pragma_update(None, "ignore_check_constraints", true)
                    .unwrap();
            }
            _ => {}
        }
        let crc = reseal(&command, &bytes) ^ u64::from(fault == "checksum");
        pg.conn.execute("UPDATE metadata_command_pending_staging_protection SET encoded_protection = ?1, protection_checksum = ?2", params![bytes, crc as i64]).unwrap();
        match fault {
            "missing" => {
                pg.conn
                    .execute(
                        "DELETE FROM metadata_command_pending_staging_protection",
                        [],
                    )
                    .unwrap();
            }
            "command checksum" => {
                pg.conn.execute("UPDATE metadata_command_pending_staging_protection SET command_checksum = command_checksum + 1", []).unwrap();
            }
            "slot epoch" => {
                pg.conn.execute("UPDATE metadata_command_pending_slot SET cluster_epoch = cluster_epoch + 1", []).unwrap();
            }
            "slot index" => {
                pg.conn
                    .execute(
                        "UPDATE metadata_command_pending_slot SET log_index = log_index + 1",
                        [],
                    )
                    .unwrap();
            }
            _ => {}
        }
        let before = pg.conn.total_changes();
        assert!(
            pg.pending_staging_protection(&topology()).is_err(),
            "{fault}"
        );
        assert_eq!(pg.conn.total_changes(), before, "{fault}");
    }
}

#[test]
fn staging_pending_index_ordinary_replacement_rolls_back_and_clears_old_protection() {
    let owner = fixture(StagingTarget::DirectPut);
    let command = introduction::command(&owner);
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    install(&pg, &owner, &command);
    let original = pg.pending_staging_protection(&topology()).unwrap();
    let original_slot = pg.pending_metadata_command_slot_any_epoch(0).unwrap();
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            command.id().cluster_epoch(),
            command.id().pg_id(),
            MetadataCommandLogIndex::new(12).unwrap(),
        ),
        command.payload().clone(),
    );
    pg.conn.execute_batch("CREATE TEMP TRIGGER fail_pending_staging BEFORE UPDATE ON metadata_command_pending_staging_protection BEGIN SELECT RAISE(ABORT, 'injected pending staging index failure'); END;").unwrap();
    assert!(pg
        .test_replace_pending_metadata_command_slot(&replacement, None)
        .is_err());
    assert_eq!(
        pg.pending_staging_protection(&topology()).unwrap(),
        original
    );
    assert_eq!(
        pg.pending_metadata_command_slot_any_epoch(0).unwrap(),
        original_slot
    );
    pg.conn
        .execute_batch("DROP TRIGGER fail_pending_staging;")
        .unwrap();
    pg.test_replace_pending_metadata_command_slot(&replacement, None)
        .unwrap();
    assert!(pg
        .pending_staging_protection(&topology())
        .unwrap()
        .unwrap()
        .window
        .is_none());
}

#[test]
fn staging_pending_index_first_insert_failure_rolls_back_all_slot_indexes() {
    let owner = fixture(StagingTarget::DirectPut);
    let command = introduction::command(&owner);
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    pg.conn.execute_batch("CREATE TEMP TRIGGER fail_pending_staging BEFORE INSERT ON metadata_command_pending_staging_protection BEGIN SELECT RAISE(ABORT, 'injected pending staging index failure'); END;").unwrap();
    let error = pg
        .try_insert_pending_metadata_command_slot(0, &command, None)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected pending staging index failure"),
        "{error}"
    );
    for table in [
        "metadata_command_pending_slot",
        "metadata_command_pending_route_dependencies",
        "metadata_command_pending_staging_protection",
        "metadata_command_pending_placed_reference_pages",
        "metadata_command_pending_reclaim_reference_pages",
    ] {
        assert_eq!(
            pg.conn
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row
                    .get::<_, u32>(0))
                .unwrap(),
            0,
            "{table}"
        );
    }
    pg.conn
        .execute_batch("DROP TRIGGER fail_pending_staging;")
        .unwrap();
    pg.try_insert_pending_metadata_command_slot(0, &command, None)
        .unwrap();
    assert!(pg
        .pending_staging_protection(&topology())
        .unwrap()
        .unwrap()
        .window
        .is_none());
}

#[test]
fn staging_pending_index_frozen_encoding_and_maximum_participants() {
    for maximum in [false, true] {
        let mut owner = fixture(StagingTarget::DirectPut);
        if maximum {
            owner.windows[0].geometry = StagingGeometry::new(
                SegmentPayloadCodec::Cobblestone256SegmentV1,
                EcShape { k: 16, m: 16 },
                INTERNAL_SEGMENT_SIZE as u64,
            )
            .unwrap();
            owner.windows[0].participants.files = (0..32).rev().map(NodeId::new).collect();
            owner.windows[0].participants.acknowledgement_primary = NodeId::new(32);
        }
        let command = introduction::command(&owner);
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        pg.test_replace_pending_metadata_command_slot(&command, None)
            .unwrap();
        let empty = raw(&pg);
        assert!(empty.0.is_empty());
        install(&pg, &owner, &command);
        assert_window(&pg, &owner, &command, &owner.windows[0]);
        let (bytes, crc) = raw(&pg);
        assert_eq!(bytes.len(), if maximum { 254 } else { 138 });
        // Literal evidence, independently checked with bitwise CRC-64/NVME.
        // Retain these specimens when advancing the schema-owned grammar.
        assert_eq!(command.checksum_crc64(), 0x7e62_2d7d_4592_cba7);
        assert_eq!(empty.1, 0x0596_286e_908f_f97a);
        let expected = if maximum {
            concat!(
                "7919ed1b46eb22dd6cd760def76c6367888b3ed3d1c4c7869363efa79f93ede5",
                "3733373436313637363936653637326436663737366536353732303030303030",
                "0000000000000005000000000000000b0000000000000005f604b5bed86ff3d0",
                "08f7ae64a68a4772000000000000000c00000000000000051010000000200000",
                "001f0000001e0000001d0000001c0000001b0000001a00000019000000180000",
                "0017000000160000001500000014000000130000001200000011000000100000",
                "000f0000000e0000000d0000000c0000000b0000000a00000009000000080000",
                "000700000006000000050000000400000003000000020000000100000000",
            )
        } else {
            concat!(
                "7919ed1b46eb22dd6cd760def76c6367888b3ed3d1c4c7869363efa79f93ede5",
                "3733373436313637363936653637326436663737366536353732303030303030",
                "0000000000000005000000000000000b0000000000000005f604b5bed86ff3d0",
                "08f7ae64a68a4772000000000000000c00000000000000050201000000010000",
                "00020000000000000003",
            )
        };
        assert_eq!(bytes, crate::tests::frozen_hex_fixture(expected));
        assert_eq!(
            crc,
            if maximum {
                0x256b_7db2_9ce1_cb93
            } else {
                0x8490_e833_7c3b_f695
            }
        );
        let mut ids = vec![];
        topology()
            .for_each_pg::<std::convert::Infallible>(|pg| {
                ids.push(pg);
                Ok(())
            })
            .unwrap();
        ids.push(1000);
        assert!(pg
            .pending_staging_protection(&PgTopology::new(&ids).unwrap())
            .is_err());
        drop(pg);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        assert_eq!(raw(&pg), (bytes, crc));
    }
}
