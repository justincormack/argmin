// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Persistence/format contract tests. Owners are seeded by the private SQL
//! writer, not yet by introducing commands. These test real schema, digest,
//! checkpoint and replacement paths, not command-custody acceptance.

use super::super::super::super::{
    command_log::{
        MetadataCheckpointValue, MetadataCommandCheckpoint,
        MetadataCommandCheckpointValidationError,
    },
    PgStore,
};
use super::*;
use crate::error::StoreError;
use crate::node_runtime::traits::PgMetadataStore;

#[path = "payload_staging_history_tests.rs"]
mod history;

#[path = "payload_staging_inventory_tests.rs"]
mod inventory_tests;

fn seed(store: &PgStore, owner: &StagingOwnerRecord) {
    store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    store
        .advance_object_generation_high_water_in_open_txn(
            &owner.subject.bucket,
            &owner.subject.key,
            owner.subject.payload_generation,
        )
        .unwrap();
    insert(&store.conn, owner);
    assert_eq!(
        store.cached_metadata_state_digest().unwrap(),
        store.metadata_state_digest().unwrap()
    );
    store.refresh_metadata_command_state_digest().unwrap();
    store.conn.execute_batch("COMMIT").unwrap();
}

fn checkpoint(owner: &StagingOwnerRecord) -> MetadataCommandCheckpoint {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
    seed(&store, owner);
    store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap()
}

fn exact_owner(store: &PgStore, owner: &StagingOwnerRecord) {
    store.conn.execute_batch("BEGIN").unwrap();
    assert_eq!(load(&store.conn, owner).unwrap().as_ref(), Some(owner));
    store.conn.execute_batch("COMMIT").unwrap();
}

fn populated_staging_checkpoint() -> MetadataCommandCheckpoint {
    let tmp = test_util::tempdir();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let store = PgStore::open(tmp.path(), pg_id.get()).unwrap();
    for index in 0..8 {
        let target = match index {
            0 | 1 => StagingTarget::DirectPut,
            2 | 3 | 6 => StagingTarget::StreamPut,
            4 | 5 | 7 => multipart_target(),
            _ => unreachable!(),
        };
        let mut owner = fixture(target);
        owner.subject.id.attempt = crate::tests::stream_session_id(format!("owner-{index}"));
        owner.subject.id.introduction = MetadataCommandId::new(
            owner.subject.originating_epoch,
            pg_id,
            MetadataCommandLogIndex::new(11 + index).unwrap(),
        );
        owner.subject.payload_generation = GenerationId::new(12 + index).unwrap();
        owner.windows[0] = window(&owner.subject, 0, owner.subject.first_vid());
        owner.reserved_end = owner.windows[0].end;
        owner.resources.generation = ResourceResponsibility::Live(StagingGenerationResource {
            reservation_id: owner.subject.id.attempt.clone(),
            generation: owner.subject.payload_generation,
        });
        if owner.subject.target != StagingTarget::DirectPut {
            append(&mut owner);
        }
        if matches!(index, 1 | 4 | 5 | 6 | 7) {
            publish(&mut owner);
        }
        match index {
            3 => {
                owner.lifecycle = StagingLifecycle::Retiring(StagingRetirement::Unpublished);
                owner.references.clear();
                release_due(&mut owner.resources.generation);
                release_due(&mut owner.resources.creation);
            }
            5 => {
                owner.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedRetained);
                for reference in &mut owner.references {
                    reference.destination = StagingDestination::Object {
                        generation: owner.subject.published_generation(),
                        part_number: Some(3),
                    };
                }
            }
            6 => {
                owner.lifecycle =
                    StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
                owner.references.clear();
            }
            7 => {
                for reference in &mut owner.references {
                    reference.destination = StagingDestination::Reclaim {
                        generation: owner.subject.published_generation(),
                        part_number: Some(3),
                    };
                }
            }
            _ => {}
        }
        for resource in [
            &mut owner.resources.creation,
            &mut owner.resources.finalization,
        ] {
            if let ResourceResponsibility::Live(proof) | ResourceResponsibility::ReleaseDue(proof) =
                resource
            {
                proof.identity.reservation_id.push_str(&format!("-{index}"));
                proof.identity.owner_token.push_str(&format!("-{index}"));
            }
        }
        seed(&store, &owner);
    }
    store
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap()
}

#[test]
fn staging_persistence_frozen_v8_v10_v11_owner_corpus_pins_all_state_tags() {
    let checkpoint = populated_staging_checkpoint();
    let bytes =
        crate::storage_rpc::encode_metadata_command_checkpoint_payload(&checkpoint).unwrap();
    let frozen = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/checkpoint_v8_canonical_v10_pg_v11_staging_generation_fence.hex"
    ));
    assert_eq!(frozen.len(), 20_942);
    assert_eq!(
        checksum::sha256::digest(&frozen),
        [
            127, 155, 113, 115, 87, 215, 190, 208, 174, 213, 189, 117, 162, 204, 173, 50, 2, 121,
            209, 192, 244, 248, 76, 112, 79, 227, 173, 24, 218, 233, 206, 183
        ]
    );
    assert_eq!(bytes, frozen);
    assert_eq!(
        (checkpoint.state_digest.value(), checkpoint.checkpoint_crc64),
        (14_654_029_971_109_158_692, 8_712_086_045_757_678_351)
    );
    let decoded = crate::storage_rpc::decode_metadata_command_checkpoint_payload(&frozen).unwrap();
    decoded.verify().unwrap();
    assert_eq!(decoded, checkpoint);

    let blocks = &decoded.table_blocks[decoded.table_blocks.len() - TABLES.len()..];
    assert_eq!(
        blocks
            .iter()
            .map(|block| block.row_count)
            .collect::<Vec<_>>(),
        [8, 14, 42, 5, 11]
    );
    // Independent literal tags, not the production enum/tag mapping. This
    // preserves the original lifecycle/role corpus. The appended obligations
    // specimen below covers the additional abandoned-finalization slots.
    for (table, column, expected) in [
        (0, "target", vec![0, 1, 2]),
        (0, "lifecycle", vec![0, 1, 2, 3, 4]),
        (0, "generation_state", vec![1, 2]),
        (3, "destination", vec![0, 1, 2, 3]),
        (4, "role", vec![0, 1]),
        (4, "state", vec![1, 2]),
    ] {
        let block = &blocks[table];
        let column_index = block
            .columns
            .iter()
            .position(|name| name == column)
            .unwrap();
        let actual = block
            .rows
            .iter()
            .map(|row| match row.values[column_index] {
                MetadataCheckpointValue::Integer(value) => value,
                ref other => panic!("non-integer {column}: {other:?}"),
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual.into_iter().collect::<Vec<_>>(), expected, "{column}");
    }
    let tmp = test_util::tempdir();
    let destination = PgStore::open(tmp.path(), decoded.pg_id.get()).unwrap();
    destination
        .install_metadata_transfer_checkpoint_base(
            0,
            ClusterEpoch::new(19).unwrap(),
            &decoded,
            &topology(),
        )
        .unwrap();
    assert_eq!(
        destination.metadata_state_digest().unwrap(),
        decoded.state_digest.value()
    );
    drop(destination);
    let reopened = PgStore::open(tmp.path(), decoded.pg_id.get()).unwrap();
    assert_eq!(
        reopened
            .metadata_command_checkpoint(0, ClusterEpoch::new(19).unwrap())
            .unwrap()
            .table_blocks,
        decoded.table_blocks
    );
}

#[test]
fn staging_obligations_frozen_checkpoint_preserves_multiple_proofs_on_import_and_restart() {
    let owner = owner_with_abandoned_obligations();
    let checkpoint = checkpoint(&owner);
    let bytes =
        crate::storage_rpc::encode_metadata_command_checkpoint_payload(&checkpoint).unwrap();
    let frozen = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/checkpoint_v8_canonical_v10_pg_v11_abandoned_finalizations.hex"
    ));
    assert_eq!(frozen.len(), 11_658);
    assert_eq!(
        checksum::sha256::digest(&frozen),
        [
            107, 112, 109, 78, 234, 164, 20, 229, 4, 13, 174, 203, 222, 129, 110, 85, 7, 194, 146,
            240, 146, 223, 242, 16, 39, 30, 76, 125, 36, 74, 134, 67
        ]
    );
    assert_eq!(bytes, frozen);
    assert_eq!(
        (checkpoint.state_digest.value(), checkpoint.checkpoint_crc64),
        (8_690_588_273_436_876_678, 3_465_898_082_936_411_471)
    );
    let decoded = crate::storage_rpc::decode_metadata_command_checkpoint_payload(&frozen).unwrap();
    decoded.verify().unwrap();
    let resources = decoded
        .table_blocks
        .iter()
        .find(|block| block.table_name == "payload_staging_resources")
        .unwrap();
    let role = resources
        .columns
        .iter()
        .position(|column| column == "role")
        .unwrap();
    assert_eq!(
        resources
            .rows
            .iter()
            .map(|row| row.values[role].clone())
            .collect::<Vec<_>>(),
        [0, 1, 2, 3].map(MetadataCheckpointValue::Integer)
    );
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), decoded.pg_id.get()).unwrap();
    pg.install_metadata_transfer_checkpoint_base(
        0,
        ClusterEpoch::new(19).unwrap(),
        &decoded,
        &topology(),
    )
    .unwrap();
    exact_owner(&pg, &owner);
    assert_eq!(
        pg.cached_metadata_state_digest().unwrap(),
        pg.metadata_state_digest().unwrap()
    );
    drop(pg);
    let pg = PgStore::open(dir.path(), decoded.pg_id.get()).unwrap();
    exact_owner(&pg, &owner);
    assert_eq!(
        pg.metadata_command_checkpoint(0, ClusterEpoch::new(19).unwrap())
            .unwrap()
            .table_blocks,
        decoded.table_blocks
    );
}

#[test]
fn staging_obligations_checkpoint_rejects_malformed_slots_before_replacement() {
    let owner = owner_with_abandoned_obligations();
    let original = checkpoint(&owner);
    for case in [
        "gap",
        "duplicate",
        "primary-duplicate",
        "live",
        "missing-primary",
        "unordered",
        "wrong-release-pg",
    ] {
        let mut malformed = original.clone();
        let resources = malformed
            .table_blocks
            .iter_mut()
            .find(|block| block.table_name == "payload_staging_resources")
            .unwrap();
        let column = |name: &str| {
            resources
                .columns
                .iter()
                .position(|column| column == name)
                .unwrap()
        };
        let role = column("role");
        let state = column("state");
        let release_pg = column("release_pg");
        match case {
            "gap" => {
                resources.rows.remove(2);
            }
            "duplicate" => {
                resources.rows[3] = resources.rows[2].clone();
                resources.rows[3].values[role] = MetadataCheckpointValue::Integer(3);
            }
            "primary-duplicate" => {
                resources.rows[2] = resources.rows[1].clone();
                resources.rows[2].values[role] = MetadataCheckpointValue::Integer(2);
            }
            "live" => resources.rows[2].values[state] = MetadataCheckpointValue::Integer(1),
            "missing-primary" => {
                resources.rows.remove(1);
            }
            "unordered" => resources.rows.swap(2, 3),
            "wrong-release-pg" => {
                resources.rows[2].values[release_pg] = MetadataCheckpointValue::Integer(99)
            }
            _ => unreachable!(),
        }
        PgStore::test_reseal_metadata_command_checkpoint(&mut malformed);
        if case != "wrong-release-pg" {
            assert!(malformed.verify().is_err(), "{case}");
        }
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), malformed.pg_id.get()).unwrap();
        seed(&pg, &fixture(StagingTarget::DirectPut));
        let before = pg
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        let lifetime = pg.allocation_lifetime.active_identity();
        let changes = pg.conn.total_changes();
        let error = pg
            .install_metadata_transfer_checkpoint_base(
                0,
                ClusterEpoch::new(19).unwrap(),
                &malformed,
                &topology(),
            )
            .unwrap_err();
        assert_eq!(pg.conn.total_changes(), changes, "{case}: {error}");
        assert_eq!(pg.allocation_lifetime.active_identity(), lifetime);
        assert_eq!(
            pg.metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            before,
            "{case}"
        );
    }
}

#[test]
fn staging_persistence_frozen_owner_without_generation_fence_is_rejected() {
    // Keep the original bytes and all their original digests. This specimen was
    // correctly sealed but semantically incomplete, not a versioning failure.
    let frozen = crate::tests::frozen_hex_fixture(include_str!(
        "testdata/checkpoint_v8_canonical_v10_pg_v11_staging.hex"
    ));
    assert_eq!(frozen.len(), 20_881);
    assert_eq!(
        checksum::sha256::digest(&frozen),
        [
            65, 31, 10, 77, 196, 42, 222, 8, 66, 99, 187, 240, 0, 178, 195, 185, 149, 227, 212, 77,
            147, 92, 20, 72, 168, 28, 186, 144, 164, 92, 12, 86,
        ]
    );
    let decoded = crate::storage_rpc::decode_metadata_command_checkpoint_payload(&frozen).unwrap();
    assert_eq!(
        (decoded.state_digest.value(), decoded.checkpoint_crc64),
        (11_951_237_481_369_300_233, 18_271_228_514_599_593_587)
    );
    assert_eq!(
        crate::storage_rpc::encode_metadata_command_checkpoint_payload(&decoded).unwrap(),
        frozen
    );
    let mut resealed = decoded.clone();
    PgStore::test_reseal_metadata_command_checkpoint(&mut resealed);
    assert_eq!(
        resealed, decoded,
        "the old specimen must still have valid checksums"
    );
    assert!(matches!(decoded.verify(),
        Err(MetadataCommandCheckpointValidationError::InvalidStagingOwners { reason })
            if reason == StagingInvariant::GenerationFence.to_string()));
}

#[test]
fn staging_persistence_generation_fence_covers_every_same_object_owner() {
    let mut checkpoint = populated_staging_checkpoint();
    checkpoint.verify().unwrap();
    let owners =
        super::super::super::checkpoint_owners(checkpoint.pg_id, &checkpoint.table_blocks).unwrap();
    assert_eq!(owners.len(), 8);
    assert_eq!(owners[0].subject.payload_generation.get(), 12);
    assert_eq!(owners.last().unwrap().subject.payload_generation.get(), 19);
    assert!(owners
        .iter()
        .all(|owner| owner.subject.bucket == owners[0].subject.bucket
            && owner.subject.key == owners[0].subject.key));
    let counter = checkpoint
        .table_blocks
        .iter_mut()
        .find(|block| block.table_name == "object_write_counters")
        .unwrap();
    assert_eq!(counter.rows.len(), 1);
    let index = counter
        .columns
        .iter()
        .position(|column| column == "max_committed_generation")
        .unwrap();
    counter.rows[0].values[index] = MetadataCheckpointValue::Integer(18);
    PgStore::test_reseal_metadata_command_checkpoint(&mut checkpoint);
    assert!(
        matches!(checkpoint.verify(),
        Err(MetadataCommandCheckpointValidationError::InvalidStagingOwners { reason })
            if reason == StagingInvariant::GenerationFence.to_string()),
        "the counter covers seven earlier owners, but must also cover the last one"
    );
}

#[test]
fn staging_persistence_all_lifecycles_require_generation_fence_before_replacement() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        for lifecycle in 0..5 {
            let mut owner = fixture(target.clone());
            if target != StagingTarget::DirectPut {
                append(&mut owner);
            }
            match lifecycle {
                0 => {}
                1 => {
                    owner.lifecycle = StagingLifecycle::Retiring(StagingRetirement::Unpublished);
                    owner.references.clear();
                    release_due(&mut owner.resources.generation);
                    release_due(&mut owner.resources.creation);
                }
                2..=4 => {
                    publish(&mut owner);
                    if lifecycle == 3 {
                        owner.lifecycle =
                            StagingLifecycle::Retiring(StagingRetirement::PublishedRetained);
                    } else if lifecycle == 4 {
                        owner.lifecycle =
                            StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
                        owner.references.clear();
                    }
                }
                _ => unreachable!(),
            }
            let original = checkpoint(&owner);
            original.verify().unwrap();
            let tmp = test_util::tempdir();
            let destination = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
            seed(&destination, &owner);
            let before = destination
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap();
            let issuer = destination.allocation_lifetime.active_identity();
            let changes = destination.conn.total_changes();
            for mutation in [
                "missing",
                "null",
                "lowered",
                "foreign-bucket",
                "foreign-key",
            ] {
                let mut malformed = original.clone();
                let counters = malformed
                    .table_blocks
                    .iter_mut()
                    .find(|block| block.table_name == "object_write_counters")
                    .unwrap();
                assert_eq!(counters.rows.len(), 1);
                let column = |name: &str| {
                    counters
                        .columns
                        .iter()
                        .position(|column| column == name)
                        .unwrap()
                };
                match mutation {
                    "missing" => counters.rows.clear(),
                    "null" => {
                        let index = column("max_committed_generation");
                        counters.rows[0].values[index] = MetadataCheckpointValue::Null;
                    }
                    "lowered" => {
                        let index = column("max_committed_generation");
                        counters.rows[0].values[index] = MetadataCheckpointValue::Integer(
                            owner.subject.payload_generation.get() as i64 - 1,
                        );
                    }
                    "foreign-bucket" | "foreign-key" => {
                        let index = column(if mutation == "foreign-bucket" {
                            "bucket"
                        } else {
                            "key"
                        });
                        counters.rows[0].values[index] =
                            MetadataCheckpointValue::Text(mutation.as_bytes().to_vec());
                    }
                    _ => unreachable!(),
                }
                PgStore::test_reseal_metadata_command_checkpoint(&mut malformed);
                let bytes =
                    crate::storage_rpc::encode_metadata_command_checkpoint_payload(&malformed)
                        .unwrap();
                let decoded =
                    crate::storage_rpc::decode_metadata_command_checkpoint_payload(&bytes).unwrap();
                assert_eq!(decoded, malformed);
                assert!(
                    matches!(decoded.verify(),
                    Err(MetadataCommandCheckpointValidationError::InvalidStagingOwners { reason })
                        if reason == StagingInvariant::GenerationFence.to_string()),
                    "{target:?}/{lifecycle}/{mutation}"
                );
                let error = destination
                    .install_metadata_transfer_checkpoint_base(
                        0,
                        ClusterEpoch::new(19).unwrap(),
                        &decoded,
                        &topology(),
                    )
                    .unwrap_err();
                assert!(
                    matches!(error, StoreError::MetadataCheckpointInvalid { ref reason, .. }
                    if reason.contains(&StagingInvariant::GenerationFence.to_string())),
                    "{error:?}"
                );
                assert_eq!(destination.conn.total_changes(), changes);
                assert_eq!(destination.allocation_lifetime.active_identity(), issuer);
                assert_eq!(
                    destination
                        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                        .unwrap(),
                    before
                );
            }
            // No live reservation exists in these retained-owner fixtures. The
            // imported/reopened counter alone must prevent generation recycling.
            for (import_index, high_water) in [
                owner.subject.payload_generation.get(),
                owner.subject.payload_generation.get() + 4,
            ]
            .into_iter()
            .enumerate()
            {
                let mut valid = original.clone();
                let counters = valid
                    .table_blocks
                    .iter_mut()
                    .find(|block| block.table_name == "object_write_counters")
                    .unwrap();
                let index = counters
                    .columns
                    .iter()
                    .position(|column| column == "max_committed_generation")
                    .unwrap();
                counters.rows[0].values[index] =
                    MetadataCheckpointValue::Integer(high_water as i64);
                PgStore::test_reseal_metadata_command_checkpoint(&mut valid);
                valid.verify().unwrap();
                destination
                    .install_metadata_transfer_checkpoint_base(
                        0,
                        ClusterEpoch::new(19 + import_index as u64).unwrap(),
                        &valid,
                        &topology(),
                    )
                    .unwrap();
                assert_eq!(
                    destination
                        .next_generation_id(&owner.subject.bucket, &owner.subject.key)
                        .unwrap()
                        .get(),
                    high_water + 1
                );
            }
            drop(destination);
            let reopened = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
            assert_eq!(
                reopened
                    .next_generation_id(&owner.subject.bucket, &owner.subject.key)
                    .unwrap()
                    .get(),
                owner.subject.payload_generation.get() + 5
            );
        }
    }
}

#[test]
fn staging_persistence_checkpoint_install_and_reopen_preserve_all_owner_rows() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let mut owner = fixture(target);
        if owner.subject.target != StagingTarget::DirectPut {
            append(&mut owner);
            append(&mut owner);
        }
        publish(&mut owner);
        let checkpoint = checkpoint(&owner);
        checkpoint.verify().unwrap();
        let bytes =
            crate::storage_rpc::encode_metadata_command_checkpoint_payload(&checkpoint).unwrap();
        let decoded =
            crate::storage_rpc::decode_metadata_command_checkpoint_payload(&bytes).unwrap();
        assert_eq!(decoded, checkpoint);
        let tmp = test_util::tempdir();
        let epoch = ClusterEpoch::new(19).unwrap();
        {
            let destination = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
            destination
                .install_metadata_transfer_checkpoint_base(0, epoch, &decoded, &topology())
                .unwrap();
            exact_owner(&destination, &owner);
            assert_eq!(
                destination.cached_metadata_state_digest().unwrap(),
                checkpoint.state_digest.value()
            );
            assert_eq!(
                destination.metadata_state_digest().unwrap(),
                checkpoint.state_digest.value()
            );
        }
        let reopened = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
        exact_owner(&reopened, &owner);
        let restored = reopened.metadata_command_checkpoint(0, epoch).unwrap();
        assert_eq!(restored.table_blocks, checkpoint.table_blocks);
        assert_eq!(restored.state_digest, checkpoint.state_digest);
    }
}

#[test]
fn staging_persistence_rejects_resealed_malformed_checkpoint_before_replacement() {
    let mut owner = fixture(multipart_target());
    append(&mut owner);
    append(&mut owner);
    publish(&mut owner);
    let original = checkpoint(&owner);
    // Every case is resealed through the real digest hierarchy. Rejection must
    // come from owner semantics, not from stale outer checksums or summaries.
    for case in 0..6 {
        let mut malformed = original.clone();
        let table = match case {
            0 | 1 => "payload_staging_references",
            2 => "payload_staging_resources",
            3 => "payload_staging_participants",
            4 => "payload_staging_owners",
            5 => "payload_staging_windows",
            _ => unreachable!(),
        };
        let block = malformed
            .table_blocks
            .iter_mut()
            .find(|block| block.table_name == table)
            .unwrap();
        match case {
            0 | 2 | 4 => {
                block.rows.remove(0);
            }
            1 => {
                block.rows[1].values[6] = MetadataCheckpointValue::Integer(2);
                block.rows[1].values[7] = MetadataCheckpointValue::Null;
                block.rows[1].values[8] = MetadataCheckpointValue::Integer(7);
            }
            3 => {
                let duplicate = block.rows[0].clone();
                block.rows.push(duplicate);
            }
            5 => {
                block.rows[0].values[11] = MetadataCheckpointValue::Integer(1);
            }
            _ => unreachable!(),
        }
        PgStore::test_reseal_metadata_command_checkpoint(&mut malformed);
        assert!(
            matches!(
                malformed.verify(),
                Err(MetadataCommandCheckpointValidationError::InvalidStagingOwners { .. })
            ),
            "case {case}"
        );
        let tmp = test_util::tempdir();
        let destination = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
        seed(&destination, &owner);
        let before = destination
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        let issuer = destination.allocation_lifetime.active_identity();
        let error = destination
            .install_metadata_transfer_checkpoint_base(
                0,
                ClusterEpoch::new(19).unwrap(),
                &malformed,
                &topology(),
            )
            .unwrap_err();
        assert!(
            matches!(error, StoreError::MetadataCheckpointInvalid { .. }),
            "{error:?}"
        );
        assert_eq!(destination.allocation_lifetime.active_identity(), issuer);
        assert_eq!(
            destination
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            before
        );
        exact_owner(&destination, &owner);
    }
}

#[test]
fn staging_persistence_import_uses_configured_topology_before_replacement() {
    for data_differs in [false, true] {
        let (owner, crossed) = crossed_topology_owner(StagingTarget::DirectPut, data_differs);
        let source = checkpoint(&owner);
        source.verify().unwrap();
        let tmp = test_util::tempdir();
        let destination = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
        let before = destination
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        let issuer = destination.allocation_lifetime.active_identity();
        let error = destination
            .install_metadata_transfer_checkpoint_base(
                0,
                ClusterEpoch::new(19).unwrap(),
                &source,
                &crossed,
            )
            .unwrap_err();
        let expected = if data_differs {
            StagingInvariant::Placement
        } else {
            StagingInvariant::Resource
        };
        assert!(
            matches!(error, StoreError::MetadataCheckpointInvalid { ref reason, .. } if *reason == expected.to_string()),
            "{error:?}"
        );
        assert_eq!(destination.allocation_lifetime.active_identity(), issuer);
        assert_eq!(
            destination
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            before
        );
        destination
            .install_metadata_transfer_checkpoint_base(
                0,
                ClusterEpoch::new(19).unwrap(),
                &source,
                &topology(),
            )
            .unwrap();
        exact_owner(&destination, &owner);
    }
}

#[test]
fn staging_persistence_rejects_oversized_release_identities_in_rows_and_checkpoints() {
    let mut owner = fixture(multipart_target());
    publish(&mut owner);
    let original = checkpoint(&owner);
    for role in [0_i64, 1] {
        for field in ["reservation_id", "owner_token"] {
            let dir = test_util::tempdir();
            let destination = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
            seed(&destination, &owner);
            let before = destination
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap();
            let issuer = destination.allocation_lifetime.active_identity();
            let mut malformed = original.clone();
            let resources = malformed
                .table_blocks
                .iter_mut()
                .find(|block| block.table_name == "payload_staging_resources")
                .unwrap();
            let role_column = resources
                .columns
                .iter()
                .position(|name| name == "role")
                .unwrap();
            let field_column = resources
                .columns
                .iter()
                .position(|name| name == field)
                .unwrap();
            let row = resources
                .rows
                .iter_mut()
                .find(|row| row.values[role_column] == MetadataCheckpointValue::Integer(role))
                .unwrap();
            row.values[field_column] = MetadataCheckpointValue::Text(vec![b'x'; 257]);
            PgStore::test_reseal_metadata_command_checkpoint(&mut malformed);
            assert!(
                matches!(malformed.verify(), Err(MetadataCommandCheckpointValidationError::InvalidStagingOwners { reason })
                if reason == StagingInvariant::Resource.to_string())
            );
            let changes = destination.conn.total_changes();
            assert!(matches!(
                destination.install_metadata_transfer_checkpoint_base(
                    0,
                    ClusterEpoch::new(19).unwrap(),
                    &malformed,
                    &topology()
                ),
                Err(StoreError::MetadataCheckpointInvalid { .. })
            ));
            assert_eq!(destination.conn.total_changes(), changes);
            assert_eq!(destination.allocation_lifetime.active_identity(), issuer);
            assert_eq!(
                destination
                    .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                    .unwrap(),
                before
            );

            // A malformed local row must fail the same central validation,
            // independently of checkpoint verification and descriptor decoding.
            destination.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            destination
                .conn
                .execute(
                    &format!("UPDATE payload_staging_resources SET {field} = ?1 WHERE role = ?2"),
                    rusqlite::params!["x".repeat(257), role],
                )
                .unwrap();
            let changes = destination.conn.total_changes();
            assert!(matches!(
                load(&destination.conn, &owner),
                Err(sql::PersistenceError::Invariant(StagingInvariant::Resource))
            ));
            assert_eq!(destination.conn.total_changes(), changes);
            destination.conn.execute_batch("ROLLBACK").unwrap();
            exact_owner(&destination, &owner);
        }
    }
}

#[test]
fn staging_persistence_late_import_failure_rolls_back_all_owner_tables() {
    let mut owner = fixture(multipart_target());
    append(&mut owner);
    publish(&mut owner);
    let source = checkpoint(&owner);
    for table in sql::TABLES {
        let tmp = test_util::tempdir();
        let destination = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
        seed(&destination, &owner);
        let before = destination
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        destination.conn.execute_batch(&format!("CREATE TEMP TRIGGER reject_owner_import BEFORE INSERT ON {} BEGIN SELECT RAISE(ABORT, 'staging import failure'); END", table.name)).unwrap();
        let error = destination
            .install_metadata_transfer_checkpoint_base(
                0,
                ClusterEpoch::new(19).unwrap(),
                &source,
                &topology(),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("staging import failure"),
            "{error:?}"
        );
        assert_eq!(
            destination
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            before
        );
        assert_eq!(
            destination.allocation_lifetime.active_identity(),
            None,
            "a started replacement retires the prior local issuer even on rollback"
        );
        assert!(destination.conn.is_autocommit());
        exact_owner(&destination, &owner);
    }
}

#[test]
fn staging_persistence_every_column_participates_in_incremental_and_full_digests() {
    let mut owner = fixture(multipart_target());
    append(&mut owner);
    publish(&mut owner);
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
    seed(&store, &owner);
    let baseline = store.metadata_state_digest().unwrap();
    // Rollback one deliberately malformed cell at a time. This pins digest
    // coverage independently of record validity; it is not an apply fixture.
    store
        .conn
        .execute_batch("PRAGMA foreign_keys = OFF; PRAGMA ignore_check_constraints = ON")
        .unwrap();
    for table in sql::TABLES {
        for column in table.columns {
            store.conn.execute_batch("SAVEPOINT cell").unwrap();
            let value: rusqlite::types::Value = store
                .conn
                .query_row(
                    &format!("SELECT {column} FROM {} LIMIT 1", table.name),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let changed = match value {
                rusqlite::types::Value::Integer(value) => {
                    rusqlite::types::Value::Integer(value + 101)
                }
                rusqlite::types::Value::Text(value) => {
                    rusqlite::types::Value::Text(format!("{value}-changed"))
                }
                rusqlite::types::Value::Blob(mut value) => {
                    value[0] ^= 1;
                    rusqlite::types::Value::Blob(value)
                }
                rusqlite::types::Value::Null => rusqlite::types::Value::Integer(101),
                rusqlite::types::Value::Real(_) => unreachable!(),
            };
            store
                .conn
                .execute(
                    &format!(
                        "UPDATE {} SET {column} = ?1 WHERE rowid = (SELECT rowid FROM {} LIMIT 1)",
                        table.name, table.name
                    ),
                    [changed],
                )
                .unwrap();
            let incremental = store.cached_metadata_state_digest().unwrap();
            assert_eq!(
                incremental,
                store.metadata_state_digest().unwrap(),
                "{}.{}",
                table.name,
                column
            );
            assert_ne!(incremental, baseline, "{}.{} omitted", table.name, column);
            store
                .conn
                .execute_batch("ROLLBACK TO cell; RELEASE cell")
                .unwrap();
            assert_eq!(store.cached_metadata_state_digest().unwrap(), baseline);
        }
    }
}
