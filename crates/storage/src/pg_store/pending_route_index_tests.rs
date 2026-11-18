// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn identity() -> (MetadataCommandId, u64) {
    (
        MetadataCommandId::new(
            ClusterEpoch::new(0x0102_0304_0506_0708).unwrap(),
            PgId::new(0x1122_3344),
            MetadataCommandLogIndex::new(0x2122_2324_2526_2728).unwrap(),
        ),
        0x8182_8384_8586_8788,
    )
}

fn dependencies() -> Vec<PendingMetadataCommandRouteDependency> {
    [
        PendingRouteTarget::BucketHash(0x9192_9394_9596_9798),
        PendingRouteTarget::ExactPg(PgId::new(7)),
        PendingRouteTarget::ExactPg(PgId::new(11)),
        PendingRouteTarget::ExactPg(PgId::new(13)),
        PendingRouteTarget::ExactPg(PgId::new(17)),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, target)| PendingMetadataCommandRouteDependency {
        cluster_epoch: ClusterEpoch::new(i as u64 + 1).unwrap(),
        target,
    })
    .collect()
}

fn install(store: &PgStore, dependencies: Vec<PendingMetadataCommandRouteDependency>) {
    let (id, checksum) = identity();
    store
        .test_insert_raw_pending_metadata_command_slot(id, checksum, b"opaque envelope", None)
        .unwrap();
    store
        .replace_pending_metadata_command_route_dependency_sidecar(
            &PendingMetadataCommandRouteDependencySidecar {
                command_checksum: checksum,
                command_sha256: checksum::sha256::digest(b"opaque envelope"),
                checksum: PgStore::pending_metadata_command_route_dependency_checksum(
                    id,
                    checksum,
                    &dependencies,
                    &checksum::sha256::digest(b"opaque envelope"),
                ),
                dependencies,
            },
        )
        .unwrap();
}

fn encoded(store: &PgStore) -> (Vec<u8>, i64) {
    store.conn.query_row(
        "SELECT encoded_dependencies, dependency_checksum FROM metadata_command_pending_route_dependencies WHERE singleton = 0",
        [], |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap()
}

#[test]
fn pending_route_index_v3_frozen_bytes_checksums_reopen_and_exact_routes() {
    // Literal V2 and V3 checksums, independently checked with bitwise
    // CRC-64/NVME. Do not regenerate these when changing the next writer.
    let specimens = [
        (0, "", (0x3cad_c84c_f5ad_f472_u64, 0x7f2b0471b4c25795_u64)),
        (
            1,
            "0000000000000001009192939495969798",
            (0x5e5d_0ae6_504b_20df_u64, 0x9f4bc7119be6ed24_u64),
        ),
        (
            2,
            concat!(
                "0000000000000001009192939495969798",
                "0000000000000002010000000000000007"
            ),
            (0x0e3d_87b0_c93c_a8e3_u64, 0x144ee564efb848b2_u64),
        ),
        (
            5,
            concat!(
                "0000000000000001009192939495969798",
                "0000000000000002010000000000000007",
                "000000000000000301000000000000000b",
                "000000000000000401000000000000000d",
                "0000000000000005010000000000000011",
            ),
            (0x5ffa_7860_1569_518e_u64, 0x5c360089831d90da_u64),
        ),
    ];
    let (id, checksum) = identity();
    for (count, bytes, (old_checksum, expected_checksum)) in specimens {
        assert_eq!(
            reseal_v2(count as u8, &crate::tests::frozen_hex_fixture(bytes)),
            old_checksum
        );
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), id.pg_id().get()).unwrap();
        let dependencies = dependencies()[..count].to_vec();
        install(&store, dependencies.clone());
        drop(store);
        let store = PgStore::open(tmp.path(), id.pg_id().get()).unwrap();
        assert_eq!(
            encoded(&store),
            (
                crate::tests::frozen_hex_fixture(bytes),
                expected_checksum as i64
            )
        );
        assert_eq!(
            store.pending_metadata_command_route_references(0).unwrap(),
            Some(PendingMetadataCommandRouteReferences {
                id,
                command_checksum: checksum,
                command_sha256: checksum::sha256::digest(b"opaque envelope"),
                dependencies: dependencies.clone(),
            })
        );
        let topology = crate::PgTopology::new(&[id.pg_id().get(), 7, 11, 13, 17]).unwrap();
        let route = |epoch, pg| {
            PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
                epoch,
                pg,
            )
        };
        let mut expected = BTreeSet::from([route(id.cluster_epoch(), id.pg_id())]);
        if count > 0 {
            expected.insert(route(
                ClusterEpoch::INITIAL,
                PgId::new(topology.pg_for_bucket_route_hash(0x9192_9394_9596_9798)),
            ));
        }
        for (i, pg) in [7, 11, 13, 17]
            .into_iter()
            .take(count.saturating_sub(1))
            .enumerate()
        {
            expected.insert(route(
                ClusterEpoch::new(i as u64 + 2).unwrap(),
                PgId::new(pg),
            ));
        }
        assert_eq!(
            store
                .cluster_map_history_route_references(&topology)
                .unwrap()
                .iter()
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert_eq!(
            store
                .cluster_map_history_reference_summary()
                .unwrap()
                .oldest_metadata_command_resource_epoch,
            Some(if count == 0 {
                id.cluster_epoch()
            } else {
                ClusterEpoch::INITIAL
            })
        );
    }
}

fn reseal_v2(count: u8, bytes: &[u8]) -> u64 {
    let mut preimage = b"argmin.metadata.pending-route-dependencies.v2".to_vec();
    preimage.extend(crate::tests::frozen_hex_fixture(concat!(
        "0102030405060708",
        "0000000011223344",
        "2122232425262728",
        "8182838485868788",
    )));
    preimage.push(count);
    preimage.extend_from_slice(bytes);
    checksum::crc64::checksum(&preimage)
}

fn reseal(count: u8, bytes: &[u8]) -> u64 {
    let mut preimage = b"argmin.metadata.pending-route-dependencies.v3".to_vec();
    preimage.extend(crate::tests::frozen_hex_fixture(concat!(
        "0102030405060708",
        "0000000011223344",
        "2122232425262728",
        "8182838485868788",
    )));
    preimage.extend_from_slice(&checksum::sha256::digest(b"opaque envelope"));
    preimage.push(count);
    preimage.extend_from_slice(bytes);
    checksum::crc64::checksum(&preimage)
}

#[test]
fn pending_route_index_v2_orders_target_tags_and_values_within_one_epoch() {
    let tmp = test_util::tempdir();
    let (id, command_checksum) = identity();
    let store = PgStore::open(tmp.path(), id.pg_id().get()).unwrap();
    let dependencies = dependencies()
        .into_iter()
        .map(|mut dependency| {
            dependency.cluster_epoch = ClusterEpoch::INITIAL;
            dependency
        })
        .collect::<Vec<_>>();
    install(&store, dependencies.clone());
    assert_eq!(
        store.pending_metadata_command_route_references(0).unwrap(),
        Some(PendingMetadataCommandRouteReferences {
            id,
            command_checksum,
            command_sha256: checksum::sha256::digest(b"opaque envelope"),
            dependencies,
        })
    );
    let original = encoded(&store).0;
    for (first, second) in [(0, 1), (1, 2)] {
        let mut bytes = original.clone();
        let saved = bytes[first * 17..(first + 1) * 17].to_vec();
        bytes.copy_within(second * 17..(second + 1) * 17, first * 17);
        bytes[second * 17..(second + 1) * 17].copy_from_slice(&saved);
        store.conn.execute(
            "UPDATE metadata_command_pending_route_dependencies SET encoded_dependencies = ?1, dependency_checksum = ?2 WHERE singleton = 0",
            params![bytes, reseal(5, &bytes) as i64],
        ).unwrap();
        assert!(matches!(
            store.pending_metadata_command_route_references(0),
            Err(StoreError::MetadataCommandLogConflict { .. })
        ));
    }
}

#[test]
fn pending_route_index_v2_rejects_resealed_malformed_rows_without_mutation() {
    let (id, _) = identity();
    for fault in [
        "zero epoch",
        "large epoch",
        "unknown tag",
        "large PG",
        "duplicate",
        "unordered",
        "truncated",
        "wrong count",
        "over count",
        "over bytes",
        "checksum",
    ] {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), id.pg_id().get()).unwrap();
        install(&store, dependencies());
        let (mut bytes, _) = encoded(&store);
        let mut count = 5_u8;
        match fault {
            "zero epoch" => bytes[..8].fill(0),
            "large epoch" => bytes[0] = 0x80,
            "unknown tag" => bytes[8] = 2,
            "large PG" => bytes[17 + 9] = 1,
            "duplicate" => {
                let record = bytes[..17].to_vec();
                bytes[17..34].copy_from_slice(&record);
            }
            "unordered" => {
                let (first, rest) = bytes.split_at_mut(17);
                first.swap_with_slice(&mut rest[..17]);
            }
            "truncated" => {
                bytes.pop();
            }
            "wrong count" => count = 4,
            "over count" => {
                count = 6;
                bytes.extend(crate::tests::frozen_hex_fixture(
                    "0000000000000006010000000000000013",
                ));
            }
            "over bytes" => bytes.resize(1_048_576, 0),
            "checksum" => {}
            _ => unreachable!(),
        }
        let mut checksum = reseal(count, &bytes);
        if fault == "checksum" {
            checksum ^= 1;
        }
        store
            .conn
            .pragma_update(None, "ignore_check_constraints", true)
            .unwrap();
        store.conn.execute(
            "UPDATE metadata_command_pending_route_dependencies SET dependency_count = ?1, encoded_dependencies = ?2, dependency_checksum = ?3 WHERE singleton = 0",
            params![count, bytes, checksum as i64],
        ).unwrap();
        let before = store.conn.total_changes();
        assert!(
            matches!(
                store.pending_metadata_command_route_references(0),
                Err(StoreError::MetadataCommandLogConflict { .. })
            ),
            "{fault}"
        );
        assert!(
            store.cluster_map_history_reference_summary().is_err(),
            "{fault}"
        );
        assert_eq!(store.conn.total_changes(), before, "{fault}");
    }
}

#[test]
fn pending_route_index_v2_rejects_unknown_topology_and_missing_sidecar() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), identity().0.pg_id().get()).unwrap();
    install(&store, dependencies());
    let topology = crate::PgTopology::new(&[identity().0.pg_id().get(), 7, 11, 13]).unwrap();
    assert!(matches!(
        store.cluster_map_history_route_references(&topology),
        Err(StoreError::RouteCapabilitySubjectMismatch {
            operation: "pending-command-route-topology"
        })
    ));
    store
        .conn
        .execute(
            "DELETE FROM metadata_command_pending_route_dependencies",
            [],
        )
        .unwrap();
    assert!(matches!(
        store.pending_metadata_command_route_references(0),
        Err(StoreError::MetadataCommandLogConflict { .. })
    ));
}

#[test]
fn pending_route_index_v2_writer_rejects_invalid_sets_before_sql() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), identity().0.pg_id().get()).unwrap();
    install(&store, dependencies());
    let original = encoded(&store);
    for fault in ["over count", "duplicate", "unordered", "large epoch"] {
        let mut bad = dependencies();
        match fault {
            "over count" => bad.push(PendingMetadataCommandRouteDependency {
                cluster_epoch: ClusterEpoch::new(6).unwrap(),
                target: PendingRouteTarget::ExactPg(PgId::new(19)),
            }),
            "duplicate" => bad[1] = bad[0],
            "unordered" => bad.swap(0, 1),
            "large epoch" => bad[4].cluster_epoch = ClusterEpoch::new(u64::MAX).unwrap(),
            _ => unreachable!(),
        }
        let before = store.conn.total_changes();
        assert!(store
            .replace_pending_metadata_command_route_dependency_sidecar(
                &PendingMetadataCommandRouteDependencySidecar {
                    command_checksum: identity().1,
                    command_sha256: checksum::sha256::digest(b"opaque envelope"),
                    dependencies: bad,
                    checksum: 0,
                }
            )
            .is_err());
        assert_eq!(store.conn.total_changes(), before);
        assert_eq!(encoded(&store), original);
    }
}

#[test]
fn pending_route_index_v2_replacement_rolls_back_and_removal_cascades() {
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), identity().0.pg_id().get()).unwrap();
    let command = MetadataCommandEnvelope::new(
        identity().0,
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            BucketName::try_from("index-rollback").unwrap(),
            ObjectKey::try_from("key").unwrap(),
            SessionId::try_from("12".repeat(16)).unwrap(),
            GenerationId::MIN,
            1,
        )),
    );
    store
        .test_replace_pending_metadata_command_slot(&command, None)
        .unwrap();
    let dependencies = dependencies();
    store
        .replace_pending_metadata_command_route_dependency_sidecar(
            &PendingMetadataCommandRouteDependencySidecar {
                command_checksum: command.checksum_crc64(),
                command_sha256: command.command_sha256(),
                checksum: PgStore::pending_metadata_command_route_dependency_checksum(
                    command.id(),
                    command.checksum_crc64(),
                    &dependencies,
                    &command.command_sha256(),
                ),
                dependencies,
            },
        )
        .unwrap();
    let before = store.pending_metadata_command_slot_any_epoch(0).unwrap();
    let before_index = encoded(&store);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            command.id().cluster_epoch(),
            command.id().pg_id(),
            MetadataCommandLogIndex::new(command.id().log_index().get() + 1).unwrap(),
        ),
        command.payload().clone(),
    );
    store.conn.execute_batch("CREATE TEMP TRIGGER reject_route_index BEFORE UPDATE ON metadata_command_pending_route_dependencies BEGIN SELECT RAISE(ABORT, 'injected route index failure'); END;").unwrap();
    assert!(store
        .test_replace_pending_metadata_command_slot(&command, None)
        .is_err());
    assert_eq!(
        store.pending_metadata_command_slot_any_epoch(0).unwrap(),
        before
    );
    assert_eq!(encoded(&store), before_index);
    store
        .conn
        .execute_batch("DROP TRIGGER reject_route_index;")
        .unwrap();
    store
        .test_replace_pending_metadata_command_slot(&command, None)
        .unwrap();
    assert!(store
        .pending_metadata_command_route_references(0)
        .unwrap()
        .unwrap()
        .dependencies
        .is_empty());
    assert!(store.test_clear_pending_metadata_command_slot().unwrap());
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM metadata_command_pending_route_dependencies",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}
