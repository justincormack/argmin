use super::*;

impl ShardStore for PgStore {
    fn write_shard(&self, key: &ShardKey, data: &[u8]) -> Result<WriteAck, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::write_shard",
            "pg_id={} shard={} bytes={}",
            self.pg_id,
            key,
            data.len()
        );
        let ack = Self::write_shard_file_durable(&self.tmp_dir, &self.shards_dir, key, data)?;
        self.register_written_shard(key, ack)?;
        Ok(ack)
    }

    fn read_shard(&self, key: &ShardKey) -> Result<ShardData, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::read_shard",
            "pg_id={} shard={}",
            self.pg_id,
            key
        );
        // Look up shard in SQLite.
        let row: Option<(i64, i64, i64)> = self
            .conn
            .query_row(
                "SELECT crc64_nvme, data_size, status FROM shards WHERE shard_key = ?1",
                params![key.as_bytes().as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|e| StoreError::Db {
                context: "lookup shard",
                source: e,
            })?;

        let (expected_crc, _size, status) = row.ok_or(StoreError::NotFound)?;
        let expected_crc = expected_crc as u64;

        // Only serve live shards.
        if status != ShardStatus::Live as i64 {
            return Err(StoreError::NotFound);
        }

        // Read file.
        let shard_path = self.shard_path(key);
        let data = fs::read(&shard_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                return StoreError::NotFound;
            }
            StoreError::Io {
                context: "read shard file",
                source: e,
            }
        })?;

        // Verify CRC.
        let actual_crc = checksum::crc64::checksum(&data);
        if actual_crc != expected_crc {
            // Quarantine the shard.
            let _ = self.conn.execute(
                "UPDATE shards SET status = ?1 WHERE shard_key = ?2",
                params![ShardStatus::Quarantined as u8, key.as_bytes().as_slice()],
            );
            return Err(StoreError::IntegrityError {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        Ok(ShardData {
            data,
            crc64: actual_crc,
        })
    }

    fn delete_shard(&self, key: &ShardKey) -> Result<(), StoreError> {
        // Mark as deleting in SQLite.
        let updated = self
            .conn
            .execute(
                "UPDATE shards SET status = ?1 WHERE shard_key = ?2",
                params![ShardStatus::Deleting as u8, key.as_bytes().as_slice()],
            )
            .map_err(|e| StoreError::Db {
                context: "mark shard deleting",
                source: e,
            })?;

        // Unlink the file (ignore ENOENT for idempotency).
        let shard_path = self.shard_path(key);
        match fs::remove_file(&shard_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(StoreError::Io {
                    context: "unlink shard file",
                    source: e,
                });
            }
        }

        // Remove the SQLite record.
        if updated > 0 {
            self.conn
                .execute(
                    "DELETE FROM shards WHERE shard_key = ?1",
                    params![key.as_bytes().as_slice()],
                )
                .map_err(|e| StoreError::Db {
                    context: "delete shard record",
                    source: e,
                })?;
        }

        Ok(())
    }

    fn stat_shard(&self, key: &ShardKey) -> Result<ShardStat, StoreError> {
        let row: Option<(i64, i64, i64, Option<i64>, i64)> = self
            .conn
            .query_row(
                "SELECT data_size, crc64_nvme, created_at, last_verified, status \
                 FROM shards WHERE shard_key = ?1",
                params![key.as_bytes().as_slice()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| StoreError::Db {
                context: "stat shard",
                source: e,
            })?;

        let (size, crc, created_at, last_verified, status) = row.ok_or(StoreError::NotFound)?;

        if status != ShardStatus::Live as i64 {
            return Err(StoreError::NotFound);
        }

        Ok(ShardStat {
            size: size as u64,
            crc64: crc as u64,
            created_at: created_at as u64,
            last_verified: last_verified.map(|v| v as u64),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stat_shard_after_quarantine() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        let key = ShardKey::new(&[0xAA; 16], 0, 0);
        store.write_shard(&key, b"hello").unwrap();

        // Corrupt the shard file on disk
        let shard_path = store.shard_path(&key);
        fs::write(&shard_path, b"corrupt data!!").unwrap();

        // Read should fail with integrity error and quarantine the shard
        let err = store.read_shard(&key).unwrap_err();
        assert!(matches!(err, StoreError::IntegrityError { .. }));

        // stat_shard should now return NotFound (quarantined)
        let err = store.stat_shard(&key).unwrap_err();
        assert!(matches!(err, StoreError::NotFound));
    }

    // ── stat_shard on live shard ──────────────────────────────────────

    #[test]
    fn stat_shard_live() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        let key = ShardKey::new(&[0xBB; 16], 1, 0);
        store.write_shard(&key, b"data").unwrap();

        let stat = store.stat_shard(&key).unwrap();
        assert_eq!(stat.size, 4);
        assert_eq!(stat.crc64, checksum::crc64::checksum(b"data"));
    }

    #[test]
    fn shard_scavenger_observation_is_location_keyed_and_non_authoritative() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();

        let shard_key = ShardKey::new(&[0xCA; 16], 42, 3);
        let ack = store.write_shard(&shard_key, b"candidate").unwrap();
        let node_zero = ShardScavengerObservationKey {
            node_id: 0,
            data_pg_id: 7,
            shard_index: shard_key.shard_index(),
            shard_key: shard_key.clone(),
        };
        let node_one = ShardScavengerObservationKey {
            node_id: 1,
            data_pg_id: 7,
            shard_index: shard_key.shard_index(),
            shard_key: shard_key.clone(),
        };

        store
            .record_shard_scavenger_observation(&ShardScavengerObservationRecord {
                key: node_zero.clone(),
                data_size: Some(ack.stored_size),
                crc64: Some(ack.crc64),
                file_exists: true,
                shard_row_exists: true,
                reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
                last_error: None,
            })
            .unwrap();
        store
            .record_shard_scavenger_observation(&ShardScavengerObservationRecord {
                key: node_zero.clone(),
                data_size: Some(ack.stored_size),
                crc64: Some(ack.crc64),
                file_exists: true,
                shard_row_exists: true,
                reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
                last_error: Some("second scan".to_owned()),
            })
            .unwrap();
        store
            .record_shard_scavenger_observation(&ShardScavengerObservationRecord {
                key: node_one.clone(),
                data_size: Some(ack.stored_size),
                crc64: Some(ack.crc64),
                file_exists: true,
                shard_row_exists: true,
                reason: ShardScavengerObservationReason::UnreferencedShardRowAndFile,
                last_error: None,
            })
            .unwrap();

        let observations = store.list_shard_scavenger_observations().unwrap();
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].key, node_zero);
        assert_eq!(observations[0].observation_count, 2);
        assert_eq!(observations[0].last_error.as_deref(), Some("second scan"));
        assert_eq!(observations[1].key, node_one);
        assert_eq!(observations[1].observation_count, 1);

        let stat = store.stat_shard(&shard_key).unwrap();
        assert_eq!(stat.size, ack.stored_size);
        assert_eq!(stat.crc64, ack.crc64);

        assert!(store
            .resolve_shard_scavenger_observation(&node_zero)
            .unwrap());
        let observations = store.list_shard_scavenger_observations().unwrap();
        assert!(observations[0].resolved_at.is_some());
        assert!(observations[1].resolved_at.is_none());
    }

    #[test]
    fn shard_scavenger_observation_rejects_inconsistent_location_identity() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let shard_key = ShardKey::new(&[0xCB; 16], 42, 3);

        let wrong_pg = ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id: 0,
                data_pg_id: 8,
                shard_index: shard_key.shard_index(),
                shard_key: shard_key.clone(),
            },
            data_size: None,
            crc64: None,
            file_exists: true,
            shard_row_exists: false,
            reason: ShardScavengerObservationReason::FileWithoutShardRow,
            last_error: None,
        };
        let err = store
            .record_shard_scavenger_observation(&wrong_pg)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerObservationWrongPg {
                store_pg_id: 7,
                observation_pg_id: 8
            }
        ));

        let wrong_index = ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id: 0,
                data_pg_id: 7,
                shard_index: ShardIndex::new(4),
                shard_key,
            },
            data_size: None,
            crc64: None,
            file_exists: true,
            shard_row_exists: false,
            reason: ShardScavengerObservationReason::FileWithoutShardRow,
            last_error: None,
        };
        let err = store
            .record_shard_scavenger_observation(&wrong_index)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerObservationShardIndexMismatch {
                observation_shard_index: 4,
                key_shard_index: 3
            }
        ));
        assert!(store
            .list_shard_scavenger_observations()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn shard_scavenger_observation_rejects_inconsistent_reason_state() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let shard_key = ShardKey::new(&[0xCC; 16], 42, 3);

        let inconsistent = ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id: 0,
                data_pg_id: 7,
                shard_index: shard_key.shard_index(),
                shard_key: shard_key.clone(),
            },
            data_size: None,
            crc64: None,
            file_exists: true,
            shard_row_exists: true,
            reason: ShardScavengerObservationReason::FileWithoutShardRow,
            last_error: None,
        };
        let err = store
            .record_shard_scavenger_observation(&inconsistent)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerObservationInconsistentReason {
                reason: ShardScavengerObservationReason::FileWithoutShardRow,
                file_exists: true,
                shard_row_exists: true
            }
        ));

        let scan_incomplete = ShardScavengerObservationRecord {
            key: ShardScavengerObservationKey {
                node_id: 0,
                data_pg_id: 7,
                shard_index: shard_key.shard_index(),
                shard_key,
            },
            data_size: None,
            crc64: None,
            file_exists: true,
            shard_row_exists: true,
            reason: ShardScavengerObservationReason::ScanIncomplete,
            last_error: Some("metadata pg unavailable".to_owned()),
        };
        store
            .record_shard_scavenger_observation(&scan_incomplete)
            .unwrap();
        let observations = store.list_shard_scavenger_observations().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(
            observations[0].reason,
            ShardScavengerObservationReason::ScanIncomplete
        );
    }

    #[test]
    fn shard_scavenger_local_audit_records_file_row_mismatches_without_deleting() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let file_without_row = ShardKey::new(&[0xCD; 16], 42, 1);
        let row_without_file = ShardKey::new(&[0xCE; 16], 42, 2);

        let file_without_row_ack = store.write_shard(&file_without_row, b"file-only").unwrap();
        store.delete_shard_record(&file_without_row).unwrap();
        let row_without_file_ack = store.write_shard(&row_without_file, b"row-only").unwrap();
        fs::remove_file(store.shard_path(&row_without_file)).unwrap();

        let observations = store.audit_local_shard_storage_for_scavenger(9).unwrap();
        assert_eq!(observations.len(), 2);
        let file_observation = observations
            .iter()
            .find(|observation| observation.key.shard_key == file_without_row)
            .unwrap();
        assert_eq!(
            file_observation.reason,
            ShardScavengerObservationReason::FileWithoutShardRow
        );
        assert!(file_observation.file_exists);
        assert!(!file_observation.shard_row_exists);
        assert_eq!(
            file_observation.data_size,
            Some(file_without_row_ack.stored_size)
        );

        let row_observation = observations
            .iter()
            .find(|observation| observation.key.shard_key == row_without_file)
            .unwrap();
        assert_eq!(
            row_observation.reason,
            ShardScavengerObservationReason::ShardRowWithoutFile
        );
        assert!(!row_observation.file_exists);
        assert!(row_observation.shard_row_exists);
        assert_eq!(row_observation.crc64, Some(row_without_file_ack.crc64));

        assert!(
            store.shard_path(&file_without_row).exists(),
            "audit-only scan must not delete file-only candidates"
        );

        store
            .register_written_shard(&file_without_row, file_without_row_ack)
            .unwrap();
        PgStore::write_shard_file_durable(
            &store.tmp_dir,
            &store.shards_dir,
            &row_without_file,
            b"row-only",
        )
        .unwrap();

        let observations = store.audit_local_shard_storage_for_scavenger(9).unwrap();
        assert_eq!(observations.len(), 2);
        assert!(
            observations
                .iter()
                .all(|observation| observation.resolved_at.is_some()),
            "later local consistency should resolve prior local mismatch observations"
        );
    }

    #[test]
    fn shard_scavenger_local_audit_does_not_make_negative_reference_claims() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let candidate_key = ShardKey::new(&[0xD1; 16], 42, 0);
        store
            .write_shard(&candidate_key, b"orphan-candidate")
            .unwrap();

        let observations = store.audit_local_shard_storage_for_scavenger(9).unwrap();
        assert!(
            observations.is_empty(),
            "PgStore-local audit can only prove local row/file mismatches; negative reference \
             classification needs a cluster-wide metadata PG scan"
        );
        assert!(
            store.shard_path(&candidate_key).exists(),
            "local audit must not delete row+file candidates"
        );
    }

    #[test]
    fn shard_scavenger_local_audit_requires_canonical_shard_file_paths() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let row_key = ShardKey::new(&[0xCF; 16], 42, 3);
        store.write_shard(&row_key, b"row").unwrap();
        fs::remove_file(store.shard_path(&row_key)).unwrap();

        let wrong_prefix = if row_key.hex_prefix() == "00" {
            "01"
        } else {
            "00"
        };
        let wrong_prefix_dir = store.shards_dir.join(wrong_prefix);
        fs::create_dir_all(&wrong_prefix_dir).unwrap();
        fs::write(wrong_prefix_dir.join(row_key.to_string()), b"wrong-prefix").unwrap();

        let err = store
            .audit_local_shard_storage_for_scavenger(9)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerScanIncomplete {
                context: "local shard file scan",
                ..
            }
        ));
        let observations = store.list_shard_scavenger_observations().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].key.shard_key, row_key);
        assert_eq!(
            observations[0].reason,
            ShardScavengerObservationReason::ShardRowWithoutFile
        );
        assert!(
            observations[0].resolved_at.is_none(),
            "non-canonical file must not satisfy or resolve the canonical shard row"
        );
    }

    #[test]
    fn shard_scavenger_local_audit_surfaces_malformed_shard_files() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let malformed_dir = store.shards_dir.join("aa");
        fs::create_dir_all(&malformed_dir).unwrap();
        fs::write(malformed_dir.join("not-a-shard-key"), b"junk").unwrap();

        let err = store
            .audit_local_shard_storage_for_scavenger(9)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerScanIncomplete {
                context: "local shard file scan",
                ..
            }
        ));
        assert!(store
            .list_shard_scavenger_observations()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn shard_scavenger_local_audit_surfaces_unexpected_shard_tree_entries() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let misplaced_key = ShardKey::new(&[0xD0; 16], 42, 4);
        fs::write(
            store.shards_dir.join(misplaced_key.to_string()),
            b"misplaced",
        )
        .unwrap();
        let canonical_dir = store.shards_dir.join(misplaced_key.hex_prefix());
        fs::create_dir_all(&canonical_dir).unwrap();
        fs::create_dir(canonical_dir.join("nested")).unwrap();

        let err = store
            .audit_local_shard_storage_for_scavenger(9)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardScavengerScanIncomplete {
                context: "local shard file scan",
                ..
            }
        ));
        assert!(store
            .list_shard_scavenger_observations()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn validate_written_shard_ack_rejects_missing_or_mismatched_rows() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let key = ShardKey::new(&[0xD2; 16], 42, 5);
        let ack = store.write_shard(&key, b"payload").unwrap();
        store.validate_written_shard_ack(&key, ack).unwrap();

        store
            .conn
            .execute(
                "UPDATE shards SET data_size = ?1 WHERE shard_key = ?2",
                rusqlite::params![(ack.stored_size + 1) as i64, key.as_bytes().as_slice()],
            )
            .unwrap();
        let err = store.validate_written_shard_ack(&key, ack).unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardAckMismatch {
                expected_size,
                actual_size,
                ..
            } if expected_size == ack.stored_size && actual_size == ack.stored_size + 1
        ));

        store.delete_shard_record(&key).unwrap();
        assert!(matches!(
            store.validate_written_shard_ack(&key, ack),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn register_written_shards_batch_exact_is_idempotent_but_not_overwriting() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 7).unwrap();
        let key = ShardKey::new(&[0xD3; 16], 42, 6);
        let ack = WriteAck {
            stored_size: 10,
            crc64: 0xD3D3,
        };
        let batch = vec![(&key, ack)];

        store.register_written_shards_batch_exact(&batch).unwrap();
        store.register_written_shards_batch_exact(&batch).unwrap();
        store.validate_written_shard_ack(&key, ack).unwrap();

        let different = WriteAck {
            stored_size: ack.stored_size + 1,
            crc64: ack.crc64,
        };
        let err = store
            .register_written_shards_batch_exact(&[(&key, different)])
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ShardAckMismatch {
                expected_size,
                actual_size,
                ..
            } if expected_size == different.stored_size && actual_size == ack.stored_size
        ));
        store.validate_written_shard_ack(&key, ack).unwrap();
    }

    // ── connection accessor ───────────────────────────────────────────
}
