// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// fsync a directory to ensure renames are durable.
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    let f = fs::File::open(dir)?;
    f.sync_all()?;
    Ok(())
}

fn prepare_durable_shard_prefix<S>(
    shards_dir: &Path,
    shard_path: &Path,
    sync_shards_dir: S,
) -> Result<(), StoreError>
where
    S: FnOnce(&Path) -> std::io::Result<()>,
{
    let prefix_dir = shard_path
        .parent()
        .expect("canonical shard paths always have a prefix directory");
    match fs::create_dir(prefix_dir) {
        Ok(()) => {}
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(prefix_dir).map_err(|source| StoreError::Io {
                context: "inspect shard prefix dir",
                source,
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(StoreError::Io {
                    context: "inspect shard prefix dir",
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "shard prefix path is not a real directory",
                    ),
                });
            }
        }
        Err(source) => {
            return Err(StoreError::Io {
                context: "create shard prefix dir",
                source,
            });
        }
    }

    // A concurrent writer can observe a newly created prefix before its
    // creator has synced `shards/`. Sync on every publication attempt so no
    // writer can acknowledge a shard through an unconfirmed prefix entry.
    sync_shards_dir(shards_dir).map_err(|source| StoreError::Io {
        context: "fsync shard root dir",
        source,
    })
}

impl PgStore {
    #[cfg(test)]
    pub(crate) fn test_delete_shard_row(&self, key: &ShardKey) -> Result<(), StoreError> {
        let changed = self
            .conn
            .execute(
                "DELETE FROM shards WHERE shard_key = ?1",
                params![key.as_bytes().as_slice()],
            )
            .map_err(|source| StoreError::Db {
                context: "delete shard row for test",
                source: source.into(),
            })?;
        if changed != 1 {
            return Err(StoreError::IntegrityError {
                expected: 1,
                actual: changed as u64,
            });
        }
        Ok(())
    }

    /// Resolve the file path for a shard key.
    fn shard_path(&self, key: &ShardKey) -> PathBuf {
        Self::shard_path_for_shards_dir(&self.shards_dir, key)
    }

    pub(crate) fn shard_path_for_shards_dir(shards_dir: &Path, key: &ShardKey) -> PathBuf {
        let hex = key.hex_bytes();
        let prefix =
            std::str::from_utf8(&hex[..SHARD_KEY_HEX_PREFIX_LEN]).expect("shard key hex is ASCII");
        let full = std::str::from_utf8(&hex).expect("shard key hex is ASCII");
        let mut path = shards_dir.to_path_buf();
        path.push(prefix);
        path.push(full);
        path
    }

    pub(crate) fn write_shard_file_durable(
        tmp_dir: &Path,
        shards_dir: &Path,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        Self::write_shard_file_durable_with_prefix_sync(tmp_dir, shards_dir, key, data, fsync_dir)
    }

    fn write_shard_file_durable_with_prefix_sync<S>(
        tmp_dir: &Path,
        shards_dir: &Path,
        key: &ShardKey,
        data: &[u8],
        sync_shards_dir: S,
    ) -> Result<WriteAck, StoreError>
    where
        S: FnOnce(&Path) -> std::io::Result<()>,
    {
        let crc = checksum::crc64::checksum(data);
        let stored_size = data.len() as u64;
        let shard_path = Self::shard_path_for_shards_dir(shards_dir, key);
        prepare_durable_shard_prefix(shards_dir, &shard_path, sync_shards_dir)?;

        // Write to temp file with O_EXCL (unique name via pid + timestamp).
        let tmp_name = format!("shard-{}-{}-{key}", std::process::id(), Self::now_secs());
        let tmp_path = tmp_dir.join(&tmp_name);

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(|e| StoreError::Io {
                context: "create temp shard file",
                source: e,
            })?;

        file.write_all(data).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            StoreError::Io {
                context: "write shard data",
                source: e,
            }
        })?;

        // fdatasync the file data (sync_data = fdatasync on Linux).
        file.sync_data().map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            StoreError::Io {
                context: "fdatasync shard",
                source: e,
            }
        })?;

        // Atomic rename.
        fs::rename(&tmp_path, &shard_path).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            StoreError::Io {
                context: "rename shard into place",
                source: e,
            }
        })?;

        // fsync parent directory to ensure rename is durable.
        if let Some(parent) = shard_path.parent() {
            fsync_dir(parent).map_err(|e| StoreError::Io {
                context: "fsync shard parent dir",
                source: e,
            })?;
        }

        Ok(WriteAck {
            crc64: crc,
            stored_size,
        })
    }

    pub(crate) fn write_shard_file_durable_if_absent(
        tmp_dir: &Path,
        shards_dir: &Path,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        Self::write_shard_file_durable_if_absent_with_directory_syncs(
            tmp_dir, shards_dir, key, data, fsync_dir, fsync_dir,
        )
    }

    fn write_shard_file_durable_if_absent_with_directory_syncs<R, P>(
        tmp_dir: &Path,
        shards_dir: &Path,
        key: &ShardKey,
        data: &[u8],
        sync_shards_dir: R,
        mut sync_prefix_dir: P,
    ) -> Result<WriteAck, StoreError>
    where
        R: FnOnce(&Path) -> std::io::Result<()>,
        P: FnMut(&Path) -> std::io::Result<()>,
    {
        let expected = WriteAck {
            crc64: checksum::crc64::checksum(data),
            stored_size: data.len() as u64,
        };
        let shard_path = Self::shard_path_for_shards_dir(shards_dir, key);
        prepare_durable_shard_prefix(shards_dir, &shard_path, sync_shards_dir)?;

        for _ in 0..2 {
            let sequence = SHARD_TMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let tmp_name = format!("shard-{}-{sequence}-{key}", std::process::id());
            let tmp_path = tmp_dir.join(&tmp_name);
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)
                .map_err(|e| StoreError::Io {
                    context: "create temp shard file",
                    source: e,
                })?;

            file.write_all(data).map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                StoreError::Io {
                    context: "write shard data",
                    source: e,
                }
            })?;
            file.sync_data().map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                StoreError::Io {
                    context: "fdatasync shard",
                    source: e,
                }
            })?;
            drop(file);

            match fs::hard_link(&tmp_path, &shard_path) {
                Ok(()) => {
                    let _ = fs::remove_file(&tmp_path);
                    if let Some(parent) = shard_path.parent() {
                        sync_prefix_dir(parent).map_err(|e| StoreError::Io {
                            context: "fsync shard parent dir",
                            source: e,
                        })?;
                    }
                    return Ok(expected);
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let _ = fs::remove_file(&tmp_path);
                    match fs::read(&shard_path) {
                        Ok(existing) => {
                            let actual = WriteAck {
                                crc64: checksum::crc64::checksum(&existing),
                                stored_size: existing.len() as u64,
                            };
                            if actual.stored_size == expected.stored_size
                                && actual.crc64 == expected.crc64
                            {
                                if let Some(parent) = shard_path.parent() {
                                    sync_prefix_dir(parent).map_err(|e| StoreError::Io {
                                        context: "fsync shard parent dir",
                                        source: e,
                                    })?;
                                }
                                return Ok(actual);
                            }
                            return Err(StoreError::ShardAckMismatch {
                                shard: key.clone(),
                                expected_size: expected.stored_size,
                                expected_crc: expected.crc64,
                                actual_size: actual.stored_size,
                                actual_crc: actual.crc64,
                            });
                        }
                        Err(read_error) if read_error.kind() == std::io::ErrorKind::NotFound => {
                            continue;
                        }
                        Err(read_error) => {
                            return Err(StoreError::Io {
                                context: "read existing shard file",
                                source: read_error,
                            });
                        }
                    }
                }
                Err(e) => {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(StoreError::Io {
                        context: "link shard into place",
                        source: e,
                    });
                }
            }
        }

        Err(StoreError::NotFound)
    }

    pub fn register_written_shard(&self, key: &ShardKey, ack: WriteAck) -> Result<(), StoreError> {
        let now = Self::now_secs();
        self.conn
            .execute(
                "INSERT OR REPLACE INTO shards (shard_key, data_size, crc64_nvme, created_at, status) \
                 VALUES (?1, ?2, ?3, ?4, 0)",
                params![
                    key.as_bytes().as_slice(),
                    ack.stored_size as i64,
                    ack.crc64 as i64,
                    now as i64
                ],
            )
            .map_err(|e| StoreError::Db {
                context: "insert shard record",
                source: e.into(),
            })?;
        Ok(())
    }

    pub fn validate_written_shard_ack(
        &self,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<(), StoreError> {
        let row: Option<(i64, i64, i64)> = self
            .conn
            .query_row(
                "SELECT data_size, crc64_nvme, status FROM shards WHERE shard_key = ?1",
                params![key.as_bytes().as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|e| StoreError::Db {
                context: "validate written shard ack",
                source: e.into(),
            })?;
        let Some((actual_size, actual_crc, status)) = row else {
            return Err(StoreError::NotFound);
        };
        if status != ShardStatus::Live as i64 {
            return Err(StoreError::NotFound);
        }
        let actual_size = actual_size as u64;
        let actual_crc = actual_crc as u64;
        if actual_size != expected.stored_size || actual_crc != expected.crc64 {
            return Err(StoreError::ShardAckMismatch {
                shard: key.clone(),
                expected_size: expected.stored_size,
                expected_crc: expected.crc64,
                actual_size,
                actual_crc,
            });
        }
        Ok(())
    }

    pub(crate) fn delete_shard_record(&self, key: &ShardKey) -> Result<(), StoreError> {
        self.conn
            .execute(
                "DELETE FROM shards WHERE shard_key = ?1",
                params![key.as_bytes().as_slice()],
            )
            .map_err(|e| StoreError::Db {
                context: "delete shard record",
                source: e.into(),
            })?;
        Ok(())
    }

    pub fn register_written_shards_batch(
        &self,
        shards: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::register_written_shards_batch",
            "pg_id={} shards={}",
            self.pg_id,
            shards.len()
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| StoreError::Db {
                context: "register written shards batch (begin txn)",
                source: e.into(),
            })?;

        let now = Self::now_secs() as i64;
        let result: Result<(), StoreError> = (|| {
            let mut stmt = self
                .conn
                .prepare_cached(
                    "INSERT OR REPLACE INTO shards (shard_key, data_size, crc64_nvme, created_at, status) \
                     VALUES (?1, ?2, ?3, ?4, 0)",
                )
                .map_err(|e| StoreError::Db {
                    context: "register written shards batch (prepare)",
                    source: e.into(),
                })?;

            for (key, ack) in shards {
                stmt.execute(params![
                    key.as_bytes().as_slice(),
                    ack.stored_size as i64,
                    ack.crc64 as i64,
                    now,
                ])
                .map_err(|e| StoreError::Db {
                    context: "register written shards batch (insert shard record)",
                    source: e.into(),
                })?;
            }

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(StoreError::Db {
                        context: "register written shards batch (commit txn)",
                        source: e.into(),
                    });
                }
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn register_written_shards_batch_exact(
        &self,
        shards: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::register_written_shards_batch_exact",
            "pg_id={} shards={}",
            self.pg_id,
            shards.len()
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| StoreError::Db {
                context: "register written shards batch exact (begin txn)",
                source: e.into(),
            })?;

        let now = Self::now_secs() as i64;
        let result: Result<(), StoreError> = (|| {
            let mut lookup = self
                .conn
                .prepare_cached(
                    "SELECT data_size, crc64_nvme, status FROM shards WHERE shard_key = ?1",
                )
                .map_err(|e| StoreError::Db {
                    context: "register written shards batch exact (prepare lookup)",
                    source: e.into(),
                })?;
            let mut insert = self
                .conn
                .prepare_cached(
                    "INSERT INTO shards (shard_key, data_size, crc64_nvme, created_at, status) \
                     VALUES (?1, ?2, ?3, ?4, 0)",
                )
                .map_err(|e| StoreError::Db {
                    context: "register written shards batch exact (prepare insert)",
                    source: e.into(),
                })?;

            for (key, ack) in shards {
                let row: Option<(i64, i64, i64)> = lookup
                    .query_row(params![key.as_bytes().as_slice()], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .optional()
                    .map_err(|e| StoreError::Db {
                        context: "register written shards batch exact (lookup shard record)",
                        source: e.into(),
                    })?;

                if let Some((actual_size, actual_crc, status)) = row {
                    let actual_size = actual_size as u64;
                    let actual_crc = actual_crc as u64;
                    if status == ShardStatus::Live as i64
                        && actual_size == ack.stored_size
                        && actual_crc == ack.crc64
                    {
                        continue;
                    }
                    return Err(StoreError::ShardAckMismatch {
                        shard: (*key).clone(),
                        expected_size: ack.stored_size,
                        expected_crc: ack.crc64,
                        actual_size,
                        actual_crc,
                    });
                }

                insert
                    .execute(params![
                        key.as_bytes().as_slice(),
                        ack.stored_size as i64,
                        ack.crc64 as i64,
                        now,
                    ])
                    .map_err(|e| StoreError::Db {
                        context: "register written shards batch exact (insert shard record)",
                        source: e.into(),
                    })?;
            }

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(StoreError::Db {
                        context: "register written shards batch exact (commit txn)",
                        source: e.into(),
                    });
                }
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn register_written_shards_and_append_stream_segment(
        &self,
        shards: &[(&ShardKey, WriteAck)],
        segment: &StreamUploadSegmentRecord,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::register_written_shards_and_append_stream_segment",
            "pg_id={} session_id={:?} segment_index={} shards={}",
            self.pg_id,
            segment.session_id,
            segment.segment_index,
            shards.len()
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "append stream segment with shard publish (begin txn)",
                source: e.into(),
            })?;

        let now = Self::now_secs() as i64;
        let result: Result<(), MetadataError> = (|| {
            let mut shard_stmt = self
                .conn
                .prepare_cached(
                    "INSERT OR REPLACE INTO shards (shard_key, data_size, crc64_nvme, created_at, status) \
                     VALUES (?1, ?2, ?3, ?4, 0)",
                )
                .map_err(|e| MetadataError::Db {
                    context: "append stream segment with shard publish (prepare shard insert)",
                    source: e.into(),
                })?;

            for (key, ack) in shards {
                shard_stmt
                    .execute(params![
                        key.as_bytes().as_slice(),
                        ack.stored_size as i64,
                        ack.crc64 as i64,
                        now,
                    ])
                    .map_err(|e| MetadataError::Db {
                        context: "append stream segment with shard publish (insert shard record)",
                        source: e.into(),
                    })?;
            }

            self.conn
                .execute(
                    "INSERT INTO stream_upload_segments \
                     (session_id, segment_index, size, segment_crc64, payload_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        segment.session_id,
                        segment.segment_index,
                        segment.size as i64,
                        segment.segment_crc64 as i64,
                        segment.payload_crc64 as i64,
                        segment.segment_okh.as_slice(),
                        segment.segment_vid.get() as i64,
                        segment.data_pg_id,
                        segment.placement_cluster_epoch.get() as i64,
                        segment.ec_k,
                        segment.ec_m,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "append stream segment with shard publish (insert segment)",
                    source: e.into(),
                })?;

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "append stream segment with shard publish (commit txn)",
                        source: e.into(),
                    });
                }
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }
}

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
                source: e.into(),
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
                source: e.into(),
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
                    source: e.into(),
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
                source: e.into(),
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
    fn first_renamed_shard_requires_durable_prefix_and_retry_reestablishes_barrier() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();
        let key = ShardKey::new(&[0xC1; 16], 17, 0);
        let shard_path = store.shard_path(&key);
        let prefix_dir = shard_path.parent().unwrap().to_path_buf();
        assert!(!prefix_dir.exists());

        let error = PgStore::write_shard_file_durable_with_prefix_sync(
            &store.tmp_dir,
            &store.shards_dir,
            &key,
            b"first renamed shard",
            |shards_dir| {
                assert_eq!(shards_dir, store.shards_dir);
                assert!(prefix_dir.is_dir());
                assert!(!shard_path.exists());
                Err(std::io::Error::other("injected shard-root sync failure"))
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            StoreError::Io {
                context: "fsync shard root dir",
                source,
            } if source.kind() == std::io::ErrorKind::Other
        ));
        assert!(!shard_path.exists());
        assert!(fs::read_dir(&store.tmp_dir).unwrap().next().is_none());

        let retry_synced_existing_prefix = std::cell::Cell::new(false);
        let ack = PgStore::write_shard_file_durable_with_prefix_sync(
            &store.tmp_dir,
            &store.shards_dir,
            &key,
            b"first renamed shard",
            |shards_dir| {
                assert!(prefix_dir.is_dir());
                retry_synced_existing_prefix.set(true);
                fsync_dir(shards_dir)
            },
        )
        .unwrap();
        assert!(retry_synced_existing_prefix.get());
        store.register_written_shard(&key, ack).unwrap();
        drop(store);

        let reopened = PgStore::open(tmp.path(), 0).unwrap();
        assert_eq!(
            reopened.read_shard(&key).unwrap().data,
            b"first renamed shard"
        );
    }

    #[test]
    fn first_linked_shard_requires_durable_prefix_before_publication() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();
        let key = ShardKey::new(&[0xD1; 16], 19, 0);
        let shard_path = store.shard_path(&key);
        let prefix_dir = shard_path.parent().unwrap().to_path_buf();
        assert!(!prefix_dir.exists());

        let error = PgStore::write_shard_file_durable_if_absent_with_directory_syncs(
            &store.tmp_dir,
            &store.shards_dir,
            &key,
            b"first linked shard",
            |_| Err(std::io::Error::other("injected shard-root sync failure")),
            fsync_dir,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            StoreError::Io {
                context: "fsync shard root dir",
                source,
            } if source.kind() == std::io::ErrorKind::Other
        ));
        assert!(prefix_dir.is_dir());
        assert!(!shard_path.exists());
        assert!(fs::read_dir(&store.tmp_dir).unwrap().next().is_none());

        let ack = PgStore::write_shard_file_durable_if_absent(
            &store.tmp_dir,
            &store.shards_dir,
            &key,
            b"first linked shard",
        )
        .unwrap();
        store.register_written_shard(&key, ack).unwrap();
        drop(store);

        let reopened = PgStore::open(tmp.path(), 0).unwrap();
        assert_eq!(
            reopened.read_shard(&key).unwrap().data,
            b"first linked shard"
        );
    }

    #[test]
    fn matching_link_writer_establishes_its_own_durability_barrier() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();
        let key = ShardKey::new(&[0xD2; 16], 20, 0);
        let shard_path = store.shard_path(&key);
        let prefix_dir = shard_path.parent().unwrap().to_path_buf();
        let payload = b"concurrent matching shard";

        let writer_tmp_dir = store.tmp_dir.clone();
        let writer_shards_dir = store.shards_dir.clone();
        let writer_key = key.clone();
        let writer_shard_path = shard_path.clone();
        let writer_prefix_dir = prefix_dir.clone();
        let (linked_tx, linked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first_writer = std::thread::spawn(move || {
            PgStore::write_shard_file_durable_if_absent_with_directory_syncs(
                &writer_tmp_dir,
                &writer_shards_dir,
                &writer_key,
                payload,
                fsync_dir,
                |parent| {
                    assert_eq!(parent, writer_prefix_dir);
                    assert!(writer_shard_path.exists());
                    linked_tx.send(()).unwrap();
                    release_rx
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    Err(std::io::Error::other(
                        "injected first-writer prefix sync failure",
                    ))
                },
            )
        });

        linked_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(shard_path.exists());

        let matching_writer_synced_prefix = std::cell::Cell::new(false);
        let ack = PgStore::write_shard_file_durable_if_absent_with_directory_syncs(
            &store.tmp_dir,
            &store.shards_dir,
            &key,
            payload,
            fsync_dir,
            |parent| {
                assert_eq!(parent, prefix_dir);
                matching_writer_synced_prefix.set(true);
                fsync_dir(parent)
            },
        )
        .unwrap();
        assert!(matching_writer_synced_prefix.get());

        release_tx.send(()).unwrap();
        let first_error = first_writer.join().unwrap().unwrap_err();
        assert!(matches!(
            first_error,
            StoreError::Io {
                context: "fsync shard parent dir",
                source,
            } if source.kind() == std::io::ErrorKind::Other
        ));

        store.register_written_shard(&key, ack).unwrap();
        drop(store);

        let reopened = PgStore::open(tmp.path(), 0).unwrap();
        assert_eq!(reopened.read_shard(&key).unwrap().data, payload);
    }

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
