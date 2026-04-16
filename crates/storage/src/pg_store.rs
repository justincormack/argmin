/// PgStore — per-PG shard I/O and object metadata, backed by filesystem + SQLite.
///
/// Owns a single `rusqlite::Connection` to the per-PG `metadata.db` and manages
/// shard files under the PG directory.
///
/// Directory layout:
/// ```text
/// pg-NNNN/
///   metadata.db
///   shards/<hex_prefix>/<shard_key_hex>
///   tmp/
/// ```
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension};

use crate::error::{MetadataError, StoreError};
use crate::schema::init_pg_schema;
use crate::traits::{PgMetadataStore, ShardStore};
use crate::types::*;

const TRACE_TARGET: &str = "storage";
const LIFECYCLE_SUBRESOURCE_KIND_SQL: i64 = BucketSubresourceKind::Lifecycle as u8 as i64;
const BUCKET_INFO_SELECT: &str = "\
SELECT name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, write_reservations_blocked, active_write_reservations, \
       public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 4 AND body IS NOT NULL) AS bucket_policy_present, \
       bucket_policy_public, bucket_policy_generation, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 5 AND body IS NOT NULL) AS bucket_lifecycle_present, \
       bucket_lifecycle_generation, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years \
FROM buckets";

/// Part segment rows use a sentinel version_id during staging (pre-CompleteMultipartUpload).
/// Must differ from any real version_id (0 for unversioned, 1+ for versioned) so that
/// in-progress staging rows are invisible to reads of completed objects.
const PART_SEGMENT_STAGING_VERSION_ID: VersionId = MULTIPART_PART_SEGMENT_STAGING_VERSION_ID;
type StreamSessionRow = (u8, u8, BucketName, ObjectKey, Option<UploadId>, Option<i64>);
type PublicAccessBlockSqlValues = (i64, i64, i64, i64, i64);
type BucketObjectLockSqlValues = (i64, Option<u8>, Option<i64>, Option<i64>);
type ObjectLockSqlValues = (Option<u8>, Option<i64>, u8);

#[cfg(test)]
fn trusted_bucket_name(name: impl Into<String>) -> BucketName {
    BucketName::try_from(name.into())
        .expect("pg_store must only construct BucketName from validated values")
}

fn bucket_not_found(name: &str) -> MetadataError {
    match BucketName::try_from(name) {
        Ok(name) => MetadataError::BucketNotFound { name },
        Err(error) => MetadataError::InvalidBucketName {
            reason: error.to_string(),
        },
    }
}

#[cfg(test)]
fn trusted_object_key(key: impl Into<String>) -> ObjectKey {
    ObjectKey::try_from(key.into())
        .expect("pg_store must only construct ObjectKey from validated values")
}

struct PreparedShardFile {
    key: ShardKey,
    ack: WriteAck,
    tmp_path: PathBuf,
    shard_path: PathBuf,
}

/// Per-PG store combining shard file I/O with SQLite metadata.
pub struct PgStore {
    pg_id: u32,
    shards_dir: PathBuf,
    tmp_dir: PathBuf,
    conn: Connection,
}

impl PgStore {
    /// Open (or create) a PG store at the given directory.
    ///
    /// Creates `shards/` and `tmp/` subdirectories if they don't exist.
    /// Initializes the SQLite schema (idempotent).
    pub fn open(pg_dir: &Path, pg_id: u32) -> Result<Self, StoreError> {
        let shards_dir = pg_dir.join("shards");
        let tmp_dir = pg_dir.join("tmp");

        fs::create_dir_all(&shards_dir).map_err(|e| StoreError::Io {
            context: "create shards dir",
            source: e,
        })?;
        fs::create_dir_all(&tmp_dir).map_err(|e| StoreError::Io {
            context: "create tmp dir",
            source: e,
        })?;

        let db_path = pg_dir.join("metadata.db");
        let conn = Connection::open(&db_path).map_err(|e| StoreError::Db {
            context: "open pg database",
            source: e,
        })?;

        init_pg_schema(&conn).map_err(|e| StoreError::Db {
            context: "init pg schema",
            source: e,
        })?;

        // Clean up any orphaned temp files from previous crashes.
        if let Ok(entries) = fs::read_dir(&tmp_dir) {
            for entry in entries.flatten() {
                let _ = fs::remove_file(entry.path());
            }
        }

        Ok(Self {
            pg_id,
            shards_dir,
            tmp_dir,
            conn,
        })
    }

    /// Return the PG ID.
    pub fn pg_id(&self) -> u32 {
        self.pg_id
    }

    /// Return a reference to the underlying SQLite connection.
    pub fn connection(&self) -> &Connection {
        &self.conn
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

    /// Get the current unix timestamp in seconds.
    fn now_secs() -> u64 {
        crate::clock::current_time_secs()
    }

    /// Get the current unix timestamp in milliseconds.
    fn now_millis() -> u64 {
        crate::clock::current_time_millis()
    }

    pub(crate) fn write_shard_file_durable(
        tmp_dir: &Path,
        shards_dir: &Path,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        let crc = checksum::crc64::checksum(data);
        let stored_size = data.len() as u64;

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

        // Ensure prefix subdirectory exists.
        let shard_path = Self::shard_path_for_shards_dir(shards_dir, key);
        if let Some(parent) = shard_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                StoreError::Io {
                    context: "create shard prefix dir",
                    source: e,
                }
            })?;
        }

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

    pub(crate) fn write_shard_files_durable(
        tmp_dir: &Path,
        shards_dir: &Path,
        shards: &[(ShardKey, &[u8])],
    ) -> Result<Vec<(ShardKey, WriteAck)>, StoreError> {
        let mut prepared: Vec<PreparedShardFile> = Vec::with_capacity(shards.len());
        let mut parents_to_fsync: Vec<PathBuf> = Vec::new();

        let cleanup = |prepared: &[PreparedShardFile]| {
            for shard in prepared {
                let _ = fs::remove_file(&shard.tmp_path);
                let _ = fs::remove_file(&shard.shard_path);
            }
        };

        for (key, data) in shards {
            let crc = checksum::crc64::checksum(data);
            let stored_size = data.len() as u64;
            let tmp_name = format!("shard-{}-{}-{key}", std::process::id(), Self::now_secs());
            let tmp_path = tmp_dir.join(&tmp_name);
            let shard_path = Self::shard_path_for_shards_dir(shards_dir, key);

            let Some(parent) = shard_path.parent() else {
                cleanup(&prepared);
                return Err(StoreError::Io {
                    context: "resolve shard prefix dir",
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "shard path missing parent directory",
                    ),
                });
            };

            fs::create_dir_all(parent).map_err(|e| {
                cleanup(&prepared);
                StoreError::Io {
                    context: "create shard prefix dir",
                    source: e,
                }
            })?;

            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)
                .map_err(|e| {
                    cleanup(&prepared);
                    StoreError::Io {
                        context: "create temp shard file",
                        source: e,
                    }
                })?;

            file.write_all(data).map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                cleanup(&prepared);
                StoreError::Io {
                    context: "write shard data",
                    source: e,
                }
            })?;

            file.sync_data().map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                cleanup(&prepared);
                StoreError::Io {
                    context: "fdatasync shard",
                    source: e,
                }
            })?;

            if !parents_to_fsync.iter().any(|p| p == parent) {
                parents_to_fsync.push(parent.to_path_buf());
            }

            prepared.push(PreparedShardFile {
                key: key.clone(),
                ack: WriteAck {
                    crc64: crc,
                    stored_size,
                },
                tmp_path,
                shard_path,
            });
        }

        for shard in &prepared {
            fs::rename(&shard.tmp_path, &shard.shard_path).map_err(|e| {
                cleanup(&prepared);
                StoreError::Io {
                    context: "rename shard into place",
                    source: e,
                }
            })?;
        }

        for parent in &parents_to_fsync {
            fsync_dir(parent).map_err(|e| {
                cleanup(&prepared);
                StoreError::Io {
                    context: "fsync shard parent dir",
                    source: e,
                }
            })?;
        }

        Ok(prepared
            .into_iter()
            .map(|shard| (shard.key, shard.ack))
            .collect())
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
                source: e,
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
                source: e,
            })?;

        let now = Self::now_secs() as i64;
        let result: Result<(), StoreError> = (|| {
            let mut stmt = self
                .conn
                .prepare(
                    "INSERT OR REPLACE INTO shards (shard_key, data_size, crc64_nvme, created_at, status) \
                     VALUES (?1, ?2, ?3, ?4, 0)",
                )
                .map_err(|e| StoreError::Db {
                    context: "register written shards batch (prepare)",
                    source: e,
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
                    source: e,
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
                        source: e,
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
                source: e,
            })?;

        let now = Self::now_secs() as i64;
        let result: Result<(), MetadataError> = (|| {
            let mut shard_stmt = self
                .conn
                .prepare(
                    "INSERT OR REPLACE INTO shards (shard_key, data_size, crc64_nvme, created_at, status) \
                     VALUES (?1, ?2, ?3, ?4, 0)",
                )
                .map_err(|e| MetadataError::Db {
                    context: "append stream segment with shard publish (prepare shard insert)",
                    source: e,
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
                        source: e,
                    })?;
            }

            self.conn
                .execute(
                    "INSERT INTO stream_upload_segments \
                     (session_id, segment_index, size, segment_crc64, segment_okh, segment_vid, shard_pg_id, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        segment.session_id,
                        segment.segment_index,
                        segment.size as i64,
                        segment.segment_crc64.map(|v| v as i64),
                        segment.segment_okh.as_slice(),
                        segment.segment_vid.get() as i64,
                        segment.shard_pg_id,
                        segment.ec_k,
                        segment.ec_m,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "append stream segment with shard publish (insert segment)",
                    source: e,
                })?;

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "append stream segment with shard publish (commit txn)",
                        source: e,
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

    fn row_to_object_part(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObjectPartRecord> {
        let part_okh = Self::blob_to_okh(row.get(7)?, 7)?;
        let checksum = row
            .get::<_, Option<Vec<u8>>>(12)?
            .map(|blob| Self::blob_to_checksum(blob, 12))
            .transpose()?;
        Ok(ObjectPartRecord {
            bucket: row.get(0)?,
            key: row.get(1)?,
            version_id: PgStore::parse_version_id(row.get::<_, i64>(2)?, 2)?,
            part_number: row.get::<_, i64>(3)? as u32,
            size: row.get::<_, i64>(4)? as u64,
            etag: row.get(5)?,
            etag_kind: Self::parse_enum(row.get::<_, u8>(6)?, 6, "etag_kind", EtagKind::from_u8)?,
            part_okh,
            part_vid: Self::parse_generation_id(row.get::<_, i64>(8)?, 8, "part_vid")?,
            ec_k: row.get::<_, u8>(9)?,
            ec_m: row.get::<_, u8>(10)?,
            shard_pg_id: row.get::<_, i64>(11)? as u32,
            checksum,
        })
    }

    fn row_to_object_part_range(
        row: &rusqlite::Row<'_>,
    ) -> rusqlite::Result<ObjectPartRangeRecord> {
        Ok(ObjectPartRangeRecord {
            part: Self::row_to_object_part(row)?,
            object_offset_start: row.get::<_, i64>(13)? as u64,
        })
    }

    /// Convert a BLOB to a 16-byte object key hash, failing on wrong length.
    fn blob_to_okh(blob: Vec<u8>, col: usize) -> Result<[u8; 16], rusqlite::Error> {
        let len = blob.len();
        blob.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col,
                rusqlite::types::Type::Blob,
                Box::from(format!("invalid part_okh length: {len} (expected 16)")),
            )
        })
    }

    /// Map a row with columns (upload_id, part_number, generation, size, etag,
    /// etag_kind, part_okh, part_vid, ec_k, ec_m, last_modified) to a
    /// MultipartPartRecord.
    fn row_to_multipart_part(
        row: &rusqlite::Row<'_>,
    ) -> Result<MultipartPartRecord, rusqlite::Error> {
        let part_okh = Self::blob_to_okh(row.get(6)?, 6)?;
        let checksum = row
            .get::<_, Option<Vec<u8>>>(11)?
            .map(|blob| Self::blob_to_checksum(blob, 11))
            .transpose()?;
        Ok(MultipartPartRecord {
            upload_id: row.get(0)?,
            part_number: row.get::<_, i64>(1)? as u32,
            generation: row.get::<_, i64>(2)? as u32,
            size: row.get::<_, i64>(3)? as u64,
            etag: row.get(4)?,
            etag_kind: Self::parse_enum(row.get::<_, u8>(5)?, 5, "etag_kind", EtagKind::from_u8)?,
            part_okh,
            part_vid: Self::parse_generation_id(row.get::<_, i64>(7)?, 7, "part_vid")?,
            ec_k: row.get::<_, u8>(8)?,
            ec_m: row.get::<_, u8>(9)?,
            last_modified: row.get::<_, i64>(10)? as u64,
            checksum,
        })
    }

    fn blob_to_checksum(
        blob: Vec<u8>,
        col: usize,
    ) -> Result<checksum::ChecksumBytes, rusqlite::Error> {
        let len = blob.len();
        checksum::ChecksumBytes::new(&blob).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col,
                rusqlite::types::Type::Blob,
                Box::from(format!("invalid checksum length: {len} (expected 1..=32)")),
            )
        })
    }

    /// Parse a 16-byte blob into a fixed-size array, returning a typed DB error
    /// instead of panicking if the length is wrong.
    fn parse_okh_blob(blob: &[u8], col_idx: usize) -> Result<[u8; 16], rusqlite::Error> {
        blob.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Blob,
                Box::from(format!("expected 16-byte segment_okh, got {}", blob.len())),
            )
        })
    }

    fn parse_generation_id(
        raw: i64,
        col_idx: usize,
        field_name: &'static str,
    ) -> Result<GenerationId, rusqlite::Error> {
        let value = u64::try_from(raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Integer,
                Box::from(format!("negative {field_name}: {raw}")),
            )
        })?;
        GenerationId::new(value).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid zero {field_name}")),
            )
        })
    }

    fn parse_stream_target(
        op_kind_raw: u8,
        upload_id: Option<UploadId>,
        part_number: Option<i64>,
        op_kind_col: usize,
    ) -> Result<StreamUploadTarget, rusqlite::Error> {
        let op_kind = StreamUploadKind::from_u8(op_kind_raw).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                op_kind_col,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid op_kind: {op_kind_raw}")),
            )
        })?;

        match op_kind {
            StreamUploadKind::PutObject => {
                if upload_id.is_some() || part_number.is_some() {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        op_kind_col,
                        rusqlite::types::Type::Integer,
                        Box::from("PutObject stream session must not carry upload_id/part_number"),
                    ));
                }
                Ok(StreamUploadTarget::PutObject)
            }
            StreamUploadKind::UploadPart => {
                let upload_id = upload_id.ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        op_kind_col,
                        rusqlite::types::Type::Integer,
                        Box::from("UploadPart stream session missing upload_id"),
                    )
                })?;
                let pn_i64 = part_number.ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        op_kind_col,
                        rusqlite::types::Type::Integer,
                        Box::from("UploadPart stream session missing part_number"),
                    )
                })?;
                if !(1..=10_000).contains(&pn_i64) {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        op_kind_col,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("invalid UploadPart part_number: {pn_i64}")),
                    ));
                }
                Ok(StreamUploadTarget::UploadPart {
                    upload_id,
                    part_number: pn_i64 as u32,
                })
            }
        }
    }

    fn parse_optional_u32(
        value: Option<i64>,
        col: usize,
        field: &str,
    ) -> Result<Option<u32>, rusqlite::Error> {
        value
            .map(|v| {
                u32::try_from(v).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        col,
                        rusqlite::types::Type::Integer,
                        Box::from(format!(
                            "invalid {field}: {v} (expected integer in 0..={})",
                            u32::MAX
                        )),
                    )
                })
            })
            .transpose()
    }

    fn parse_u32(value: i64, col: usize, field: &str) -> Result<u32, rusqlite::Error> {
        u32::try_from(value).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col,
                rusqlite::types::Type::Integer,
                Box::from(format!(
                    "invalid {field}: {value} (expected integer in 0..={})",
                    u32::MAX
                )),
            )
        })
    }

    fn parse_optional_u64(
        value: Option<i64>,
        col: usize,
        field: &str,
    ) -> Result<Option<u64>, rusqlite::Error> {
        value
            .map(|v| {
                u64::try_from(v).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        col,
                        rusqlite::types::Type::Integer,
                        Box::from(format!(
                            "invalid {field}: {v} (expected integer in 0..={})",
                            u64::MAX
                        )),
                    )
                })
            })
            .transpose()
    }

    fn parse_bucket_object_lock(
        (enabled_raw, default_mode_raw, default_days_raw, default_years_raw): BucketObjectLockSqlValues,
        [enabled_col, mode_col, days_col, years_col]: [usize; 4],
    ) -> Result<BucketObjectLockConfig, rusqlite::Error> {
        let enabled = match enabled_raw {
            0 => false,
            1 => true,
            _ => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    enabled_col,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid object_lock_enabled: {enabled_raw}")),
                ));
            }
        };
        let default_mode = default_mode_raw
            .map(|raw| {
                Self::parse_enum(
                    raw,
                    mode_col,
                    "object_lock_default_mode",
                    ObjectLockMode::from_u8,
                )
            })
            .transpose()?;
        let default_days =
            Self::parse_optional_u32(default_days_raw, days_col, "object_lock_default_days")?;
        let default_years =
            Self::parse_optional_u32(default_years_raw, years_col, "object_lock_default_years")?;
        let default_retention = match (default_mode, default_days, default_years) {
            (None, None, None) => None,
            (Some(mode), Some(days), None) => Some(ObjectLockDefaultRetention {
                mode,
                period: RetentionPeriod::days(days).ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        days_col,
                        rusqlite::types::Type::Integer,
                        Box::from("object_lock_default_days must be > 0"),
                    )
                })?,
            }),
            (Some(mode), None, Some(years)) => Some(ObjectLockDefaultRetention {
                mode,
                period: RetentionPeriod::years(years).ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        years_col,
                        rusqlite::types::Type::Integer,
                        Box::from("object_lock_default_years must be > 0"),
                    )
                })?,
            }),
            _ => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    mode_col,
                    rusqlite::types::Type::Integer,
                    Box::from("invalid bucket object lock default retention state"),
                ));
            }
        };
        Ok(BucketObjectLockConfig {
            enabled,
            default_retention,
        })
    }

    fn parse_public_access_block(
        (
            present_raw,
            block_public_acls_raw,
            ignore_public_acls_raw,
            block_public_policy_raw,
            restrict_public_buckets_raw,
        ): PublicAccessBlockSqlValues,
        [present_col, block_public_acls_col, ignore_public_acls_col, block_public_policy_col, restrict_public_buckets_col]: [usize; 5],
    ) -> Result<Option<PublicAccessBlockConfig>, rusqlite::Error> {
        fn parse_flag(raw: i64, col: usize, field: &'static str) -> Result<bool, rusqlite::Error> {
            match raw {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(rusqlite::Error::FromSqlConversionFailure(
                    col,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid {field}: {raw}")),
                )),
            }
        }

        if !parse_flag(present_raw, present_col, "public_access_block_present")? {
            return Ok(None);
        }

        Ok(Some(PublicAccessBlockConfig {
            block_public_acls: parse_flag(
                block_public_acls_raw,
                block_public_acls_col,
                "public_access_block_block_public_acls",
            )?,
            ignore_public_acls: parse_flag(
                ignore_public_acls_raw,
                ignore_public_acls_col,
                "public_access_block_ignore_public_acls",
            )?,
            block_public_policy: parse_flag(
                block_public_policy_raw,
                block_public_policy_col,
                "public_access_block_block_public_policy",
            )?,
            restrict_public_buckets: parse_flag(
                restrict_public_buckets_raw,
                restrict_public_buckets_col,
                "public_access_block_restrict_public_buckets",
            )?,
        }))
    }

    fn parse_ownership_controls(
        raw: Option<i64>,
        col: usize,
    ) -> Result<Option<BucketOwnershipControls>, rusqlite::Error> {
        let raw = match raw {
            Some(raw) => raw,
            None => return Ok(None),
        };
        let raw = u8::try_from(raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid ownership_controls_mode: {raw}")),
            )
        })?;
        let object_ownership = BucketObjectOwnership::from_u8(raw).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                col,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid ownership_controls_mode: {raw}")),
            )
        })?;
        Ok(Some(BucketOwnershipControls { object_ownership }))
    }

    fn parse_object_lock_state(
        retention_mode_raw: Option<u8>,
        retain_until_raw: Option<i64>,
        legal_hold_raw: u8,
        mode_col: usize,
        retain_until_col: usize,
        legal_hold_col: usize,
    ) -> Result<ObjectLockState, rusqlite::Error> {
        let retention_mode = retention_mode_raw
            .map(|raw| {
                Self::parse_enum(
                    raw,
                    mode_col,
                    "object_lock_retention_mode",
                    ObjectLockMode::from_u8,
                )
            })
            .transpose()?;
        let retain_until = Self::parse_optional_u64(
            retain_until_raw,
            retain_until_col,
            "object_lock_retain_until",
        )?;
        let legal_hold = Self::parse_enum(
            legal_hold_raw,
            legal_hold_col,
            "object_lock_legal_hold",
            StoredLegalHoldStatus::from_u8,
        )?;
        let retention = match (retention_mode, retain_until) {
            (None, None) => None,
            (Some(mode), Some(retain_until_unix_seconds)) if retain_until_unix_seconds > 0 => {
                Some(ObjectRetention {
                    retain_until_unix_seconds,
                    mode,
                })
            }
            (Some(_), Some(_)) => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    retain_until_col,
                    rusqlite::types::Type::Integer,
                    Box::from("object_lock_retain_until must be > 0"),
                ));
            }
            _ => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    mode_col,
                    rusqlite::types::Type::Integer,
                    Box::from("invalid object lock retention state"),
                ));
            }
        };
        Ok(ObjectLockState {
            retention,
            legal_hold,
        })
    }

    fn bucket_object_lock_sql_values(
        config: BucketObjectLockConfig,
    ) -> Result<BucketObjectLockSqlValues, rusqlite::Error> {
        let enabled = i64::from(config.enabled);
        let (default_mode, default_days, default_years) = match config.default_retention {
            None => (None, None, None),
            Some(default_retention) => {
                let (days, years) = match default_retention.period {
                    RetentionPeriod::Days(days) => (Some(i64::from(days.get())), None),
                    RetentionPeriod::Years(years) => (None, Some(i64::from(years.get()))),
                };
                (Some(default_retention.mode as u8), days, years)
            }
        };
        Ok((enabled, default_mode, default_days, default_years))
    }

    fn object_lock_sql_values(
        object_lock: ObjectLockState,
    ) -> Result<ObjectLockSqlValues, rusqlite::Error> {
        let (retention_mode, retain_until) = match object_lock.retention {
            None => (None, None),
            Some(retention) => {
                let retain_until =
                    i64::try_from(retention.retain_until_unix_seconds).map_err(|_| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                            format!(
                                "object lock retain-until exceeds SQLite INTEGER: {}",
                                retention.retain_until_unix_seconds
                            ),
                        )))
                    })?;
                (Some(retention.mode as u8), Some(retain_until))
            }
        };
        Ok((retention_mode, retain_until, object_lock.legal_hold as u8))
    }

    fn public_access_block_sql_values(
        config: Option<PublicAccessBlockConfig>,
    ) -> PublicAccessBlockSqlValues {
        match config {
            None => (0, 0, 0, 0, 0),
            Some(config) => (
                1,
                i64::from(config.block_public_acls),
                i64::from(config.ignore_public_acls),
                i64::from(config.block_public_policy),
                i64::from(config.restrict_public_buckets),
            ),
        }
    }

    fn ownership_controls_sql_value(config: Option<BucketOwnershipControls>) -> Option<i64> {
        config.map(|config| config.object_ownership as u8 as i64)
    }

    fn row_to_bucket_info(row: &rusqlite::Row<'_>) -> Result<BucketInfo, rusqlite::Error> {
        let owner_canonical_id_raw: String = row.get(2)?;
        let owner_canonical_id =
            Self::parse_canonical_user_id(owner_canonical_id_raw, 2, "owner_canonical_id")?;
        let public_access_block = Self::parse_public_access_block(
            (
                row.get::<_, i64>(12)?,
                row.get::<_, i64>(13)?,
                row.get::<_, i64>(14)?,
                row.get::<_, i64>(15)?,
                row.get::<_, i64>(16)?,
            ),
            [12, 13, 14, 15, 16],
        )?;
        let ownership_controls = Self::parse_ownership_controls(row.get(17)?, 17)?;
        let object_lock = Self::parse_bucket_object_lock(
            (
                row.get::<_, i64>(25)?,
                row.get::<_, Option<u8>>(26)?,
                row.get::<_, Option<i64>>(27)?,
                row.get::<_, Option<i64>>(28)?,
            ),
            [25, 26, 27, 28],
        )?;
        let acl_grants = Self::parse_acl_grants(row.get::<_, String>(7)?, 7, "acl_grants")?;
        Ok(BucketInfo {
            name: row.get(0)?,
            owner_principal: row.get(1)?,
            owner_canonical_id,
            created_at: row.get::<_, i64>(3)? as u64,
            region: row.get::<_, i64>(4)? as u16,
            state: Self::parse_enum(row.get::<_, u8>(5)?, 5, "state", BucketState::from_u8)?,
            versioning: Self::parse_enum(
                row.get::<_, u8>(6)?,
                6,
                "versioning",
                BucketVersioningState::from_u8,
            )?,
            object_lock,
            acl_grants,
            public_read: row.get::<_, i64>(8)? != 0,
            public_write: row.get::<_, i64>(9)? != 0,
            write_reservations_blocked: row.get::<_, i64>(10)? != 0,
            active_write_reservations: Self::parse_u32(
                row.get::<_, i64>(11)?,
                11,
                "active_write_reservations",
            )?,
            public_access_block,
            ownership_controls,
            bucket_policy_present: row.get::<_, i64>(18)? != 0,
            bucket_policy_public: row.get::<_, i64>(19)? != 0,
            bucket_policy_generation: row.get::<_, i64>(20)? as u64,
            bucket_lifecycle_present: row.get::<_, i64>(21)? != 0,
            bucket_lifecycle_generation: row.get::<_, i64>(22)? as u64,
            encryption: BucketEncryptionConfig {
                default_encryption: row
                    .get::<_, Option<u8>>(23)?
                    .map(|value| {
                        ManagedEncryptionAlgorithm::from_u8(value).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                23,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid default_encryption_type: {value}")),
                            )
                        })
                    })
                    .transpose()?,
                sse_c_blocked: row.get::<_, i64>(24)? != 0,
            }
            .effective(),
        })
    }

    fn ensure_bucket_exists(&self, name: &str, context: &'static str) -> Result<(), MetadataError> {
        self.conn
            .query_row(
                "SELECT 1 FROM buckets WHERE name = ?1",
                params![name],
                |_| Ok(()),
            )
            .optional()
            .map_err(|source| MetadataError::Db { context, source })?
            .ok_or_else(|| bucket_not_found(name))
    }

    fn bucket_subresource_invalid_aux(
        kind: BucketSubresourceKind,
        aux: BucketSubresourceAux,
    ) -> MetadataError {
        MetadataError::Db {
            context: "put bucket subresource",
            source: rusqlite::Error::InvalidParameterName(format!(
                "{kind:?} does not support aux {aux:?}"
            )),
        }
    }

    fn bucket_subresource_aux_int_1_to_sql(aux: BucketSubresourceAux) -> Option<i64> {
        match aux {
            BucketSubresourceAux::None => None,
            BucketSubresourceAux::Policy { is_public, .. } => Some(i64::from(is_public)),
        }
    }

    fn bucket_subresource_aux_from_sql(
        kind: BucketSubresourceKind,
        raw_int_1: Option<i64>,
    ) -> Result<BucketSubresourceAux, rusqlite::Error> {
        match kind {
            BucketSubresourceKind::Policy => match raw_int_1 {
                Some(0 | 1) => Ok(BucketSubresourceAux::policy(raw_int_1 == Some(1))),
                Some(value) => Err(rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid policy aux_int_1: {value}")),
                )),
                None => Err(rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Null,
                    Box::from("missing policy aux_int_1"),
                )),
            },
            _ => match raw_int_1 {
                None => Ok(BucketSubresourceAux::None),
                Some(value) => Err(rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("unexpected aux_int_1 for {kind:?}: {value}")),
                )),
            },
        }
    }

    fn parse_bucket_subresource_generation(
        raw: i64,
        col_idx: usize,
    ) -> Result<u64, rusqlite::Error> {
        if raw < 0 {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Integer,
                Box::from(format!(
                    "invalid negative bucket subresource generation: {raw}"
                )),
            ));
        }
        Ok(raw as u64)
    }

    fn put_bucket_subresource_internal(
        &self,
        name: &str,
        kind: BucketSubresourceKind,
        body: &str,
        aux: BucketSubresourceAux,
    ) -> Result<(), MetadataError> {
        if !kind.supports_aux(aux) {
            return Err(Self::bucket_subresource_invalid_aux(kind, aux));
        }
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put bucket subresource (begin txn)",
                source: e,
            })?;
        let result: Result<(), MetadataError> = (|| {
            self.ensure_bucket_exists(name, "put bucket subresource (check bucket exists)")?;

            match kind {
                BucketSubresourceKind::Cors | BucketSubresourceKind::Tagging => {}
                BucketSubresourceKind::Policy => {
                    self.conn
                        .execute(
                            "UPDATE buckets SET bucket_policy_public = ?1 WHERE name = ?2",
                            params![i32::from(aux.policy_is_public().unwrap()), name],
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "put bucket subresource (update bucket policy summary)",
                            source: e,
                        })?;
                }
                BucketSubresourceKind::Lifecycle => {}
            }

            self.conn
                .execute(
                    "INSERT INTO bucket_subresources (bucket_name, kind, body, generation, aux_int_1) \
                     VALUES (?1, ?2, ?3, 1, ?4) \
                     ON CONFLICT(bucket_name, kind) DO UPDATE SET \
                         body = excluded.body, \
                         generation = bucket_subresources.generation + 1, \
                         aux_int_1 = excluded.aux_int_1",
                    params![
                        name,
                        kind as u8 as i64,
                        body,
                        Self::bucket_subresource_aux_int_1_to_sql(aux),
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put bucket subresource (upsert subresource row)",
                    source: e,
                })?;

            let generation = self
                .conn
                .query_row(
                    "SELECT generation FROM bucket_subresources \
                     WHERE bucket_name = ?1 AND kind = ?2",
                    params![name, kind as u8 as i64],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(|e| MetadataError::Db {
                    context: "put bucket subresource (load generation)",
                    source: e,
                })
                .and_then(|raw| {
                    Self::parse_bucket_subresource_generation(raw, 0).map_err(|source| {
                        MetadataError::Db {
                            context: "put bucket subresource (parse generation)",
                            source,
                        }
                    })
                })?;

            match kind {
                BucketSubresourceKind::Policy => {
                    self.conn
                        .execute(
                            "UPDATE buckets SET bucket_policy_generation = ?1 WHERE name = ?2",
                            params![generation as i64, name],
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "put bucket subresource (update policy generation mirror)",
                            source: e,
                        })?;
                }
                BucketSubresourceKind::Lifecycle => {
                    self.conn
                        .execute(
                            "UPDATE buckets SET bucket_lifecycle_generation = ?1 WHERE name = ?2",
                            params![generation as i64, name],
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "put bucket subresource (update lifecycle generation mirror)",
                            source: e,
                        })?;
                }
                _ => {}
            }

            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn
                    .execute_batch("COMMIT")
                    .map_err(|e| MetadataError::Db {
                        context: "put bucket subresource (commit txn)",
                        source: e,
                    })?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn delete_bucket_subresource_internal(
        &self,
        name: &str,
        kind: BucketSubresourceKind,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "delete bucket subresource (begin txn)",
                source: e,
            })?;
        let result: Result<(), MetadataError> = (|| {
            self.ensure_bucket_exists(name, "delete bucket subresource (check bucket exists)")?;

            match kind {
                BucketSubresourceKind::Cors | BucketSubresourceKind::Tagging => {}
                BucketSubresourceKind::Policy => {
                    self.conn
                        .execute(
                            "UPDATE buckets SET bucket_policy_public = 0 WHERE name = ?1",
                            params![name],
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "delete bucket subresource (clear bucket policy summary)",
                            source: e,
                        })?;
                }
                BucketSubresourceKind::Lifecycle => {}
            }

            self.conn
                .execute(
                    "INSERT INTO bucket_subresources (bucket_name, kind, body, generation, aux_int_1) \
                     VALUES (?1, ?2, NULL, 1, NULL) \
                     ON CONFLICT(bucket_name, kind) DO UPDATE SET \
                         body = NULL, \
                         generation = bucket_subresources.generation + 1, \
                         aux_int_1 = NULL",
                    params![name, kind as u8 as i64],
                )
                .map_err(|e| MetadataError::Db {
                    context: "delete bucket subresource (tombstone subresource row)",
                    source: e,
                })?;

            let generation = self
                .conn
                .query_row(
                    "SELECT generation FROM bucket_subresources \
                     WHERE bucket_name = ?1 AND kind = ?2",
                    params![name, kind as u8 as i64],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(|e| MetadataError::Db {
                    context: "delete bucket subresource (load generation)",
                    source: e,
                })
                .and_then(|raw| {
                    Self::parse_bucket_subresource_generation(raw, 0).map_err(|source| {
                        MetadataError::Db {
                            context: "delete bucket subresource (parse generation)",
                            source,
                        }
                    })
                })?;

            match kind {
                BucketSubresourceKind::Policy => {
                    self.conn
                        .execute(
                            "UPDATE buckets SET bucket_policy_generation = ?1 WHERE name = ?2",
                            params![generation as i64, name],
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "delete bucket subresource (update policy generation mirror)",
                            source: e,
                        })?;
                }
                BucketSubresourceKind::Lifecycle => {
                    self.conn
                        .execute(
                            "UPDATE buckets SET bucket_lifecycle_generation = ?1 WHERE name = ?2",
                            params![generation as i64, name],
                        )
                        .map_err(|e| MetadataError::Db {
                            context:
                                "delete bucket subresource (update lifecycle generation mirror)",
                            source: e,
                        })?;
                }
                _ => {}
            }

            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn
                    .execute_batch("COMMIT")
                    .map_err(|e| MetadataError::Db {
                        context: "delete bucket subresource (commit txn)",
                        source: e,
                    })?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn get_bucket_subresource_internal(
        &self,
        name: &str,
        kind: BucketSubresourceKind,
    ) -> Result<Option<StoredBucketSubresource>, MetadataError> {
        self.conn
            .query_row(
                "SELECT s.body, s.generation, s.aux_int_1 \
                 FROM buckets b \
                 LEFT JOIN bucket_subresources s \
                   ON s.bucket_name = b.name AND s.kind = ?2 \
                 WHERE b.name = ?1",
                params![name, kind as u8 as i64],
                |row| {
                    let body = row.get::<_, Option<String>>(0)?;
                    let generation = row.get::<_, Option<i64>>(1)?;
                    let aux_int_1 = row.get::<_, Option<i64>>(2)?;
                    match body {
                        Some(body) => Ok(Some(StoredBucketSubresource {
                            body,
                            generation: Some(Self::parse_bucket_subresource_generation(
                                generation.ok_or_else(|| {
                                    rusqlite::Error::FromSqlConversionFailure(
                                        1,
                                        rusqlite::types::Type::Null,
                                        Box::from("missing bucket subresource generation"),
                                    )
                                })?,
                                1,
                            )?),
                            aux: Self::bucket_subresource_aux_from_sql(kind, aux_int_1)?,
                        })),
                        None => Ok(None),
                    }
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket subresource",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name))
    }

    /// Map a row with columns (bucket, key, version_id, generation_id, size,
    /// etag, etag_kind, last_modified, storage_class, ec_k, ec_m, status,
    /// tags, data_layout, parts_count, metadata_blob, system_metadata_blob,
    /// encryption_type, encryption_state, owner_principal, owner_canonical_id,
    /// acl_grants, public_read, object_lock_retention_mode,
    /// object_lock_retain_until, object_lock_legal_hold, became_noncurrent_at)
    /// to a StoredObject.
    fn parse_canonical_user_id(
        raw: String,
        col_idx: usize,
        field_name: &'static str,
    ) -> Result<CanonicalUserId, rusqlite::Error> {
        CanonicalUserId::parse_stored(&raw).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Text,
                Box::from(format!("invalid {field_name}: {raw}")),
            )
        })
    }

    fn parse_owner_identity(
        row: &rusqlite::Row<'_>,
        principal_col: usize,
        canonical_col: usize,
        principal_field: &'static str,
        canonical_field: &'static str,
    ) -> Result<OwnerIdentity, rusqlite::Error> {
        let principal: String = row.get(principal_col)?;
        if principal.is_empty() {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                principal_col,
                rusqlite::types::Type::Text,
                Box::from(format!("invalid empty {principal_field}")),
            ));
        }
        let canonical_raw: String = row.get(canonical_col)?;
        let canonical_id =
            Self::parse_canonical_user_id(canonical_raw, canonical_col, canonical_field)?;
        Ok(OwnerIdentity::new(principal, canonical_id))
    }

    fn parse_optional_owner_identity(
        row: &rusqlite::Row<'_>,
        principal_col: usize,
        canonical_col: usize,
        principal_field: &'static str,
        canonical_field: &'static str,
    ) -> Result<Option<OwnerIdentity>, rusqlite::Error> {
        let principal: Option<String> = row.get(principal_col)?;
        let canonical_raw: Option<String> = row.get(canonical_col)?;
        match (principal, canonical_raw) {
            (None, None) => Ok(None),
            (Some(principal), Some(canonical_raw)) => {
                if principal.is_empty() {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        principal_col,
                        rusqlite::types::Type::Text,
                        Box::from(format!("invalid empty {principal_field}")),
                    ));
                }
                let canonical_id =
                    Self::parse_canonical_user_id(canonical_raw, canonical_col, canonical_field)?;
                Ok(Some(OwnerIdentity::new(principal, canonical_id)))
            }
            _ => Err(rusqlite::Error::FromSqlConversionFailure(
                principal_col,
                rusqlite::types::Type::Text,
                Box::from(format!(
                    "inconsistent optional owner identity for {principal_field}/{canonical_field}"
                )),
            )),
        }
    }

    fn parse_acl_grants(
        raw: String,
        col_idx: usize,
        field_name: &'static str,
    ) -> Result<AclGrants, rusqlite::Error> {
        AclGrants::parse(&raw).map_err(|msg| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Text,
                Box::from(format!("invalid {field_name}: {msg}")),
            )
        })
    }

    /// Parse a u8-backed enum from a row column.
    fn parse_enum<T>(
        raw: u8,
        col_idx: usize,
        name: &str,
        from_u8: fn(u8) -> Option<T>,
    ) -> Result<T, rusqlite::Error> {
        from_u8(raw).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid {name}: {raw}")),
            )
        })
    }

    /// Parse a version_id from a signed i64 column with checked conversion.
    fn parse_version_id(raw: i64, col_idx: usize) -> Result<VersionId, rusqlite::Error> {
        let v = u64::try_from(raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Integer,
                Box::from(format!("negative version_id: {raw}")),
            )
        })?;
        Ok(VersionId::from_u64(v))
    }

    fn parse_object_encryption(
        raw_type: u8,
        raw_state: Option<Vec<u8>>,
        type_col_idx: usize,
        state_col_idx: usize,
    ) -> Result<ObjectEncryption, rusqlite::Error> {
        let encryption_type = Self::parse_enum(
            raw_type,
            type_col_idx,
            "encryption_type",
            ObjectEncryptionType::from_u8,
        )?;
        ObjectEncryption::decode(encryption_type, raw_state).map_err(|msg| {
            rusqlite::Error::FromSqlConversionFailure(
                state_col_idx,
                rusqlite::types::Type::Blob,
                Box::from(msg),
            )
        })
    }

    fn row_to_object_record(row: &rusqlite::Row<'_>) -> Result<StoredObject, rusqlite::Error> {
        let status = Self::parse_enum(row.get::<_, u8>(11)?, 11, "status", ObjectState::from_u8)?;
        let bucket: BucketName = row.get(0)?;
        let key: ObjectKey = row.get(1)?;
        let version_id = Self::parse_version_id(row.get::<_, i64>(2)?, 2)?;
        let last_modified = row.get::<_, i64>(7)? as u64;
        let owner =
            Self::parse_owner_identity(row, 19, 20, "owner_principal", "owner_canonical_id")?;
        let acl_grants = Self::parse_acl_grants(row.get::<_, String>(21)?, 21, "acl_grants")?;
        let public_read = row.get::<_, i64>(22)? != 0;
        let object_lock = Self::parse_object_lock_state(
            row.get::<_, Option<u8>>(23)?,
            row.get::<_, Option<i64>>(24)?,
            row.get::<_, u8>(25)?,
            23,
            24,
            25,
        )?;
        let became_noncurrent_at =
            Self::parse_optional_u64(row.get::<_, Option<i64>>(26)?, 26, "became_noncurrent_at")?;

        match status {
            ObjectState::DeleteMarker => {
                let generation_id: Option<i64> = row.get(3)?;
                let size = row.get::<_, i64>(4)?;
                let etag: Vec<u8> = row.get(5)?;
                let etag_kind = row.get::<_, u8>(6)?;
                let storage_class = row.get::<_, u8>(8)?;
                let ec_k = row.get::<_, u8>(9)?;
                let ec_m = row.get::<_, u8>(10)?;
                let tags: Option<SerializedTagSet> = row
                    .get::<_, Option<String>>(12)?
                    .map(SerializedTagSet::from);
                let metadata_blob: Option<SerializedMetadataBlob> = row
                    .get::<_, Option<Vec<u8>>>(15)?
                    .map(SerializedMetadataBlob::from);
                let system_metadata_blob: Option<SerializedSystemMetadataBlob> = row
                    .get::<_, Option<Vec<u8>>>(16)?
                    .map(SerializedSystemMetadataBlob::from);
                let encryption = Self::parse_object_encryption(
                    row.get::<_, u8>(17)?,
                    row.get::<_, Option<Vec<u8>>>(18)?,
                    17,
                    18,
                )?;
                if size != 0
                    || generation_id.is_some()
                    || !etag.is_empty()
                    || etag_kind != 0
                    || storage_class != 0
                    || ec_k != 0
                    || ec_m != 0
                    || tags.is_some()
                    || metadata_blob.is_some()
                    || system_metadata_blob.is_some()
                    || encryption != ObjectEncryption::None
                    || !acl_grants.is_empty()
                    || public_read
                    || object_lock != ObjectLockState::default()
                {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        11,
                        rusqlite::types::Type::Integer,
                        Box::from("delete marker has non-canonical field values"),
                    ));
                }
                Ok(StoredObject::DeleteMarker(DeleteMarkerRecord {
                    bucket,
                    key,
                    version_id,
                    owner,
                    last_modified,
                }))
            }
            ObjectState::Live => {
                let generation_id =
                    Self::parse_generation_id(row.get::<_, i64>(3)?, 3, "generation_id")?;
                let etag_kind =
                    Self::parse_enum(row.get::<_, u8>(6)?, 6, "etag_kind", EtagKind::from_u8)?;
                let storage_class = Self::parse_enum(
                    row.get::<_, u8>(8)?,
                    8,
                    "storage_class",
                    StorageClass::from_u8,
                )?;
                let data_layout = Self::parse_enum(
                    row.get::<_, u8>(13)?,
                    13,
                    "data_layout",
                    DataLayout::from_u8,
                )?;
                let parts_count =
                    Self::parse_optional_u32(row.get::<_, Option<i64>>(14)?, 14, "parts_count")?;
                let layout = ObjectLayout::from_parts(data_layout, parts_count).map_err(|msg| {
                    rusqlite::Error::FromSqlConversionFailure(
                        14,
                        rusqlite::types::Type::Integer,
                        Box::from(msg),
                    )
                })?;

                let etag_bytes: Vec<u8> = row.get(5)?;
                let etag =
                    ObjectEtag::from_parts(&etag_bytes, etag_kind, parts_count).map_err(|msg| {
                        rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Blob,
                            Box::from(msg),
                        )
                    })?;
                let encryption = Self::parse_object_encryption(
                    row.get::<_, u8>(17)?,
                    row.get::<_, Option<Vec<u8>>>(18)?,
                    17,
                    18,
                )?;

                Ok(StoredObject::Live(LiveObjectRecord {
                    bucket,
                    key,
                    version_id,
                    owner,
                    acl_grants,
                    public_read,
                    generation_id,
                    size: row.get::<_, i64>(4)? as u64,
                    etag,
                    last_modified,
                    became_noncurrent_at,
                    storage_class,
                    ec: EcShape {
                        k: row.get::<_, u8>(9)?,
                        m: row.get::<_, u8>(10)?,
                    },
                    layout,
                    tags: row
                        .get::<_, Option<String>>(12)?
                        .map(SerializedTagSet::from),
                    metadata_blob: row
                        .get::<_, Option<Vec<u8>>>(15)?
                        .map(SerializedMetadataBlob::from),
                    system_metadata_blob: row
                        .get::<_, Option<Vec<u8>>>(16)?
                        .map(SerializedSystemMetadataBlob::from),
                    object_lock,
                    encryption,
                }))
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

impl PgStore {
    pub fn create_bucket_with_config(
        &self,
        config: &CreateBucketConfig<'_>,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_bucket_with_config",
            "pg_id={} bucket={:?} owner={} versioning={:?} object_lock_enabled={}",
            self.pg_id,
            config.name,
            config.owner_principal,
            config.versioning,
            config.object_lock.enabled
        );
        let now = PgStore::now_millis() as i64;
        let (
            object_lock_enabled,
            object_lock_default_mode,
            object_lock_default_days,
            object_lock_default_years,
        ) = Self::bucket_object_lock_sql_values(config.object_lock).map_err(|e| {
            MetadataError::Db {
                context: "create bucket (encode object lock)",
                source: e,
            }
        })?;
        let result = self.conn.execute(
            "INSERT INTO buckets \
             (name, owner_principal, owner_canonical_id, created_at, state, versioning, acl_grants, public_read, public_write, write_reservations_blocked, active_write_reservations, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, 0, ?10, 0, ?11, ?12, ?13, ?14)",
            params![
                config.name,
                config.owner_principal,
                config.owner_canonical_id.as_str(),
                now,
                BucketState::Active as u8,
                config.versioning as u8 as i64,
                config.acl_grants.serialized(),
                i32::from(config.public_read),
                i32::from(config.public_write),
                Option::<u8>::None,
                object_lock_enabled,
                object_lock_default_mode,
                object_lock_default_days,
                object_lock_default_years,
            ],
        );
        match result {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ffi::ErrorCode::ConstraintViolation =>
            {
                Err(MetadataError::BucketAlreadyExists)
            }
            Err(e) => Err(MetadataError::Db {
                context: "create bucket",
                source: e,
            }),
        }
    }

    fn next_object_write_sequence(&self, bucket: &str, key: &str) -> Result<u64, MetadataError> {
        let max: Option<i64> = self
            .conn
            .query_row(
                "SELECT MAX(write_sequence) FROM objects WHERE bucket = ?1 AND key = ?2",
                params![bucket, key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "next object write sequence",
                source: e,
            })?
            .flatten();

        match max {
            None => Ok(1),
            Some(value) => {
                let current = u64::try_from(value).map_err(|_| MetadataError::Db {
                    context: "negative write_sequence in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(write_sequence): {value}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "write_sequence overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("MAX(write_sequence) overflow"),
                    ),
                })
            }
        }
    }

    fn object_write_sequence(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<Option<u64>, MetadataError> {
        self.conn
            .query_row(
                "SELECT write_sequence FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object write sequence",
                source: e,
            })?
            .map(|value| {
                u64::try_from(value).map_err(|_| MetadataError::Db {
                    context: "negative write_sequence in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative write_sequence: {value}")),
                    ),
                })
            })
            .transpose()
    }

    fn current_object_head(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<(VersionId, ObjectState)>, rusqlite::Error> {
        self.conn
            .query_row(
                "SELECT version_id, status FROM objects \
                 WHERE bucket = ?1 AND key = ?2 \
                 ORDER BY write_sequence DESC LIMIT 1",
                params![bucket, key],
                |row| {
                    let version_id = Self::parse_version_id(row.get::<_, i64>(0)?, 0)?;
                    let status =
                        Self::parse_enum(row.get::<_, u8>(1)?, 1, "status", ObjectState::from_u8)?;
                    Ok((version_id, status))
                },
            )
            .optional()
    }

    fn mark_current_live_noncurrent(
        &self,
        bucket: &str,
        key: &str,
        replacement_version_id: VersionId,
        transition_time: u64,
    ) -> Result<(), rusqlite::Error> {
        let Some((current_version_id, status)) = self.current_object_head(bucket, key)? else {
            return Ok(());
        };
        if status != ObjectState::Live || current_version_id == replacement_version_id {
            return Ok(());
        }

        self.conn.execute(
            "UPDATE objects SET became_noncurrent_at = ?1 \
             WHERE bucket = ?2 AND key = ?3 AND version_id = ?4 AND status = ?5 \
               AND became_noncurrent_at IS NULL",
            params![
                transition_time as i64,
                bucket,
                key,
                current_version_id.to_u64() as i64,
                ObjectState::Live as u8,
            ],
        )?;
        Ok(())
    }

    fn clear_current_live_noncurrent(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(), rusqlite::Error> {
        let Some((current_version_id, status)) = self.current_object_head(bucket, key)? else {
            return Ok(());
        };
        if status != ObjectState::Live {
            return Ok(());
        }

        self.conn.execute(
            "UPDATE objects SET became_noncurrent_at = NULL \
             WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND status = ?4 \
               AND became_noncurrent_at IS NOT NULL",
            params![
                bucket,
                key,
                current_version_id.to_u64() as i64,
                ObjectState::Live as u8,
            ],
        )?;
        Ok(())
    }

    pub fn next_completed_multipart_upload_order_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, MetadataError> {
        let bucket = bucket.as_str();
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET completed_multipart_upload_sequence = completed_multipart_upload_sequence + 1 \
                 WHERE name = ?1",
                params![bucket],
            )
            .map_err(|e| MetadataError::Db {
                context: "increment completed multipart upload sequence",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(bucket));
        }
        self.conn
            .query_row(
                "SELECT completed_multipart_upload_sequence FROM buckets WHERE name = ?1",
                params![bucket],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|e| MetadataError::Db {
                context: "read completed multipart upload sequence",
                source: e,
            })?
            .try_into()
            .map_err(|_| MetadataError::Db {
                context: "decode completed multipart upload sequence",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("negative completed multipart upload sequence"),
                ),
            })
    }

    pub fn list_completed_multipart_uploads_for_bucket(
        &self,
        bucket: &str,
    ) -> Result<Vec<(UploadId, u64)>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT upload_id, completion_order \
                 FROM completed_multipart_uploads \
                 WHERE bucket = ?1",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list completed multipart uploads for bucket",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![bucket], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(|e| MetadataError::Db {
                context: "query list completed multipart uploads for bucket",
                source: e,
            })?;
        let mut uploads = Vec::new();
        for row in rows {
            let (upload_id, completion_order) = row.map_err(|e| MetadataError::Db {
                context: "row list completed multipart uploads for bucket",
                source: e,
            })?;
            uploads.push((
                UploadId::try_from(upload_id).map_err(|error| MetadataError::Db {
                    context: "decode completed multipart upload id",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    ),
                })?,
                completion_order.try_into().map_err(|_| MetadataError::Db {
                    context: "decode completed multipart upload completion order",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Integer,
                        Box::from("negative completion order"),
                    ),
                })?,
            ));
        }
        Ok(uploads)
    }

    pub fn delete_completed_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM completed_multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
            )
            .map(|_| ())
            .map_err(|e| MetadataError::Db {
                context: "delete completed multipart upload",
                source: e,
            })
    }
}

impl PgMetadataStore for PgStore {
    fn create_bucket(
        &self,
        name: &BucketName,
        owner_principal: &str,
        owner_canonical_id: &CanonicalUserId,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError> {
        self.create_bucket_with_config(&CreateBucketConfig {
            name: name.as_str(),
            owner_principal,
            owner_canonical_id,
            acl_grants,
            public_read,
            public_write,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
        })
    }

    fn delete_bucket(&self, name: &BucketName) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_bucket",
            "pg_id={} bucket={:?}",
            self.pg_id,
            name
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "delete bucket (begin txn)",
                source: e,
            })?;
        let result = (|| -> Result<usize, rusqlite::Error> {
            let deleted = self.conn.execute(
                "DELETE FROM buckets WHERE name = ?1",
                params![name.as_str()],
            )?;
            if deleted != 0 {
                self.conn.execute(
                    "DELETE FROM completed_multipart_uploads WHERE bucket = ?1",
                    params![name.as_str()],
                )?;
            }
            Ok(deleted)
        })();
        let deleted = match result {
            Ok(deleted) => {
                self.conn
                    .execute_batch("COMMIT")
                    .map_err(|e| MetadataError::Db {
                        context: "delete bucket (commit txn)",
                        source: e,
                    })?;
                deleted
            }
            Err(source) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                return Err(MetadataError::Db {
                    context: "delete bucket",
                    source,
                });
            }
        };
        if deleted == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn head_bucket(&self, name: &BucketName) -> Result<BucketInfo, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::head_bucket",
            "pg_id={} bucket={:?}",
            self.pg_id,
            name
        );
        let info = self.head_bucket_raw(name)?;
        if info.state != BucketState::Active {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(info)
    }

    fn head_bucket_raw(&self, name: &BucketName) -> Result<BucketInfo, MetadataError> {
        self.conn
            .query_row(
                &format!("{BUCKET_INFO_SELECT} WHERE name = ?1"),
                params![name.as_str()],
                Self::row_to_bucket_info,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "head bucket raw",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    fn list_buckets(&self, owner_canonical_id: &str) -> Result<Vec<BucketInfo>, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_buckets",
            "pg_id={} owner_canonical_id={}",
            self.pg_id,
            owner_canonical_id
        );
        let mut stmt = self
            .conn
            .prepare(&format!(
                "{BUCKET_INFO_SELECT} WHERE owner_canonical_id = ?1 AND state = ?2 ORDER BY name ASC"
            ))
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets",
                source: e,
            })?;
        let rows = stmt
            .query_map(
                params![owner_canonical_id, BucketState::Active as u8],
                Self::row_to_bucket_info,
            )
            .map_err(|e| MetadataError::Db {
                context: "list buckets query",
                source: e,
            })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| MetadataError::Db {
                context: "list buckets row",
                source: e,
            })?);
        }
        Ok(buckets)
    }

    fn list_buckets_with_lifecycle(&self) -> Result<Vec<BucketInfo>, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_buckets_with_lifecycle",
            "pg_id={}",
            self.pg_id
        );
        let mut stmt = self
            .conn
            .prepare(&format!(
                "{BUCKET_INFO_SELECT} \
                 JOIN bucket_subresources AS lifecycle \
                   ON lifecycle.bucket_name = buckets.name \
                  AND lifecycle.kind = {LIFECYCLE_SUBRESOURCE_KIND_SQL} \
                  AND lifecycle.body IS NOT NULL \
                 WHERE state = ?1 ORDER BY name ASC"
            ))
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets with lifecycle",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![BucketState::Active as u8], Self::row_to_bucket_info)
            .map_err(|e| MetadataError::Db {
                context: "list buckets with lifecycle query",
                source: e,
            })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| MetadataError::Db {
                context: "list buckets with lifecycle row",
                source: e,
            })?);
        }
        Ok(buckets)
    }

    fn list_buckets_with_aborting_multipart_uploads(
        &self,
    ) -> Result<Vec<BucketName>, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_buckets_with_aborting_multipart_uploads",
            "pg_id={}",
            self.pg_id
        );
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT bucket FROM multipart_uploads \
                 WHERE state = ?1 ORDER BY bucket ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets with aborting multipart uploads",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![UploadState::Aborting as u8], |row| row.get(0))
            .map_err(|e| MetadataError::Db {
                context: "list buckets with aborting multipart uploads query",
                source: e,
            })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| MetadataError::Db {
                context: "list buckets with aborting multipart uploads row",
                source: e,
            })?);
        }
        Ok(buckets)
    }

    fn mark_bucket_deleting(&self, name: &BucketName) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET state = ?1 \
                 WHERE name = ?2 \
                   AND state = ?3 \
                   AND write_reservations_blocked = 1 \
                   AND active_write_reservations = 0",
                params![
                    BucketState::Deleting as u8,
                    name.as_str(),
                    BucketState::Active as u8
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "mark bucket deleting",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn acquire_bucket_write_reservation(
        &self,
        name: &BucketName,
    ) -> Result<BucketInfo, MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET active_write_reservations = active_write_reservations + 1 \
                 WHERE name = ?1 AND state = ?2 AND write_reservations_blocked = 0",
                params![name.as_str(), BucketState::Active as u8],
            )
            .map_err(|e| MetadataError::Db {
                context: "acquire bucket write reservation",
                source: e,
            })?;
        if updated == 1 {
            return self.head_bucket_raw(name);
        }
        let info = self.head_bucket_raw(name)?;
        if info.state == BucketState::Active && info.write_reservations_blocked {
            return Err(MetadataError::BucketWriteDraining);
        }
        Err(bucket_not_found(name.as_str()))
    }

    fn release_bucket_write_reservation(&self, name: &BucketName) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET active_write_reservations = active_write_reservations - 1 \
                 WHERE name = ?1 AND active_write_reservations > 0",
                params![name.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "release bucket write reservation",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn begin_bucket_write_drain(&self, name: &BucketName) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET write_reservations_blocked = 1 \
                 WHERE name = ?1 AND state = ?2 AND write_reservations_blocked = 0",
                params![name.as_str(), BucketState::Active as u8],
            )
            .map_err(|e| MetadataError::Db {
                context: "begin bucket write drain",
                source: e,
            })?;
        if updated == 0 {
            let info = self.head_bucket_raw(name)?;
            if info.state == BucketState::Active && info.write_reservations_blocked {
                return Err(MetadataError::BucketWriteDraining);
            }
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn end_bucket_write_drain(&self, name: &BucketName) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET write_reservations_blocked = 0 \
                 WHERE name = ?1 AND state = ?2 AND write_reservations_blocked = 1",
                params![name.as_str(), BucketState::Active as u8],
            )
            .map_err(|e| MetadataError::Db {
                context: "end bucket write drain",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn put_bucket_versioning(
        &self,
        name: &BucketName,
        state: BucketVersioningState,
    ) -> Result<(), MetadataError> {
        let current: BucketVersioningState = self
            .conn
            .query_row(
                "SELECT versioning FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| row.get::<_, u8>(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket versioning",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name.as_str()))
            .and_then(|raw| {
                BucketVersioningState::from_u8(raw).ok_or_else(|| MetadataError::Db {
                    context: "invalid versioning state in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("invalid versioning: {raw}")),
                    ),
                })
            })?;
        if state == BucketVersioningState::Disabled && current != BucketVersioningState::Disabled {
            return Err(MetadataError::InvalidVersioningTransition {
                from: current,
                to: state,
            });
        }

        self.conn
            .execute(
                "UPDATE buckets SET versioning = ?1 WHERE name = ?2",
                params![state as u8 as i64, name.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket versioning",
                source: e,
            })?;
        Ok(())
    }

    fn put_bucket_object_lock(
        &self,
        name: &BucketName,
        config: BucketObjectLockConfig,
    ) -> Result<(), MetadataError> {
        let (enabled, default_mode, default_days, default_years) =
            Self::bucket_object_lock_sql_values(config).map_err(|e| MetadataError::Db {
                context: "put bucket object lock (encode)",
                source: e,
            })?;
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET object_lock_enabled = ?1, \
                     object_lock_default_mode = ?2, \
                     object_lock_default_days = ?3, \
                     object_lock_default_years = ?4 \
                 WHERE name = ?5",
                params![
                    enabled,
                    default_mode,
                    default_days,
                    default_years,
                    name.as_str()
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket object lock",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn put_bucket_acl(
        &self,
        name: &BucketName,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET acl_grants = ?1, public_read = ?2, public_write = ?3 WHERE name = ?4",
                params![
                    acl_grants.serialized(),
                    i32::from(public_read),
                    i32::from(public_write),
                    name.as_str()
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket acl",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn put_bucket_subresource(
        &self,
        name: &BucketName,
        req: PutBucketSubresource<'_>,
    ) -> Result<(), MetadataError> {
        self.put_bucket_subresource_internal(name.as_str(), req.kind, req.body, req.aux)
    }

    fn get_bucket_subresource(
        &self,
        name: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<StoredBucketSubresource>, MetadataError> {
        self.get_bucket_subresource_internal(name.as_str(), kind)
    }

    fn delete_bucket_subresource(
        &self,
        name: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<(), MetadataError> {
        self.delete_bucket_subresource_internal(name.as_str(), kind)
    }

    fn put_bucket_public_access_block(
        &self,
        name: &BucketName,
        config: PublicAccessBlockConfig,
    ) -> Result<(), MetadataError> {
        let (
            present,
            block_public_acls,
            ignore_public_acls,
            block_public_policy,
            restrict_public_buckets,
        ) = Self::public_access_block_sql_values(Some(config));
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET \
                     public_access_block_present = ?1, \
                     public_access_block_block_public_acls = ?2, \
                     public_access_block_ignore_public_acls = ?3, \
                     public_access_block_block_public_policy = ?4, \
                     public_access_block_restrict_public_buckets = ?5 \
                 WHERE name = ?6",
                params![
                    present,
                    block_public_acls,
                    ignore_public_acls,
                    block_public_policy,
                    restrict_public_buckets,
                    name.as_str(),
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket public access block",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn get_bucket_public_access_block(
        &self,
        name: &BucketName,
    ) -> Result<Option<PublicAccessBlockConfig>, MetadataError> {
        self.conn
            .query_row(
                "SELECT \
                     public_access_block_present, \
                     public_access_block_block_public_acls, \
                     public_access_block_ignore_public_acls, \
                     public_access_block_block_public_policy, \
                     public_access_block_restrict_public_buckets \
                 FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| {
                    Self::parse_public_access_block(
                        (
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                        ),
                        [0, 1, 2, 3, 4],
                    )
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket public access block",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    fn delete_bucket_public_access_block(&self, name: &BucketName) -> Result<(), MetadataError> {
        let (
            present,
            block_public_acls,
            ignore_public_acls,
            block_public_policy,
            restrict_public_buckets,
        ) = Self::public_access_block_sql_values(None);
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET \
                     public_access_block_present = ?1, \
                     public_access_block_block_public_acls = ?2, \
                     public_access_block_ignore_public_acls = ?3, \
                     public_access_block_block_public_policy = ?4, \
                     public_access_block_restrict_public_buckets = ?5 \
                 WHERE name = ?6",
                params![
                    present,
                    block_public_acls,
                    ignore_public_acls,
                    block_public_policy,
                    restrict_public_buckets,
                    name.as_str(),
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete bucket public access block",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn put_bucket_ownership_controls(
        &self,
        name: &BucketName,
        config: BucketOwnershipControls,
    ) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET ownership_controls_mode = ?1 WHERE name = ?2",
                params![
                    Self::ownership_controls_sql_value(Some(config)),
                    name.as_str()
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket ownership controls",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn get_bucket_ownership_controls(
        &self,
        name: &BucketName,
    ) -> Result<Option<BucketOwnershipControls>, MetadataError> {
        self.conn
            .query_row(
                "SELECT ownership_controls_mode FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| Self::parse_ownership_controls(row.get(0)?, 0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket ownership controls",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    fn delete_bucket_ownership_controls(&self, name: &BucketName) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET ownership_controls_mode = ?1 WHERE name = ?2",
                params![Self::ownership_controls_sql_value(None), name.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete bucket ownership controls",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn put_bucket_encryption(
        &self,
        name: &BucketName,
        config: BucketEncryptionConfig,
    ) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET default_encryption_type = ?1, sse_c_blocked = ?2 WHERE name = ?3",
                params![
                    config.default_encryption.map(|value| value as u8),
                    i32::from(config.sse_c_blocked),
                    name.as_str()
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket encryption",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn get_bucket_encryption(
        &self,
        name: &BucketName,
    ) -> Result<BucketEncryptionConfig, MetadataError> {
        self.conn
            .query_row(
                "SELECT default_encryption_type, sse_c_blocked FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| {
                    Ok(BucketEncryptionConfig {
                        default_encryption: row
                            .get::<_, Option<u8>>(0)?
                            .map(|value| {
                                ManagedEncryptionAlgorithm::from_u8(value).ok_or_else(|| {
                                    rusqlite::Error::FromSqlConversionFailure(
                                        0,
                                        rusqlite::types::Type::Integer,
                                        Box::from(format!(
                                            "invalid default_encryption_type: {value}"
                                        )),
                                    )
                                })
                            })
                            .transpose()?,
                        sse_c_blocked: row.get::<_, i64>(1)? != 0,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket encryption",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    fn put_object_meta(&self, req: &PutObjectReq) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::put_object_meta",
            "pg_id={}",
            self.pg_id
        );
        let now = PgStore::now_millis();
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put object meta (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| match req {
            PutObjectReq::Live(req) => {
                let write_sequence =
                    self.next_object_write_sequence(req.bucket.as_str(), req.key.as_str())?;
                req.validate().map_err(|msg| MetadataError::Db {
                    context: "put object meta (etag/layout mismatch)",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from(msg),
                    ),
                })?;
                self.mark_current_live_noncurrent(
                    req.bucket.as_str(),
                    req.key.as_str(),
                    req.version_id,
                    now,
                )
                .map_err(|e| MetadataError::Db {
                    context: "put object meta (mark noncurrent)",
                    source: e,
                })?;
                let data_layout_u8 = req.layout.data_layout() as u8;
                let etag_kind_u8 = req.etag.etag_kind() as u8;
                let status_u8 = ObjectState::Live as u8;
                let parts_count = req.layout.parts_count().map(|n| n as i64);
                let tags = req.tags.as_ref().map(SerializedTagSet::as_str);
                let metadata_blob = req
                    .metadata_blob
                    .as_ref()
                    .map(SerializedMetadataBlob::as_slice);
                let system_metadata_blob = req
                    .system_metadata_blob
                    .as_ref()
                    .map(SerializedSystemMetadataBlob::as_slice);
                let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
                    Self::object_lock_sql_values(req.object_lock).map_err(|e| {
                        MetadataError::Db {
                            context: "put object meta (encode object lock)",
                            source: e,
                        }
                    })?;
                let encryption_type = req.encryption.encryption_type() as u8;
                let encryption_state = req.encryption.encode_state();
                let sql = if req.version_id.is_null() {
                    "INSERT OR REPLACE INTO objects \
                     (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
                } else {
                    "INSERT INTO objects \
                     (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
                };
                self.conn
                    .execute(
                        sql,
                        params![
                            req.bucket,
                            req.key,
                            req.version_id.to_u64() as i64,
                            write_sequence as i64,
                            req.generation_id.get() as i64,
                            req.size as i64,
                            req.etag.as_bytes().as_slice(),
                            etag_kind_u8,
                            now as i64,
                            req.ec.k,
                            req.ec.m,
                            status_u8,
                            data_layout_u8,
                            parts_count,
                            tags,
                            metadata_blob,
                            system_metadata_blob,
                            encryption_type,
                            encryption_state,
                            req.owner.principal,
                            req.owner.canonical_id.as_str(),
                            req.acl_grants.serialized(),
                            i32::from(req.public_read),
                            object_lock_retention_mode,
                            object_lock_retain_until,
                            object_lock_legal_hold,
                        ],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "put object meta",
                        source: e,
                    })?;
                Ok(())
            }
            PutObjectReq::DeleteMarker(req) => {
                let write_sequence =
                    self.next_object_write_sequence(req.bucket.as_str(), req.key.as_str())?;
                self.mark_current_live_noncurrent(
                    req.bucket.as_str(),
                    req.key.as_str(),
                    req.version_id,
                    now,
                )
                .map_err(|e| MetadataError::Db {
                    context: "put object meta (mark noncurrent delete marker)",
                    source: e,
                })?;
                let sql = if req.version_id.is_null() {
                    "INSERT OR REPLACE INTO objects \
                     (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read) \
                     VALUES (?1, ?2, ?3, ?4, NULL, 0, zeroblob(0), 0, ?5, 0, 0, 0, 1, 0, NULL, NULL, NULL, 0, NULL, ?6, ?7, ?8, 0)"
                } else {
                    "INSERT INTO objects \
                     (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read) \
                     VALUES (?1, ?2, ?3, ?4, NULL, 0, zeroblob(0), 0, ?5, 0, 0, 0, 1, 0, NULL, NULL, NULL, 0, NULL, ?6, ?7, ?8, 0)"
                };
                self.conn
                    .execute(
                        sql,
                        params![
                            req.bucket,
                            req.key,
                            req.version_id.to_u64() as i64,
                            write_sequence as i64,
                            now as i64,
                            req.owner.principal,
                            req.owner.canonical_id.as_str(),
                            "",
                        ],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "put object meta (delete marker)",
                        source: e,
                    })?;
                Ok(())
            }
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put object meta (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_object_meta(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<StoredObject, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::get_object_meta",
            "pg_id={} bucket={:?} key={:?}",
            self.pg_id,
            bucket,
            key
        );
        self.conn
            .query_row(
                "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
                 last_modified, storage_class, ec_k, ec_m, status, tags, \
                 data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold \
                 , became_noncurrent_at \
                 FROM objects WHERE bucket = ?1 AND key = ?2 \
                 ORDER BY write_sequence DESC LIMIT 1",
                params![bucket, key],
                Self::row_to_object_record,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object meta",
                source: e,
            })?
            .ok_or(MetadataError::ObjectNotFound)
    }

    fn get_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<StoredObject, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::get_object_version",
            "pg_id={} bucket={:?} key={:?} version_id={}",
            self.pg_id,
            bucket,
            key,
            version_id
        );
        self.conn
            .query_row(
                "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
                 last_modified, storage_class, ec_k, ec_m, status, tags, \
                 data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold \
                 , became_noncurrent_at \
                 FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                Self::row_to_object_record,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object version",
                source: e,
            })?
            .ok_or(MetadataError::ObjectNotFound)
    }

    fn put_object_acl(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        acl_grants: &AclGrants,
        public_read: bool,
    ) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE objects SET acl_grants = ?1, public_read = ?2 \
                 WHERE bucket = ?3 AND key = ?4 AND version_id = ?5 AND status = ?6",
                params![
                    acl_grants.serialized(),
                    i32::from(public_read),
                    bucket,
                    key,
                    version_id.to_u64() as i64,
                    ObjectState::Live as u8
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put object acl",
                source: e,
            })?;
        if updated == 0 {
            let status = self
                .conn
                .query_row(
                    "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![bucket, key, version_id.to_u64() as i64],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "put object acl (check status)",
                    source: e,
                })?;
            return match status {
                Some(v) if v == ObjectState::DeleteMarker as u8 => {
                    Err(MetadataError::MethodNotAllowedOnDeleteMarker)
                }
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    fn put_object_retention(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        retention: ObjectRetention,
    ) -> Result<(), MetadataError> {
        let retain_until =
            i64::try_from(retention.retain_until_unix_seconds).map_err(|_| MetadataError::Db {
                context: "put object retention (encode retain-until)",
                source: rusqlite::Error::ToSqlConversionFailure(Box::from(format!(
                    "object lock retain-until exceeds SQLite INTEGER: {}",
                    retention.retain_until_unix_seconds
                ))),
            })?;
        let updated = self
            .conn
            .execute(
                "UPDATE objects \
                 SET object_lock_retention_mode = ?1, object_lock_retain_until = ?2 \
                 WHERE bucket = ?3 AND key = ?4 AND version_id = ?5 AND status = ?6",
                params![
                    retention.mode as u8,
                    retain_until,
                    bucket,
                    key,
                    version_id.to_u64() as i64,
                    ObjectState::Live as u8
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put object retention",
                source: e,
            })?;
        if updated == 0 {
            let status = self
                .conn
                .query_row(
                    "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![bucket, key, version_id.to_u64() as i64],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "put object retention (check status)",
                    source: e,
                })?;
            return match status {
                Some(v) if v == ObjectState::DeleteMarker as u8 => {
                    Err(MetadataError::MethodNotAllowedOnDeleteMarker)
                }
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    fn put_object_legal_hold(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        legal_hold: StoredLegalHoldStatus,
    ) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE objects SET object_lock_legal_hold = ?1 \
                 WHERE bucket = ?2 AND key = ?3 AND version_id = ?4 AND status = ?5",
                params![
                    legal_hold as u8,
                    bucket,
                    key,
                    version_id.to_u64() as i64,
                    ObjectState::Live as u8
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put object legal hold",
                source: e,
            })?;
        if updated == 0 {
            let status = self
                .conn
                .query_row(
                    "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![bucket, key, version_id.to_u64() as i64],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "put object legal hold (check status)",
                    source: e,
                })?;
            return match status {
                Some(v) if v == ObjectState::DeleteMarker as u8 => {
                    Err(MetadataError::MethodNotAllowedOnDeleteMarker)
                }
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    fn delete_object_meta(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_object_meta",
            "pg_id={} bucket={:?} key={:?}",
            self.pg_id,
            bucket,
            key
        );
        self.conn
            .execute(
                "DELETE FROM objects WHERE bucket = ?1 AND key = ?2",
                params![bucket, key],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object meta",
                source: e,
            })?;
        Ok(())
    }

    fn delete_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_object_version",
            "pg_id={} bucket={:?} key={:?} version_id={}",
            self.pg_id,
            bucket,
            key,
            version_id
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "delete object version (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            let deleted_was_current = self
                .current_object_head(bucket.as_str(), key.as_str())
                .map_err(|e| MetadataError::Db {
                    context: "delete object version (lookup current)",
                    source: e,
                })?
                .is_some_and(|(current_version_id, _)| current_version_id == version_id);

            self.conn
                .execute(
                    "DELETE FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![bucket, key, version_id.to_u64() as i64],
                )
                .map_err(|e| MetadataError::Db {
                    context: "delete object version",
                    source: e,
                })?;

            if deleted_was_current {
                self.clear_current_live_noncurrent(bucket.as_str(), key.as_str())
                    .map_err(|e| MetadataError::Db {
                        context: "delete object version (restore current)",
                        source: e,
                    })?;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "delete object version (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn list_objects(&self, req: &ListObjectsReq) -> Result<ListObjectsResp, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_objects",
            "pg_id={} bucket={:?} max_keys={}",
            self.pg_id,
            req.bucket.as_str(),
            req.max_keys
        );
        // Select the current record per key using the durable per-key write
        // sequence. Timestamp equality cannot reliably determine currentness
        // for suspended buckets because a newer null version may share the
        // same last_modified millisecond as an older numbered version.
        let limit = req.max_keys as i64 + 1;

        // Build WHERE clause fragments for key filtering
        let mut where_clauses = vec!["o.bucket = ?1".to_string()];
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.bucket.clone())];
        let mut param_idx = 2;

        if let Some(ref start_after) = req.start_after {
            where_clauses.push(format!("o.key > ?{param_idx}"));
            params_vec.push(Box::new(start_after.clone()));
            param_idx += 1;
        } else if let Some(ref start_at) = req.start_at {
            where_clauses.push(format!("o.key >= ?{param_idx}"));
            params_vec.push(Box::new(start_at.clone()));
            param_idx += 1;
        }

        if let Some(ref prefix) = req.prefix {
            where_clauses.push(format!("o.key >= ?{param_idx}"));
            params_vec.push(Box::new(prefix.clone()));
            param_idx += 1;

            if let Some(end) = object_key_prefix_upper_bound(prefix) {
                where_clauses.push(format!("o.key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        let where_str = where_clauses.join(" AND ");

        let sql = format!(
            "SELECT o.bucket, o.key, o.version_id, o.generation_id, o.size, o.etag, o.etag_kind, \
                   o.last_modified, o.storage_class, o.ec_k, o.ec_m, o.status, o.tags, \
                   o.data_layout, o.parts_count, o.metadata_blob, o.system_metadata_blob, \
                   o.encryption_type, o.encryption_state, o.owner_principal, o.owner_canonical_id, \
                   o.acl_grants, o.public_read, o.object_lock_retention_mode, \
                   o.object_lock_retain_until, o.object_lock_legal_hold, \
                   o.became_noncurrent_at \
            FROM objects o \
            WHERE {where_str} AND o.status = 0 \
              AND NOT EXISTS ( \
                  SELECT 1 FROM objects newer \
                  WHERE newer.bucket = o.bucket AND newer.key = o.key \
                    AND newer.write_sequence > o.write_sequence \
              ) \
            ORDER BY o.key ASC LIMIT ?{param_idx}"
        );
        params_vec.push(Box::new(limit));

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql).map_err(|e| MetadataError::Db {
            context: "prepare list objects",
            source: e,
        })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list objects query",
                source: e,
            })?;

        let mut objects: Vec<StoredObject> = Vec::new();
        for row in rows {
            objects.push(row.map_err(|e| MetadataError::Db {
                context: "list objects row",
                source: e,
            })?);
        }

        let is_truncated = objects.len() as i64 > req.max_keys as i64;
        if is_truncated {
            objects.truncate(req.max_keys as usize);
        }

        let next_start_after = if is_truncated {
            objects.last().map(|o| o.key().clone())
        } else {
            None
        };

        Ok(ListObjectsResp {
            objects,
            is_truncated,
            next_start_after,
        })
    }

    fn list_object_versions(
        &self,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_object_versions",
            "pg_id={} bucket={:?} max_keys={}",
            self.pg_id,
            req.bucket.as_str(),
            req.max_keys
        );
        let limit = req.max_keys as i64 + 1;

        let mut where_clauses = vec!["bucket = ?1".to_string()];
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.bucket.clone())];
        let mut param_idx = 2;

        if let Some(ref key_marker) = req.key_marker {
            if let Some(vid_marker) = req.version_id_marker {
                if let Some(write_sequence) = self.object_write_sequence(
                    req.bucket.as_str(),
                    key_marker.as_str(),
                    vid_marker,
                )? {
                    where_clauses.push(format!(
                        "(key > ?{} OR (key = ?{} AND write_sequence < ?{}))",
                        param_idx,
                        param_idx,
                        param_idx + 1
                    ));
                    params_vec.push(Box::new(key_marker.clone()));
                    params_vec.push(Box::new(write_sequence as i64));
                    param_idx += 2;
                } else {
                    where_clauses.push(format!("key > ?{param_idx}"));
                    params_vec.push(Box::new(key_marker.clone()));
                    param_idx += 1;
                }
            } else {
                where_clauses.push(format!("key > ?{param_idx}"));
                params_vec.push(Box::new(key_marker.clone()));
                param_idx += 1;
            }
        }

        if let Some(ref prefix) = req.prefix {
            where_clauses.push(format!("key >= ?{param_idx}"));
            params_vec.push(Box::new(prefix.clone()));
            param_idx += 1;

            if let Some(end) = object_key_prefix_upper_bound(prefix) {
                where_clauses.push(format!("key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        let where_str = where_clauses.join(" AND ");

        let sql = format!(
            "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
             last_modified, storage_class, ec_k, ec_m, status, tags, \
             data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold \
             , became_noncurrent_at \
             FROM objects \
             WHERE {where_str} \
             ORDER BY key ASC, write_sequence DESC LIMIT ?{param_idx}"
        );
        params_vec.push(Box::new(limit));

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql).map_err(|e| MetadataError::Db {
            context: "prepare list object versions",
            source: e,
        })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list object versions query",
                source: e,
            })?;

        let mut versions: Vec<StoredObject> = Vec::new();
        for row in rows {
            versions.push(row.map_err(|e| MetadataError::Db {
                context: "list object versions row",
                source: e,
            })?);
        }

        let is_truncated = versions.len() as i64 > req.max_keys as i64;
        if is_truncated {
            versions.truncate(req.max_keys as usize);
        }

        let (next_key_marker, next_version_id_marker) = if is_truncated {
            versions.last().map_or((None, None), |o| {
                (Some(o.key().clone()), Some(o.version_id()))
            })
        } else {
            (None, None)
        };

        Ok(ListObjectVersionsResp {
            versions,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        })
    }

    fn list_object_versions_for_key(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
                 last_modified, storage_class, ec_k, ec_m, status, tags, \
                 data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold, became_noncurrent_at \
                 FROM objects \
                 WHERE bucket = ?1 AND key = ?2 \
                 ORDER BY write_sequence DESC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list object versions for key",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![bucket, key], Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list object versions for key query",
                source: e,
            })?;

        let mut versions = Vec::new();
        for row in rows {
            versions.push(row.map_err(|e| MetadataError::Db {
                context: "list object versions for key row",
                source: e,
            })?);
        }
        Ok(versions)
    }

    fn next_version_id(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, MetadataError> {
        let max: Option<i64> = self
            .conn
            .query_row(
                "SELECT MAX(version_id) FROM objects WHERE bucket = ?1 AND key = ?2",
                params![bucket, key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "next version id",
                source: e,
            })?
            .flatten();

        let next = match max {
            None => 1u64,
            Some(v) => {
                let current = u64::try_from(v).map_err(|_| MetadataError::Db {
                    context: "negative version_id in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(version_id): {v}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "version_id overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("MAX(version_id) overflow"),
                    ),
                })?
            }
        };
        Ok(VersionId::from_u64(next))
    }

    fn next_generation_id(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, MetadataError> {
        let max: Option<i64> = self
            .conn
            .query_row(
                "SELECT MAX(generation_id) FROM (
                     SELECT generation_id FROM objects WHERE bucket = ?1 AND key = ?2
                     UNION ALL
                     SELECT generation_id FROM simple_payload_reclaims WHERE bucket = ?1 AND key = ?2
                     UNION ALL
                     SELECT generation_id FROM object_segments_reclaims WHERE bucket = ?1 AND key = ?2
                     UNION ALL
                     SELECT generation_id FROM multipart_reclaims WHERE bucket = ?1 AND key = ?2
                 )",
                params![bucket, key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "next generation id",
                source: e,
            })?
            .flatten();

        let next = match max {
            None => 1u64,
            Some(v) => {
                let current = u64::try_from(v).map_err(|_| MetadataError::Db {
                    context: "negative generation_id in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(generation_id): {v}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "generation_id overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("MAX(generation_id) overflow"),
                    ),
                })?
            }
        };
        GenerationId::new(next).ok_or_else(|| MetadataError::Db {
            context: "invalid next generation id",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("next generation id must be nonzero"),
            ),
        })
    }

    fn put_simple_payload_reclaim(
        &self,
        reclaim: &SimplePayloadReclaimRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO simple_payload_reclaims \
                 (bucket, key, generation_id, ec_k, ec_m, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    reclaim.bucket,
                    reclaim.key,
                    reclaim.generation_id.get() as i64,
                    reclaim.ec.k,
                    reclaim.ec.m,
                    reclaim.created_at as i64,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put simple payload reclaim",
                source: e,
            })?;
        Ok(())
    }

    fn get_simple_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<SimplePayloadReclaimRecord>, MetadataError> {
        self.conn
            .query_row(
                "SELECT bucket, key, generation_id, ec_k, ec_m, created_at \
                 FROM simple_payload_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
                |row| {
                    Ok(SimplePayloadReclaimRecord {
                        bucket: row.get(0)?,
                        key: row.get(1)?,
                        generation_id: Self::parse_generation_id(
                            row.get::<_, i64>(2)?,
                            2,
                            "generation_id",
                        )?,
                        ec: EcShape {
                            k: row.get(3)?,
                            m: row.get(4)?,
                        },
                        created_at: row.get::<_, i64>(5)? as u64,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get simple payload reclaim",
                source: e,
            })
    }

    fn delete_simple_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM simple_payload_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete simple payload reclaim",
                source: e,
            })?;
        Ok(())
    }

    fn put_object_segments_reclaim(
        &self,
        reclaim: &ObjectSegmentsReclaimRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put object segments reclaim (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO object_segments_reclaims \
                     (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![
                        reclaim.bucket,
                        reclaim.key,
                        reclaim.generation_id.get() as i64,
                        reclaim.created_at as i64,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put object segments reclaim (root)",
                    source: e,
                })?;

            for segment in &reclaim.segments {
                self.conn
                    .execute(
                        "INSERT OR REPLACE INTO object_segment_reclaim_segments \
                         (bucket, key, generation_id, segment_index, segment_okh, segment_vid, \
                          shard_pg_id, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            reclaim.bucket,
                            reclaim.key,
                            reclaim.generation_id.get() as i64,
                            segment.segment_index as i64,
                            &segment.segment_okh[..],
                            segment.segment_vid.get() as i64,
                            segment.shard_pg_id as i64,
                            segment.ec.k,
                            segment.ec.m,
                        ],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "put object segments reclaim (segment)",
                        source: e,
                    })?;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put object segments reclaim (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectSegmentsReclaimRecord>, MetadataError> {
        let root = self
            .conn
            .query_row(
                "SELECT bucket, key, generation_id, created_at \
                 FROM object_segments_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
                |row| {
                    Ok((
                        row.get::<_, BucketName>(0)?,
                        row.get::<_, ObjectKey>(1)?,
                        Self::parse_generation_id(row.get::<_, i64>(2)?, 2, "generation_id")?,
                        row.get::<_, i64>(3)? as u64,
                    ))
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (root)",
                source: e,
            })?;

        let Some((bucket_name, key_name, generation_id, created_at)) = root else {
            return Ok(None);
        };

        let mut stmt = self
            .conn
            .prepare(
                "SELECT segment_index, segment_okh, segment_vid, shard_pg_id, ec_k, ec_m \
                 FROM object_segment_reclaim_segments \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 \
                 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (prepare segments)",
                source: e,
            })?;

        let segments = stmt
            .query_map(params![bucket, key, generation_id.get() as i64], |row| {
                Ok(ObjectSegmentsReclaimSegmentRecord {
                    segment_index: row.get::<_, i64>(0)? as u32,
                    segment_okh: row.get_ref(1)?.as_blob()?.try_into().map_err(|_| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Blob,
                            Box::from("segment_okh must be 16 bytes"),
                        )
                    })?,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(2)?,
                        2,
                        "segment_vid",
                    )?,
                    shard_pg_id: row.get::<_, i64>(3)? as u32,
                    ec: EcShape {
                        k: row.get(4)?,
                        m: row.get(5)?,
                    },
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (query segments)",
                source: e,
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (collect segments)",
                source: e,
            })?;

        Ok(Some(ObjectSegmentsReclaimRecord {
            bucket: bucket_name,
            key: key_name,
            generation_id,
            created_at,
            segments,
        }))
    }

    fn delete_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_segments_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object segments reclaim",
                source: e,
            })?;
        Ok(())
    }

    fn put_multipart_reclaim(&self, reclaim: &MultipartReclaimRecord) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put multipart reclaim (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO multipart_reclaims \
                     (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![
                        reclaim.bucket,
                        reclaim.key,
                        reclaim.generation_id.get() as i64,
                        reclaim.created_at as i64,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put multipart reclaim (root)",
                    source: e,
                })?;

            for part in &reclaim.parts {
                match part {
                    MultipartReclaimPartRecord::ShardSet {
                        part_number,
                        part_okh,
                        part_vid,
                        shard_pg_id,
                        ec,
                    } => {
                        self.conn
                            .execute(
                                "INSERT OR REPLACE INTO multipart_reclaim_parts \
                                 (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                                  part_vid, shard_pg_id, ec_k, ec_m) \
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                                params![
                                    reclaim.bucket,
                                    reclaim.key,
                                    reclaim.generation_id.get() as i64,
                                    *part_number as i64,
                                    MultipartReclaimPartKind::ShardSet as u8,
                                    &part_okh[..],
                                    part_vid.get() as i64,
                                    *shard_pg_id as i64,
                                    ec.k,
                                    ec.m,
                                ],
                            )
                            .map_err(|e| MetadataError::Db {
                                context: "put multipart reclaim (part shard set)",
                                source: e,
                            })?;
                    }
                    MultipartReclaimPartRecord::Segments {
                        part_number,
                        segments,
                    } => {
                        self.conn
                            .execute(
                                "INSERT OR REPLACE INTO multipart_reclaim_parts \
                                 (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                                  part_vid, shard_pg_id, ec_k, ec_m) \
                                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, NULL, NULL, NULL)",
                                params![
                                    reclaim.bucket,
                                    reclaim.key,
                                    reclaim.generation_id.get() as i64,
                                    *part_number as i64,
                                    MultipartReclaimPartKind::Segments as u8,
                                ],
                            )
                            .map_err(|e| MetadataError::Db {
                                context: "put multipart reclaim (part segments)",
                                source: e,
                            })?;

                        for segment in segments {
                            self.conn
                                .execute(
                                    "INSERT OR REPLACE INTO multipart_reclaim_part_segments \
                                     (bucket, key, generation_id, part_number, segment_index, \
                                      segment_okh, segment_vid, shard_pg_id, ec_k, ec_m) \
                                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                                    params![
                                        reclaim.bucket,
                                        reclaim.key,
                                        reclaim.generation_id.get() as i64,
                                        segment.part_number as i64,
                                        segment.segment_index as i64,
                                        &segment.segment_okh[..],
                                        segment.segment_vid.get() as i64,
                                        segment.shard_pg_id as i64,
                                        segment.ec.k,
                                        segment.ec.m,
                                    ],
                                )
                                .map_err(|e| MetadataError::Db {
                                    context: "put multipart reclaim (part segment)",
                                    source: e,
                                })?;
                        }
                    }
                }
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put multipart reclaim (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<MultipartReclaimRecord>, MetadataError> {
        let root = self
            .conn
            .query_row(
                "SELECT bucket, key, generation_id, created_at \
                 FROM multipart_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
                |row| {
                    Ok((
                        row.get::<_, BucketName>(0)?,
                        row.get::<_, ObjectKey>(1)?,
                        Self::parse_generation_id(row.get::<_, i64>(2)?, 2, "generation_id")?,
                        row.get::<_, i64>(3)? as u64,
                    ))
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (root)",
                source: e,
            })?;

        let Some((bucket_name, key_name, generation_id, created_at)) = root else {
            return Ok(None);
        };

        let mut stmt = self
            .conn
            .prepare(
                "SELECT part_number, storage_kind, part_okh, part_vid, shard_pg_id, ec_k, ec_m \
                 FROM multipart_reclaim_parts \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (prepare parts)",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![bucket, key, generation_id.get() as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u32,
                    Self::parse_enum(
                        row.get::<_, u8>(1)?,
                        1,
                        "storage_kind",
                        MultipartReclaimPartKind::from_u8,
                    )?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<u8>>(5)?,
                    row.get::<_, Option<u8>>(6)?,
                ))
            })
            .map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (query parts)",
                source: e,
            })?;

        let mut parts = Vec::new();
        for row in rows {
            let (part_number, kind, part_okh, part_vid, shard_pg_id, ec_k, ec_m) =
                row.map_err(|e| MetadataError::Db {
                    context: "get multipart reclaim (part row)",
                    source: e,
                })?;
            match kind {
                MultipartReclaimPartKind::ShardSet => {
                    let part_okh = part_okh.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing part_okh)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing part_okh"),
                        ),
                    })?;
                    let part_okh =
                        Self::parse_okh_blob(&part_okh, 2).map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (invalid part_okh)",
                            source: e,
                        })?;
                    let part_vid = part_vid.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing part_vid)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing part_vid"),
                        ),
                    })?;
                    let shard_pg_id = shard_pg_id.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing shard_pg_id)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing shard_pg_id"),
                        ),
                    })?;
                    let ec_k = ec_k.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing ec_k)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing ec_k"),
                        ),
                    })?;
                    let ec_m = ec_m.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing ec_m)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing ec_m"),
                        ),
                    })?;
                    parts.push(MultipartReclaimPartRecord::ShardSet {
                        part_number,
                        part_okh,
                        part_vid: Self::parse_generation_id(part_vid, 3, "part_vid").map_err(
                            |e| MetadataError::Db {
                                context: "get multipart reclaim (invalid part_vid)",
                                source: e,
                            },
                        )?,
                        shard_pg_id: shard_pg_id as u32,
                        ec: EcShape { k: ec_k, m: ec_m },
                    });
                }
                MultipartReclaimPartKind::Segments => {
                    let mut segment_stmt = self
                        .conn
                        .prepare(
                            "SELECT part_number, segment_index, segment_okh, segment_vid, shard_pg_id, ec_k, ec_m \
                             FROM multipart_reclaim_part_segments \
                             WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 AND part_number = ?4 \
                             ORDER BY segment_index ASC",
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (prepare part segments)",
                            source: e,
                        })?;

                    let segments = segment_stmt
                        .query_map(
                            params![bucket, key, generation_id.get() as i64, part_number],
                            |row| {
                                let segment_okh = Self::blob_to_okh(row.get(2)?, 2)?;
                                Ok(MultipartReclaimPartSegmentRecord {
                                    part_number: row.get::<_, i64>(0)? as u32,
                                    segment_index: row.get::<_, i64>(1)? as u32,
                                    segment_okh,
                                    segment_vid: Self::parse_generation_id(
                                        row.get::<_, i64>(3)?,
                                        3,
                                        "segment_vid",
                                    )?,
                                    shard_pg_id: row.get::<_, i64>(4)? as u32,
                                    ec: EcShape {
                                        k: row.get(5)?,
                                        m: row.get(6)?,
                                    },
                                })
                            },
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (query part segments)",
                            source: e,
                        })?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (collect part segments)",
                            source: e,
                        })?;

                    parts.push(MultipartReclaimPartRecord::Segments {
                        part_number,
                        segments,
                    });
                }
            }
        }

        Ok(Some(MultipartReclaimRecord {
            bucket: bucket_name,
            key: key_name,
            generation_id,
            created_at,
            parts,
        }))
    }

    fn delete_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart reclaim",
                source: e,
            })?;
        Ok(())
    }

    fn payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, MetadataError> {
        self.conn
            .query_row(
                "SELECT
                    EXISTS(
                        SELECT 1 FROM simple_payload_reclaims
                        WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3
                    )
                    OR EXISTS(
                        SELECT 1 FROM object_segments_reclaims
                        WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3
                    )
                    OR EXISTS(
                        SELECT 1 FROM multipart_reclaims
                        WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3
                    )",
                params![bucket, key, generation_id.get() as i64],
                |row| Ok(row.get::<_, i64>(0)? != 0),
            )
            .map_err(|e| MetadataError::Db {
                context: "payload reclaim exists",
                source: e,
            })
    }

    fn get_bucket_payload_reclaim_root(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, MetadataError> {
        self.conn
            .query_row(
                "SELECT bucket, key, generation_id FROM (
                     SELECT bucket, key, generation_id FROM simple_payload_reclaims WHERE bucket = ?1
                     UNION ALL
                     SELECT bucket, key, generation_id FROM object_segments_reclaims WHERE bucket = ?1
                     UNION ALL
                     SELECT bucket, key, generation_id FROM multipart_reclaims WHERE bucket = ?1
                 )
                 ORDER BY key ASC, generation_id ASC
                 LIMIT 1",
                params![bucket],
                |row| {
                    Ok(PayloadReclaimRoot {
                        bucket: row.get(0)?,
                        key: row.get(1)?,
                        generation_id: Self::parse_generation_id(
                            row.get::<_, i64>(2)?,
                            2,
                            "generation_id",
                        )?,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket payload reclaim root",
                source: e,
            })
    }

    fn put_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        tags: &str,
    ) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE objects SET tags = ?1 WHERE bucket = ?2 AND key = ?3 AND version_id = ?4 AND status = 0",
                params![tags, bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "put object tags",
                source: e,
            })?;
        if updated == 0 {
            let status: Option<u8> = self
                .conn
                .query_row(
                    "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![bucket, key, version_id.to_u64() as i64],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "put object tags (check status)",
                    source: e,
                })?;
            return match status {
                Some(1) => Err(MetadataError::MethodNotAllowedOnDeleteMarker),
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    fn get_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<String>, MetadataError> {
        let result = self.conn
            .query_row(
                "SELECT tags FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND status = 0",
                params![bucket, key, version_id.to_u64() as i64],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object tags",
                source: e,
            })?;
        if let Some(tags) = result {
            Ok(tags)
        } else {
            let status: Option<u8> = self
                .conn
                .query_row(
                    "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![bucket, key, version_id.to_u64() as i64],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "get object tags (check status)",
                    source: e,
                })?;
            match status {
                Some(1) => Err(MetadataError::MethodNotAllowedOnDeleteMarker),
                _ => Err(MetadataError::ObjectNotFound),
            }
        }
    }

    fn delete_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE objects SET tags = NULL WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND status = 0",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object tags",
                source: e,
            })?;
        if updated == 0 {
            let status: Option<u8> = self
                .conn
                .query_row(
                    "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![bucket, key, version_id.to_u64() as i64],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "delete object tags (check status)",
                    source: e,
                })?;
            return match status {
                Some(1) => Err(MetadataError::MethodNotAllowedOnDeleteMarker),
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    // ── Multipart upload methods ──────────────────────────────────

    fn create_multipart_upload(&self, req: &CreateMultipartUploadReq) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_multipart_upload",
            "pg_id={} upload_id={:?} bucket={:?} key={:?}",
            self.pg_id,
            req.upload_id.as_str(),
            req.bucket.as_str(),
            req.key.as_str()
        );
        let now = PgStore::now_millis();
        let algo = req.checksum.map(|c| c.algorithm() as u8);
        let ctype = req.checksum.map(|c| c.checksum_type() as u8);
        let tags = req.tags.as_ref().map(SerializedTagSet::as_str);
        let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
            Self::object_lock_sql_values(req.object_lock).map_err(|e| MetadataError::Db {
                context: "create multipart upload (encode object lock)",
                source: e,
            })?;
        let encryption_type = req.encryption.encryption_type() as u8;
        let encryption_state = req.encryption.encode_state();
        let system_metadata_blob = req.system_metadata_blob.as_slice();
        self.conn
            .execute(
                "INSERT INTO multipart_uploads \
                 (upload_id, bucket, key, initiated_at, state, tags, metadata_blob, system_metadata_blob, owner_principal, owner_canonical_id, \
                  initiator_principal, initiator_canonical_id, checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)",
                params![
                    req.upload_id,
                    req.bucket,
                    req.key,
                    now as i64,
                    tags,
                    req.metadata_blob.as_slice(),
                    system_metadata_blob,
                    req.owner.principal,
                    req.owner.canonical_id.as_str(),
                    req.initiator.as_ref().map(|owner| owner.principal.as_str()),
                    req.initiator
                        .as_ref()
                        .map(|owner| owner.canonical_id.as_str()),
                    algo,
                    ctype,
                    encryption_type,
                    encryption_state,
                    req.acl_grants.serialized(),
                    i32::from(req.public_read),
                    object_lock_retention_mode,
                    object_lock_retain_until,
                    object_lock_legal_hold,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "create multipart upload",
                source: e,
            })?;
        Ok(())
    }

    fn get_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, bucket, key, initiated_at, state, tags, metadata_blob, \
                 system_metadata_blob, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id, \
                 checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold \
                 FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
                |row| {
                    let state_raw = row.get::<_, u8>(4)?;
                    let algo_raw: Option<u8> = row.get(12)?;
                    let ctype_raw: Option<u8> = row.get(13)?;
                    let object_lock = Self::parse_object_lock_state(
                        row.get::<_, Option<u8>>(18)?,
                        row.get::<_, Option<i64>>(19)?,
                        row.get::<_, u8>(20)?,
                        18,
                        19,
                        20,
                    )?;
                    let checksum = if let Some(algo_val) = algo_raw {
                        let algo = ChecksumAlgorithm::from_u8(algo_val).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                12,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid checksum algorithm: {algo_val}")),
                            )
                        })?;
                        let ctype = ctype_raw
                            .map(|v| {
                                ChecksumType::from_u8(v).ok_or_else(|| {
                                    rusqlite::Error::FromSqlConversionFailure(
                                        13,
                                        rusqlite::types::Type::Integer,
                                        Box::from(format!("invalid checksum type: {v}")),
                                    )
                                })
                            })
                            .transpose()?;
                        Some(MultipartChecksumConfig::new(algo, ctype).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                12,
                                rusqlite::types::Type::Integer,
                                Box::from(e.reason),
                            )
                        })?)
                    } else {
                        None
                    };
                    let owner = Self::parse_owner_identity(
                        row,
                        8,
                        9,
                        "owner_principal",
                        "owner_canonical_id",
                    )?;
                    let initiator = Self::parse_optional_owner_identity(
                        row,
                        10,
                        11,
                        "initiator_principal",
                        "initiator_canonical_id",
                    )?;
                    Ok(MultipartUploadRecord {
                        upload_id: row.get(0)?,
                        bucket: row.get(1)?,
                        key: row.get(2)?,
                        initiated_at: row.get::<_, i64>(3)? as u64,
                        state: UploadState::from_u8(state_raw).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                4,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid upload state: {state_raw}")),
                            )
                        })?,
                        tags: row.get::<_, Option<String>>(5)?.map(SerializedTagSet::from),
                        metadata_blob: SerializedMetadataBlob::from(row.get::<_, Vec<u8>>(6)?),
                        system_metadata_blob: SerializedSystemMetadataBlob::from(
                            row.get::<_, Vec<u8>>(7)?,
                        ),
                        initiator,
                        owner,
                        acl_grants: Self::parse_acl_grants(
                            row.get::<_, String>(16)?,
                            16,
                            "multipart acl_grants",
                        )?,
                        public_read: row.get::<_, i64>(17)? != 0,
                        object_lock,
                        checksum,
                        encryption: Self::parse_object_encryption(
                            row.get::<_, u8>(14)?,
                            row.get::<_, Option<Vec<u8>>>(15)?,
                            14,
                            15,
                        )?,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart upload",
                source: e,
            })?
            .ok_or_else(|| MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            })
    }

    fn set_upload_state(
        &self,
        upload_id: &UploadId,
        new_state: UploadState,
    ) -> Result<(), MetadataError> {
        // Only Completing and Aborting are valid transition targets.
        // Check existence first so we return NoSuchUpload accurately.
        if new_state == UploadState::InProgress {
            let current = self
                .conn
                .query_row(
                    "SELECT state FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id.as_str()],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "set upload state (exists check)",
                    source: e,
                })?;
            return match current {
                Some(state) => Err(MetadataError::UploadNotInProgress { state }),
                None => Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }),
            };
        }
        let updated = self
            .conn
            .execute(
                "UPDATE multipart_uploads SET state = ?1 \
                 WHERE upload_id = ?2 AND state = 0",
                params![new_state as u8, upload_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "set upload state",
                source: e,
            })?;
        if updated == 0 {
            // Either the upload doesn't exist or it's not InProgress.
            let current = self
                .conn
                .query_row(
                    "SELECT state FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id.as_str()],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "set upload state (check)",
                    source: e,
                })?;
            return match current {
                None => Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }),
                Some(s) => Err(MetadataError::UploadNotInProgress { state: s }),
            };
        }
        Ok(())
    }

    fn delete_multipart_upload(&self, upload_id: &UploadId) -> Result<(), MetadataError> {
        let deleted = self
            .conn
            .execute(
                "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart upload",
                source: e,
            })?;
        if deleted == 0 {
            return Err(MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        Ok(())
    }

    fn get_completed_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Option<CompletedMultipartUploadRecord>, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, bucket, key, completed_at, owner_principal, owner_canonical_id, \
                 initiator_principal, initiator_canonical_id \
                 FROM completed_multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
                |row| {
                    let owner = Self::parse_owner_identity(
                        row,
                        4,
                        5,
                        "owner_principal",
                        "owner_canonical_id",
                    )?;
                    let initiator = Self::parse_optional_owner_identity(
                        row,
                        6,
                        7,
                        "initiator_principal",
                        "initiator_canonical_id",
                    )?;
                    Ok(CompletedMultipartUploadRecord {
                        upload_id: row.get(0)?,
                        bucket: row.get(1)?,
                        key: row.get(2)?,
                        completed_at: row.get::<_, i64>(3)? as u64,
                        initiator,
                        owner,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get completed multipart upload",
                source: e,
            })
    }

    fn delete_completed_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM completed_multipart_uploads WHERE bucket = ?1",
                params![bucket],
            )
            .map(|_| ())
            .map_err(|e| MetadataError::Db {
                context: "delete completed multipart uploads for bucket",
                source: e,
            })
    }

    fn list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_multipart_uploads",
            "pg_id={} bucket={:?} max_uploads={}",
            self.pg_id,
            req.bucket.as_str(),
            req.max_uploads
        );
        let limit = req.max_uploads as i64 + 1;
        let mut where_clauses = vec!["bucket = ?1".to_string()];
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.bucket.clone())];
        let mut param_idx = 2;

        if let Some(ref prefix) = req.prefix {
            where_clauses.push(format!("key >= ?{param_idx}"));
            params_vec.push(Box::new(prefix.clone()));
            param_idx += 1;

            if let Some(end) = object_key_prefix_upper_bound(prefix) {
                where_clauses.push(format!("key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        if let Some(ref key_marker) = req.key_marker {
            if let Some(uid_marker) = req.upload_id_marker.as_ref().map(UploadId::as_str) {
                // Resume after (key_marker, initiated_at of marker, uid_marker).
                // Use a subquery to resolve the marker's initiated_at so the
                // cursor is consistent with the (key, initiated_at, upload_id)
                // sort order. COALESCE to 0 so a deleted marker row safely
                // returns all remaining uploads for that key (duplicates are
                // preferable to silently dropped entries).
                where_clauses.push(format!(
                    "(key > ?{km} OR (key = ?{km} AND (\
                     initiated_at > COALESCE((SELECT initiated_at FROM multipart_uploads \
                     WHERE upload_id = ?{um} AND bucket = ?{bkt} AND key = ?{km}), 0) \
                     OR (initiated_at = COALESCE((SELECT initiated_at FROM multipart_uploads \
                     WHERE upload_id = ?{um} AND bucket = ?{bkt} AND key = ?{km}), 0) \
                     AND upload_id > ?{um}))))",
                    km = param_idx,
                    um = param_idx + 1,
                    bkt = param_idx + 2
                ));
                params_vec.push(Box::new(key_marker.clone()));
                params_vec.push(Box::new(uid_marker.to_string()));
                params_vec.push(Box::new(req.bucket.clone()));
                param_idx += 3;
            } else {
                where_clauses.push(format!("key > ?{param_idx}"));
                params_vec.push(Box::new(key_marker.clone()));
                param_idx += 1;
            }
        }

        let where_str = where_clauses.join(" AND ");
        let sql = format!(
            "SELECT upload_id, bucket, key, initiated_at, state, tags, metadata_blob, \
             system_metadata_blob, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id, \
             checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold \
             FROM multipart_uploads \
             WHERE {where_str} \
             ORDER BY key ASC, initiated_at ASC, upload_id ASC \
             LIMIT ?{param_idx}"
        );
        params_vec.push(Box::new(limit));

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql).map_err(|e| MetadataError::Db {
            context: "prepare list multipart uploads",
            source: e,
        })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let state_raw = row.get::<_, u8>(4)?;
                let algo_raw: Option<u8> = row.get(12)?;
                let ctype_raw: Option<u8> = row.get(13)?;
                let object_lock = Self::parse_object_lock_state(
                    row.get::<_, Option<u8>>(18)?,
                    row.get::<_, Option<i64>>(19)?,
                    row.get::<_, u8>(20)?,
                    18,
                    19,
                    20,
                )?;
                let checksum = if let Some(algo_val) = algo_raw {
                    let algo = ChecksumAlgorithm::from_u8(algo_val).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            12,
                            rusqlite::types::Type::Integer,
                            Box::from(format!("invalid checksum algorithm: {algo_val}")),
                        )
                    })?;
                    let ctype = ctype_raw
                        .map(|v| {
                            ChecksumType::from_u8(v).ok_or_else(|| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    13,
                                    rusqlite::types::Type::Integer,
                                    Box::from(format!("invalid checksum type: {v}")),
                                )
                            })
                        })
                        .transpose()?;
                    Some(MultipartChecksumConfig::new(algo, ctype).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            12,
                            rusqlite::types::Type::Integer,
                            Box::from(e.reason),
                        )
                    })?)
                } else {
                    None
                };
                let owner =
                    Self::parse_owner_identity(row, 8, 9, "owner_principal", "owner_canonical_id")?;
                let initiator = Self::parse_optional_owner_identity(
                    row,
                    10,
                    11,
                    "initiator_principal",
                    "initiator_canonical_id",
                )?;
                Ok(MultipartUploadRecord {
                    upload_id: row.get(0)?,
                    bucket: row.get(1)?,
                    key: row.get(2)?,
                    initiated_at: row.get::<_, i64>(3)? as u64,
                    state: UploadState::from_u8(state_raw).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Integer,
                            Box::from(format!("invalid upload state: {state_raw}")),
                        )
                    })?,
                    tags: row.get::<_, Option<String>>(5)?.map(SerializedTagSet::from),
                    metadata_blob: SerializedMetadataBlob::from(row.get::<_, Vec<u8>>(6)?),
                    system_metadata_blob: SerializedSystemMetadataBlob::from(
                        row.get::<_, Vec<u8>>(7)?,
                    ),
                    initiator,
                    owner,
                    acl_grants: Self::parse_acl_grants(
                        row.get::<_, String>(16)?,
                        16,
                        "multipart acl_grants",
                    )?,
                    public_read: row.get::<_, i64>(17)? != 0,
                    object_lock,
                    checksum,
                    encryption: Self::parse_object_encryption(
                        row.get::<_, u8>(14)?,
                        row.get::<_, Option<Vec<u8>>>(15)?,
                        14,
                        15,
                    )?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "list multipart uploads query",
                source: e,
            })?;

        let mut uploads: Vec<MultipartUploadRecord> = Vec::new();
        for row in rows {
            uploads.push(row.map_err(|e| MetadataError::Db {
                context: "list multipart uploads row",
                source: e,
            })?);
        }

        let is_truncated = uploads.len() as i64 > req.max_uploads as i64;
        if is_truncated {
            uploads.truncate(req.max_uploads as usize);
        }

        let (next_key_marker, next_upload_id_marker) = if is_truncated {
            uploads.last().map_or((None, None), |u| {
                (Some(u.key.clone()), Some(u.upload_id.clone()))
            })
        } else {
            (None, None)
        };

        Ok(ListMultipartUploadsResp {
            uploads,
            is_truncated,
            next_key_marker,
            next_upload_id_marker,
        })
    }

    fn upsert_multipart_part(
        &self,
        part: &MultipartPartRecord,
    ) -> Result<Option<u32>, MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "upsert part (begin txn)",
                source: e,
            })?;

        let result = (|| -> Result<Option<u32>, rusqlite::Error> {
            // Read previous generation before overwrite.
            let prev_gen: Option<u32> = self
                .conn
                .query_row(
                    "SELECT generation FROM multipart_parts \
                     WHERE upload_id = ?1 AND part_number = ?2",
                    params![part.upload_id, part.part_number],
                    |row| row.get::<_, i64>(0).map(|v| v as u32),
                )
                .optional()?;

            self.conn.execute(
                "INSERT OR REPLACE INTO multipart_parts \
                 (upload_id, part_number, generation, size, etag, etag_kind, \
                  part_okh, part_vid, ec_k, ec_m, last_modified, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    part.upload_id,
                    part.part_number,
                    part.generation,
                    part.size as i64,
                    part.etag,
                    part.etag_kind as u8,
                    part.part_okh.as_slice(),
                    part.part_vid.get() as i64,
                    part.ec_k,
                    part.ec_m,
                    part.last_modified as i64,
                    part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                ],
            )?;

            Ok(prev_gen)
        })();

        match result {
            Ok(prev_gen) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "upsert part (commit txn)",
                        source: e,
                    });
                }
                Ok(prev_gen)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                // FK violation means the upload_id doesn't exist.
                if let rusqlite::Error::SqliteFailure(ref err, _) = e {
                    if err.code == rusqlite::ffi::ErrorCode::ConstraintViolation {
                        return Err(MetadataError::NoSuchUpload {
                            upload_id: part.upload_id.to_string(),
                        });
                    }
                }
                Err(MetadataError::Db {
                    context: "upsert multipart part",
                    source: e,
                })
            }
        }
    }

    fn upsert_multipart_part_segments(
        &self,
        part: &MultipartPartRecord,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(Option<u32>, Vec<MultipartPartSegmentRecord>), MetadataError> {
        if part.part_okh != [0u8; 16] {
            return Err(MetadataError::Db {
                context: "upsert multipart part segments (non-segment part)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("segmented multipart parts must use zero part_okh sentinel"),
                ),
            });
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "upsert multipart part segments (begin txn)",
                source: e,
            })?;

        let result =
            (|| -> Result<(Option<u32>, Vec<MultipartPartSegmentRecord>), rusqlite::Error> {
                let prev_gen: Option<u32> = self
                    .conn
                    .query_row(
                        "SELECT generation FROM multipart_parts \
                     WHERE upload_id = ?1 AND part_number = ?2",
                        params![part.upload_id, part.part_number],
                        |row| row.get::<_, i64>(0).map(|v| v as u32),
                    )
                    .optional()?;

                let mut prev_stmt = self.conn.prepare(
                    "SELECT bucket, key, upload_id, version_id, part_number, segment_index, size, \
                 segment_crc64, segment_okh, segment_vid, shard_pg_id, ec_k, ec_m \
                 FROM multipart_part_segments \
                 WHERE upload_id = ?1 AND version_id = ?2 AND part_number = ?3 \
                 ORDER BY segment_index ASC",
                )?;
                let prev_rows = prev_stmt.query_map(
                    params![
                        part.upload_id,
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                        part.part_number
                    ],
                    |row| {
                        let okh_blob: Vec<u8> = row.get(8)?;
                        let okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                        Ok(MultipartPartSegmentRecord {
                            bucket: row.get(0)?,
                            key: row.get(1)?,
                            upload_id: row.get(2)?,
                            version_id: row.get::<_, i64>(3)? as u64,
                            part_number: row.get(4)?,
                            segment_index: row.get(5)?,
                            size: row.get::<_, i64>(6)? as u64,
                            segment_crc64: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                            segment_okh: okh,
                            segment_vid: Self::parse_generation_id(
                                row.get::<_, i64>(9)?,
                                9,
                                "segment_vid",
                            )?,
                            shard_pg_id: row.get(10)?,
                            ec_k: row.get(11)?,
                            ec_m: row.get(12)?,
                        })
                    },
                )?;
                let prev_segments = prev_rows.collect::<Result<Vec<_>, _>>()?;

                self.conn.execute(
                    "INSERT OR REPLACE INTO multipart_parts \
                 (upload_id, part_number, generation, size, etag, etag_kind, \
                  part_okh, part_vid, ec_k, ec_m, last_modified, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    params![
                        part.upload_id,
                        part.part_number,
                        part.generation,
                        part.size as i64,
                        part.etag,
                        part.etag_kind as u8,
                        part.part_okh.as_slice(),
                        part.part_vid.get() as i64,
                        part.ec_k,
                        part.ec_m,
                        part.last_modified as i64,
                        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                    ],
                )?;

                self.conn.execute(
                    "DELETE FROM multipart_part_segments \
                 WHERE upload_id = ?1 AND version_id = ?2 AND part_number = ?3",
                    params![
                        part.upload_id,
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                        part.part_number
                    ],
                )?;

                let mut stmt = self.conn.prepare(
                    "INSERT INTO multipart_part_segments \
                 (bucket, key, upload_id, version_id, part_number, segment_index, size, \
                  segment_crc64, segment_okh, segment_vid, shard_pg_id, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                )?;
                for segment in segments {
                    if segment.upload_id != part.upload_id
                        || segment.part_number != part.part_number
                        || segment.version_id != PART_SEGMENT_STAGING_VERSION_ID.to_u64()
                    {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Null,
                            Box::from("segment row does not match multipart part identity"),
                        ));
                    }
                    stmt.execute(params![
                        segment.bucket,
                        segment.key,
                        segment.upload_id,
                        segment.version_id as i64,
                        segment.part_number,
                        segment.segment_index,
                        segment.size as i64,
                        segment.segment_crc64.map(|v| v as i64),
                        segment.segment_okh.as_slice(),
                        segment.segment_vid.get() as i64,
                        segment.shard_pg_id,
                        segment.ec_k,
                        segment.ec_m,
                    ])?;
                }

                Ok((prev_gen, prev_segments))
            })();

        match result {
            Ok(prev_state) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "upsert multipart part segments (commit txn)",
                        source: e,
                    });
                }
                Ok(prev_state)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                if let rusqlite::Error::SqliteFailure(ref err, _) = e {
                    if err.code == rusqlite::ffi::ErrorCode::ConstraintViolation {
                        return Err(MetadataError::NoSuchUpload {
                            upload_id: part.upload_id.to_string(),
                        });
                    }
                }
                Err(MetadataError::Db {
                    context: "upsert multipart part segments",
                    source: e,
                })
            }
        }
    }

    fn get_multipart_part(
        &self,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<MultipartPartRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, part_number, generation, size, etag, etag_kind, \
                 part_okh, part_vid, ec_k, ec_m, last_modified, checksum \
                 FROM multipart_parts WHERE upload_id = ?1 AND part_number = ?2",
                params![upload_id.as_str(), part_number],
                Self::row_to_multipart_part,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart part",
                source: e,
            })?
            .ok_or(MetadataError::PartNotFound {
                upload_id: upload_id.to_string(),
                part_number,
            })
    }

    fn list_multipart_parts(&self, req: &ListPartsReq) -> Result<ListPartsResp, MetadataError> {
        // Verify the upload exists so we return NoSuchUpload, not an empty list.
        let exists = self
            .conn
            .query_row(
                "SELECT 1 FROM multipart_uploads WHERE upload_id = ?1",
                params![req.upload_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "list parts (upload exists check)",
                source: e,
            })?;
        if exists.is_none() {
            return Err(MetadataError::NoSuchUpload {
                upload_id: req.upload_id.to_string(),
            });
        }

        let limit = req.max_parts as i64 + 1;

        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.upload_id.clone())];
        let sql = if let Some(marker) = req.part_number_marker {
            params_vec.push(Box::new(marker));
            params_vec.push(Box::new(limit));
            "SELECT upload_id, part_number, generation, size, etag, etag_kind, \
             part_okh, part_vid, ec_k, ec_m, last_modified, checksum \
             FROM multipart_parts \
             WHERE upload_id = ?1 AND part_number > ?2 \
             ORDER BY part_number ASC LIMIT ?3"
                .to_string()
        } else {
            params_vec.push(Box::new(limit));
            "SELECT upload_id, part_number, generation, size, etag, etag_kind, \
             part_okh, part_vid, ec_k, ec_m, last_modified, checksum \
             FROM multipart_parts \
             WHERE upload_id = ?1 \
             ORDER BY part_number ASC LIMIT ?2"
                .to_string()
        };

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql).map_err(|e| MetadataError::Db {
            context: "prepare list multipart parts",
            source: e,
        })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_multipart_part)
            .map_err(|e| MetadataError::Db {
                context: "list multipart parts query",
                source: e,
            })?;

        let mut parts: Vec<MultipartPartRecord> = Vec::new();
        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "list multipart parts row",
                source: e,
            })?);
        }

        let is_truncated = parts.len() as i64 > req.max_parts as i64;
        if is_truncated {
            parts.truncate(req.max_parts as usize);
        }

        let next_part_number_marker = if is_truncated {
            parts.last().map(|p| p.part_number)
        } else {
            None
        };

        Ok(ListPartsResp {
            parts,
            is_truncated,
            next_part_number_marker,
        })
    }

    fn commit_object_parts(&self, parts: &[ObjectPartRecord]) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "commit object parts (begin txn)",
                source: e,
            })?;

        let result = (|| {
            let mut stmt = self.conn.prepare(
                "INSERT INTO object_parts \
                 (bucket, key, version_id, part_number, object_offset_start, size, etag, etag_kind, \
                  part_okh, part_vid, ec_k, ec_m, shard_pg_id, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            )?;

            let mut ordered_parts: Vec<&ObjectPartRecord> = parts.iter().collect();
            ordered_parts.sort_by_key(|part| part.part_number);
            let mut object_offset_start = 0u64;
            for part in ordered_parts {
                stmt.execute(params![
                    part.bucket,
                    part.key,
                    part.version_id.to_u64() as i64,
                    part.part_number,
                    object_offset_start as i64,
                    part.size as i64,
                    part.etag,
                    part.etag_kind as u8,
                    part.part_okh.as_slice(),
                    part.part_vid.get() as i64,
                    part.ec_k,
                    part.ec_m,
                    part.shard_pg_id,
                    part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                ])?;
                object_offset_start += part.size;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "commit object parts (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(MetadataError::Db {
                    context: "commit object parts",
                    source: e,
                })
            }
        }
    }

    fn get_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectPartRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT bucket, key, version_id, part_number, size, etag, etag_kind, \
                 part_okh, part_vid, ec_k, ec_m, shard_pg_id, checksum \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts",
                source: e,
            })?;

        let rows = stmt
            .query_map(
                params![bucket, key, version_id.to_u64() as i64],
                Self::row_to_object_part,
            )
            .map_err(|e| MetadataError::Db {
                context: "get object parts query",
                source: e,
            })?;

        let mut parts = Vec::new();
        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "get object parts row",
                source: e,
            })?);
        }
        Ok(parts)
    }

    fn get_object_parts_overlapping_range(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        start: u64,
        end_exclusive: u64,
    ) -> Result<Vec<ObjectPartRangeRecord>, MetadataError> {
        if start >= end_exclusive {
            return Ok(Vec::new());
        }

        let mut first_stmt = self
            .conn
            .prepare(
                "SELECT bucket, key, version_id, part_number, size, etag, etag_kind, \
                 part_okh, part_vid, ec_k, ec_m, shard_pg_id, checksum, object_offset_start \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND object_offset_start <= ?4 \
                   AND object_offset_start + size > ?4 \
                 ORDER BY object_offset_start DESC, part_number ASC \
                 LIMIT 1",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts overlapping range first part",
                source: e,
            })?;

        let first = first_stmt
            .query_row(
                params![bucket, key, version_id.to_u64() as i64, start as i64],
                Self::row_to_object_part_range,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object parts overlapping range first part",
                source: e,
            })?;

        let Some(first) = first else {
            return Ok(Vec::new());
        };

        let first_part_number = first.part.part_number;
        let first_part_end = first.object_offset_start + first.part.size;
        if first_part_end >= end_exclusive {
            return Ok(vec![first]);
        }

        let mut parts = vec![first];
        let mut tail_stmt = self
            .conn
            .prepare(
                "SELECT bucket, key, version_id, part_number, size, etag, etag_kind, \
                 part_okh, part_vid, ec_k, ec_m, shard_pg_id, checksum, object_offset_start \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND part_number > ?4 \
                   AND object_offset_start < ?5 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts overlapping range tail parts",
                source: e,
            })?;

        let rows = tail_stmt
            .query_map(
                params![
                    bucket,
                    key,
                    version_id.to_u64() as i64,
                    first_part_number,
                    end_exclusive as i64
                ],
                Self::row_to_object_part_range,
            )
            .map_err(|e| MetadataError::Db {
                context: "get object parts overlapping range tail parts",
                source: e,
            })?;

        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "get object parts overlapping range tail row",
                source: e,
            })?);
        }

        Ok(parts)
    }

    fn delete_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object parts",
                source: e,
            })?;
        Ok(())
    }

    fn complete_multipart_commit(
        &self,
        upload_id: &UploadId,
        completion_order: u64,
        obj: &CommitMultipartReq,
        parts: &[ObjectPartRecord],
    ) -> Result<(), MetadataError> {
        if parts.is_empty() {
            return Err(MetadataError::Db {
                context: "complete multipart commit (empty parts)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("multipart commit requires at least one part"),
                ),
            });
        }
        let now = PgStore::now_millis();
        let data_layout = DataLayout::MultipartManifest as u8;
        let parts_count = Some(parts.len() as i64);
        let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
        let metadata_blob = obj
            .metadata_blob
            .as_ref()
            .map(SerializedMetadataBlob::as_slice);
        let system_metadata_blob = obj
            .system_metadata_blob
            .as_ref()
            .map(SerializedSystemMetadataBlob::as_slice);
        let encryption_type = obj.encryption.encryption_type() as u8;
        let encryption_state = obj.encryption.encode_state();

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "complete multipart commit (begin txn)",
                source: e,
            })?;

        let result = (|| -> Result<(), rusqlite::Error> {
            // Validate part identity matches object.
            for part in parts {
                if part.bucket != obj.bucket
                    || part.key != obj.key
                    || part.version_id != obj.version_id
                {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from("part does not match object"),
                    ));
                }
            }

            // 1. Transition upload to Completing.
            let updated = self.conn.execute(
                "UPDATE multipart_uploads SET state = ?1 \
                 WHERE upload_id = ?2 AND state = 0",
                params![UploadState::Completing as u8, upload_id.as_str()],
            )?;
            if updated == 0 {
                // Check if it's already Completing (idempotent retry).
                let current: Option<u8> = self
                    .conn
                    .query_row(
                        "SELECT state FROM multipart_uploads WHERE upload_id = ?1",
                        params![upload_id.as_str()],
                        |row| row.get(0),
                    )
                    .optional()?;
                match current {
                    Some(1) => { /* Already Completing — allow idempotent retry */ }
                    _ => {
                        return Err(rusqlite::Error::QueryReturnedNoRows);
                    }
                }
            }
            let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
                Self::object_lock_sql_values(obj.object_lock)?;
            let write_sequence = self
                .next_object_write_sequence(obj.bucket.as_str(), obj.key.as_str())
                .map_err(|error| match error {
                    MetadataError::Db { source, .. } => source,
                    other => rusqlite::Error::ToSqlConversionFailure(Box::new(
                        std::io::Error::other(other.to_string()),
                    )),
                })?;
            self.mark_current_live_noncurrent(
                obj.bucket.as_str(),
                obj.key.as_str(),
                obj.version_id,
                now,
            )?;

            // 2. Write/overwrite object metadata row.
            let obj_sql = if obj.version_id.is_null() {
                "INSERT OR REPLACE INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, tags, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            } else {
                "INSERT INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, tags, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            };
            self.conn.execute(
                obj_sql,
                params![
                    obj.bucket,
                    obj.key,
                    obj.version_id.to_u64() as i64,
                    write_sequence as i64,
                    obj.generation_id.get() as i64,
                    obj.size as i64,
                    obj.etag_crc64.as_slice(),
                    EtagKind::MultipartComposite as u8,
                    now as i64,
                    obj.ec.k,
                    obj.ec.m,
                    ObjectState::Live as u8,
                    tags,
                    data_layout,
                    parts_count,
                    metadata_blob,
                    system_metadata_blob,
                    encryption_type,
                    encryption_state,
                    obj.owner.principal,
                    obj.owner.canonical_id.as_str(),
                    obj.acl_grants.serialized(),
                    i32::from(obj.public_read),
                    object_lock_retention_mode,
                    object_lock_retain_until,
                    object_lock_legal_hold,
                ],
            )?;

            // 3. Delete prior object_parts (null-version overwrite).
            self.conn.execute(
                "DELETE FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![obj.bucket, obj.key, obj.version_id.to_u64() as i64],
            )?;

            // 4. Insert new manifest rows.
            {
                let mut stmt = self.conn.prepare(
                    "INSERT INTO object_parts \
                     (bucket, key, version_id, part_number, object_offset_start, size, etag, etag_kind, \
                      part_okh, part_vid, ec_k, ec_m, shard_pg_id, checksum) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                )?;
                let mut ordered_parts: Vec<&ObjectPartRecord> = parts.iter().collect();
                ordered_parts.sort_by_key(|part| part.part_number);
                let mut object_offset_start = 0u64;
                for part in ordered_parts {
                    stmt.execute(params![
                        part.bucket,
                        part.key,
                        part.version_id.to_u64() as i64,
                        part.part_number,
                        object_offset_start as i64,
                        part.size as i64,
                        part.etag,
                        part.etag_kind as u8,
                        part.part_okh.as_slice(),
                        part.part_vid.get() as i64,
                        part.ec_k,
                        part.ec_m,
                        part.shard_pg_id,
                        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                    ])?;
                    object_offset_start += part.size;
                }
            }

            // 5. Clean up stale multipart_part_segments from prior uploads to
            //    the same key+version_id (e.g. overwriting in unversioned mode).
            self.conn.execute(
                "DELETE FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND upload_id != ?4",
                params![
                    obj.bucket,
                    obj.key,
                    obj.version_id.to_u64() as i64,
                    upload_id
                ],
            )?;

            // 6. Reparent this upload's segments from staging version_id to
            //    the real object version_id so reads can find them.
            self.conn.execute(
                "UPDATE multipart_part_segments \
                 SET version_id = ?1 \
                 WHERE bucket = ?2 AND key = ?3 AND upload_id = ?4 \
                 AND version_id = ?5",
                params![
                    obj.version_id.to_u64() as i64,
                    obj.bucket,
                    obj.key,
                    upload_id,
                    PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                ],
            )?;

            // 7. Record this upload as completed so AbortMultipartUpload can
            //    remain idempotently successful for the exact completed upload_id.
            let (initiator_principal, initiator_canonical_id): (Option<String>, Option<String>) =
                self.conn.query_row(
                    "SELECT initiator_principal, initiator_canonical_id \
                     FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
            self.conn.execute(
                "INSERT OR REPLACE INTO completed_multipart_uploads \
                 (upload_id, bucket, key, completion_order, completed_at, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    upload_id,
                    obj.bucket,
                    obj.key,
                    completion_order as i64,
                    now as i64,
                    obj.owner.principal,
                    obj.owner.canonical_id.as_str(),
                    initiator_principal,
                    initiator_canonical_id,
                ],
            )?;

            // 8. Delete in-progress upload + parts (CASCADE).
            self.conn.execute(
                "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id],
            )?;

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "complete multipart commit (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(MetadataError::Db {
                    context: "complete multipart commit",
                    source: e,
                })
            }
        }
    }
    // ── Streaming upload session methods ──────────────────────────────

    fn create_stream_upload(&self, req: &CreateStreamUploadReq) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_stream_upload",
            "pg_id={} session_id={:?} bucket={:?} key={:?}",
            self.pg_id,
            req.session_id.as_str(),
            req.bucket.as_str(),
            req.key.as_str()
        );
        let now = PgStore::now_millis();
        let op_kind = req.target.op_kind() as u8;
        let upload_id = req.target.upload_id();
        let part_number = req.target.part_number().map(|n| n as i64);
        let encryption_type = req.encryption.encryption_type() as u8;
        let encryption_state = req.encryption.encode_state();
        self.conn
            .execute(
                "INSERT INTO stream_uploads \
                 (session_id, bucket, key, op_kind, upload_id, part_number, state, created_at, encryption_type, encryption_state) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9)",
                params![
                    req.session_id,
                    req.bucket,
                    req.key,
                    op_kind,
                    upload_id,
                    part_number,
                    now as i64,
                    encryption_type,
                    encryption_state,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "create stream upload",
                source: e,
            })?;
        Ok(())
    }

    fn get_stream_upload(
        &self,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT session_id, bucket, key, op_kind, upload_id, part_number, state, \
                 created_at, encryption_type, encryption_state FROM stream_uploads WHERE session_id = ?1",
                params![session_id.as_str()],
                |row| {
                    let op_kind_raw: u8 = row.get(3)?;
                    let state_raw: u8 = row.get(6)?;
                    let upload_id: Option<UploadId> = row.get(4)?;
                    let part_number: Option<i64> = row.get(5)?;
                    Ok(StreamUploadRecord {
                        session_id: row.get(0)?,
                        bucket: row.get(1)?,
                        key: row.get(2)?,
                        target: PgStore::parse_stream_target(
                            op_kind_raw,
                            upload_id,
                            part_number,
                            3,
                        )?,
                        state: StreamUploadState::from_u8(state_raw).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                6,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid stream state: {state_raw}")),
                            )
                        })?,
                        created_at: row.get::<_, i64>(7)? as u64,
                        encryption: Self::parse_object_encryption(
                            row.get::<_, u8>(8)?,
                            row.get::<_, Option<Vec<u8>>>(9)?,
                            8,
                            9,
                        )?,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get stream upload",
                source: e,
            })?
            .ok_or_else(|| MetadataError::StreamSessionNotFound {
                session_id: session_id.to_string(),
            })
    }

    fn set_stream_upload_state(
        &self,
        session_id: &SessionId,
        new_state: StreamUploadState,
    ) -> Result<(), MetadataError> {
        let current: u8 = self
            .conn
            .query_row(
                "SELECT state FROM stream_uploads WHERE session_id = ?1",
                params![session_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get stream upload state",
                source: e,
            })?
            .ok_or_else(|| MetadataError::StreamSessionNotFound {
                session_id: session_id.to_string(),
            })?;

        if current != StreamUploadState::InProgress as u8 {
            return Err(MetadataError::StreamSessionNotInProgress { state: current });
        }

        self.conn
            .execute(
                "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                params![new_state as u8, session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "set stream upload state",
                source: e,
            })?;
        Ok(())
    }

    fn delete_stream_upload(&self, session_id: &SessionId) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM stream_uploads WHERE session_id = ?1",
                params![session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete stream upload",
                source: e,
            })?;
        Ok(())
    }

    fn list_all_stream_uploads(&self) -> Result<Vec<StreamUploadRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT session_id, bucket, key, op_kind, upload_id, part_number, state, \
                 created_at, encryption_type, encryption_state FROM stream_uploads",
            )
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (prepare)",
                source: e,
            })?;
        let rows = stmt
            .query_map([], |row| {
                let op_kind_raw: u8 = row.get(3)?;
                let state_raw: u8 = row.get(6)?;
                let upload_id: Option<UploadId> = row.get(4)?;
                let part_number: Option<i64> = row.get(5)?;
                Ok(StreamUploadRecord {
                    session_id: row.get(0)?,
                    bucket: row.get(1)?,
                    key: row.get(2)?,
                    target: PgStore::parse_stream_target(op_kind_raw, upload_id, part_number, 3)?,
                    state: StreamUploadState::from_u8(state_raw).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Integer,
                            Box::from(format!("invalid stream state: {state_raw}")),
                        )
                    })?,
                    created_at: row.get::<_, i64>(7)? as u64,
                    encryption: Self::parse_object_encryption(
                        row.get::<_, u8>(8)?,
                        row.get::<_, Option<Vec<u8>>>(9)?,
                        8,
                        9,
                    )?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (query)",
                source: e,
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (collect)",
                source: e,
            })
    }

    fn allocate_stream_segment_vid(
        &self,
        session_id: &SessionId,
    ) -> Result<GenerationId, MetadataError> {
        let (state, next_segment_vid): (u8, i64) = self
            .conn
            .query_row(
                "SELECT state, next_segment_vid FROM stream_uploads WHERE session_id = ?1",
                params![session_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get next stream segment vid",
                source: e,
            })?
            .ok_or_else(|| MetadataError::StreamSessionNotFound {
                session_id: session_id.to_string(),
            })?;

        if state != StreamUploadState::InProgress as u8 {
            return Err(MetadataError::StreamSessionNotInProgress { state });
        }

        let segment_vid = Self::parse_generation_id(next_segment_vid, 1, "next_segment_vid")
            .map_err(|e| MetadataError::Db {
                context: "parse next stream segment vid",
                source: e,
            })?;
        let next_segment_vid =
            next_segment_vid
                .checked_add(1)
                .ok_or_else(|| MetadataError::Db {
                    context: "stream segment vid overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Integer,
                        Box::from("next_segment_vid overflow"),
                    ),
                })?;

        self.conn
            .execute(
                "UPDATE stream_uploads SET next_segment_vid = ?1 WHERE session_id = ?2",
                params![next_segment_vid, session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "advance next stream segment vid",
                source: e,
            })?;

        Ok(segment_vid)
    }

    fn append_stream_segment(
        &self,
        segment: &StreamUploadSegmentRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "INSERT INTO stream_upload_segments \
                 (session_id, segment_index, size, segment_crc64, segment_okh, segment_vid, shard_pg_id, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    segment.session_id,
                    segment.segment_index,
                    segment.size as i64,
                    segment.segment_crc64.map(|v| v as i64),
                    segment.segment_okh.as_slice(),
                    segment.segment_vid.get() as i64,
                    segment.shard_pg_id,
                    segment.ec_k,
                    segment.ec_m,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "append stream segment",
                source: e,
            })?;
        Ok(())
    }

    fn list_stream_segments(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT session_id, segment_index, size, segment_okh, segment_vid, shard_pg_id, \
                 segment_crc64, ec_k, ec_m FROM stream_upload_segments \
                 WHERE session_id = ?1 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list stream segments",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![session_id.as_str()], |row| {
                let okh_blob: Vec<u8> = row.get(3)?;
                let okh = PgStore::parse_okh_blob(&okh_blob, 3)?;
                Ok(StreamUploadSegmentRecord {
                    session_id: row.get(0)?,
                    segment_index: row.get(1)?,
                    size: row.get::<_, i64>(2)? as u64,
                    segment_crc64: row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                    segment_okh: okh,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(4)?,
                        4,
                        "segment_vid",
                    )?,
                    shard_pg_id: row.get(5)?,
                    ec_k: row.get(7)?,
                    ec_m: row.get(8)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "list stream segments",
                source: e,
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "list stream segments row",
                source: e,
            })?);
        }
        Ok(segments)
    }

    fn commit_stream_put(
        &self,
        session_id: &SessionId,
        obj: &CommitStreamPutReq,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::commit_stream_put",
            "pg_id={} session_id={:?} bucket={:?} key={:?} segments={}",
            self.pg_id,
            session_id,
            obj.bucket.as_str(),
            obj.key.as_str(),
            segments.len()
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "commit stream put (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            // 1. Verify session exists, is InProgress, is PutObject kind, and matches
            //    the target bucket/key. Then transition to Completing.
            let row: Option<StreamSessionRow> = self
                .conn
                .query_row(
                    "SELECT state, op_kind, bucket, key, upload_id, part_number \
                     FROM stream_uploads \
                     WHERE session_id = ?1",
                    params![session_id.as_str()],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (lookup session)",
                    source: e,
                })?;

            let (current, op_kind_raw, sess_bucket, sess_key, upload_id, part_number) = row
                .ok_or_else(|| MetadataError::StreamSessionNotFound {
                    session_id: session_id.to_string(),
                })?;
            let target = PgStore::parse_stream_target(op_kind_raw, upload_id, part_number, 1)
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (parse session target)",
                    source: e,
                })?;

            if current != StreamUploadState::InProgress as u8 {
                return Err(MetadataError::StreamSessionNotInProgress { state: current });
            }
            if target != StreamUploadTarget::PutObject
                || sess_bucket != obj.bucket
                || sess_key != obj.key
            {
                return Err(MetadataError::StreamSessionNotFound {
                    session_id: session_id.to_string(),
                });
            }

            self.conn
                .execute(
                    "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                    params![StreamUploadState::Completing as u8, session_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (set completing)",
                    source: e,
                })?;

            // 2. Write/overwrite object metadata row.
            let now = PgStore::now_millis();
            let data_layout = DataLayout::StandardInternal as u8;
            let parts_count: Option<i64> = None;
            let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
            let encryption_type = obj.encryption.encryption_type() as u8;
            let encryption_state = obj.encryption.encode_state();
            let system_metadata_blob = obj
                .system_metadata_blob
                .as_ref()
                .map(SerializedSystemMetadataBlob::as_slice);
            let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
                Self::object_lock_sql_values(obj.object_lock).map_err(|e| MetadataError::Db {
                    context: "commit stream put (encode object lock)",
                    source: e,
                })?;
            let write_sequence =
                self.next_object_write_sequence(obj.bucket.as_str(), obj.key.as_str())?;
            self.mark_current_live_noncurrent(
                obj.bucket.as_str(),
                obj.key.as_str(),
                obj.version_id,
                now,
            )
            .map_err(|e| MetadataError::Db {
                context: "commit stream put (mark noncurrent)",
                source: e,
            })?;

            let obj_sql = if obj.version_id.is_null() {
                "INSERT OR REPLACE INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            } else {
                "INSERT INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            };
            self.conn
                .execute(
                    obj_sql,
                    params![
                        obj.bucket,
                        obj.key,
                        obj.version_id.to_u64() as i64,
                        write_sequence as i64,
                        obj.generation_id.get() as i64,
                        obj.size as i64,
                        obj.etag_crc64.to_be_bytes().as_slice(),
                        EtagKind::Crc64 as u8,
                        now as i64,
                        obj.ec.k,
                        obj.ec.m,
                        ObjectState::Live as u8,
                        data_layout,
                        parts_count,
                        tags,
                        obj.metadata_blob
                            .as_ref()
                            .map(SerializedMetadataBlob::as_slice),
                        system_metadata_blob,
                        encryption_type,
                        encryption_state,
                        obj.owner.principal,
                        obj.owner.canonical_id.as_str(),
                        obj.acl_grants.serialized(),
                        i32::from(obj.public_read),
                        object_lock_retention_mode,
                        object_lock_retain_until,
                        object_lock_legal_hold,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (write object)",
                    source: e,
                })?;

            // 3. Delete any prior object_segments for this version.
            self.conn
                .execute(
                    "DELETE FROM object_segments \
                     WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![obj.bucket, obj.key, obj.version_id.to_u64() as i64],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (delete prior segments)",
                    source: e,
                })?;

            // 4. Insert committed object segment rows.
            {
                let mut stmt = self
                    .conn
                    .prepare(
                        "INSERT INTO object_segments \
                         (bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, segment_vid, \
                          shard_pg_id, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream put (prepare insert segments)",
                        source: e,
                    })?;
                for segment in segments {
                    if segment.bucket != obj.bucket
                        || segment.key != obj.key
                        || segment.version_id != obj.version_id
                    {
                        return Err(MetadataError::StreamSessionNotFound {
                            session_id: session_id.to_string(),
                        });
                    }
                    stmt.execute(params![
                        segment.bucket,
                        segment.key,
                        segment.version_id.to_u64() as i64,
                        segment.segment_index,
                        segment.size as i64,
                        segment.segment_crc64.map(|v| v as i64),
                        segment.segment_okh.as_slice(),
                        segment.segment_vid.get() as i64,
                        segment.shard_pg_id,
                        segment.ec_k,
                        segment.ec_m,
                    ])
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream put (insert segment)",
                        source: e,
                    })?;
                }
            }

            // 5. Delete staging rows.
            self.conn
                .execute(
                    "DELETE FROM stream_uploads WHERE session_id = ?1",
                    params![session_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (delete staging)",
                    source: e,
                })?;

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "commit stream put (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn put_object_with_segments(
        &self,
        obj: &PutLiveObjectReq,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), MetadataError> {
        obj.validate().map_err(|msg| MetadataError::Db {
            context: "put segment object (etag/layout mismatch)",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Null,
                Box::from(msg),
            ),
        })?;
        if obj.layout != ObjectLayout::Standard {
            return Err(MetadataError::Db {
                context: "put segment object (non-segment layout)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("put_object_with_segments requires Standard layout"),
                ),
            });
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put segment object (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            let now = PgStore::now_millis();
            let data_layout = obj.layout.data_layout() as u8;
            let etag_kind = obj.etag.etag_kind() as u8;
            let status = ObjectState::Live as u8;
            let parts_count = obj.layout.parts_count().map(|n| n as i64);
            let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
            let metadata_blob = obj
                .metadata_blob
                .as_ref()
                .map(SerializedMetadataBlob::as_slice);
            let system_metadata_blob = obj
                .system_metadata_blob
                .as_ref()
                .map(SerializedSystemMetadataBlob::as_slice);
            let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
                Self::object_lock_sql_values(obj.object_lock).map_err(|e| MetadataError::Db {
                    context: "put segment object (encode object lock)",
                    source: e,
                })?;
            let encryption_type = obj.encryption.encryption_type() as u8;
            let encryption_state = obj.encryption.encode_state();
            let write_sequence =
                self.next_object_write_sequence(obj.bucket.as_str(), obj.key.as_str())?;
            self.mark_current_live_noncurrent(
                obj.bucket.as_str(),
                obj.key.as_str(),
                obj.version_id,
                now,
            )
            .map_err(|e| MetadataError::Db {
                context: "put segment object (mark noncurrent)",
                source: e,
            })?;

            let obj_sql = if obj.version_id.is_null() {
                "INSERT OR REPLACE INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            } else {
                "INSERT INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            };
            self.conn
                .execute(
                    obj_sql,
                    params![
                        obj.bucket,
                        obj.key,
                        obj.version_id.to_u64() as i64,
                        write_sequence as i64,
                        obj.generation_id.get() as i64,
                        obj.size as i64,
                        obj.etag.as_bytes().as_slice(),
                        etag_kind,
                        now as i64,
                        obj.ec.k,
                        obj.ec.m,
                        status,
                        data_layout,
                        parts_count,
                        tags,
                        metadata_blob,
                        system_metadata_blob,
                        encryption_type,
                        encryption_state,
                        obj.owner.principal,
                        obj.owner.canonical_id.as_str(),
                        obj.acl_grants.serialized(),
                        i32::from(obj.public_read),
                        object_lock_retention_mode,
                        object_lock_retain_until,
                        object_lock_legal_hold,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (write object)",
                    source: e,
                })?;

            self.conn
                .execute(
                    "DELETE FROM object_segments \
                     WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![obj.bucket, obj.key, obj.version_id.to_u64() as i64],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (delete prior segments)",
                    source: e,
                })?;

            let mut stmt = self
                .conn
                .prepare(
                    "INSERT INTO object_segments \
                     (bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, segment_vid, \
                      shard_pg_id, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                )
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (prepare insert segments)",
                    source: e,
                })?;
            for segment in segments {
                if segment.bucket != obj.bucket
                    || segment.key != obj.key
                    || segment.version_id != obj.version_id
                {
                    return Err(MetadataError::Db {
                        context: "put segment object (segment object mismatch)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Null,
                            Box::from("segment row does not match object identity"),
                        ),
                    });
                }
                stmt.execute(params![
                    segment.bucket,
                    segment.key,
                    segment.version_id.to_u64() as i64,
                    segment.segment_index,
                    segment.size as i64,
                    segment.segment_crc64.map(|v| v as i64),
                    segment.segment_okh.as_slice(),
                    segment.segment_vid.get() as i64,
                    segment.shard_pg_id,
                    segment.ec_k,
                    segment.ec_m,
                ])
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (insert segment)",
                    source: e,
                })?;
            }

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put segment object (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn commit_stream_part(
        &self,
        session_id: &SessionId,
        part: &MultipartPartRecord,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "commit stream part (begin txn)",
                source: e,
            })?;

        let result: Result<Vec<MultipartPartSegmentRecord>, MetadataError> = (|| {
            // 1. Verify session exists, is InProgress, is UploadPart kind, and matches
            //    the target bucket/key/upload_id/part_number. Then transition to Completing.
            let sess_row =
                self.conn
                    .query_row(
                        "SELECT session_id, state, op_kind, bucket, key, upload_id, part_number \
                     FROM stream_uploads WHERE session_id = ?1",
                        params![session_id.as_str()],
                        |row| {
                            let op_kind_raw: u8 = row.get(2)?;
                            let upload_id: Option<UploadId> = row.get(5)?;
                            let part_number: Option<i64> = row.get(6)?;
                            Ok(StreamUploadRecord {
                                session_id: row.get(0)?,
                                state: StreamUploadState::from_u8(row.get::<_, u8>(1)?)
                                    .ok_or_else(|| {
                                        rusqlite::Error::FromSqlConversionFailure(
                                            1,
                                            rusqlite::types::Type::Integer,
                                            Box::from("invalid stream state"),
                                        )
                                    })?,
                                target: PgStore::parse_stream_target(
                                    op_kind_raw,
                                    upload_id,
                                    part_number,
                                    1,
                                )?,
                                bucket: row.get(3)?,
                                key: row.get(4)?,
                                created_at: 0,
                                encryption: ObjectEncryption::None,
                            })
                        },
                    )
                    .optional()
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (lookup session)",
                        source: e,
                    })?
                    .ok_or_else(|| MetadataError::StreamSessionNotFound {
                        session_id: session_id.to_string(),
                    })?;

            if sess_row.state != StreamUploadState::InProgress {
                return Err(MetadataError::StreamSessionNotInProgress {
                    state: sess_row.state as u8,
                });
            }
            // Validate session binding matches commit target.
            match &sess_row.target {
                StreamUploadTarget::UploadPart {
                    upload_id,
                    part_number,
                } if upload_id == &part.upload_id && *part_number == part.part_number => {}
                _ => {
                    return Err(MetadataError::StreamSessionNotFound {
                        session_id: session_id.to_string(),
                    })
                }
            }
            let sess_bucket = sess_row.bucket;
            let sess_key = sess_row.key;

            self.conn
                .execute(
                    "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                    params![StreamUploadState::Completing as u8, session_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (set completing)",
                    source: e,
                })?;

            // 2. Upsert multipart part metadata.
            let prev_gen: Option<u32> = self
                .conn
                .query_row(
                    "SELECT generation FROM multipart_parts \
                     WHERE upload_id = ?1 AND part_number = ?2",
                    params![part.upload_id, part.part_number],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (read prev gen)",
                    source: e,
                })?;

            let new_gen = prev_gen.map_or(0, |g| g + 1);

            self.conn
                .execute(
                    "INSERT OR REPLACE INTO multipart_parts \
                     (upload_id, part_number, generation, size, etag, etag_kind, \
                      part_okh, part_vid, ec_k, ec_m, last_modified, checksum) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    params![
                        part.upload_id,
                        part.part_number,
                        new_gen,
                        part.size as i64,
                        part.etag,
                        part.etag_kind as u8,
                        part.part_okh.as_slice(),
                        part.part_vid.get() as i64,
                        part.ec_k,
                        part.ec_m,
                        part.last_modified as i64,
                        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (upsert part)",
                    source: e,
                })?;

            // 3. Capture prior part segments for this upload+part before deleting
            //    their metadata rows so the caller can reclaim their shards.
            let displaced_segments = {
                let mut stmt = self
                    .conn
                    .prepare(
                        "SELECT bucket, key, upload_id, version_id, part_number, segment_index, size, segment_crc64, segment_okh, \
                         segment_vid, shard_pg_id, ec_k, ec_m FROM multipart_part_segments \
                         WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 AND part_number = ?4 \
                         ORDER BY segment_index ASC",
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (prepare displaced segments)",
                        source: e,
                    })?;

                let rows = stmt
                    .query_map(
                        params![sess_bucket, sess_key, part.upload_id, part.part_number],
                        |row| {
                            let okh_blob: Vec<u8> = row.get(8)?;
                            let okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                            Ok(MultipartPartSegmentRecord {
                                bucket: row.get(0)?,
                                key: row.get(1)?,
                                upload_id: row.get(2)?,
                                version_id: row.get::<_, i64>(3)? as u64,
                                part_number: row.get(4)?,
                                segment_index: row.get(5)?,
                                size: row.get::<_, i64>(6)? as u64,
                                segment_crc64: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                                segment_okh: okh,
                                segment_vid: Self::parse_generation_id(
                                    row.get::<_, i64>(9)?,
                                    9,
                                    "segment_vid",
                                )?,
                                shard_pg_id: row.get(10)?,
                                ec_k: row.get(11)?,
                                ec_m: row.get(12)?,
                            })
                        },
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (query displaced segments)",
                        source: e,
                    })?;

                let mut displaced = Vec::new();
                for row in rows {
                    displaced.push(row.map_err(|e| MetadataError::Db {
                        context: "commit stream part (read displaced segment row)",
                        source: e,
                    })?);
                }
                displaced
            };

            // 4. Delete prior part segments for this upload+part (re-upload support).
            //    Scoped by upload_id to avoid clobbering concurrent uploads for the same key.
            self.conn
                .execute(
                    "DELETE FROM multipart_part_segments \
                     WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 \
                     AND part_number = ?4",
                    params![sess_bucket, sess_key, part.upload_id, part.part_number],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (delete prior segments)",
                    source: e,
                })?;

            // 5. Insert committed multipart part segment rows.
            {
                let mut stmt = self
                    .conn
                    .prepare(
                        "INSERT INTO multipart_part_segments \
                         (bucket, key, upload_id, version_id, part_number, segment_index, size, segment_crc64, segment_okh, \
                          segment_vid, shard_pg_id, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (prepare insert segments)",
                        source: e,
                    })?;
                for segment in segments {
                    if segment.bucket != sess_bucket
                        || segment.key != sess_key
                        || segment.upload_id != part.upload_id
                        || segment.version_id != PART_SEGMENT_STAGING_VERSION_ID.to_u64()
                        || segment.part_number != part.part_number
                    {
                        return Err(MetadataError::StreamSessionNotFound {
                            session_id: session_id.to_string(),
                        });
                    }
                    stmt.execute(params![
                        segment.bucket,
                        segment.key,
                        segment.upload_id,
                        segment.version_id as i64,
                        segment.part_number,
                        segment.segment_index,
                        segment.size as i64,
                        segment.segment_crc64.map(|v| v as i64),
                        segment.segment_okh.as_slice(),
                        segment.segment_vid.get() as i64,
                        segment.shard_pg_id,
                        segment.ec_k,
                        segment.ec_m,
                    ])
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (insert segment)",
                        source: e,
                    })?;
                }
            }

            // 5. Delete staging rows.
            self.conn
                .execute(
                    "DELETE FROM stream_uploads WHERE session_id = ?1",
                    params![session_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (delete staging)",
                    source: e,
                })?;

            Ok(displaced_segments)
        })();

        match result {
            Ok(displaced_segments) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "commit stream part (commit txn)",
                        source: e,
                    });
                }
                Ok(displaced_segments)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, segment_vid, \
                 shard_pg_id, ec_k, ec_m FROM object_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get stream object segments",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![bucket, key, version_id.to_u64() as i64], |row| {
                let okh_blob: Vec<u8> = row.get(6)?;
                let okh = PgStore::parse_okh_blob(&okh_blob, 6)?;
                Ok(ObjectSegmentRecord {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    version_id: PgStore::parse_version_id(row.get::<_, i64>(2)?, 2)?,
                    segment_index: row.get(3)?,
                    size: row.get::<_, i64>(4)? as u64,
                    segment_crc64: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                    segment_okh: okh,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(7)?,
                        7,
                        "segment_vid",
                    )?,
                    shard_pg_id: row.get(8)?,
                    ec_k: row.get(9)?,
                    ec_m: row.get(10)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "get stream object segments",
                source: e,
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "get stream object segments row",
                source: e,
            })?);
        }
        Ok(segments)
    }

    fn delete_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete stream object segments",
                source: e,
            })?;
        Ok(())
    }

    fn get_multipart_part_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        part_number: u32,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT bucket, key, upload_id, version_id, part_number, segment_index, size, segment_crc64, segment_okh, \
                 segment_vid, shard_pg_id, ec_k, ec_m FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND part_number = ?4 \
                 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get multipart part segments",
                source: e,
            })?;

        let rows = stmt
            .query_map(
                params![bucket, key, version_id.to_u64() as i64, part_number],
                |row| {
                    let okh_blob: Vec<u8> = row.get(8)?;
                    let okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                    Ok(MultipartPartSegmentRecord {
                        bucket: row.get(0)?,
                        key: row.get(1)?,
                        upload_id: row.get(2)?,
                        version_id: row.get::<_, i64>(3)? as u64,
                        part_number: row.get(4)?,
                        segment_index: row.get(5)?,
                        size: row.get::<_, i64>(6)? as u64,
                        segment_crc64: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                        segment_okh: okh,
                        segment_vid: Self::parse_generation_id(
                            row.get::<_, i64>(9)?,
                            9,
                            "segment_vid",
                        )?,
                        shard_pg_id: row.get(10)?,
                        ec_k: row.get(11)?,
                        ec_m: row.get(12)?,
                    })
                },
            )
            .map_err(|e| MetadataError::Db {
                context: "get multipart part segments",
                source: e,
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "get multipart part segments row",
                source: e,
            })?);
        }
        Ok(segments)
    }

    fn delete_multipart_part_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart part segments",
                source: e,
            })?;
        Ok(())
    }

    fn get_all_multipart_part_segments_for_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT bucket, key, upload_id, version_id, part_number, segment_index, \
                 size, segment_crc64, segment_okh, segment_vid, shard_pg_id, ec_k, ec_m \
                 FROM multipart_part_segments \
                 WHERE upload_id = ?1 \
                 ORDER BY part_number, segment_index",
            )
            .map_err(|e| MetadataError::Db {
                context: "get all multipart part segments for upload (prepare)",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![upload_id.as_str()], |row| {
                let okh_blob: Vec<u8> = row.get(8)?;
                let segment_okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                Ok(MultipartPartSegmentRecord {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    upload_id: row.get(2)?,
                    version_id: row.get::<_, i64>(3)? as u64,
                    part_number: row.get(4)?,
                    segment_index: row.get(5)?,
                    size: row.get::<_, i64>(6)? as u64,
                    segment_crc64: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                    segment_okh,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(9)?,
                        9,
                        "segment_vid",
                    )?,
                    shard_pg_id: row.get(10)?,
                    ec_k: row.get(11)?,
                    ec_m: row.get(12)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "get all multipart part segments for upload (query)",
                source: e,
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "get all multipart part segments for upload (collect)",
                source: e,
            })
    }

    fn delete_multipart_part_segments_by_upload_id(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments WHERE upload_id = ?1",
                params![upload_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart part segments by upload_id",
                source: e,
            })?;
        Ok(())
    }
}

/// Compute the exclusive end of a prefix range for efficient SQL queries.
///
/// For prefix "foo", returns Some("fop") — the next string after all strings
/// starting with "foo". Returns None if the prefix is all 0xFF bytes (no upper bound).
/// fsync a directory to ensure renames are durable.
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    let f = fs::File::open(dir)?;
    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::PgMetadataStore;

    fn test_owner() -> OwnerIdentity {
        OwnerIdentity::from_principal("owner")
    }

    // ── prefix_end ────────────────────────────────────────────────────

    #[test]
    fn key_prefix_upper_bound_basic() {
        assert_eq!(key_prefix_upper_bound("foo"), Some("fop".to_string()));
    }

    #[test]
    fn key_prefix_upper_bound_empty() {
        assert_eq!(key_prefix_upper_bound(""), None);
    }

    #[test]
    fn key_prefix_upper_bound_del_char() {
        assert_eq!(key_prefix_upper_bound("\x7f"), Some("\u{80}".to_string()));
    }

    #[test]
    fn key_prefix_upper_bound_trailing_del() {
        assert_eq!(
            key_prefix_upper_bound("abc\x7f"),
            Some("abc\u{80}".to_string())
        );
    }

    #[test]
    fn key_prefix_upper_bound_tilde() {
        assert_eq!(key_prefix_upper_bound("~"), Some("\x7f".to_string()));
    }

    // ── pg_id accessor ────────────────────────────────────────────────

    #[test]
    fn pg_id_accessor() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 42).unwrap();
        assert_eq!(store.pg_id(), 42);
    }

    // ── list_objects with prefix + start_after ────────────────────────

    #[test]
    fn list_objects_prefix_and_start_after() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        // Insert several objects
        for key in &["photos/a.jpg", "photos/b.jpg", "photos/c.jpg", "docs/x"] {
            store
                .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                    bucket: trusted_bucket_name("bucket"),
                    key: trusted_object_key(*key),
                    version_id: VersionId::Null,
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 10,
                    etag: ObjectEtag::SinglePart([0; 8]),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                }))
                .unwrap();
        }

        // List with prefix=photos/ and start_after=photos/a.jpg
        let resp = store
            .list_objects(&ListObjectsReq {
                bucket: trusted_bucket_name("bucket"),
                prefix: Some(trusted_object_key("photos/")),
                start_after: Some(trusted_object_key("photos/a.jpg")),
                start_at: None,
                max_keys: 10,
            })
            .unwrap();

        assert_eq!(resp.objects.len(), 2);
        assert_eq!(resp.objects[0].key(), "photos/b.jpg");
        assert_eq!(resp.objects[1].key(), "photos/c.jpg");
        assert!(!resp.is_truncated);
    }

    // ── stat_shard on quarantined shard ───────────────────────────────

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

    // ── connection accessor ───────────────────────────────────────────

    #[test]
    fn connection_accessor() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();
        // Just verify we can call it without panicking
        let _conn = store.connection();
    }

    #[test]
    fn row_to_object_record_rejects_negative_parts_count() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();
        let err = store
            .connection()
            .query_row(
                "SELECT \
                    'bucket' AS bucket, \
                    'k' AS key, \
                    0 AS version_id, \
                    1 AS generation_id, \
                    1 AS size, \
                    zeroblob(8) AS etag, \
                    1 AS etag_kind, \
                    0 AS last_modified, \
                    0 AS storage_class, \
                    4 AS ec_k, \
                    2 AS ec_m, \
                    0 AS status, \
                    NULL AS tags, \
                    1 AS data_layout, \
                    -1 AS parts_count, \
                    NULL AS metadata_blob, \
                    NULL AS system_metadata_blob, \
                    0 AS encryption_type, \
                    NULL AS encryption_state, \
                    'owner' AS owner_principal, \
                    ?1 AS owner_canonical_id, \
                    '' AS acl_grants, \
                    0 AS public_read, \
                    NULL AS object_lock_retention_mode, \
                    NULL AS object_lock_retain_until, \
                    0 AS object_lock_legal_hold, \
                    NULL AS became_noncurrent_at",
                params![CanonicalUserId::from_principal("owner").as_str()],
                PgStore::row_to_object_record,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            rusqlite::Error::FromSqlConversionFailure(14, rusqlite::types::Type::Integer, _)
        ));
    }

    #[test]
    fn row_to_object_record_rejects_pending_delete_status() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();
        let err = store
            .connection()
            .query_row(
                "SELECT \
                    'b' AS bucket, \
                    'k' AS key, \
                    0 AS version_id, \
                    1 AS generation_id, \
                    1 AS size, \
                    zeroblob(8) AS etag, \
                    1 AS etag_kind, \
                    0 AS last_modified, \
                    0 AS storage_class, \
                    4 AS ec_k, \
                    2 AS ec_m, \
                    2 AS status, \
                    NULL AS tags, \
                    0 AS data_layout, \
                    NULL AS parts_count, \
                    NULL AS metadata_blob, \
                    NULL AS system_metadata_blob, \
                    0 AS encryption_type, \
                    NULL AS encryption_state, \
                    'owner' AS owner_principal, \
                    ?1 AS owner_canonical_id, \
                    '' AS acl_grants, \
                    0 AS public_read, \
                    NULL AS object_lock_retention_mode, \
                    NULL AS object_lock_retain_until, \
                    0 AS object_lock_legal_hold, \
                    NULL AS became_noncurrent_at",
                params![CanonicalUserId::from_principal("owner").as_str()],
                PgStore::row_to_object_record,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Integer, _)
        ));
    }

    // ── list_objects pagination ──────────────────────────────────────

    #[test]
    fn list_objects_pagination() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        for i in 0..5 {
            store
                .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                    bucket: trusted_bucket_name("bucket"),
                    key: trusted_object_key(format!("key-{:02}", i)),
                    version_id: VersionId::Null,
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 0,
                    etag: ObjectEtag::SinglePart([0; 8]),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                }))
                .unwrap();
        }

        // First page
        let resp = store
            .list_objects(&ListObjectsReq {
                bucket: trusted_bucket_name("bucket"),
                prefix: None,
                start_after: None,
                start_at: None,
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(resp.objects.len(), 2);
        assert!(resp.is_truncated);
        assert_eq!(resp.objects[0].key(), "key-00");
        assert_eq!(resp.objects[1].key(), "key-01");

        // Second page
        let resp2 = store
            .list_objects(&ListObjectsReq {
                bucket: trusted_bucket_name("bucket"),
                prefix: None,
                start_after: resp.next_start_after,
                start_at: None,
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(resp2.objects.len(), 2);
        assert!(resp2.is_truncated);
        assert_eq!(resp2.objects[0].key(), "key-02");
    }

    #[test]
    fn list_objects_start_at_is_inclusive() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        for key in ["alpha", "beta", "gamma"] {
            store
                .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                    bucket: trusted_bucket_name("bucket"),
                    key: trusted_object_key(key),
                    version_id: VersionId::Null,
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 0,
                    etag: ObjectEtag::SinglePart([0; 8]),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                }))
                .unwrap();
        }

        let resp = store
            .list_objects(&ListObjectsReq {
                bucket: trusted_bucket_name("bucket"),
                prefix: None,
                start_after: None,
                start_at: Some(trusted_object_key("beta")),
                max_keys: 10,
            })
            .unwrap();

        assert_eq!(resp.objects.len(), 2);
        assert_eq!(resp.objects[0].key(), "beta");
        assert_eq!(resp.objects[1].key(), "gamma");
    }
}
