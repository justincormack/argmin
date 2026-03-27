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
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension};

use crate::error::{MetadataError, StoreError};
use crate::schema::init_pg_schema;
use crate::traits::{PgMetadataStore, ShardStore};
use crate::types::*;

const TRACE_TARGET: &str = "storage";

/// Part segment rows use a sentinel version_id during staging (pre-CompleteMultipartUpload).
/// Must differ from any real version_id (0 for unversioned, 1+ for versioned) so that
/// in-progress staging rows are invisible to reads of completed objects.
const PART_SEGMENT_STAGING_VERSION_ID: VersionId = MULTIPART_PART_SEGMENT_STAGING_VERSION_ID;
type StreamSessionRow = (u8, u8, BucketName, ObjectKey, Option<UploadId>, Option<i64>);

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
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Get the current unix timestamp in milliseconds.
    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
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
            "pg_id={} session_id={} segment_index={} shards={}",
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

    fn row_to_bucket_info(row: &rusqlite::Row<'_>) -> Result<BucketInfo, rusqlite::Error> {
        let owner_canonical_id_raw: String = row.get(2)?;
        let owner_canonical_id =
            CanonicalUserId::new(&owner_canonical_id_raw).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::from(format!(
                        "invalid owner_canonical_id: {owner_canonical_id_raw}"
                    )),
                )
            })?;
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
            public_read: row.get::<_, i64>(7)? != 0,
            public_write: row.get::<_, i64>(8)? != 0,
            write_reservations_blocked: row.get::<_, i64>(9)? != 0,
            active_write_reservations: Self::parse_u32(
                row.get::<_, i64>(10)?,
                10,
                "active_write_reservations",
            )?,
            cors_config: row.get(11)?,
            tags: row
                .get::<_, Option<String>>(12)?
                .map(SerializedTagSet::from),
            public_access_block: row.get(13)?,
            ownership_controls: row.get(14)?,
        })
    }

    /// Map a row with columns (bucket, key, version_id, generation_id, size,
    /// etag, etag_kind, last_modified, storage_class, ec_k, ec_m, status,
    /// tags, data_layout, parts_count, metadata_blob) to a StoredObject.
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

    fn row_to_object_record(row: &rusqlite::Row<'_>) -> Result<StoredObject, rusqlite::Error> {
        let status = Self::parse_enum(row.get::<_, u8>(11)?, 11, "status", ObjectState::from_u8)?;
        let bucket: BucketName = row.get(0)?;
        let key: ObjectKey = row.get(1)?;
        let version_id = Self::parse_version_id(row.get::<_, i64>(2)?, 2)?;
        let last_modified = row.get::<_, i64>(7)? as u64;

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
                if size != 0
                    || generation_id.is_some()
                    || !etag.is_empty()
                    || etag_kind != 0
                    || storage_class != 0
                    || ec_k != 0
                    || ec_m != 0
                    || tags.is_some()
                    || metadata_blob.is_some()
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

                Ok(StoredObject::Live(LiveObjectRecord {
                    bucket,
                    key,
                    version_id,
                    generation_id,
                    size: row.get::<_, i64>(4)? as u64,
                    etag,
                    last_modified,
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

impl PgMetadataStore for PgStore {
    fn create_bucket(
        &self,
        name: &str,
        owner_principal: &str,
        owner_canonical_id: &CanonicalUserId,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_bucket",
            "pg_id={} bucket={} owner={}",
            self.pg_id,
            name,
            owner_principal
        );
        let now = PgStore::now_millis() as i64;
        let result = self.conn.execute(
            "INSERT INTO buckets (name, owner_principal, owner_canonical_id, created_at, state, public_read, public_write, write_reservations_blocked, active_write_reservations) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 0)",
            params![
                name,
                owner_principal,
                owner_canonical_id.as_str(),
                now,
                BucketState::Active as u8,
                i32::from(public_read),
                i32::from(public_write)
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

    fn delete_bucket(&self, name: &str) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_bucket",
            "pg_id={} bucket={}",
            self.pg_id,
            name
        );
        let deleted = self
            .conn
            .execute("DELETE FROM buckets WHERE name = ?1", params![name])
            .map_err(|e| MetadataError::Db {
                context: "delete bucket",
                source: e,
            })?;
        if deleted == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn head_bucket(&self, name: &str) -> Result<BucketInfo, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::head_bucket",
            "pg_id={} bucket={}",
            self.pg_id,
            name
        );
        let info = self.head_bucket_raw(name)?;
        if info.state != BucketState::Active {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(info)
    }

    fn head_bucket_raw(&self, name: &str) -> Result<BucketInfo, MetadataError> {
        self.conn
            .query_row(
                "SELECT name, owner_principal, owner_canonical_id, created_at, region, state, versioning, public_read, public_write, write_reservations_blocked, active_write_reservations, cors_config, tags, public_access_block, ownership_controls \
                 FROM buckets WHERE name = ?1",
                params![name],
                Self::row_to_bucket_info,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "head bucket raw",
                source: e,
            })?
            .ok_or(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            })
    }

    fn list_buckets(&self, owner_principal: &str) -> Result<Vec<BucketInfo>, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_buckets",
            "pg_id={} owner={}",
            self.pg_id,
            owner_principal
        );
        let mut stmt = self
            .conn
            .prepare(
                "SELECT name, owner_principal, owner_canonical_id, created_at, region, state, versioning, public_read, public_write, write_reservations_blocked, active_write_reservations, cors_config, tags, public_access_block, ownership_controls \
                 FROM buckets WHERE owner_principal = ?1 AND state = ?2 ORDER BY name ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets",
                source: e,
            })?;
        let rows = stmt
            .query_map(
                params![owner_principal, BucketState::Active as u8],
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

    fn mark_bucket_deleting(&self, name: &str) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET state = ?1 \
                 WHERE name = ?2 \
                   AND state = ?3 \
                   AND write_reservations_blocked = 1 \
                   AND active_write_reservations = 0",
                params![BucketState::Deleting as u8, name, BucketState::Active as u8],
            )
            .map_err(|e| MetadataError::Db {
                context: "mark bucket deleting",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn acquire_bucket_write_reservation(&self, name: &str) -> Result<BucketInfo, MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET active_write_reservations = active_write_reservations + 1 \
                 WHERE name = ?1 AND state = ?2 AND write_reservations_blocked = 0",
                params![name, BucketState::Active as u8],
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
        Err(MetadataError::BucketNotFound {
            name: BucketName::from(name),
        })
    }

    fn release_bucket_write_reservation(&self, name: &str) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET active_write_reservations = active_write_reservations - 1 \
                 WHERE name = ?1 AND active_write_reservations > 0",
                params![name],
            )
            .map_err(|e| MetadataError::Db {
                context: "release bucket write reservation",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn begin_bucket_write_drain(&self, name: &str) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET write_reservations_blocked = 1 \
                 WHERE name = ?1 AND state = ?2 AND write_reservations_blocked = 0",
                params![name, BucketState::Active as u8],
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
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn end_bucket_write_drain(&self, name: &str) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET write_reservations_blocked = 0 \
                 WHERE name = ?1 AND state = ?2 AND write_reservations_blocked = 1",
                params![name, BucketState::Active as u8],
            )
            .map_err(|e| MetadataError::Db {
                context: "end bucket write drain",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn put_bucket_versioning(
        &self,
        name: &str,
        state: BucketVersioningState,
    ) -> Result<(), MetadataError> {
        let current: BucketVersioningState = self
            .conn
            .query_row(
                "SELECT versioning FROM buckets WHERE name = ?1",
                params![name],
                |row| row.get::<_, u8>(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket versioning",
                source: e,
            })?
            .ok_or(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            })
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
                params![state as u8 as i64, name],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket versioning",
                source: e,
            })?;
        Ok(())
    }

    fn put_bucket_cors(&self, name: &str, config: &str) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET cors_config = ?1 WHERE name = ?2",
                params![config, name],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket cors",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn get_bucket_cors(&self, name: &str) -> Result<Option<String>, MetadataError> {
        self.conn
            .query_row(
                "SELECT cors_config FROM buckets WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket cors",
                source: e,
            })?
            .ok_or(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            })
    }

    fn delete_bucket_cors(&self, name: &str) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET cors_config = NULL WHERE name = ?1",
                params![name],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete bucket cors",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn put_bucket_tags(&self, name: &str, tags: &str) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET tags = ?1 WHERE name = ?2",
                params![tags, name],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket tags",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn get_bucket_tags(&self, name: &str) -> Result<Option<String>, MetadataError> {
        self.conn
            .query_row(
                "SELECT tags FROM buckets WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket tags",
                source: e,
            })?
            .ok_or(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            })
    }

    fn delete_bucket_tags(&self, name: &str) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET tags = NULL WHERE name = ?1",
                params![name],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete bucket tags",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn put_bucket_public_access_block(
        &self,
        name: &str,
        config: &str,
    ) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET public_access_block = ?1 WHERE name = ?2",
                params![config, name],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket public access block",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn get_bucket_public_access_block(&self, name: &str) -> Result<Option<String>, MetadataError> {
        self.conn
            .query_row(
                "SELECT public_access_block FROM buckets WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket public access block",
                source: e,
            })?
            .ok_or(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            })
    }

    fn delete_bucket_public_access_block(&self, name: &str) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET public_access_block = NULL WHERE name = ?1",
                params![name],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete bucket public access block",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn put_bucket_acl(
        &self,
        name: &str,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET public_read = ?1, public_write = ?2 WHERE name = ?3",
                params![i32::from(public_read), i32::from(public_write), name],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket acl",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn put_bucket_ownership_controls(&self, name: &str, config: &str) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET ownership_controls = ?1 WHERE name = ?2",
                params![config, name],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket ownership controls",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn get_bucket_ownership_controls(&self, name: &str) -> Result<Option<String>, MetadataError> {
        self.conn
            .query_row(
                "SELECT ownership_controls FROM buckets WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket ownership controls",
                source: e,
            })?
            .ok_or(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            })
    }

    fn delete_bucket_ownership_controls(&self, name: &str) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets SET ownership_controls = NULL WHERE name = ?1",
                params![name],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete bucket ownership controls",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::BucketNotFound {
                name: BucketName::from(name),
            });
        }
        Ok(())
    }

    fn put_object_meta(&self, req: &PutObjectReq) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::put_object_meta",
            "pg_id={}",
            self.pg_id
        );
        let now = PgStore::now_millis();
        match req {
            PutObjectReq::Live(req) => {
                req.validate().map_err(|msg| MetadataError::Db {
                    context: "put object meta (etag/layout mismatch)",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from(msg),
                    ),
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
                let sql = if req.version_id.is_null() {
                    "INSERT OR REPLACE INTO objects \
                     (bucket, key, version_id, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
                } else {
                    "INSERT INTO objects \
                     (bucket, key, version_id, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
                };
                self.conn
                    .execute(
                        sql,
                        params![
                            req.bucket,
                            req.key,
                            req.version_id.to_u64() as i64,
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
                        ],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "put object meta",
                        source: e,
                    })?;
            }
            PutObjectReq::DeleteMarker(req) => {
                let sql = if req.version_id.is_null() {
                    "INSERT OR REPLACE INTO objects \
                     (bucket, key, version_id, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, metadata_blob) \
                     VALUES (?1, ?2, ?3, NULL, 0, zeroblob(0), 0, ?4, 0, 0, 0, 1, 0, NULL, NULL)"
                } else {
                    "INSERT INTO objects \
                     (bucket, key, version_id, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, metadata_blob) \
                     VALUES (?1, ?2, ?3, NULL, 0, zeroblob(0), 0, ?4, 0, 0, 0, 1, 0, NULL, NULL)"
                };
                self.conn
                    .execute(
                        sql,
                        params![
                            req.bucket,
                            req.key,
                            req.version_id.to_u64() as i64,
                            now as i64,
                        ],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "put object meta (delete marker)",
                        source: e,
                    })?;
            }
        }
        Ok(())
    }

    fn get_object_meta(&self, bucket: &str, key: &str) -> Result<StoredObject, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::get_object_meta",
            "pg_id={} bucket={} key={}",
            self.pg_id,
            bucket,
            key
        );
        self.conn
            .query_row(
                "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
                 last_modified, storage_class, ec_k, ec_m, status, tags, \
                 data_layout, parts_count, metadata_blob \
                 FROM objects WHERE bucket = ?1 AND key = ?2 \
                 ORDER BY last_modified DESC, version_id DESC LIMIT 1",
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
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<StoredObject, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::get_object_version",
            "pg_id={} bucket={} key={} version_id={}",
            self.pg_id,
            bucket,
            key,
            version_id
        );
        self.conn
            .query_row(
                "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
                 last_modified, storage_class, ec_k, ec_m, status, tags, \
                 data_layout, parts_count, metadata_blob \
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

    fn delete_object_meta(&self, bucket: &str, key: &str) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_object_meta",
            "pg_id={} bucket={} key={}",
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
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_object_version",
            "pg_id={} bucket={} key={} version_id={}",
            self.pg_id,
            bucket,
            key,
            version_id
        );
        self.conn
            .execute(
                "DELETE FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object version",
                source: e,
            })?;
        Ok(())
    }

    fn list_objects(&self, req: &ListObjectsReq) -> Result<ListObjectsResp, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_objects",
            "pg_id={} bucket={} max_keys={}",
            self.pg_id,
            req.bucket.as_str(),
            req.max_keys
        );
        // Use a CTE to find the latest version per key, then filter to live objects.
        // This correctly handles versioned buckets where delete markers hide keys.
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
        }

        if let Some(ref prefix) = req.prefix {
            where_clauses.push(format!("o.key >= ?{param_idx}"));
            params_vec.push(Box::new(prefix.clone()));
            param_idx += 1;

            if let Some(end) = prefix_end(prefix) {
                where_clauses.push(format!("o.key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        let where_str = where_clauses.join(" AND ");

        let sql = format!(
            "WITH latest AS ( \
                SELECT bucket, key, MAX(version_id) AS max_vid \
                FROM objects \
                WHERE bucket = ?1 \
                GROUP BY bucket, key \
            ) \
            SELECT o.bucket, o.key, o.version_id, o.generation_id, o.size, o.etag, o.etag_kind, \
                   o.last_modified, o.storage_class, o.ec_k, o.ec_m, o.status, o.tags, \
                   o.data_layout, o.parts_count, o.metadata_blob \
            FROM objects o \
            INNER JOIN latest l ON o.bucket = l.bucket AND o.key = l.key AND o.version_id = l.max_vid \
            WHERE {where_str} AND o.status = 0 \
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
            "pg_id={} bucket={} max_keys={}",
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
                // Resume after (key_marker, vid_marker)
                where_clauses.push(format!(
                    "(key > ?{} OR (key = ?{} AND version_id < ?{}))",
                    param_idx,
                    param_idx,
                    param_idx + 1
                ));
                params_vec.push(Box::new(key_marker.clone()));
                params_vec.push(Box::new(vid_marker.to_u64() as i64));
                param_idx += 2;
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

            if let Some(end) = prefix_end(prefix) {
                where_clauses.push(format!("key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        let where_str = where_clauses.join(" AND ");

        let sql = format!(
            "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
             last_modified, storage_class, ec_k, ec_m, status, tags, \
             data_layout, parts_count, metadata_blob \
             FROM objects \
             WHERE {where_str} \
             ORDER BY key ASC, version_id DESC LIMIT ?{param_idx}"
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

    fn next_version_id(&self, bucket: &str, key: &str) -> Result<VersionId, MetadataError> {
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

    fn next_generation_id(&self, bucket: &str, key: &str) -> Result<GenerationId, MetadataError> {
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
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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

    fn get_bucket_payload_reclaim_root(
        &self,
        bucket: &str,
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
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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
            "pg_id={} upload_id={} bucket={} key={}",
            self.pg_id,
            req.upload_id.as_str(),
            req.bucket.as_str(),
            req.key.as_str()
        );
        let now = PgStore::now_millis();
        let algo = req.checksum.map(|c| c.algorithm() as u8);
        let ctype = req.checksum.map(|c| c.checksum_type() as u8);
        let tags = req.tags.as_ref().map(SerializedTagSet::as_str);
        self.conn
            .execute(
                "INSERT INTO multipart_uploads \
                 (upload_id, bucket, key, initiated_at, state, tags, metadata_blob, owner_principal, \
                  checksum_algorithm, checksum_type) \
                 VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, ?8, ?9)",
                params![
                    req.upload_id,
                    req.bucket,
                    req.key,
                    now as i64,
                    tags,
                    req.metadata_blob.as_slice(),
                    req.owner_principal,
                    algo,
                    ctype,
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
        upload_id: &str,
    ) -> Result<MultipartUploadRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, bucket, key, initiated_at, state, tags, metadata_blob, \
                 owner_principal, checksum_algorithm, checksum_type \
                 FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id],
                |row| {
                    let state_raw = row.get::<_, u8>(4)?;
                    let algo_raw: Option<u8> = row.get(8)?;
                    let ctype_raw: Option<u8> = row.get(9)?;
                    let checksum = if let Some(algo_val) = algo_raw {
                        let algo = ChecksumAlgorithm::from_u8(algo_val).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                8,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid checksum algorithm: {algo_val}")),
                            )
                        })?;
                        let ctype = ctype_raw
                            .map(|v| {
                                ChecksumType::from_u8(v).ok_or_else(|| {
                                    rusqlite::Error::FromSqlConversionFailure(
                                        9,
                                        rusqlite::types::Type::Integer,
                                        Box::from(format!("invalid checksum type: {v}")),
                                    )
                                })
                            })
                            .transpose()?;
                        Some(MultipartChecksumConfig::new(algo, ctype).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                8,
                                rusqlite::types::Type::Integer,
                                Box::from(e.reason),
                            )
                        })?)
                    } else {
                        None
                    };
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
                        owner_principal: row.get(7)?,
                        checksum,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart upload",
                source: e,
            })?
            .ok_or_else(|| MetadataError::NoSuchUpload {
                upload_id: UploadId::from(upload_id),
            })
    }

    fn set_upload_state(
        &self,
        upload_id: &str,
        new_state: UploadState,
    ) -> Result<(), MetadataError> {
        // Only Completing and Aborting are valid transition targets.
        // Check existence first so we return NoSuchUpload accurately.
        if new_state == UploadState::InProgress {
            let current = self
                .conn
                .query_row(
                    "SELECT state FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id],
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
                    upload_id: UploadId::from(upload_id),
                }),
            };
        }
        let updated = self
            .conn
            .execute(
                "UPDATE multipart_uploads SET state = ?1 \
                 WHERE upload_id = ?2 AND state = 0",
                params![new_state as u8, upload_id],
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
                    params![upload_id],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "set upload state (check)",
                    source: e,
                })?;
            return match current {
                None => Err(MetadataError::NoSuchUpload {
                    upload_id: UploadId::from(upload_id),
                }),
                Some(s) => Err(MetadataError::UploadNotInProgress { state: s }),
            };
        }
        Ok(())
    }

    fn delete_multipart_upload(&self, upload_id: &str) -> Result<(), MetadataError> {
        let deleted = self
            .conn
            .execute(
                "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart upload",
                source: e,
            })?;
        if deleted == 0 {
            return Err(MetadataError::NoSuchUpload {
                upload_id: UploadId::from(upload_id),
            });
        }
        Ok(())
    }

    fn list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_multipart_uploads",
            "pg_id={} bucket={} max_uploads={}",
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

            if let Some(end) = prefix_end(prefix) {
                where_clauses.push(format!("key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        if let Some(ref key_marker) = req.key_marker {
            if let Some(ref uid_marker) = req.upload_id_marker {
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
                params_vec.push(Box::new(uid_marker.clone()));
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
             owner_principal, checksum_algorithm, checksum_type \
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
                let algo_raw: Option<u8> = row.get(8)?;
                let ctype_raw: Option<u8> = row.get(9)?;
                let checksum = if let Some(algo_val) = algo_raw {
                    let algo = ChecksumAlgorithm::from_u8(algo_val).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            8,
                            rusqlite::types::Type::Integer,
                            Box::from(format!("invalid checksum algorithm: {algo_val}")),
                        )
                    })?;
                    let ctype = ctype_raw
                        .map(|v| {
                            ChecksumType::from_u8(v).ok_or_else(|| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    9,
                                    rusqlite::types::Type::Integer,
                                    Box::from(format!("invalid checksum type: {v}")),
                                )
                            })
                        })
                        .transpose()?;
                    Some(MultipartChecksumConfig::new(algo, ctype).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            8,
                            rusqlite::types::Type::Integer,
                            Box::from(e.reason),
                        )
                    })?)
                } else {
                    None
                };
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
                    owner_principal: row.get(7)?,
                    checksum,
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
                            upload_id: part.upload_id.clone(),
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
                            upload_id: part.upload_id.clone(),
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
        upload_id: &str,
        part_number: u32,
    ) -> Result<MultipartPartRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, part_number, generation, size, etag, etag_kind, \
                 part_okh, part_vid, ec_k, ec_m, last_modified, checksum \
                 FROM multipart_parts WHERE upload_id = ?1 AND part_number = ?2",
                params![upload_id, part_number],
                Self::row_to_multipart_part,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart part",
                source: e,
            })?
            .ok_or(MetadataError::PartNotFound {
                upload_id: UploadId::from(upload_id),
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
                upload_id: req.upload_id.clone(),
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
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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
        upload_id: &str,
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
                params![UploadState::Completing as u8, upload_id],
            )?;
            if updated == 0 {
                // Check if it's already Completing (idempotent retry).
                let current: Option<u8> = self
                    .conn
                    .query_row(
                        "SELECT state FROM multipart_uploads WHERE upload_id = ?1",
                        params![upload_id],
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

            // 2. Write/overwrite object metadata row.
            let obj_sql = if obj.version_id.is_null() {
                "INSERT OR REPLACE INTO objects \
                 (bucket, key, version_id, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, tags, data_layout, parts_count, metadata_blob) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
            } else {
                "INSERT INTO objects \
                 (bucket, key, version_id, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, tags, data_layout, parts_count, metadata_blob) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
            };
            self.conn.execute(
                obj_sql,
                params![
                    obj.bucket,
                    obj.key,
                    obj.version_id.to_u64() as i64,
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

            // 7. Delete in-progress upload + parts (CASCADE).
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
            "pg_id={} session_id={} bucket={} key={}",
            self.pg_id,
            req.session_id.as_str(),
            req.bucket.as_str(),
            req.key.as_str()
        );
        let now = PgStore::now_millis();
        let op_kind = req.target.op_kind() as u8;
        let upload_id = req.target.upload_id();
        let part_number = req.target.part_number().map(|n| n as i64);
        self.conn
            .execute(
                "INSERT INTO stream_uploads \
                 (session_id, bucket, key, op_kind, upload_id, part_number, state, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7)",
                params![
                    req.session_id,
                    req.bucket,
                    req.key,
                    op_kind,
                    upload_id,
                    part_number,
                    now as i64,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "create stream upload",
                source: e,
            })?;
        Ok(())
    }

    fn get_stream_upload(&self, session_id: &str) -> Result<StreamUploadRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT session_id, bucket, key, op_kind, upload_id, part_number, state, \
                 created_at FROM stream_uploads WHERE session_id = ?1",
                params![session_id],
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
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get stream upload",
                source: e,
            })?
            .ok_or_else(|| MetadataError::StreamSessionNotFound {
                session_id: SessionId::from(session_id),
            })
    }

    fn set_stream_upload_state(
        &self,
        session_id: &str,
        new_state: StreamUploadState,
    ) -> Result<(), MetadataError> {
        let current: u8 = self
            .conn
            .query_row(
                "SELECT state FROM stream_uploads WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get stream upload state",
                source: e,
            })?
            .ok_or_else(|| MetadataError::StreamSessionNotFound {
                session_id: SessionId::from(session_id),
            })?;

        if current != StreamUploadState::InProgress as u8 {
            return Err(MetadataError::StreamSessionNotInProgress { state: current });
        }

        self.conn
            .execute(
                "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                params![new_state as u8, session_id],
            )
            .map_err(|e| MetadataError::Db {
                context: "set stream upload state",
                source: e,
            })?;
        Ok(())
    }

    fn delete_stream_upload(&self, session_id: &str) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM stream_uploads WHERE session_id = ?1",
                params![session_id],
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
                 created_at FROM stream_uploads",
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
        session_id: &str,
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
            .query_map(params![session_id], |row| {
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
        session_id: &str,
        obj: &CommitStreamPutReq,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::commit_stream_put",
            "pg_id={} session_id={} bucket={} key={} segments={}",
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
                    params![session_id],
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
                    session_id: SessionId::from(session_id),
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
                    session_id: SessionId::from(session_id),
                });
            }

            self.conn
                .execute(
                    "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                    params![StreamUploadState::Completing as u8, session_id],
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

            let obj_sql = if obj.version_id.is_null() {
                "INSERT OR REPLACE INTO objects \
                 (bucket, key, version_id, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
            } else {
                "INSERT INTO objects \
                 (bucket, key, version_id, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
            };
            self.conn
                .execute(
                    obj_sql,
                    params![
                        obj.bucket,
                        obj.key,
                        obj.version_id.to_u64() as i64,
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
                            session_id: SessionId::from(session_id),
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
                    params![session_id],
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

            let obj_sql = if obj.version_id.is_null() {
                "INSERT OR REPLACE INTO objects \
                 (bucket, key, version_id, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
            } else {
                "INSERT INTO objects \
                 (bucket, key, version_id, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
            };
            self.conn
                .execute(
                    obj_sql,
                    params![
                        obj.bucket,
                        obj.key,
                        obj.version_id.to_u64() as i64,
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
        session_id: &str,
        part: &MultipartPartRecord,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "commit stream part (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            // 1. Verify session exists, is InProgress, is UploadPart kind, and matches
            //    the target bucket/key/upload_id/part_number. Then transition to Completing.
            let sess_row =
                self.conn
                    .query_row(
                        "SELECT state, op_kind, bucket, key, upload_id, part_number \
                     FROM stream_uploads WHERE session_id = ?1",
                        params![session_id],
                        |row| {
                            let op_kind_raw: u8 = row.get(1)?;
                            let upload_id: Option<UploadId> = row.get(4)?;
                            let part_number: Option<i64> = row.get(5)?;
                            Ok(StreamUploadRecord {
                                session_id: SessionId::from(session_id),
                                state: StreamUploadState::from_u8(row.get::<_, u8>(0)?)
                                    .ok_or_else(|| {
                                        rusqlite::Error::FromSqlConversionFailure(
                                            0,
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
                                bucket: row.get(2)?,
                                key: row.get(3)?,
                                created_at: 0,
                            })
                        },
                    )
                    .optional()
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (lookup session)",
                        source: e,
                    })?
                    .ok_or_else(|| MetadataError::StreamSessionNotFound {
                        session_id: SessionId::from(session_id),
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
                        session_id: SessionId::from(session_id),
                    })
                }
            }
            let sess_bucket = sess_row.bucket;
            let sess_key = sess_row.key;

            self.conn
                .execute(
                    "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                    params![StreamUploadState::Completing as u8, session_id],
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

            // 3. Delete prior part segments for this upload+part (re-upload support).
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

            // 4. Insert committed multipart part segment rows.
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
                            session_id: SessionId::from(session_id),
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
                    params![session_id],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (delete staging)",
                    source: e,
                })?;

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "commit stream part (commit txn)",
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

    fn get_object_segments(
        &self,
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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
        bucket: &str,
        key: &str,
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
        upload_id: &str,
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
            .query_map(params![upload_id], |row| {
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
        upload_id: &str,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments WHERE upload_id = ?1",
                params![upload_id],
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
fn prefix_end(prefix: &str) -> Option<String> {
    let bytes = prefix.as_bytes();
    let mut end = bytes.to_vec();
    // Increment the last byte; if it overflows, pop and try the next.
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return String::from_utf8(end).ok();
        }
    }
    None
}

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

    // ── prefix_end ────────────────────────────────────────────────────

    #[test]
    fn prefix_end_basic() {
        assert_eq!(prefix_end("foo"), Some("fop".to_string()));
    }

    #[test]
    fn prefix_end_empty() {
        assert_eq!(prefix_end(""), None);
    }

    #[test]
    fn prefix_end_del_char() {
        // 0x7F (DEL) is valid in a Rust &str; incrementing gives 0x80 which
        // is not valid UTF-8, so from_utf8 fails and prefix_end returns None.
        let s = "\x7f";
        assert_eq!(prefix_end(s), None);
    }

    #[test]
    fn prefix_end_trailing_del() {
        // "abc" + DEL(0x7F) → increment 0x7F to 0x80 → not valid UTF-8 → None
        // So prefix_end returns None for this input (can't produce valid UTF-8 upper bound)
        let s = "abc\x7f";
        assert_eq!(prefix_end(s), None);
    }

    #[test]
    fn prefix_end_tilde() {
        // '~' is 0x7E, incrementing gives 0x7F which is valid UTF-8
        assert_eq!(prefix_end("~"), Some("\x7f".to_string()));
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
                    bucket: "bucket".into(),
                    key: ObjectKey::from(*key),
                    version_id: VersionId::Null,
                    generation_id: GenerationId::MIN,
                    size: 10,
                    etag: ObjectEtag::SinglePart([0; 8]),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                }))
                .unwrap();
        }

        // List with prefix=photos/ and start_after=photos/a.jpg
        let resp = store
            .list_objects(&ListObjectsReq {
                bucket: "bucket".into(),
                prefix: Some("photos/".into()),
                start_after: Some("photos/a.jpg".into()),
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
                    0 AS status, \
                    NULL AS tags, \
                    1 AS data_layout, \
                    -1 AS parts_count, \
                    NULL AS metadata_blob",
                [],
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
                    NULL AS metadata_blob",
                [],
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
                    bucket: "b".into(),
                    key: ObjectKey::from(format!("key-{:02}", i)),
                    version_id: VersionId::Null,
                    generation_id: GenerationId::MIN,
                    size: 0,
                    etag: ObjectEtag::SinglePart([0; 8]),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                }))
                .unwrap();
        }

        // First page
        let resp = store
            .list_objects(&ListObjectsReq {
                bucket: "b".into(),
                prefix: None,
                start_after: None,
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
                bucket: "b".into(),
                prefix: None,
                start_after: resp.next_start_after,
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(resp2.objects.len(), 2);
        assert!(resp2.is_truncated);
        assert_eq!(resp2.objects[0].key(), "key-02");
    }
}
