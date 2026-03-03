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

/// Per-PG store combining shard file I/O with SQLite metadata.
pub struct PgStore {
    pg_id: u32,
    pg_dir: PathBuf,
    conn: Connection,
}

impl PgStore {
    /// Open (or create) a PG store at the given directory.
    ///
    /// Creates `shards/` and `tmp/` subdirectories if they don't exist.
    /// Initializes the SQLite schema (idempotent).
    pub fn open(pg_dir: &Path, pg_id: u32) -> Result<Self, StoreError> {
        fs::create_dir_all(pg_dir.join("shards")).map_err(|e| StoreError::Io {
            context: "create shards dir",
            source: e,
        })?;
        fs::create_dir_all(pg_dir.join("tmp")).map_err(|e| StoreError::Io {
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
        let tmp_dir = pg_dir.join("tmp");
        if let Ok(entries) = fs::read_dir(&tmp_dir) {
            for entry in entries.flatten() {
                let _ = fs::remove_file(entry.path());
            }
        }

        Ok(Self {
            pg_id,
            pg_dir: pg_dir.to_path_buf(),
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
        let prefix = key.hex_prefix();
        let hex = key.hex();
        self.pg_dir.join("shards").join(prefix).join(hex)
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

    /// Map a row with columns (bucket, key, version_id, size, total_size, etag,
    /// etag_kind, last_modified, storage_class, ec_k, ec_m, status, tags,
    /// data_layout, parts_count, metadata_blob) to an ObjectRecord.
    fn row_to_object_record(row: &rusqlite::Row<'_>) -> Result<ObjectRecord, rusqlite::Error> {
        Ok(ObjectRecord {
            bucket: row.get(0)?,
            key: row.get(1)?,
            version_id: row.get::<_, i64>(2)? as u64,
            size: row.get::<_, i64>(3)? as u64,
            total_size: row.get::<_, i64>(4)? as u64,
            etag: row.get(5)?,
            etag_kind: row.get::<_, u8>(6)?,
            last_modified: row.get::<_, i64>(7)? as u64,
            storage_class: row.get::<_, u8>(8)?,
            ec_k: row.get::<_, u8>(9)?,
            ec_m: row.get::<_, u8>(10)?,
            status: row.get::<_, u8>(11)?,
            tags: row.get(12)?,
            data_layout: {
                let raw = row.get::<_, u8>(13)?;
                DataLayout::from_u8(raw).ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        13,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("invalid data_layout: {raw}")),
                    )
                })?
            },
            parts_count: row.get::<_, Option<i64>>(14)?.map(|v| v as u32),
            metadata_blob: row.get(15)?,
        })
    }
}

impl ShardStore for PgStore {
    fn write_shard(&self, key: &ShardKey, data: &[u8]) -> Result<WriteAck, StoreError> {
        let crc = crc64::checksum(data);
        let stored_size = data.len() as u64;

        // Write to temp file with O_EXCL (unique name via pid + timestamp).
        let tmp_dir = self.pg_dir.join("tmp");
        let tmp_name = format!(
            "shard-{}-{}-{}",
            std::process::id(),
            Self::now_secs(),
            key.hex()
        );
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
        let shard_path = self.shard_path(key);
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

        // Record in SQLite.
        let now = Self::now_secs();
        self.conn
            .execute(
                "INSERT OR REPLACE INTO shards (shard_key, data_size, crc64_nvme, created_at, status) \
                 VALUES (?1, ?2, ?3, ?4, 0)",
                params![key.as_bytes().as_slice(), stored_size as i64, crc as i64, now as i64],
            )
            .map_err(|e| StoreError::Db {
                context: "insert shard record",
                source: e,
            })?;

        Ok(WriteAck {
            crc64: crc,
            stored_size,
        })
    }

    fn read_shard(&self, key: &ShardKey) -> Result<ShardData, StoreError> {
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
        let actual_crc = crc64::checksum(&data);
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
    fn put_object_meta(&self, req: &PutObjectMetaReq) -> Result<(), MetadataError> {
        let now = PgStore::now_millis();
        if req.version_id == 0 {
            // Unversioned: INSERT OR REPLACE (overwrite null version)
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO objects \
                     (bucket, key, version_id, size, total_size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10, ?11)",
                    params![
                        req.bucket,
                        req.key,
                        req.version_id as i64,
                        req.size as i64,
                        req.total_size as i64,
                        req.etag,
                        req.etag_kind,
                        now as i64,
                        req.ec_k,
                        req.ec_m,
                        req.status,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put object meta",
                    source: e,
                })?;
        } else {
            // Versioned: INSERT only (new version)
            self.conn
                .execute(
                    "INSERT INTO objects \
                     (bucket, key, version_id, size, total_size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?10, ?11)",
                    params![
                        req.bucket,
                        req.key,
                        req.version_id as i64,
                        req.size as i64,
                        req.total_size as i64,
                        req.etag,
                        req.etag_kind,
                        now as i64,
                        req.ec_k,
                        req.ec_m,
                        req.status,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put object meta (versioned)",
                    source: e,
                })?;
        }
        Ok(())
    }

    fn get_object_meta(&self, bucket: &str, key: &str) -> Result<ObjectRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT bucket, key, version_id, size, total_size, etag, etag_kind, \
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
        version_id: u64,
    ) -> Result<ObjectRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT bucket, key, version_id, size, total_size, etag, etag_kind, \
                 last_modified, storage_class, ec_k, ec_m, status, tags, \
                 data_layout, parts_count, metadata_blob \
                 FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id as i64],
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
        version_id: u64,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object version",
                source: e,
            })?;
        Ok(())
    }

    fn list_objects(&self, req: &ListObjectsReq) -> Result<ListObjectsResp, MetadataError> {
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
            SELECT o.bucket, o.key, o.version_id, o.size, o.total_size, o.etag, o.etag_kind, \
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

        let mut objects: Vec<ObjectRecord> = Vec::new();
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
            objects.last().map(|o| o.key.clone())
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
                params_vec.push(Box::new(vid_marker as i64));
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
            "SELECT bucket, key, version_id, size, total_size, etag, etag_kind, \
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

        let mut versions: Vec<ObjectRecord> = Vec::new();
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
            versions
                .last()
                .map(|o| (Some(o.key.clone()), Some(o.version_id)))
                .unwrap_or((None, None))
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

    fn next_version_id(&self, bucket: &str, key: &str) -> Result<u64, MetadataError> {
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

        Ok(max.map(|v| v as u64 + 1).unwrap_or(1))
    }

    fn put_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
        tags: &str,
    ) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE objects SET tags = ?1 WHERE bucket = ?2 AND key = ?3 AND version_id = ?4",
                params![tags, bucket, key, version_id as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "put object tags",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::ObjectNotFound);
        }
        Ok(())
    }

    fn get_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<Option<String>, MetadataError> {
        self.conn
            .query_row(
                "SELECT tags FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id as i64],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object tags",
                source: e,
            })?
            .ok_or(MetadataError::ObjectNotFound)
    }

    fn delete_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE objects SET tags = NULL WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object tags",
                source: e,
            })?;
        if updated == 0 {
            return Err(MetadataError::ObjectNotFound);
        }
        Ok(())
    }

    // ── Multipart upload methods (stubs — implemented in Step 3) ──

    fn create_multipart_upload(
        &self,
        _req: &CreateMultipartUploadReq,
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn get_multipart_upload(
        &self,
        _upload_id: &str,
    ) -> Result<MultipartUploadRecord, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn set_upload_state(
        &self,
        _upload_id: &str,
        _new_state: UploadState,
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn delete_multipart_upload(&self, _upload_id: &str) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn list_multipart_uploads(
        &self,
        _req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn upsert_multipart_part(
        &self,
        _part: &MultipartPartRecord,
    ) -> Result<Option<u32>, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn get_multipart_part(
        &self,
        _upload_id: &str,
        _part_number: u32,
    ) -> Result<MultipartPartRecord, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn list_multipart_parts(&self, _req: &ListPartsReq) -> Result<ListPartsResp, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn commit_object_parts(&self, _parts: &[ObjectPartRecord]) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn get_object_parts(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: u64,
    ) -> Result<Vec<ObjectPartRecord>, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn delete_object_parts(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: u64,
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
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
        let tmp = tempfile::tempdir().unwrap();
        let store = PgStore::open(tmp.path(), 42).unwrap();
        assert_eq!(store.pg_id(), 42);
    }

    // ── list_objects with prefix + start_after ────────────────────────

    #[test]
    fn list_objects_prefix_and_start_after() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        // Insert several objects
        for key in &["photos/a.jpg", "photos/b.jpg", "photos/c.jpg", "docs/x"] {
            store
                .put_object_meta(&PutObjectMetaReq {
                    bucket: "bucket".into(),
                    key: key.to_string(),
                    version_id: 0,
                    status: 0,
                    size: 10,
                    total_size: 0,
                    etag: vec![0; 8],
                    etag_kind: 0,
                    ec_k: 4,
                    ec_m: 2,
                })
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
        assert_eq!(resp.objects[0].key, "photos/b.jpg");
        assert_eq!(resp.objects[1].key, "photos/c.jpg");
        assert!(!resp.is_truncated);
    }

    // ── stat_shard on quarantined shard ───────────────────────────────

    #[test]
    fn stat_shard_after_quarantine() {
        let tmp = tempfile::tempdir().unwrap();
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
        let tmp = tempfile::tempdir().unwrap();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        let key = ShardKey::new(&[0xBB; 16], 1, 0);
        store.write_shard(&key, b"data").unwrap();

        let stat = store.stat_shard(&key).unwrap();
        assert_eq!(stat.size, 4);
        assert_eq!(stat.crc64, crc64::checksum(b"data"));
    }

    // ── connection accessor ───────────────────────────────────────────

    #[test]
    fn connection_accessor() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PgStore::open(tmp.path(), 0).unwrap();
        // Just verify we can call it without panicking
        let _conn = store.connection();
    }

    // ── list_objects pagination ──────────────────────────────────────

    #[test]
    fn list_objects_pagination() {
        let tmp = tempfile::tempdir().unwrap();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        for i in 0..5 {
            store
                .put_object_meta(&PutObjectMetaReq {
                    bucket: "b".into(),
                    key: format!("key-{:02}", i),
                    version_id: 0,
                    status: 0,
                    size: 0,
                    total_size: 0,
                    etag: vec![0; 8],
                    etag_kind: 0,
                    ec_k: 4,
                    ec_m: 2,
                })
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
        assert_eq!(resp.objects[0].key, "key-00");
        assert_eq!(resp.objects[1].key, "key-01");

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
        assert_eq!(resp2.objects[0].key, "key-02");
    }
}
