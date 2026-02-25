/// SqliteBucketDb — global bucket metadata backed by SQLite.
///
/// For v1-minimal this is a local SQLite file. In the distributed version
/// it becomes Raft-replicated.
use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};

use crate::error::MetadataError;
use crate::schema::init_bucket_schema;
use crate::traits::GlobalService;
use crate::types::BucketInfo;

/// Global bucket metadata store backed by a single SQLite database.
pub struct SqliteBucketDb {
    conn: Connection,
}

impl SqliteBucketDb {
    /// Open (or create) the bucket database at the given path.
    pub fn open(db_path: &Path) -> Result<Self, MetadataError> {
        let conn = Connection::open(db_path).map_err(|e| MetadataError::Db {
            context: "open bucket database",
            source: e,
        })?;

        init_bucket_schema(&conn).map_err(|e| MetadataError::Db {
            context: "init bucket schema",
            source: e,
        })?;

        Ok(Self { conn })
    }

    /// Open an in-memory bucket database (for testing).
    pub fn open_in_memory() -> Result<Self, MetadataError> {
        let conn = Connection::open_in_memory().map_err(|e| MetadataError::Db {
            context: "open in-memory bucket database",
            source: e,
        })?;

        init_bucket_schema(&conn).map_err(|e| MetadataError::Db {
            context: "init bucket schema",
            source: e,
        })?;

        Ok(Self { conn })
    }

    /// Return a reference to the underlying connection.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

impl GlobalService for SqliteBucketDb {
    fn create_bucket(&self, name: &str, owner_id: u64) -> Result<(), MetadataError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let result = self.conn.execute(
            "INSERT INTO buckets (name, owner_id, created_at) VALUES (?1, ?2, ?3)",
            params![name, owner_id as i64, now],
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
        // Check the bucket exists first.
        let exists: bool = self
            .conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM buckets WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .map_err(|e| MetadataError::Db {
                context: "check bucket exists",
                source: e,
            })?;

        if !exists {
            return Err(MetadataError::BucketNotFound {
                name: name.to_string(),
            });
        }

        // Note: emptiness check is done by the coordinator, which queries
        // all PG metadata DBs. The bucket_db itself doesn't have access to
        // per-PG object tables. For v1-minimal, the caller is responsible
        // for this check before calling delete_bucket.

        self.conn
            .execute("DELETE FROM buckets WHERE name = ?1", params![name])
            .map_err(|e| MetadataError::Db {
                context: "delete bucket",
                source: e,
            })?;

        Ok(())
    }

    fn head_bucket(&self, name: &str) -> Result<BucketInfo, MetadataError> {
        self.conn
            .query_row(
                "SELECT name, owner_id, created_at, region, versioning \
                 FROM buckets WHERE name = ?1",
                params![name],
                |row| {
                    Ok(BucketInfo {
                        name: row.get(0)?,
                        owner_id: row.get::<_, i64>(1)? as u64,
                        created_at: row.get::<_, i64>(2)? as u64,
                        region: row.get::<_, i64>(3)? as u16,
                        versioning: row.get::<_, i64>(4)? as u8,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "head bucket",
                source: e,
            })?
            .ok_or(MetadataError::BucketNotFound {
                name: name.to_string(),
            })
    }

    fn list_buckets(&self, owner_id: u64) -> Result<Vec<BucketInfo>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT name, owner_id, created_at, region, versioning \
                 FROM buckets WHERE owner_id = ?1 ORDER BY name ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![owner_id as i64], |row| {
                Ok(BucketInfo {
                    name: row.get(0)?,
                    owner_id: row.get::<_, i64>(1)? as u64,
                    created_at: row.get::<_, i64>(2)? as u64,
                    region: row.get::<_, i64>(3)? as u16,
                    versioning: row.get::<_, i64>(4)? as u8,
                })
            })
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
}
