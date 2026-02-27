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
    fn create_bucket(
        &self,
        name: &str,
        owner_principal: &str,
        public_read: bool,
    ) -> Result<(), MetadataError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let result = self.conn.execute(
            "INSERT INTO buckets (name, owner_principal, created_at, public_read) VALUES (?1, ?2, ?3, ?4)",
            params![name, owner_principal, now, if public_read { 1 } else { 0 }],
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
                "SELECT name, owner_principal, created_at, region, versioning, public_read \
                 FROM buckets WHERE name = ?1",
                params![name],
                |row| {
                    Ok(BucketInfo {
                        name: row.get(0)?,
                        owner_principal: row.get(1)?,
                        created_at: row.get::<_, i64>(2)? as u64,
                        region: row.get::<_, i64>(3)? as u16,
                        versioning: row.get::<_, i64>(4)? as u8,
                        public_read: row.get::<_, i64>(5)? != 0,
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

    fn put_bucket_versioning(&self, name: &str, state: u8) -> Result<(), MetadataError> {
        // Get current versioning state
        let current: u8 = self
            .conn
            .query_row(
                "SELECT versioning FROM buckets WHERE name = ?1",
                params![name],
                |row| row.get::<_, i64>(0).map(|v| v as u8),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket versioning",
                source: e,
            })?
            .ok_or(MetadataError::BucketNotFound {
                name: name.to_string(),
            })?;

        // Validate transition: cannot go back to Disabled (0) from Enabled (1) or Suspended (2)
        if state == 0 && current != 0 {
            return Err(MetadataError::InvalidVersioningTransition {
                from: current,
                to: state,
            });
        }

        self.conn
            .execute(
                "UPDATE buckets SET versioning = ?1 WHERE name = ?2",
                params![state as i64, name],
            )
            .map_err(|e| MetadataError::Db {
                context: "put bucket versioning",
                source: e,
            })?;

        Ok(())
    }

    fn list_buckets(&self, owner_principal: &str) -> Result<Vec<BucketInfo>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT name, owner_principal, created_at, region, versioning, public_read \
                 FROM buckets WHERE owner_principal = ?1 ORDER BY name ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![owner_principal], |row| {
                Ok(BucketInfo {
                    name: row.get(0)?,
                    owner_principal: row.get(1)?,
                    created_at: row.get::<_, i64>(2)? as u64,
                    region: row.get::<_, i64>(3)? as u16,
                    versioning: row.get::<_, i64>(4)? as u8,
                    public_read: row.get::<_, i64>(5)? != 0,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::GlobalService;

    #[test]
    fn connection_accessor() {
        let db = SqliteBucketDb::open_in_memory().unwrap();
        let _conn = db.connection();
    }

    #[test]
    fn open_bad_path() {
        let result = SqliteBucketDb::open(std::path::Path::new(
            "/nonexistent/deeply/nested/path/db.sqlite",
        ));
        assert!(result.is_err());
    }

    #[test]
    fn list_buckets_multiple() {
        let db = SqliteBucketDb::open_in_memory().unwrap();
        db.create_bucket("alpha", "owner-1", false).unwrap();
        db.create_bucket("beta", "owner-1", false).unwrap();
        db.create_bucket("gamma", "owner-1", false).unwrap();

        let buckets = db.list_buckets("owner-1").unwrap();
        assert_eq!(buckets.len(), 3);
        // Sorted by name
        assert_eq!(buckets[0].name, "alpha");
        assert_eq!(buckets[1].name, "beta");
        assert_eq!(buckets[2].name, "gamma");
        // All have correct owner
        for b in &buckets {
            assert_eq!(b.owner_principal, "owner-1");
            assert_eq!(b.versioning, 0);
            assert_eq!(b.region, 0);
            assert!(!b.public_read);
        }
    }

    #[test]
    fn head_bucket_fields() {
        let db = SqliteBucketDb::open_in_memory().unwrap();
        db.create_bucket("test", "owner-42", true).unwrap();

        let info = db.head_bucket("test").unwrap();
        assert_eq!(info.name, "test");
        assert_eq!(info.owner_principal, "owner-42");
        assert!(info.created_at > 0);
        assert_eq!(info.region, 0);
        assert_eq!(info.versioning, 0);
        assert!(info.public_read);
    }

    #[test]
    fn head_bucket_not_found() {
        let db = SqliteBucketDb::open_in_memory().unwrap();
        let err = db.head_bucket("nope").unwrap_err();
        assert!(matches!(err, MetadataError::BucketNotFound { .. }));
    }

    #[test]
    fn list_buckets_by_owner() {
        let db = SqliteBucketDb::open_in_memory().unwrap();
        db.create_bucket("owner1-b", "owner-1", false).unwrap();
        db.create_bucket("owner2-b", "owner-2", false).unwrap();

        let b1 = db.list_buckets("owner-1").unwrap();
        assert_eq!(b1.len(), 1);
        assert_eq!(b1[0].name, "owner1-b");

        let b2 = db.list_buckets("owner-2").unwrap();
        assert_eq!(b2.len(), 1);
        assert_eq!(b2[0].name, "owner2-b");
    }

    #[test]
    fn delete_nonexistent_bucket() {
        let db = SqliteBucketDb::open_in_memory().unwrap();
        let err = db.delete_bucket("nope").unwrap_err();
        assert!(matches!(err, MetadataError::BucketNotFound { .. }));
    }

    #[test]
    fn create_bucket_public_read_flag_persists() {
        let db = SqliteBucketDb::open_in_memory().unwrap();
        db.create_bucket("public-bucket", "owner", true).unwrap();

        let info = db.head_bucket("public-bucket").unwrap();
        assert!(info.public_read);
    }
}
