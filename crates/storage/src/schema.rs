/// SQL schema definitions and initialization.
use rusqlite::Connection;

/// Per-PG shard tracking table.
const CREATE_SHARDS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS shards (
    shard_key       BLOB PRIMARY KEY,
    data_size       INTEGER NOT NULL,
    crc64_nvme      INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    last_verified   INTEGER,
    status          INTEGER NOT NULL DEFAULT 0
)";

/// Per-PG object metadata table.
const CREATE_OBJECTS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS objects (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    version_id    INTEGER NOT NULL,
    size          INTEGER NOT NULL,
    total_size    INTEGER NOT NULL DEFAULT 0,
    etag          BLOB NOT NULL,
    etag_kind     INTEGER NOT NULL,
    last_modified INTEGER NOT NULL,
    storage_class INTEGER NOT NULL DEFAULT 0,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    status        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket, key, version_id)
)";

/// Index for list operations: bucket + key ordering.
const CREATE_OBJECTS_LIST_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_objects_list ON objects (bucket, key)";

/// Index for version queries: bucket + key + version_id descending for fast latest-version lookup.
const CREATE_OBJECTS_VERSIONS_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_objects_versions ON objects (bucket, key, version_id DESC)";

/// Global bucket metadata table.
const CREATE_BUCKETS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS buckets (
    name             TEXT PRIMARY KEY,
    owner_principal  TEXT NOT NULL,
    created_at       INTEGER NOT NULL,
    region           INTEGER NOT NULL DEFAULT 0,
    versioning       INTEGER NOT NULL DEFAULT 0,
    public_read      INTEGER NOT NULL DEFAULT 0,
    cors_config      TEXT
)";

/// SQLite pragmas for per-PG databases: WAL mode, NORMAL synchronous.
const PG_PRAGMAS: &str = "\
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
";

/// SQLite pragmas for the bucket database.
const BUCKET_PRAGMAS: &str = "\
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
";

/// Initialize the per-PG database schema (shards + objects tables).
///
/// Idempotent — uses `CREATE TABLE IF NOT EXISTS`.
pub fn init_pg_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(PG_PRAGMAS)?;
    conn.execute(CREATE_SHARDS_TABLE, [])?;
    conn.execute(CREATE_OBJECTS_TABLE, [])?;
    conn.execute(CREATE_OBJECTS_LIST_INDEX, [])?;
    conn.execute(CREATE_OBJECTS_VERSIONS_INDEX, [])?;
    Ok(())
}

/// Initialize the global bucket database schema.
///
/// Idempotent — uses `CREATE TABLE IF NOT EXISTS`.
pub fn init_bucket_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(BUCKET_PRAGMAS)?;
    conn.execute(CREATE_BUCKETS_TABLE, [])?;
    Ok(())
}
