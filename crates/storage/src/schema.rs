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
    version_id    INTEGER NOT NULL CHECK (version_id >= 0),
    generation_id INTEGER,
    size          INTEGER NOT NULL,
    etag          BLOB NOT NULL,
    etag_kind     INTEGER NOT NULL,
    last_modified INTEGER NOT NULL,
    storage_class INTEGER NOT NULL DEFAULT 0 CHECK (storage_class IN (0)),
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    status        INTEGER NOT NULL DEFAULT 0,
    tags          TEXT,
    data_layout   INTEGER NOT NULL DEFAULT 0,
    parts_count   INTEGER,
    metadata_blob BLOB,
    CHECK (status IN (0, 1)),
    CHECK (etag_kind IN (0, 1)),
    CHECK (data_layout IN (0, 1)),
    CHECK (
        (status = 0 AND (
            generation_id IS NOT NULL AND generation_id > 0 AND
            (data_layout = 0 AND parts_count IS NULL) OR
            (data_layout = 1 AND parts_count IS NOT NULL AND parts_count > 0)
        )) OR
        (status = 1 AND generation_id IS NULL AND data_layout = 0 AND parts_count IS NULL AND tags IS NULL AND metadata_blob IS NULL
         AND size = 0 AND etag = X'' AND etag_kind = 0 AND storage_class = 0 AND ec_k = 0 AND ec_m = 0)
    ),
    PRIMARY KEY (bucket, key, version_id)
)";

/// In-progress multipart upload tracking table.
const CREATE_MULTIPART_UPLOADS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id        TEXT PRIMARY KEY,
    bucket           TEXT NOT NULL,
    key              TEXT NOT NULL,
    initiated_at     INTEGER NOT NULL,
    state            INTEGER NOT NULL DEFAULT 0,
    metadata_blob    BLOB NOT NULL,
    owner_principal  TEXT
)";

/// Index for listing multipart uploads by bucket/key.
const CREATE_MPU_BUCKET_KEY_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_mpu_bucket_key \
    ON multipart_uploads (bucket, key, initiated_at, upload_id)";

/// In-progress multipart part tracking table.
const CREATE_MULTIPART_PARTS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS multipart_parts (
    upload_id        TEXT NOT NULL,
    part_number      INTEGER NOT NULL,
    generation       INTEGER NOT NULL,
    size             INTEGER NOT NULL,
    etag             BLOB NOT NULL,
    etag_kind        INTEGER NOT NULL CHECK (etag_kind IN (0, 1)),
    part_okh         BLOB NOT NULL,
    part_vid         INTEGER NOT NULL CHECK (part_vid > 0),
    ec_k             INTEGER NOT NULL,
    ec_m             INTEGER NOT NULL,
    last_modified    INTEGER NOT NULL,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
)";

/// Committed multipart manifest table for completed objects.
const CREATE_OBJECT_PARTS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS object_parts (
    bucket           TEXT NOT NULL,
    key              TEXT NOT NULL,
    version_id       INTEGER NOT NULL CHECK (version_id >= 0),
    part_number      INTEGER NOT NULL,
    size             INTEGER NOT NULL,
    etag             BLOB NOT NULL,
    etag_kind        INTEGER NOT NULL CHECK (etag_kind IN (0, 1)),
    part_okh         BLOB NOT NULL,
    part_vid         INTEGER NOT NULL CHECK (part_vid > 0),
    ec_k             INTEGER NOT NULL,
    ec_m             INTEGER NOT NULL,
    shard_pg_id      INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, version_id, part_number)
)";

/// In-progress streaming upload session table.
const CREATE_STREAM_UPLOADS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS stream_uploads (
    session_id    TEXT PRIMARY KEY,
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    op_kind       INTEGER NOT NULL,
    upload_id     TEXT,
    part_number   INTEGER,
    state         INTEGER NOT NULL DEFAULT 0,
    created_at    INTEGER NOT NULL,
    CHECK (op_kind IN (0, 1)),
    CHECK (state IN (0, 1, 2, 3)),
    CHECK (
        (op_kind = 0 AND upload_id IS NULL AND part_number IS NULL) OR
        (op_kind = 1 AND upload_id IS NOT NULL AND part_number BETWEEN 1 AND 10000)
    )
)";

/// Staging chunk records for in-progress streaming sessions.
const CREATE_STREAM_UPLOAD_CHUNKS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS stream_upload_chunks (
    session_id    TEXT NOT NULL,
    chunk_index   INTEGER NOT NULL,
    size          INTEGER NOT NULL,
    chunk_okh     BLOB NOT NULL,
    chunk_vid     INTEGER NOT NULL CHECK (chunk_vid > 0),
    shard_pg_id   INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (session_id, chunk_index),
    FOREIGN KEY (session_id) REFERENCES stream_uploads(session_id) ON DELETE CASCADE
)";

/// Committed chunk manifest for normal PutObject.
const CREATE_STREAM_OBJECT_CHUNKS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS stream_object_chunks (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    version_id    INTEGER NOT NULL CHECK (version_id >= 0),
    chunk_index   INTEGER NOT NULL,
    size          INTEGER NOT NULL,
    chunk_okh     BLOB NOT NULL,
    chunk_vid     INTEGER NOT NULL CHECK (chunk_vid > 0),
    shard_pg_id   INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, version_id, chunk_index)
)";

/// Committed chunk manifest for multipart parts.
const CREATE_MULTIPART_PART_CHUNKS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS multipart_part_chunks (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    upload_id     TEXT NOT NULL,
    version_id    INTEGER NOT NULL,
    part_number   INTEGER NOT NULL,
    chunk_index   INTEGER NOT NULL,
    size          INTEGER NOT NULL,
    chunk_okh     BLOB NOT NULL,
    chunk_vid     INTEGER NOT NULL CHECK (chunk_vid > 0),
    shard_pg_id   INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, upload_id, part_number, chunk_index)
)";

/// Index for reading multipart part chunks by version_id after completion.
const CREATE_MULTIPART_PART_CHUNKS_VERSION_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_mpc_version \
ON multipart_part_chunks (bucket, key, version_id, part_number)";

/// Index for list operations: bucket + key ordering.
const CREATE_OBJECTS_LIST_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_objects_list ON objects (bucket, key)";

/// Index for version queries: bucket + key + version_id descending for fast latest-version lookup.
const CREATE_OBJECTS_VERSIONS_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_objects_versions ON objects (bucket, key, version_id DESC)";

/// Bucket metadata table.
const CREATE_BUCKETS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS buckets (
    name             TEXT PRIMARY KEY,
    owner_principal  TEXT NOT NULL,
    created_at       INTEGER NOT NULL,
    region           INTEGER NOT NULL DEFAULT 0,
    versioning       INTEGER NOT NULL DEFAULT 0 CHECK (versioning IN (0, 1, 2)),
    public_read      INTEGER NOT NULL DEFAULT 0,
    cors_config      TEXT,
    tags             TEXT,
    public_access_block TEXT,
    ownership_controls TEXT
)";

/// Index for bucket listing by owner and bucket name.
const CREATE_BUCKETS_OWNER_LIST_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_buckets_owner_list ON buckets (owner_principal, name)";

/// SQLite pragmas for per-PG databases: WAL mode, NORMAL synchronous.
const PG_PRAGMAS: &str = "\
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
";

/// Initialize the per-PG database schema (shards + objects + multipart tables).
///
/// Idempotent — uses `CREATE TABLE IF NOT EXISTS`.
pub fn init_pg_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(PG_PRAGMAS)?;
    conn.execute(CREATE_SHARDS_TABLE, [])?;
    conn.execute(CREATE_OBJECTS_TABLE, [])?;
    conn.execute(CREATE_OBJECTS_LIST_INDEX, [])?;
    conn.execute(CREATE_OBJECTS_VERSIONS_INDEX, [])?;
    conn.execute(CREATE_MULTIPART_UPLOADS_TABLE, [])?;
    conn.execute(CREATE_MPU_BUCKET_KEY_INDEX, [])?;
    conn.execute(CREATE_MULTIPART_PARTS_TABLE, [])?;
    conn.execute(CREATE_OBJECT_PARTS_TABLE, [])?;
    conn.execute(CREATE_STREAM_UPLOADS_TABLE, [])?;
    conn.execute(CREATE_STREAM_UPLOAD_CHUNKS_TABLE, [])?;
    conn.execute(CREATE_STREAM_OBJECT_CHUNKS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_PART_CHUNKS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_PART_CHUNKS_VERSION_INDEX, [])?;
    conn.execute(CREATE_BUCKETS_TABLE, [])?;
    conn.execute(CREATE_BUCKETS_OWNER_LIST_INDEX, [])?;
    migrate_checksum_columns(conn)?;
    Ok(())
}

/// Add checksum columns to multipart tables.
///
/// Idempotent — silently ignores "duplicate column name" errors.
fn migrate_checksum_columns(conn: &Connection) -> Result<(), rusqlite::Error> {
    let migrations = [
        "ALTER TABLE multipart_uploads ADD COLUMN checksum_algorithm INTEGER",
        "ALTER TABLE multipart_uploads ADD COLUMN checksum_type INTEGER",
        "ALTER TABLE multipart_parts ADD COLUMN checksum BLOB",
        "ALTER TABLE object_parts ADD COLUMN checksum BLOB",
    ];
    for sql in &migrations {
        match conn.execute(sql, []) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(_, Some(ref msg)))
                if msg.contains("duplicate column name") =>
            {
                // Column already exists, skip.
            }
            Err(e) => return Err(e),
        }
    }

    // Enforce valid checksum algorithm + type combinations via trigger.
    // SQLite cannot add CHECK constraints to existing tables, so we use
    // BEFORE INSERT/UPDATE triggers instead.
    //
    // Rules:
    //   - SHA1 (2) / SHA256 (3) + FULL_OBJECT (1) → invalid
    //   - CRC64NVME (4) + COMPOSITE (0) → invalid
    //   - checksum_type without checksum_algorithm → invalid
    conn.execute_batch(
        "CREATE TRIGGER IF NOT EXISTS check_multipart_checksum_insert
         BEFORE INSERT ON multipart_uploads
         WHEN NEW.checksum_algorithm IS NOT NULL OR NEW.checksum_type IS NOT NULL
         BEGIN
           SELECT RAISE(ABORT, 'checksum_type without checksum_algorithm')
             WHERE NEW.checksum_type IS NOT NULL AND NEW.checksum_algorithm IS NULL;
           SELECT RAISE(ABORT, 'SHA + FULL_OBJECT is invalid')
             WHERE NEW.checksum_algorithm IN (2, 3) AND NEW.checksum_type = 1;
           SELECT RAISE(ABORT, 'CRC64NVME + COMPOSITE is invalid')
             WHERE NEW.checksum_algorithm = 4 AND NEW.checksum_type = 0;
         END;
         CREATE TRIGGER IF NOT EXISTS check_multipart_checksum_update
         BEFORE UPDATE ON multipart_uploads
         WHEN NEW.checksum_algorithm IS NOT NULL OR NEW.checksum_type IS NOT NULL
         BEGIN
           SELECT RAISE(ABORT, 'checksum_type without checksum_algorithm')
             WHERE NEW.checksum_type IS NOT NULL AND NEW.checksum_algorithm IS NULL;
           SELECT RAISE(ABORT, 'SHA + FULL_OBJECT is invalid')
             WHERE NEW.checksum_algorithm IN (2, 3) AND NEW.checksum_type = 1;
           SELECT RAISE(ABORT, 'CRC64NVME + COMPOSITE is invalid')
             WHERE NEW.checksum_algorithm = 4 AND NEW.checksum_type = 0;
         END;",
    )?;

    Ok(())
}
