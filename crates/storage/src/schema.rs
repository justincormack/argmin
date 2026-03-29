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
    system_metadata_blob BLOB,
    encryption_type INTEGER NOT NULL DEFAULT 0,
    encryption_state BLOB,
    owner_principal TEXT NOT NULL CHECK (length(owner_principal) BETWEEN 1 AND 256),
    owner_canonical_id TEXT NOT NULL CHECK (length(owner_canonical_id) = 64),
    acl_grants TEXT NOT NULL DEFAULT '',
    public_read INTEGER NOT NULL DEFAULT 0 CHECK (public_read IN (0, 1)),
    object_lock_retention_mode INTEGER CHECK (
        object_lock_retention_mode IS NULL OR object_lock_retention_mode IN (0, 1)
    ),
    object_lock_retain_until INTEGER CHECK (
        object_lock_retain_until IS NULL OR object_lock_retain_until > 0
    ),
    object_lock_legal_hold INTEGER NOT NULL DEFAULT 0 CHECK (object_lock_legal_hold IN (0, 1, 2)),
    CHECK (status IN (0, 1)),
    CHECK (etag_kind IN (0, 1)),
    CHECK (data_layout IN (0, 1)),
    CHECK (encryption_type IN (0, 1)),
    CHECK (
        (status = 0 AND (
            generation_id IS NOT NULL AND generation_id > 0 AND
            (data_layout = 0 AND parts_count IS NULL) OR
            (data_layout = 1 AND parts_count IS NOT NULL AND parts_count > 0)
        )) OR
        (status = 1 AND generation_id IS NULL AND data_layout = 0 AND parts_count IS NULL AND tags IS NULL AND metadata_blob IS NULL AND system_metadata_blob IS NULL
         AND size = 0 AND etag = X'' AND etag_kind = 0 AND storage_class = 0 AND ec_k = 0 AND ec_m = 0
         AND encryption_type = 0 AND encryption_state IS NULL AND public_read = 0
         AND object_lock_retention_mode IS NULL AND object_lock_retain_until IS NULL
         AND object_lock_legal_hold = 0)
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
    tags             TEXT,
    metadata_blob    BLOB NOT NULL,
    system_metadata_blob BLOB NOT NULL,
    owner_principal  TEXT NOT NULL CHECK (length(owner_principal) BETWEEN 1 AND 256),
    encryption_type  INTEGER NOT NULL DEFAULT 0 CHECK (encryption_type IN (0, 1)),
    encryption_state BLOB,
    owner_canonical_id TEXT NOT NULL CHECK (length(owner_canonical_id) = 64),
    initiator_principal TEXT CHECK (
        initiator_principal IS NULL OR length(initiator_principal) BETWEEN 1 AND 256
    ),
    initiator_canonical_id TEXT CHECK (
        initiator_canonical_id IS NULL OR length(initiator_canonical_id) = 64
    ),
    acl_grants TEXT NOT NULL DEFAULT '',
    public_read INTEGER NOT NULL DEFAULT 0 CHECK (public_read IN (0, 1)),
    object_lock_retention_mode INTEGER CHECK (
        object_lock_retention_mode IS NULL OR object_lock_retention_mode IN (0, 1)
    ),
    object_lock_retain_until INTEGER CHECK (
        object_lock_retain_until IS NULL OR object_lock_retain_until > 0
    ),
    object_lock_legal_hold INTEGER NOT NULL DEFAULT 0 CHECK (object_lock_legal_hold IN (0, 1, 2))
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
    object_offset_start INTEGER NOT NULL,
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

const CREATE_OBJECT_PARTS_OFFSET_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_object_parts_offset \
ON object_parts (bucket, key, version_id, object_offset_start, part_number)";

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
    encryption_type INTEGER NOT NULL DEFAULT 0 CHECK (encryption_type IN (0, 1)),
    encryption_state BLOB,
    CHECK (op_kind IN (0, 1)),
    CHECK (state IN (0, 1, 2, 3)),
    CHECK (
        (op_kind = 0 AND upload_id IS NULL AND part_number IS NULL) OR
        (op_kind = 1 AND upload_id IS NOT NULL AND part_number BETWEEN 1 AND 10000)
    )
)";

/// Staging segment records for in-progress streaming sessions.
const CREATE_STREAM_UPLOAD_SEGMENTS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS stream_upload_segments (
    session_id    TEXT NOT NULL,
    segment_index INTEGER NOT NULL,
    size          INTEGER NOT NULL,
    segment_crc64 INTEGER,
    segment_okh   BLOB NOT NULL,
    segment_vid   INTEGER NOT NULL CHECK (segment_vid > 0),
    shard_pg_id   INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (session_id, segment_index),
    FOREIGN KEY (session_id) REFERENCES stream_uploads(session_id) ON DELETE CASCADE
)";

/// Committed object segments for normal PutObject.
const CREATE_STREAM_OBJECT_CHUNKS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS object_segments (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    version_id    INTEGER NOT NULL CHECK (version_id >= 0),
    segment_index INTEGER NOT NULL,
    size          INTEGER NOT NULL,
    segment_crc64 INTEGER,
    segment_okh   BLOB NOT NULL,
    segment_vid   INTEGER NOT NULL CHECK (segment_vid > 0),
    shard_pg_id   INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, version_id, segment_index)
)";

/// Durable reclaim queue for simple single-shard-set payload generations.
const CREATE_SIMPLE_PAYLOAD_RECLAIMS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS simple_payload_reclaims (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    generation_id INTEGER NOT NULL CHECK (generation_id > 0),
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    created_at    INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, generation_id)
)";

/// Durable reclaim queue for standard segmented payload generations.
const CREATE_CHUNK_MANIFEST_RECLAIMS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS object_segments_reclaims (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    generation_id INTEGER NOT NULL CHECK (generation_id > 0),
    created_at    INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, generation_id)
)";

/// Child segment rows for standard segmented reclaim generations.
const CREATE_CHUNK_MANIFEST_RECLAIM_CHUNKS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS object_segment_reclaim_segments (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    generation_id INTEGER NOT NULL CHECK (generation_id > 0),
    segment_index   INTEGER NOT NULL,
    segment_okh     BLOB NOT NULL,
    segment_vid     INTEGER NOT NULL CHECK (segment_vid > 0),
    shard_pg_id   INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, generation_id, segment_index),
    FOREIGN KEY (bucket, key, generation_id)
        REFERENCES object_segments_reclaims(bucket, key, generation_id)
        ON DELETE CASCADE
)";

/// Durable reclaim queue for multipart payload generations.
const CREATE_MULTIPART_RECLAIMS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS multipart_reclaims (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    generation_id INTEGER NOT NULL CHECK (generation_id > 0),
    created_at    INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, generation_id)
)";

/// Per-part reclaim rows for multipart payload generations.
const CREATE_MULTIPART_RECLAIM_PARTS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS multipart_reclaim_parts (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    generation_id INTEGER NOT NULL CHECK (generation_id > 0),
    part_number   INTEGER NOT NULL,
    storage_kind  INTEGER NOT NULL CHECK (storage_kind IN (0, 1)),
    part_okh      BLOB,
    part_vid      INTEGER,
    shard_pg_id   INTEGER,
    ec_k          INTEGER,
    ec_m          INTEGER,
    PRIMARY KEY (bucket, key, generation_id, part_number),
    FOREIGN KEY (bucket, key, generation_id)
        REFERENCES multipart_reclaims(bucket, key, generation_id)
        ON DELETE CASCADE,
    CHECK (
        (storage_kind = 0 AND part_okh IS NOT NULL AND part_vid IS NOT NULL AND shard_pg_id IS NOT NULL AND ec_k IS NOT NULL AND ec_m IS NOT NULL) OR
        (storage_kind = 1 AND part_okh IS NULL AND part_vid IS NULL AND shard_pg_id IS NULL AND ec_k IS NULL AND ec_m IS NULL)
    )
)";

/// Child segment rows for streamed multipart part reclaim generations.
const CREATE_MULTIPART_RECLAIM_PART_CHUNKS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS multipart_reclaim_part_segments (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    generation_id INTEGER NOT NULL CHECK (generation_id > 0),
    part_number   INTEGER NOT NULL,
    segment_index   INTEGER NOT NULL,
    segment_okh     BLOB NOT NULL,
    segment_vid     INTEGER NOT NULL CHECK (segment_vid > 0),
    shard_pg_id   INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, generation_id, part_number, segment_index),
    FOREIGN KEY (bucket, key, generation_id, part_number)
        REFERENCES multipart_reclaim_parts(bucket, key, generation_id, part_number)
        ON DELETE CASCADE
)";

/// Committed multipart part segments.
const CREATE_MULTIPART_PART_CHUNKS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS multipart_part_segments (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    upload_id     TEXT NOT NULL,
    version_id    INTEGER NOT NULL,
    part_number   INTEGER NOT NULL,
    segment_index INTEGER NOT NULL,
    size          INTEGER NOT NULL,
    segment_crc64 INTEGER,
    segment_okh   BLOB NOT NULL,
    segment_vid   INTEGER NOT NULL CHECK (segment_vid > 0),
    shard_pg_id   INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, upload_id, part_number, segment_index)
)";

/// Index for reading multipart part segments by version_id after completion.
const CREATE_MULTIPART_PART_CHUNKS_VERSION_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_mpc_version \
ON multipart_part_segments (bucket, key, version_id, part_number)";

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
    owner_principal  TEXT NOT NULL CHECK (length(owner_principal) BETWEEN 1 AND 256),
    owner_canonical_id TEXT NOT NULL CHECK (length(owner_canonical_id) = 64),
    created_at       INTEGER NOT NULL,
    region           INTEGER NOT NULL DEFAULT 0,
    state            INTEGER NOT NULL DEFAULT 0 CHECK (state IN (0, 1)),
    versioning       INTEGER NOT NULL DEFAULT 0 CHECK (versioning IN (0, 1, 2)),
    acl_grants       TEXT NOT NULL DEFAULT '',
    public_read      INTEGER NOT NULL DEFAULT 0 CHECK (public_read IN (0, 1)),
    public_write     INTEGER NOT NULL DEFAULT 0 CHECK (public_write IN (0, 1)),
    write_reservations_blocked INTEGER NOT NULL DEFAULT 0 CHECK (write_reservations_blocked IN (0, 1)),
    active_write_reservations INTEGER NOT NULL DEFAULT 0 CHECK (active_write_reservations >= 0),
    cors_config      TEXT,
    tags             TEXT,
    public_access_block TEXT,
    ownership_controls TEXT,
    bucket_policy    TEXT,
    bucket_policy_public INTEGER NOT NULL DEFAULT 0 CHECK (bucket_policy_public IN (0, 1)),
    bucket_policy_generation INTEGER NOT NULL DEFAULT 0 CHECK (bucket_policy_generation >= 0),
    sse_c_blocked    INTEGER NOT NULL DEFAULT 0 CHECK (sse_c_blocked IN (0, 1)),
    object_lock_enabled INTEGER NOT NULL DEFAULT 0 CHECK (object_lock_enabled IN (0, 1)),
    object_lock_default_mode INTEGER CHECK (
        object_lock_default_mode IS NULL OR object_lock_default_mode IN (0, 1)
    ),
    object_lock_default_days INTEGER CHECK (
        object_lock_default_days IS NULL OR object_lock_default_days > 0
    ),
    object_lock_default_years INTEGER CHECK (
        object_lock_default_years IS NULL OR object_lock_default_years > 0
    )
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
    conn.execute(CREATE_STREAM_UPLOAD_SEGMENTS_TABLE, [])?;
    conn.execute(CREATE_STREAM_OBJECT_CHUNKS_TABLE, [])?;
    conn.execute(CREATE_SIMPLE_PAYLOAD_RECLAIMS_TABLE, [])?;
    conn.execute(CREATE_CHUNK_MANIFEST_RECLAIMS_TABLE, [])?;
    conn.execute(CREATE_CHUNK_MANIFEST_RECLAIM_CHUNKS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_RECLAIMS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_RECLAIM_PARTS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_RECLAIM_PART_CHUNKS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_PART_CHUNKS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_PART_CHUNKS_VERSION_INDEX, [])?;
    conn.execute(CREATE_BUCKETS_TABLE, [])?;
    conn.execute(CREATE_BUCKETS_OWNER_LIST_INDEX, [])?;
    conn.execute(CREATE_OBJECT_PARTS_OFFSET_INDEX, [])?;
    migrate_checksum_columns(conn)?;
    migrate_segment_crc_columns(conn)?;
    migrate_owner_identity_columns(conn)?;
    migrate_acl_grant_columns(conn)?;
    migrate_bucket_write_reservation_columns(conn)?;
    migrate_bucket_policy_columns(conn)?;
    migrate_multipart_upload_tag_columns(conn)?;
    create_object_lock_triggers(conn)?;
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
        "ALTER TABLE buckets ADD COLUMN public_write INTEGER NOT NULL DEFAULT 0 CHECK (public_write IN (0, 1))",
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

/// Add bucket policy storage to bucket metadata.
///
/// Idempotent — silently ignores "duplicate column name" errors.
fn migrate_bucket_policy_columns(conn: &Connection) -> Result<(), rusqlite::Error> {
    match conn.execute("ALTER TABLE buckets ADD COLUMN bucket_policy TEXT", []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(ref msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn migrate_segment_crc_columns(conn: &Connection) -> Result<(), rusqlite::Error> {
    let migrations = [
        "ALTER TABLE stream_upload_segments ADD COLUMN segment_crc64 INTEGER",
        "ALTER TABLE object_segments ADD COLUMN segment_crc64 INTEGER",
        "ALTER TABLE multipart_part_segments ADD COLUMN segment_crc64 INTEGER",
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
    Ok(())
}

fn migrate_multipart_upload_tag_columns(conn: &Connection) -> Result<(), rusqlite::Error> {
    match conn.execute("ALTER TABLE multipart_uploads ADD COLUMN tags TEXT", []) {
        Ok(_) => {}
        Err(rusqlite::Error::SqliteFailure(_, Some(ref msg)))
            if msg.contains("duplicate column name") => {}
        Err(e) => return Err(e),
    }
    Ok(())
}

fn migrate_acl_grant_columns(conn: &Connection) -> Result<(), rusqlite::Error> {
    add_column_if_missing(
        conn,
        "buckets",
        "acl_grants",
        "ALTER TABLE buckets ADD COLUMN acl_grants TEXT NOT NULL DEFAULT ''",
    )?;
    add_column_if_missing(
        conn,
        "objects",
        "acl_grants",
        "ALTER TABLE objects ADD COLUMN acl_grants TEXT NOT NULL DEFAULT ''",
    )?;
    add_column_if_missing(
        conn,
        "multipart_uploads",
        "acl_grants",
        "ALTER TABLE multipart_uploads ADD COLUMN acl_grants TEXT NOT NULL DEFAULT ''",
    )?;
    Ok(())
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool, rusqlite::Error> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let cols = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for col in cols {
        if col? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn add_column_if_missing(
    conn: &Connection,
    table: &str,
    column: &str,
    sql: &str,
) -> Result<(), rusqlite::Error> {
    if !table_has_column(conn, table, column)? {
        conn.execute(sql, [])?;
    }
    Ok(())
}

fn migrate_owner_identity_columns(conn: &Connection) -> Result<(), rusqlite::Error> {
    add_column_if_missing(
        conn,
        "buckets",
        "owner_canonical_id",
        "ALTER TABLE buckets ADD COLUMN owner_canonical_id TEXT",
    )?;

    let mut stmt = conn.prepare(
        "SELECT name, owner_principal FROM buckets \
         WHERE owner_canonical_id IS NULL OR owner_canonical_id = ''",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut bucket_updates = Vec::new();
    for row in rows {
        let (name, owner_principal) = row?;
        bucket_updates.push((
            name,
            s3_types::CanonicalUserId::from_principal(&owner_principal).into_string(),
        ));
    }
    for (name, owner_canonical_id) in bucket_updates {
        conn.execute(
            "UPDATE buckets SET owner_canonical_id = ?1 WHERE name = ?2",
            [&owner_canonical_id, &name],
        )?;
    }

    add_column_if_missing(
        conn,
        "objects",
        "owner_principal",
        "ALTER TABLE objects ADD COLUMN owner_principal TEXT",
    )?;
    add_column_if_missing(
        conn,
        "objects",
        "owner_canonical_id",
        "ALTER TABLE objects ADD COLUMN owner_canonical_id TEXT",
    )?;
    add_column_if_missing(
        conn,
        "objects",
        "public_read",
        "ALTER TABLE objects ADD COLUMN public_read INTEGER NOT NULL DEFAULT 0 CHECK (public_read IN (0, 1))",
    )?;

    let mut stmt = conn.prepare(
        "SELECT o.bucket, o.key, o.version_id, b.owner_principal, b.owner_canonical_id \
         FROM objects o \
         INNER JOIN buckets b ON b.name = o.bucket \
         WHERE o.owner_principal IS NULL OR o.owner_principal = '' \
            OR o.owner_canonical_id IS NULL OR o.owner_canonical_id = ''",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;
    let mut object_updates = Vec::new();
    for row in rows {
        object_updates.push(row?);
    }
    for (bucket, key, version_id, owner_principal, owner_canonical_id) in object_updates {
        conn.execute(
            "UPDATE objects SET owner_principal = ?1, owner_canonical_id = ?2 \
             WHERE bucket = ?3 AND key = ?4 AND version_id = ?5",
            (
                &owner_principal,
                &owner_canonical_id,
                &bucket,
                &key,
                version_id,
            ),
        )?;
    }

    add_column_if_missing(
        conn,
        "multipart_uploads",
        "owner_canonical_id",
        "ALTER TABLE multipart_uploads ADD COLUMN owner_canonical_id TEXT",
    )?;
    add_column_if_missing(
        conn,
        "multipart_uploads",
        "initiator_principal",
        "ALTER TABLE multipart_uploads ADD COLUMN initiator_principal TEXT",
    )?;
    add_column_if_missing(
        conn,
        "multipart_uploads",
        "initiator_canonical_id",
        "ALTER TABLE multipart_uploads ADD COLUMN initiator_canonical_id TEXT",
    )?;
    add_column_if_missing(
        conn,
        "multipart_uploads",
        "public_read",
        "ALTER TABLE multipart_uploads ADD COLUMN public_read INTEGER NOT NULL DEFAULT 0 CHECK (public_read IN (0, 1))",
    )?;

    let mut stmt = conn.prepare(
        "SELECT m.upload_id, m.owner_principal, m.owner_canonical_id, \
                m.initiator_principal, m.initiator_canonical_id, b.owner_principal \
         FROM multipart_uploads m \
         LEFT JOIN buckets b ON b.name = m.bucket \
         WHERE m.owner_canonical_id IS NULL OR m.owner_canonical_id = '' \
            OR (m.initiator_principal IS NOT NULL AND \
                (m.initiator_canonical_id IS NULL OR m.initiator_canonical_id = '')) \
            OR (m.initiator_principal IS NULL AND m.owner_principal IS NOT NULL)",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
        ))
    })?;
    let mut multipart_updates = Vec::new();
    for row in rows {
        multipart_updates.push(row?);
    }
    for (
        upload_id,
        owner_principal,
        owner_canonical_id,
        initiator_principal,
        initiator_canonical_id,
        bucket_owner_principal,
    ) in multipart_updates
    {
        let effective_owner_principal =
            owner_principal.or(bucket_owner_principal).ok_or_else(|| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(format!(
                    "multipart upload {upload_id} is missing both stored and bucket owner identity"
                ))))
            })?;
        let effective_owner_canonical_id = owner_canonical_id.unwrap_or_else(|| {
            s3_types::CanonicalUserId::from_principal(&effective_owner_principal).into_string()
        });
        let effective_initiator_principal =
            initiator_principal.or_else(|| Some(effective_owner_principal.clone()));
        let effective_initiator_canonical_id = effective_initiator_principal.as_ref().map(|name| {
            initiator_canonical_id
                .unwrap_or_else(|| s3_types::CanonicalUserId::from_principal(name).into_string())
        });

        conn.execute(
            "UPDATE multipart_uploads \
             SET owner_principal = ?1, owner_canonical_id = ?2, \
                 initiator_principal = ?3, initiator_canonical_id = ?4 \
             WHERE upload_id = ?5",
            (
                &effective_owner_principal,
                &effective_owner_canonical_id,
                effective_initiator_principal.as_deref(),
                effective_initiator_canonical_id.as_deref(),
                &upload_id,
            ),
        )?;
    }

    Ok(())
}

fn migrate_bucket_write_reservation_columns(conn: &Connection) -> Result<(), rusqlite::Error> {
    let migrations = [
        "ALTER TABLE buckets ADD COLUMN write_reservations_blocked INTEGER NOT NULL DEFAULT 0 CHECK (write_reservations_blocked IN (0, 1))",
        "ALTER TABLE buckets ADD COLUMN active_write_reservations INTEGER NOT NULL DEFAULT 0 CHECK (active_write_reservations >= 0)",
    ];
    for sql in &migrations {
        match conn.execute(sql, []) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(_, Some(ref msg)))
                if msg.contains("duplicate column name") => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn create_object_lock_triggers(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "CREATE TRIGGER IF NOT EXISTS check_bucket_object_lock_insert
         BEFORE INSERT ON buckets
         BEGIN
           SELECT RAISE(ABORT, 'invalid bucket object lock default mode')
             WHERE NEW.object_lock_default_mode IS NOT NULL
               AND NEW.object_lock_default_mode NOT IN (0, 1);
           SELECT RAISE(ABORT, 'invalid bucket object lock default days')
             WHERE NEW.object_lock_default_days IS NOT NULL
               AND NEW.object_lock_default_days <= 0;
           SELECT RAISE(ABORT, 'invalid bucket object lock default years')
             WHERE NEW.object_lock_default_years IS NOT NULL
               AND NEW.object_lock_default_years <= 0;
           SELECT RAISE(ABORT, 'bucket object lock defaults require object lock enabled')
             WHERE NEW.object_lock_enabled = 0
               AND (
                 NEW.object_lock_default_mode IS NOT NULL
                 OR NEW.object_lock_default_days IS NOT NULL
                 OR NEW.object_lock_default_years IS NOT NULL
               );
           SELECT RAISE(ABORT, 'bucket object lock default period requires mode')
             WHERE NEW.object_lock_default_mode IS NULL
               AND (
                 NEW.object_lock_default_days IS NOT NULL
                 OR NEW.object_lock_default_years IS NOT NULL
               );
           SELECT RAISE(ABORT, 'bucket object lock mode requires period')
             WHERE NEW.object_lock_default_mode IS NOT NULL
               AND NEW.object_lock_default_days IS NULL
               AND NEW.object_lock_default_years IS NULL;
           SELECT RAISE(ABORT, 'bucket object lock default days/years are mutually exclusive')
             WHERE NEW.object_lock_default_days IS NOT NULL
               AND NEW.object_lock_default_years IS NOT NULL;
         END;
         CREATE TRIGGER IF NOT EXISTS check_bucket_object_lock_update
         BEFORE UPDATE ON buckets
         BEGIN
           SELECT RAISE(ABORT, 'invalid bucket object lock default mode')
             WHERE NEW.object_lock_default_mode IS NOT NULL
               AND NEW.object_lock_default_mode NOT IN (0, 1);
           SELECT RAISE(ABORT, 'invalid bucket object lock default days')
             WHERE NEW.object_lock_default_days IS NOT NULL
               AND NEW.object_lock_default_days <= 0;
           SELECT RAISE(ABORT, 'invalid bucket object lock default years')
             WHERE NEW.object_lock_default_years IS NOT NULL
               AND NEW.object_lock_default_years <= 0;
           SELECT RAISE(ABORT, 'bucket object lock defaults require object lock enabled')
             WHERE NEW.object_lock_enabled = 0
               AND (
                 NEW.object_lock_default_mode IS NOT NULL
                 OR NEW.object_lock_default_days IS NOT NULL
                 OR NEW.object_lock_default_years IS NOT NULL
               );
           SELECT RAISE(ABORT, 'bucket object lock default period requires mode')
             WHERE NEW.object_lock_default_mode IS NULL
               AND (
                 NEW.object_lock_default_days IS NOT NULL
                 OR NEW.object_lock_default_years IS NOT NULL
               );
           SELECT RAISE(ABORT, 'bucket object lock mode requires period')
             WHERE NEW.object_lock_default_mode IS NOT NULL
               AND NEW.object_lock_default_days IS NULL
               AND NEW.object_lock_default_years IS NULL;
           SELECT RAISE(ABORT, 'bucket object lock default days/years are mutually exclusive')
             WHERE NEW.object_lock_default_days IS NOT NULL
               AND NEW.object_lock_default_years IS NOT NULL;
         END;
         CREATE TRIGGER IF NOT EXISTS check_object_lock_state_insert
         BEFORE INSERT ON objects
         BEGIN
           SELECT RAISE(ABORT, 'invalid object lock retention mode')
             WHERE NEW.object_lock_retention_mode IS NOT NULL
               AND NEW.object_lock_retention_mode NOT IN (0, 1);
           SELECT RAISE(ABORT, 'invalid object lock retain-until timestamp')
             WHERE NEW.object_lock_retain_until IS NOT NULL
               AND NEW.object_lock_retain_until <= 0;
           SELECT RAISE(ABORT, 'invalid object lock legal hold')
             WHERE NEW.object_lock_legal_hold NOT IN (0, 1, 2);
           SELECT RAISE(ABORT, 'object lock retention mode/date must be set together')
             WHERE (NEW.object_lock_retention_mode IS NULL) != (NEW.object_lock_retain_until IS NULL);
           SELECT RAISE(ABORT, 'delete markers cannot carry object lock state')
             WHERE NEW.status = 1
               AND (
                 NEW.object_lock_retention_mode IS NOT NULL
                 OR NEW.object_lock_retain_until IS NOT NULL
                 OR NEW.object_lock_legal_hold != 0
               );
         END;
         CREATE TRIGGER IF NOT EXISTS check_object_lock_state_update
         BEFORE UPDATE ON objects
         BEGIN
           SELECT RAISE(ABORT, 'invalid object lock retention mode')
             WHERE NEW.object_lock_retention_mode IS NOT NULL
               AND NEW.object_lock_retention_mode NOT IN (0, 1);
           SELECT RAISE(ABORT, 'invalid object lock retain-until timestamp')
             WHERE NEW.object_lock_retain_until IS NOT NULL
               AND NEW.object_lock_retain_until <= 0;
           SELECT RAISE(ABORT, 'invalid object lock legal hold')
             WHERE NEW.object_lock_legal_hold NOT IN (0, 1, 2);
           SELECT RAISE(ABORT, 'object lock retention mode/date must be set together')
             WHERE (NEW.object_lock_retention_mode IS NULL) != (NEW.object_lock_retain_until IS NULL);
           SELECT RAISE(ABORT, 'delete markers cannot carry object lock state')
             WHERE NEW.status = 1
               AND (
                 NEW.object_lock_retention_mode IS NOT NULL
                 OR NEW.object_lock_retain_until IS NOT NULL
                 OR NEW.object_lock_legal_hold != 0
               );
         END;
         CREATE TRIGGER IF NOT EXISTS check_multipart_object_lock_insert
         BEFORE INSERT ON multipart_uploads
         BEGIN
           SELECT RAISE(ABORT, 'invalid multipart object lock retention mode')
             WHERE NEW.object_lock_retention_mode IS NOT NULL
               AND NEW.object_lock_retention_mode NOT IN (0, 1);
           SELECT RAISE(ABORT, 'invalid multipart object lock retain-until timestamp')
             WHERE NEW.object_lock_retain_until IS NOT NULL
               AND NEW.object_lock_retain_until <= 0;
           SELECT RAISE(ABORT, 'invalid multipart object lock legal hold')
             WHERE NEW.object_lock_legal_hold NOT IN (0, 1, 2);
           SELECT RAISE(ABORT, 'multipart object lock retention mode/date must be set together')
             WHERE (NEW.object_lock_retention_mode IS NULL) != (NEW.object_lock_retain_until IS NULL);
         END;
         CREATE TRIGGER IF NOT EXISTS check_multipart_object_lock_update
         BEFORE UPDATE ON multipart_uploads
         BEGIN
           SELECT RAISE(ABORT, 'invalid multipart object lock retention mode')
             WHERE NEW.object_lock_retention_mode IS NOT NULL
               AND NEW.object_lock_retention_mode NOT IN (0, 1);
           SELECT RAISE(ABORT, 'invalid multipart object lock retain-until timestamp')
             WHERE NEW.object_lock_retain_until IS NOT NULL
               AND NEW.object_lock_retain_until <= 0;
           SELECT RAISE(ABORT, 'invalid multipart object lock legal hold')
             WHERE NEW.object_lock_legal_hold NOT IN (0, 1, 2);
           SELECT RAISE(ABORT, 'multipart object lock retention mode/date must be set together')
             WHERE (NEW.object_lock_retention_mode IS NULL) != (NEW.object_lock_retain_until IS NULL);
         END;",
    )?;

    Ok(())
}
