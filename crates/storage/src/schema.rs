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

/// Per-data-PG audit observations for apparent physical shard orphans.
///
/// These rows are non-authoritative telemetry. They must never be used as
/// deletion proof; they record only that one scan could not prove a durable
/// reference for a physical shard location.
const CREATE_SHARD_SCAVENGER_OBSERVATIONS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS shard_scavenger_observations (
    node_id             INTEGER NOT NULL CHECK (node_id >= 0),
    data_pg_id          INTEGER NOT NULL CHECK (data_pg_id >= 0),
    shard_index         INTEGER NOT NULL CHECK (shard_index >= 0 AND shard_index <= 255),
    shard_key           BLOB NOT NULL,
    first_seen_at       INTEGER NOT NULL CHECK (first_seen_at >= 0),
    last_seen_at        INTEGER NOT NULL CHECK (last_seen_at >= 0),
    observation_count   INTEGER NOT NULL CHECK (observation_count > 0),
    data_size           INTEGER CHECK (data_size IS NULL OR data_size >= 0),
    crc64_nvme          INTEGER,
    file_exists         INTEGER NOT NULL CHECK (file_exists IN (0, 1)),
    shard_row_exists    INTEGER NOT NULL CHECK (shard_row_exists IN (0, 1)),
    reason              INTEGER NOT NULL CHECK (reason IN (0, 1, 2, 3)),
    last_error          TEXT,
    resolved_at         INTEGER CHECK (resolved_at IS NULL OR resolved_at >= 0),
    PRIMARY KEY (node_id, data_pg_id, shard_index, shard_key)
)";

/// Durable repair queue for placed segment shards observed missing or corrupt.
const CREATE_PLACED_SEGMENT_SHARD_REPAIRS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS placed_segment_shard_repairs (
    data_pg_id          INTEGER NOT NULL CHECK (data_pg_id >= 0),
    segment_okh         BLOB NOT NULL CHECK (length(segment_okh) = 16),
    segment_vid         INTEGER NOT NULL CHECK (segment_vid > 0),
    stored_size         INTEGER NOT NULL CHECK (stored_size >= 0),
    segment_crc64       INTEGER,
    ec_k                INTEGER NOT NULL CHECK (ec_k > 0),
    ec_m                INTEGER NOT NULL CHECK (ec_m >= 0),
    shard_index         INTEGER NOT NULL CHECK (shard_index >= 0 AND shard_index <= 255),
    first_seen_at       INTEGER NOT NULL CHECK (first_seen_at >= 0),
    last_seen_at        INTEGER NOT NULL CHECK (last_seen_at >= 0),
    observation_count   INTEGER NOT NULL CHECK (observation_count > 0),
    claim_id            TEXT,
    owner_token         TEXT,
    cluster_epoch       INTEGER CHECK (cluster_epoch IS NULL OR cluster_epoch > 0),
    claimed_at          INTEGER CHECK (claimed_at IS NULL OR claimed_at >= 0),
    lease_deadline      INTEGER CHECK (lease_deadline IS NULL OR lease_deadline >= 0),
    attempt_count       INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_after  INTEGER NOT NULL DEFAULT 0 CHECK (next_attempt_after >= 0),
    last_error          TEXT,
    CHECK (
        (claim_id IS NULL AND owner_token IS NULL AND cluster_epoch IS NULL AND claimed_at IS NULL AND lease_deadline IS NULL)
        OR
        (claim_id IS NOT NULL AND owner_token IS NOT NULL AND cluster_epoch IS NOT NULL AND claimed_at IS NOT NULL AND lease_deadline IS NOT NULL)
    ),
    PRIMARY KEY (segment_okh, segment_vid, shard_index)
)";

/// Per-PG object metadata table.
const CREATE_OBJECTS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS objects (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    version_id    INTEGER NOT NULL CHECK (version_id >= 0),
    write_sequence INTEGER NOT NULL CHECK (write_sequence > 0),
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
    owner_canonical_id TEXT NOT NULL CHECK (length(owner_canonical_id) IN (32, 64)),
    acl_grants TEXT NOT NULL DEFAULT '',
    public_read INTEGER NOT NULL DEFAULT 0 CHECK (public_read IN (0, 1)),
    object_lock_retention_mode INTEGER CHECK (
        object_lock_retention_mode IS NULL OR object_lock_retention_mode IN (0, 1)
    ),
    object_lock_retain_until INTEGER CHECK (
        object_lock_retain_until IS NULL OR object_lock_retain_until > 0
    ),
    object_lock_legal_hold INTEGER NOT NULL DEFAULT 0 CHECK (object_lock_legal_hold IN (0, 1, 2)),
    became_noncurrent_at INTEGER CHECK (
        became_noncurrent_at IS NULL OR became_noncurrent_at > 0
    ),
    CHECK (status IN (0, 1)),
    CHECK (etag_kind IN (0, 1)),
    CHECK (data_layout IN (0, 1)),
    CHECK (encryption_type IN (0, 1, 2)),
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
         AND object_lock_legal_hold = 0 AND became_noncurrent_at IS NULL)
    ),
    PRIMARY KEY (bucket, key, version_id)
)";

/// Per-object-key durable version allocator.
const CREATE_OBJECT_VERSION_COUNTERS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS object_version_counters (
    bucket          TEXT NOT NULL,
    key             TEXT NOT NULL,
    next_version_id INTEGER NOT NULL CHECK (next_version_id > 0),
    PRIMARY KEY (bucket, key)
)";

/// Per-object-key durable write-order fence.
const CREATE_OBJECT_WRITE_COUNTERS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS object_write_counters (
    bucket                    TEXT NOT NULL,
    key                       TEXT NOT NULL,
    next_write_sequence       INTEGER NOT NULL CHECK (next_write_sequence > 0),
    max_committed_generation  INTEGER CHECK (max_committed_generation IS NULL OR max_committed_generation > 0),
    PRIMARY KEY (bucket, key)
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
    encryption_type  INTEGER NOT NULL DEFAULT 0 CHECK (encryption_type IN (0, 1, 2)),
    encryption_state BLOB,
    owner_canonical_id TEXT NOT NULL CHECK (length(owner_canonical_id) IN (32, 64)),
    initiator_principal TEXT CHECK (
        initiator_principal IS NULL OR length(initiator_principal) BETWEEN 1 AND 256
    ),
    initiator_canonical_id TEXT CHECK (
        initiator_canonical_id IS NULL OR length(initiator_canonical_id) IN (32, 64)
    ),
    acl_grants TEXT NOT NULL DEFAULT '',
    public_read INTEGER NOT NULL DEFAULT 0 CHECK (public_read IN (0, 1)),
    object_generation_id INTEGER NOT NULL CHECK (object_generation_id > 0),
    object_lock_retention_mode INTEGER CHECK (
        object_lock_retention_mode IS NULL OR object_lock_retention_mode IN (0, 1)
    ),
    object_lock_retain_until INTEGER CHECK (
        object_lock_retain_until IS NULL OR object_lock_retain_until > 0
    ),
    object_lock_legal_hold INTEGER NOT NULL DEFAULT 0 CHECK (object_lock_legal_hold IN (0, 1, 2))
)";

/// Completed multipart uploads retained for AbortMultipartUpload semantics.
const CREATE_COMPLETED_MULTIPART_UPLOADS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS completed_multipart_uploads (
    upload_id        TEXT PRIMARY KEY,
    bucket           TEXT NOT NULL,
    key              TEXT NOT NULL,
    completion_order INTEGER NOT NULL CHECK (completion_order > 0),
    completed_at     INTEGER NOT NULL,
    owner_principal  TEXT NOT NULL CHECK (length(owner_principal) BETWEEN 1 AND 256),
    owner_canonical_id TEXT NOT NULL CHECK (length(owner_canonical_id) IN (32, 64)),
    initiator_principal TEXT CHECK (
        initiator_principal IS NULL OR length(initiator_principal) BETWEEN 1 AND 256
    ),
    initiator_canonical_id TEXT CHECK (
        initiator_canonical_id IS NULL OR length(initiator_canonical_id) IN (32, 64)
    )
)";

/// Index for pruning old completed multipart tombstones per bucket.
const CREATE_COMPLETED_MULTIPART_UPLOADS_BUCKET_ORDER_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_completed_multipart_uploads_bucket_order \
    ON completed_multipart_uploads (bucket, completion_order)";

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
    data_pg_id      INTEGER NOT NULL,
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
    encryption_type INTEGER NOT NULL DEFAULT 0 CHECK (encryption_type IN (0, 1, 2)),
    encryption_state BLOB,
    next_segment_vid INTEGER NOT NULL DEFAULT 1 CHECK (next_segment_vid > 0),
    bucket_write_reservation_id TEXT,
    bucket_write_owner_token TEXT,
    bucket_write_cluster_epoch INTEGER,
    bucket_write_execution_generation INTEGER,
    bucket_write_incarnation_generation INTEGER,
    bucket_write_operation_kind TEXT,
    bucket_write_created_at INTEGER,
    bucket_write_lease_deadline INTEGER,
    bucket_write_target_context TEXT,
    CHECK (op_kind IN (0, 1)),
    CHECK (state IN (0, 1, 2, 3)),
    CHECK (
        (
            bucket_write_reservation_id IS NULL AND
            bucket_write_owner_token IS NULL AND
            bucket_write_cluster_epoch IS NULL AND
            bucket_write_execution_generation IS NULL AND
            bucket_write_incarnation_generation IS NULL AND
            bucket_write_operation_kind IS NULL AND
            bucket_write_created_at IS NULL AND
            bucket_write_lease_deadline IS NULL AND
            bucket_write_target_context IS NULL
        ) OR (
            bucket_write_reservation_id IS NOT NULL AND
            bucket_write_owner_token IS NOT NULL AND
            bucket_write_cluster_epoch IS NOT NULL AND
            bucket_write_execution_generation IS NOT NULL AND
            bucket_write_incarnation_generation IS NOT NULL AND
            bucket_write_operation_kind IS NOT NULL AND
            bucket_write_created_at IS NOT NULL
        )
    ),
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
    data_pg_id   INTEGER NOT NULL,
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
    data_pg_id   INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, version_id, segment_index)
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
    data_pg_id   INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, generation_id, segment_index),
    FOREIGN KEY (bucket, key, generation_id)
        REFERENCES object_segments_reclaims(bucket, key, generation_id)
        ON DELETE CASCADE
)";

/// Durable object payload generation reservations for in-flight PutObject writes.
const CREATE_OBJECT_GENERATION_RESERVATIONS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS object_generation_reservations (
    reservation_id TEXT PRIMARY KEY,
    bucket         TEXT NOT NULL,
    key            TEXT NOT NULL,
    generation_id  INTEGER NOT NULL CHECK (generation_id > 0),
    created_at     INTEGER NOT NULL,
    UNIQUE (bucket, key, generation_id)
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
    data_pg_id   INTEGER,
    ec_k          INTEGER,
    ec_m          INTEGER,
    PRIMARY KEY (bucket, key, generation_id, part_number),
    FOREIGN KEY (bucket, key, generation_id)
        REFERENCES multipart_reclaims(bucket, key, generation_id)
        ON DELETE CASCADE,
    CHECK (
        (storage_kind = 0 AND part_okh IS NOT NULL AND part_vid IS NOT NULL AND data_pg_id IS NOT NULL AND ec_k IS NOT NULL AND ec_m IS NOT NULL) OR
        (storage_kind = 1 AND part_okh IS NULL AND part_vid IS NULL AND data_pg_id IS NULL AND ec_k IS NULL AND ec_m IS NULL)
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
    data_pg_id   INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, generation_id, part_number, segment_index),
    FOREIGN KEY (bucket, key, generation_id, part_number)
        REFERENCES multipart_reclaim_parts(bucket, key, generation_id, part_number)
        ON DELETE CASCADE
)";

/// Durable object payload reclaim worker claim.
const CREATE_OBJECT_PAYLOAD_RECLAIM_CLAIMS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS object_payload_reclaim_claims (
    singleton       INTEGER PRIMARY KEY CHECK (singleton = 0),
    bucket          TEXT NOT NULL,
    bucket_incarnation_generation INTEGER NOT NULL CHECK (bucket_incarnation_generation >= 0),
    key             TEXT NOT NULL,
    generation_id   INTEGER NOT NULL CHECK (generation_id > 0),
    reclaim_kind    INTEGER NOT NULL CHECK (reclaim_kind IN (0, 1)),
    claim_id        TEXT NOT NULL CHECK (length(claim_id) BETWEEN 1 AND 256),
    owner_token     TEXT NOT NULL CHECK (length(owner_token) BETWEEN 1 AND 256),
    cluster_epoch   INTEGER NOT NULL CHECK (cluster_epoch > 0),
    pg_id           INTEGER NOT NULL CHECK (pg_id >= 0),
    claimed_at      INTEGER NOT NULL CHECK (claimed_at >= 0),
    lease_deadline  INTEGER CHECK (lease_deadline IS NULL OR lease_deadline >= 0),
    attempt_count   INTEGER NOT NULL CHECK (attempt_count >= 0),
    last_error      TEXT
)";

/// Durable bucket delete finalizer worker claim.
const CREATE_BUCKET_DELETE_FINALIZE_CLAIMS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS bucket_delete_finalize_claims (
    singleton       INTEGER PRIMARY KEY CHECK (singleton = 0),
    bucket          TEXT NOT NULL,
    bucket_incarnation_generation INTEGER NOT NULL CHECK (bucket_incarnation_generation >= 0),
    claim_id        TEXT NOT NULL CHECK (length(claim_id) BETWEEN 1 AND 256),
    owner_token     TEXT NOT NULL CHECK (length(owner_token) BETWEEN 1 AND 256),
    cluster_epoch   INTEGER NOT NULL CHECK (cluster_epoch > 0),
    pg_id           INTEGER NOT NULL CHECK (pg_id >= 0),
    claimed_at      INTEGER NOT NULL CHECK (claimed_at >= 0),
    lease_deadline  INTEGER CHECK (lease_deadline IS NULL OR lease_deadline >= 0),
    attempt_count   INTEGER NOT NULL CHECK (attempt_count >= 0),
    last_error      TEXT,
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
)";

/// Durable lifecycle sweep worker claims, keyed by bucket incarnation.
const CREATE_LIFECYCLE_SWEEP_CLAIMS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS lifecycle_sweep_claims (
    bucket          TEXT NOT NULL,
    bucket_incarnation_generation INTEGER NOT NULL CHECK (bucket_incarnation_generation >= 0),
    claim_id        TEXT NOT NULL CHECK (length(claim_id) BETWEEN 1 AND 256),
    owner_token     TEXT NOT NULL CHECK (length(owner_token) BETWEEN 1 AND 256),
    cluster_epoch   INTEGER NOT NULL CHECK (cluster_epoch > 0),
    pg_id           INTEGER NOT NULL CHECK (pg_id >= 0),
    claimed_at      INTEGER NOT NULL CHECK (claimed_at >= 0),
    heartbeat_at    INTEGER NOT NULL CHECK (heartbeat_at >= 0),
    lease_deadline  INTEGER CHECK (lease_deadline IS NULL OR lease_deadline >= 0),
    attempt_count   INTEGER NOT NULL CHECK (attempt_count >= 0),
    last_error      TEXT,
    PRIMARY KEY (bucket, bucket_incarnation_generation),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
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
    data_pg_id   INTEGER NOT NULL,
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

/// Index for current-object and version ordering queries.
const CREATE_OBJECTS_WRITE_SEQUENCE_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_objects_write_sequence ON objects (bucket, key, write_sequence DESC)";

/// Bucket metadata table.
const CREATE_BUCKETS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS buckets (
    name             TEXT PRIMARY KEY,
    owner_principal  TEXT NOT NULL CHECK (length(owner_principal) BETWEEN 1 AND 256),
    owner_canonical_id TEXT NOT NULL CHECK (length(owner_canonical_id) IN (32, 64)),
    created_at       INTEGER NOT NULL,
    region           INTEGER NOT NULL DEFAULT 0,
    state            INTEGER NOT NULL DEFAULT 0 CHECK (state IN (0, 1)),
    versioning       INTEGER NOT NULL DEFAULT 0 CHECK (versioning IN (0, 1, 2)),
    acl_grants       TEXT NOT NULL DEFAULT '',
    public_read      INTEGER NOT NULL DEFAULT 0 CHECK (public_read IN (0, 1)),
    public_write     INTEGER NOT NULL DEFAULT 0 CHECK (public_write IN (0, 1)),
    public_access_block_present INTEGER NOT NULL DEFAULT 0 CHECK (public_access_block_present IN (0, 1)),
    public_access_block_block_public_acls INTEGER NOT NULL DEFAULT 0 CHECK (public_access_block_block_public_acls IN (0, 1)),
    public_access_block_ignore_public_acls INTEGER NOT NULL DEFAULT 0 CHECK (public_access_block_ignore_public_acls IN (0, 1)),
    public_access_block_block_public_policy INTEGER NOT NULL DEFAULT 0 CHECK (public_access_block_block_public_policy IN (0, 1)),
    public_access_block_restrict_public_buckets INTEGER NOT NULL DEFAULT 0 CHECK (public_access_block_restrict_public_buckets IN (0, 1)),
    ownership_controls_mode INTEGER CHECK (ownership_controls_mode IN (0, 1, 2)),
    bucket_policy_public INTEGER NOT NULL DEFAULT 0 CHECK (bucket_policy_public IN (0, 1)),
    bucket_policy_generation INTEGER NOT NULL DEFAULT 0 CHECK (bucket_policy_generation >= 0),
    bucket_lifecycle_generation INTEGER NOT NULL DEFAULT 0 CHECK (bucket_lifecycle_generation >= 0),
    bucket_execution_generation INTEGER NOT NULL DEFAULT 0 CHECK (bucket_execution_generation >= 0),
    bucket_incarnation_generation INTEGER NOT NULL DEFAULT 0 CHECK (bucket_incarnation_generation >= 0),
    completed_multipart_upload_sequence INTEGER NOT NULL DEFAULT 0 CHECK (completed_multipart_upload_sequence >= 0),
    bucket_abac_enabled INTEGER NOT NULL DEFAULT 0 CHECK (bucket_abac_enabled IN (0, 1)),
    default_encryption_type INTEGER CHECK (
        default_encryption_type IS NULL OR default_encryption_type IN (1)
    ),
    sse_c_blocked    INTEGER NOT NULL DEFAULT 1 CHECK (sse_c_blocked IN (0, 1)),
    object_lock_enabled INTEGER NOT NULL DEFAULT 0 CHECK (object_lock_enabled IN (0, 1)),
    object_lock_default_mode INTEGER CHECK (
        object_lock_default_mode IS NULL OR object_lock_default_mode IN (0, 1)
    ),
    object_lock_default_days INTEGER CHECK (
        object_lock_default_days IS NULL OR object_lock_default_days > 0
    ),
    object_lock_default_years INTEGER CHECK (
        object_lock_default_years IS NULL OR object_lock_default_years > 0
    ),
    CHECK (object_lock_enabled = 0 OR versioning = 1)
)";

/// Durable bucket write reservation records.
const CREATE_BUCKET_WRITE_RESERVATIONS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS bucket_write_reservations (
    bucket_name      TEXT NOT NULL,
    reservation_id   TEXT NOT NULL CHECK (length(reservation_id) BETWEEN 1 AND 256),
    owner_token      TEXT NOT NULL CHECK (length(owner_token) BETWEEN 1 AND 256),
    cluster_epoch    INTEGER NOT NULL CHECK (cluster_epoch > 0),
    bucket_execution_generation INTEGER NOT NULL CHECK (bucket_execution_generation >= 0),
    bucket_incarnation_generation INTEGER NOT NULL CHECK (bucket_incarnation_generation >= 0),
    operation_kind   TEXT NOT NULL CHECK (length(operation_kind) BETWEEN 1 AND 64),
    created_at       INTEGER NOT NULL CHECK (created_at >= 0),
    lease_deadline   INTEGER CHECK (lease_deadline IS NULL OR lease_deadline >= 0),
    target_context   TEXT,
    PRIMARY KEY (bucket_name, reservation_id),
    FOREIGN KEY (bucket_name) REFERENCES buckets(name) ON DELETE CASCADE
)";

/// Durable bucket delete write-drain records.
const CREATE_BUCKET_WRITE_DRAINS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS bucket_write_drains (
    bucket_name      TEXT PRIMARY KEY,
    drain_id         TEXT NOT NULL CHECK (length(drain_id) BETWEEN 1 AND 256),
    owner_token      TEXT NOT NULL CHECK (length(owner_token) BETWEEN 1 AND 256),
    cluster_epoch    INTEGER NOT NULL CHECK (cluster_epoch > 0),
    bucket_execution_generation INTEGER NOT NULL CHECK (bucket_execution_generation >= 0),
    state            INTEGER NOT NULL CHECK (state IN (0)),
    created_at       INTEGER NOT NULL CHECK (created_at >= 0),
    lease_deadline   INTEGER CHECK (lease_deadline IS NULL OR lease_deadline >= 0),
    FOREIGN KEY (bucket_name) REFERENCES buckets(name) ON DELETE CASCADE
)";

/// Index for bucket listing by owner and bucket name.
const CREATE_BUCKETS_OWNER_LIST_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_buckets_owner_list ON buckets (owner_principal, name)";

/// Per-PG monotonic counters for bucket execution freshness.
const CREATE_PG_COUNTERS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS pg_counters (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 0),
    next_bucket_execution_generation INTEGER NOT NULL DEFAULT 0 CHECK (next_bucket_execution_generation >= 0)
)";

/// Per-PG metadata command log entries accepted by this replica.
const CREATE_METADATA_COMMAND_LOG_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS metadata_command_log (
    cluster_epoch    INTEGER NOT NULL CHECK (cluster_epoch > 0),
    pg_id            INTEGER NOT NULL CHECK (pg_id >= 0),
    log_index        INTEGER NOT NULL CHECK (log_index > 0),
    command_checksum INTEGER NOT NULL,
    command_bytes    BLOB NOT NULL,
    abandoned        INTEGER NOT NULL DEFAULT 0 CHECK (abandoned IN (0, 1)),
    previous_log_hash INTEGER,
    log_hash         INTEGER,
    PRIMARY KEY (cluster_epoch, pg_id, log_index)
)";

/// Per-PG unresolved metadata command slot.
///
/// Each PG database owns at most one slot. The command bytes are the canonical
/// applied command bytes, not serving metadata; this table is runtime
/// coordination state for retry and convergence.
const CREATE_METADATA_COMMAND_PENDING_SLOT_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS metadata_command_pending_slot (
    singleton        INTEGER PRIMARY KEY CHECK (singleton = 0),
    cluster_epoch    INTEGER NOT NULL CHECK (cluster_epoch > 0),
    pg_id            INTEGER NOT NULL CHECK (pg_id >= 0),
    log_index        INTEGER NOT NULL CHECK (log_index > 0),
    command_checksum INTEGER NOT NULL,
    command_bytes    BLOB NOT NULL,
    scope_bucket     TEXT
)";

/// Per-PG durable metadata command replay state for this replica.
const CREATE_METADATA_COMMAND_REPLICA_STATE_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS metadata_command_replica_state (
    singleton         INTEGER PRIMARY KEY CHECK (singleton = 0),
    cluster_epoch     INTEGER NOT NULL CHECK (cluster_epoch > 0),
    applied_log_index INTEGER NOT NULL DEFAULT 0 CHECK (applied_log_index >= 0),
    applied_log_hash  INTEGER NOT NULL DEFAULT 0,
    state_digest      INTEGER NOT NULL DEFAULT 0
)";

/// Per-table canonical digest cache used to update replica state cheaply after
/// command apply. Restart validation recomputes the materialized digest from
/// the command-owned tables and does not trust this cache.
const CREATE_METADATA_TABLE_DIGESTS_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS metadata_table_digests (
    table_name   TEXT PRIMARY KEY,
    table_digest INTEGER NOT NULL,
    row_count    INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    row_hash_xor INTEGER NOT NULL DEFAULT 0,
    row_hash_sum INTEGER NOT NULL DEFAULT 0
)";

/// Durable cross-connection revision for the metadata digest cache. Digest
/// triggers bump this whenever command-owned metadata changes, allowing an open
/// PgStore handle to skip digest scans only when its clean revision still
/// matches the database.
const CREATE_METADATA_DIGEST_REVISION_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS metadata_digest_revision (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 0),
    revision  INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0)
)";

/// Durable marker proving metadata digest triggers and cache stats were
/// bootstrapped atomically. Missing marker forces a one-time full refresh on
/// open, including for stores created before this marker existed.
const CREATE_METADATA_DIGEST_BOOTSTRAP_STATE_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS metadata_digest_bootstrap_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 0),
    completed INTEGER NOT NULL CHECK (completed IN (0, 1))
)";

/// Bucket-scoped opaque subresource storage.
const CREATE_BUCKET_SUBRESOURCES_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS bucket_subresources (
    bucket_name     TEXT NOT NULL,
    kind            INTEGER NOT NULL CHECK (kind IN (0, 1, 2, 3, 4, 5)),
    body            TEXT,
    generation      INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
    aux_int_1       INTEGER,
    PRIMARY KEY (bucket_name, kind),
    FOREIGN KEY (bucket_name) REFERENCES buckets(name) ON DELETE CASCADE,
    CHECK (aux_int_1 IS NULL OR aux_int_1 IN (0, 1))
)";

/// Index for scanning buckets by subresource kind without touching tombstones.
const CREATE_BUCKET_SUBRESOURCES_KIND_BUCKET_INDEX: &str = "\
CREATE INDEX IF NOT EXISTS idx_bucket_subresources_kind_bucket \
ON bucket_subresources (kind, bucket_name) WHERE body IS NOT NULL";

/// SQLite pragmas for per-PG databases: WAL mode, NORMAL synchronous.
const PG_PRAGMAS: &str = "\
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA foreign_keys=ON;
";

/// Initialize the per-PG database schema (shards + objects + multipart tables).
///
/// Idempotent — uses `CREATE TABLE IF NOT EXISTS`.
pub fn init_pg_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(PG_PRAGMAS)?;
    conn.execute(CREATE_SHARDS_TABLE, [])?;
    conn.execute(CREATE_SHARD_SCAVENGER_OBSERVATIONS_TABLE, [])?;
    conn.execute(CREATE_PLACED_SEGMENT_SHARD_REPAIRS_TABLE, [])?;
    conn.execute(CREATE_OBJECTS_TABLE, [])?;
    conn.execute(CREATE_OBJECT_VERSION_COUNTERS_TABLE, [])?;
    conn.execute(CREATE_OBJECT_WRITE_COUNTERS_TABLE, [])?;
    conn.execute(CREATE_OBJECTS_LIST_INDEX, [])?;
    conn.execute(CREATE_OBJECTS_VERSIONS_INDEX, [])?;
    conn.execute(CREATE_OBJECTS_WRITE_SEQUENCE_INDEX, [])?;
    conn.execute(CREATE_MULTIPART_UPLOADS_TABLE, [])?;
    conn.execute(CREATE_COMPLETED_MULTIPART_UPLOADS_TABLE, [])?;
    conn.execute(CREATE_COMPLETED_MULTIPART_UPLOADS_BUCKET_ORDER_INDEX, [])?;
    conn.execute(CREATE_MPU_BUCKET_KEY_INDEX, [])?;
    conn.execute(CREATE_MULTIPART_PARTS_TABLE, [])?;
    conn.execute(CREATE_OBJECT_PARTS_TABLE, [])?;
    conn.execute(CREATE_STREAM_UPLOADS_TABLE, [])?;
    conn.execute(CREATE_STREAM_UPLOAD_SEGMENTS_TABLE, [])?;
    conn.execute(CREATE_STREAM_OBJECT_CHUNKS_TABLE, [])?;
    conn.execute(CREATE_CHUNK_MANIFEST_RECLAIMS_TABLE, [])?;
    conn.execute(CREATE_CHUNK_MANIFEST_RECLAIM_CHUNKS_TABLE, [])?;
    conn.execute(CREATE_OBJECT_GENERATION_RESERVATIONS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_RECLAIMS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_RECLAIM_PARTS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_RECLAIM_PART_CHUNKS_TABLE, [])?;
    conn.execute(CREATE_OBJECT_PAYLOAD_RECLAIM_CLAIMS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_PART_CHUNKS_TABLE, [])?;
    conn.execute(CREATE_MULTIPART_PART_CHUNKS_VERSION_INDEX, [])?;
    conn.execute(CREATE_BUCKETS_TABLE, [])?;
    conn.execute(CREATE_BUCKET_WRITE_RESERVATIONS_TABLE, [])?;
    conn.execute(CREATE_BUCKET_WRITE_DRAINS_TABLE, [])?;
    conn.execute(CREATE_BUCKET_DELETE_FINALIZE_CLAIMS_TABLE, [])?;
    conn.execute(CREATE_LIFECYCLE_SWEEP_CLAIMS_TABLE, [])?;
    conn.execute(CREATE_BUCKETS_OWNER_LIST_INDEX, [])?;
    conn.execute(CREATE_PG_COUNTERS_TABLE, [])?;
    conn.execute(CREATE_METADATA_COMMAND_LOG_TABLE, [])?;
    conn.execute(CREATE_METADATA_COMMAND_PENDING_SLOT_TABLE, [])?;
    conn.execute(CREATE_METADATA_COMMAND_REPLICA_STATE_TABLE, [])?;
    conn.execute(CREATE_METADATA_TABLE_DIGESTS_TABLE, [])?;
    conn.execute(CREATE_METADATA_DIGEST_REVISION_TABLE, [])?;
    conn.execute(CREATE_METADATA_DIGEST_BOOTSTRAP_STATE_TABLE, [])?;
    conn.execute(
        "INSERT INTO pg_counters (singleton, next_bucket_execution_generation) \
         VALUES (0, 0) \
         ON CONFLICT(singleton) DO NOTHING",
        [],
    )?;
    conn.execute(
        "INSERT INTO metadata_digest_revision (singleton, revision) \
         VALUES (0, 0) \
         ON CONFLICT(singleton) DO NOTHING",
        [],
    )?;
    conn.execute(CREATE_BUCKET_SUBRESOURCES_TABLE, [])?;
    conn.execute(CREATE_BUCKET_SUBRESOURCES_KIND_BUCKET_INDEX, [])?;
    conn.execute(CREATE_OBJECT_PARTS_OFFSET_INDEX, [])?;
    migrate_checksum_columns(conn)?;
    migrate_segment_crc_columns(conn)?;
    migrate_owner_identity_columns(conn)?;
    migrate_acl_grant_columns(conn)?;
    migrate_object_write_sequence_columns(conn)?;
    migrate_object_became_noncurrent_columns(conn)?;
    migrate_multipart_upload_tag_columns(conn)?;
    migrate_multipart_upload_object_generation_columns(conn)?;
    migrate_metadata_table_digest_columns(conn)?;
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

fn migrate_object_write_sequence_columns(conn: &Connection) -> Result<(), rusqlite::Error> {
    add_column_if_missing(
        conn,
        "objects",
        "write_sequence",
        "ALTER TABLE objects ADD COLUMN write_sequence INTEGER NOT NULL DEFAULT 0 CHECK (write_sequence >= 0)",
    )?;
    conn.execute(CREATE_OBJECTS_WRITE_SEQUENCE_INDEX, [])?;

    let mut stmt = conn.prepare(
        "SELECT rowid, bucket, key \
         FROM objects \
         ORDER BY bucket ASC, key ASC, last_modified ASC, rowid ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    let mut current_bucket = String::new();
    let mut current_key = String::new();
    let mut sequence = 0i64;
    let mut updates = Vec::new();
    for row in rows {
        let (rowid, bucket, key) = row?;
        if bucket != current_bucket || key != current_key {
            current_bucket = bucket;
            current_key = key;
            sequence = 1;
        } else {
            sequence += 1;
        }
        updates.push((rowid, sequence));
    }

    for (rowid, write_sequence) in updates {
        conn.execute(
            "UPDATE objects SET write_sequence = ?1 WHERE rowid = ?2 AND write_sequence = 0",
            [write_sequence, rowid],
        )?;
    }

    Ok(())
}

fn migrate_metadata_table_digest_columns(conn: &Connection) -> Result<(), rusqlite::Error> {
    add_column_if_missing(
        conn,
        "metadata_table_digests",
        "row_count",
        "ALTER TABLE metadata_table_digests ADD COLUMN row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0)",
    )?;
    add_column_if_missing(
        conn,
        "metadata_table_digests",
        "row_hash_xor",
        "ALTER TABLE metadata_table_digests ADD COLUMN row_hash_xor INTEGER NOT NULL DEFAULT 0",
    )?;
    add_column_if_missing(
        conn,
        "metadata_table_digests",
        "row_hash_sum",
        "ALTER TABLE metadata_table_digests ADD COLUMN row_hash_sum INTEGER NOT NULL DEFAULT 0",
    )?;
    Ok(())
}

fn migrate_object_became_noncurrent_columns(conn: &Connection) -> Result<(), rusqlite::Error> {
    add_column_if_missing(
        conn,
        "objects",
        "became_noncurrent_at",
        "ALTER TABLE objects ADD COLUMN became_noncurrent_at INTEGER",
    )
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

fn migrate_multipart_upload_object_generation_columns(
    conn: &Connection,
) -> Result<(), rusqlite::Error> {
    add_column_if_missing(
        conn,
        "multipart_uploads",
        "object_generation_id",
        "ALTER TABLE multipart_uploads ADD COLUMN object_generation_id INTEGER NOT NULL DEFAULT 1 CHECK (object_generation_id > 0)",
    )
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
           SELECT RAISE(ABORT, 'bucket object lock requires enabled versioning')
             WHERE NEW.object_lock_enabled = 1
               AND NEW.versioning != 1;
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
           SELECT RAISE(ABORT, 'bucket object lock requires enabled versioning')
             WHERE NEW.object_lock_enabled = 1
               AND NEW.versioning != 1;
           SELECT RAISE(ABORT, 'bucket object lock cannot be disabled once enabled')
             WHERE OLD.object_lock_enabled = 1
               AND NEW.object_lock_enabled = 0;
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
