# S3 Object Versioning

## Context

Adding S3-compatible object versioning. Once enabled on a bucket, every PUT creates a
new version (preserving old versions) and DELETE inserts a delete marker (soft delete)
rather than physically removing data.

Key architectural goal: **distribute version shard data across PGs** while keeping
metadata lookups fast. This also enables future tiering (metadata on SSD, shards on HDD)
and separating metadata and data clusters.

## Architecture: Split Metadata/Shard Placement

The central design decision: **metadata and shard data go to different PGs**.

- **Metadata PG** = `derive_pg(bucket, key)` — same for ALL versions of a key
- **Shard PG** = `derive_pg(bucket, key, version_id)` — different per version

This gives:
- **Fast latest-version lookup**: all version metadata for a key is in one PG, O(1)
- **Distributed shard data**: different versions' shards spread across PGs/nodes
- **Efficient ListObjectVersions**: query one metadata PG per key
- **Future tiering**: metadata PGs on fast storage, shard PGs on bulk storage

For v1 single-node, both PGs are local — zero performance difference. But the
architecture is ready for the distributed case.

### Write path
```
meta_pg  = get_pg(derive_pg(bucket, key))
shard_pg = get_pg(derive_pg(bucket, key, version_id))
1. Write shards to shard_pg
2. Write metadata record to meta_pg
```

### Read path (GET latest)
```
meta_pg  = get_pg(derive_pg(bucket, key))
record   = meta_pg.get_object_meta(bucket, key)        // latest version
shard_pg = get_pg(derive_pg(bucket, key, record.version_id))
data     = read shards from shard_pg
```

### Read path (GET specific version)
```
meta_pg  = get_pg(derive_pg(bucket, key))
record   = meta_pg.get_object_version(bucket, key, vid) // specific version
shard_pg = get_pg(derive_pg(bucket, key, vid))
data     = read shards from shard_pg
```

For unversioned objects (version_id=0), both PGs are the same:
`derive_pg(bucket, key, 0) == derive_pg(bucket, key)` if we use
`rapidhash(bucket/key)` for version 0 and `rapidhash(bucket/key/version_id)` for
versioned. Or we can just accept that even for unversioned objects the shard PG
might differ from the metadata PG — it makes the code uniform.

**Decision**: Keep it uniform. Even unversioned writes use the split model. For
version_id=0, `derive_pg_shards(bucket, key, 0)` gives a deterministic PG that may
differ from `derive_pg(bucket, key)`. The code path is the same for all cases.

## Version ID Format: Sequential u64

Use a **per-key sequential u64 counter**. `0` = null version (unversioned).
Versioned writes start at 1 and increment.

Generation: `SELECT MAX(version_id) FROM objects WHERE bucket=? AND key=?` + 1,
executed on the metadata PG (which we're already querying).

- Fits existing 8-byte shard key field — no format change
- No collision risk (sequential, serialized through single writer)
- Chronologically ordered by construction
- For S3 API: formatted as decimal string, opaque to clients
- For distributed case: PG primary serializes writes, so sequential is safe

**Deviation from territory map**: The territory map says ULID (16 bytes). We use u64
instead because: ULID would require expanding the shard key from 25 to 33 bytes
(breaking on-disk format), and the "globally unique without coordination" property of
ULID is unnecessary since our architecture serializes writes through the PG primary.
The territory map should be updated.

## Versioning Semantics

### Bucket states (already in schema as `versioning INTEGER`)

| State | Value | PUT | DELETE (no versionId) | DELETE (with versionId) |
|---|---|---|---|---|
| Disabled | 0 | Overwrite null version | Physical delete | N/A |
| Enabled | 1 | Create new version | Insert delete marker | Permanent delete |
| Suspended | 2 | Overwrite null version | Insert delete marker | Permanent delete |

- Versioning is one-way: Disabled → Enabled → Suspended → Enabled. Cannot go back to Disabled.
- Pre-existing objects (before versioning enabled) have version_id=0 ("null version").

### Object status (already in schema as `status INTEGER`)

| Value | Meaning |
|---|---|
| 0 | Live |
| 1 | DeleteMarker (no shard data, size=0) |
| 2 | PendingDelete (future: background GC) |

### GET/HEAD behavior

- No versionId: find latest version (highest version_id). If it's a delete marker → 404
  with `x-amz-delete-marker: true` header. If live → return data.
- With versionId: find that specific version. Delete markers return 404 with header.

## What Does Not Change

- **Shard key format** (25 bytes): already includes 8-byte version_id field
- **ShardStore trait**: read/write/delete shards by ShardKey — unchanged
- **EC encoding, metadata blob, CRC64 integrity**: unchanged
- **object_key_hash**: still SHA-256(bucket/key)[0..16]

## Implementation Steps

### Step 1: PG derivation for shards

**`crates/server/src/pg.rs`**

Add a new function for shard PG derivation:

```rust
pub fn derive_pg_shards(bucket: &str, key: &str, version_id: u64, pg_count: u32) -> u32 {
    let full_key = format!("{}/{}/{}", bucket, key, version_id);
    let hash = rapidhash::rapidhash(full_key.as_bytes());
    (hash % pg_count as u64) as u32
}
```

Existing `derive_pg(bucket, key, pg_count)` stays as-is for metadata PG derivation.

### Step 2: Storage type changes

**`crates/storage/src/types.rs`**

- Change `ObjectRecord.version_id` from `String` to `u64`
- Change `PutObjectMetaReq.version_id` from `String` to `u64`
- Add `status: u8` to `PutObjectMetaReq` (for delete markers)
- Add `ListObjectVersionsReq` and `ListObjectVersionsResp` types

**`crates/storage/src/schema.rs`**

- Change `version_id` column from `TEXT` to `INTEGER` (no migration needed pre-production)
- Add index: `CREATE INDEX IF NOT EXISTS idx_objects_versions ON objects (bucket, key, version_id DESC)`

**`crates/storage/src/error.rs`**

- Add `MetadataError::InvalidVersioningTransition`

### Step 3: Storage trait expansion

**`crates/storage/src/traits.rs`**

Add to `PgMetadataStore`:
- `get_object_version(bucket, key, version_id) -> Result<ObjectRecord>` — specific version
- `delete_object_version(bucket, key, version_id) -> Result<()>` — permanent delete
- `list_object_versions(req) -> Result<ListObjectVersionsResp>` — all versions + delete markers

Change `get_object_meta` semantics: returns latest version (highest version_id).
If latest is a delete marker, still returns it (coordinator decides what to do).

Add to `GlobalService`:
- `put_bucket_versioning(name, state) -> Result<()>` — with transition validation

### Step 4: PgStore SQL implementation

**`crates/storage/src/pg_store.rs`**

- `put_object_meta`: If version_id=0, `INSERT OR REPLACE` (overwrite). If version_id>0,
  `INSERT` only (new version).
- `get_object_meta`: `SELECT ... WHERE bucket=? AND key=? ORDER BY version_id DESC LIMIT 1`
  (returns latest, whether live or delete marker)
- `get_object_version`: `SELECT ... WHERE bucket=? AND key=? AND version_id=?`
- `delete_object_version`: `DELETE FROM objects WHERE bucket=? AND key=? AND version_id=?`
- `list_objects`: Must now return only latest LIVE version per key (exclude keys where
  latest is a delete marker). SQL: filter where status=0 and version_id is max for that key.
- `list_object_versions`: All versions ordered by (key ASC, version_id DESC), including
  delete markers.

**`crates/storage/src/bucket_db.rs`**

- `put_bucket_versioning`: UPDATE versioning column, reject Enabled→Disabled transition.

**`crates/storage/src/memory_store.rs`**

- Update in-memory store to match new trait methods (for tests).

### Step 5: Coordinator versioning logic

**`crates/server/src/coordinator.rs`**

This is the largest change. Key modifications:

- **`write_object_inner`**: Accept `version_id: u64` parameter. Use `derive_pg_shards`
  for shard PG, `derive_pg` for metadata PG. Write shards to shard PG, metadata to meta PG.

- **`put_object`**: Check `bucket_info.versioning`. If Enabled, generate next version_id
  via MAX+1 query. If Disabled/Suspended, use version_id=0. For Enabled, do NOT delete
  old shards (old version persists). For Disabled/Suspended, delete old shards before
  writing (overwrite semantics).

- **`get_object` / `head_object`**: Accept optional `version_id: Option<u64>` parameter.
  Query metadata PG for latest or specific version. If latest is a delete marker, return
  appropriate error. Use `derive_pg_shards(bucket, key, record.version_id)` for shard reads
  (NOT hardcoded 0).

- **`delete_object`**: Accept optional `version_id: Option<u64>`.
  - Unversioned bucket: physical delete (current behavior).
  - Versioned + no versionId: insert delete marker (status=1, size=0, no shards).
  - Versioned + versionId: permanent delete of that version's shards + metadata.

- **`list_objects_v2`**: No major change — PgStore now returns only latest live versions.

- **`list_object_versions`** (currently reuses list_objects_v2): Replace with proper
  implementation that fans out to all PGs, collects all versions including delete markers,
  merge-sorts by (key ASC, version_id DESC), marks `is_latest` per key.

- **All 11 places** with `let version_id: u64 = 0`: Replace with actual version_id from
  ObjectRecord or generated value.

- **`copy_object`**: Support `?versionId=` on source. Destination follows its bucket's
  versioning rules.

- **Return types**: `PutObjectResult` and `DeleteObjectResult` need to include version_id
  string and delete_marker flag for response headers.

### Step 6: HTTP routing and dispatch

**`crates/server/src/http/router.rs`**

Add to S3Operation enum:
- `PutBucketVersioning { bucket }`
- `GetBucketVersioning { bucket }`

Route `PUT /<bucket>?versioning` and `GET /<bucket>?versioning`.

**`crates/server/src/http/mod.rs`**

- Dispatch new operations to coordinator
- Pass `versionId` query param to get_object, head_object, delete_object
- Update ListObjectVersions dispatch to use new coordinator method

**`crates/server/src/http/request.rs`**

- Update `parse_copy_source` to return optional versionId (currently stripped)

### Step 7: XML and response

**`crates/server/src/http/xml.rs`**

- `parse_versioning_config_xml`: Parse PutBucketVersioning request body
- `get_bucket_versioning_xml`: Generate GetBucketVersioning response
- Update `list_object_versions_xml`: Real version IDs, `<IsLatest>`, `<DeleteMarker>`
  elements separate from `<Version>` elements, pagination markers

**`crates/server/src/http/response.rs`**

- New: `get_bucket_versioning`, `put_bucket_versioning` response builders
- Add `x-amz-version-id` header to put_object, get_object, head_object, delete_object responses
- Add `x-amz-delete-marker: true` header when applicable

**`crates/server/src/error.rs`**

- Add `DeleteMarkerFound { version_id: u64 }` error variant (or extend ObjectNotFound)
  for proper `x-amz-delete-marker` header in 404 responses

### Step 8: Tests

**Storage layer:**
- put/get with version_id=0 (regression: current behavior unchanged)
- Put multiple versions, get_object_meta returns latest
- get_object_version retrieves specific version
- Delete marker as latest → get_object_meta returns it (status=1)
- list_objects skips keys hidden by delete markers
- list_object_versions returns all versions in correct order
- delete_object_version removes one version, others remain
- Bucket versioning state transitions (Disabled→Enabled ok, Enabled→Disabled rejected)

**Coordinator:**
- Versioned PUT creates new version, old preserved
- Unversioned PUT overwrites (regression)
- Suspended PUT overwrites null version, other versions intact
- GET latest when multiple versions exist
- GET with versionId returns specific version
- GET returns 404 with delete marker header when latest is delete marker
- DELETE on versioned bucket creates delete marker
- DELETE with versionId permanently deletes
- Shards go to derive_pg_shards PG, metadata to derive_pg PG

**HTTP/integration:**
- Route detection for ?versioning
- XML round-trip for versioning config
- x-amz-version-id in response headers
- Full workflow: enable versioning → PUT v1 → PUT v2 → GET (v2) → DELETE (marker) →
  GET (404) → GET ?versionId=1 (v1) → DELETE ?versionId=1 (permanent) → ListVersions

## Files Modified

| File | Change |
|---|---|
| `crates/server/src/pg.rs` | Add `derive_pg_shards()` |
| `crates/storage/src/types.rs` | version_id String→u64, add status to PutObjectMetaReq, new list types |
| `crates/storage/src/schema.rs` | version_id TEXT→INTEGER, add version index |
| `crates/storage/src/error.rs` | Add InvalidVersioningTransition |
| `crates/storage/src/traits.rs` | New trait methods for versioning |
| `crates/storage/src/pg_store.rs` | SQL implementation of all new methods |
| `crates/storage/src/memory_store.rs` | Update in-memory store |
| `crates/storage/src/bucket_db.rs` | put_bucket_versioning |
| `crates/server/src/coordinator.rs` | Split meta/shard PG routing, version-aware put/get/delete/list |
| `crates/server/src/error.rs` | DeleteMarkerFound variant |
| `crates/server/src/http/router.rs` | PutBucketVersioning, GetBucketVersioning routes |
| `crates/server/src/http/mod.rs` | Dispatch + pass versionId param |
| `crates/server/src/http/request.rs` | parse_copy_source returns versionId |
| `crates/server/src/http/response.rs` | Version headers, new response builders |
| `crates/server/src/http/xml.rs` | Versioning XML parse/generate |

## Verification

1. `cargo test --workspace` — all new + existing tests pass
2. `cargo clippy --workspace` — no new warnings
3. Manual test with AWS CLI:
   ```bash
   aws s3api put-bucket-versioning --bucket b --versioning-configuration Status=Enabled
   aws s3api get-bucket-versioning --bucket b
   aws s3 cp file.txt s3://b/key  # creates version 1
   aws s3 cp file2.txt s3://b/key  # creates version 2
   aws s3api list-object-versions --bucket b
   aws s3api get-object --bucket b --key key --version-id 1 out.txt
   aws s3api delete-object --bucket b --key key  # creates delete marker
   aws s3api delete-object --bucket b --key key --version-id 1  # permanent delete
   ```
