# Multipart Upload Core Design

## Context

`plans/s3-integration-tests.md` identifies multipart upload as the next large
foundational feature.

The current object write path assumes a single contiguous object payload
(metadata blob + user bytes) and a single shard set per object version. That
model does not scale to very large objects (up to tens of TB) and would force
expensive full-object rewrites at completion.

## Core Decision

Completed multipart objects are represented as a **composite manifest** of
independent parts. We do **not** assemble/rewrite the entire object into one
contiguous payload on `CompleteMultipartUpload`.

Implications:

1. Parts remain separate physical shard sets after completion.
2. Part shard sets can be placed on different PGs.
3. Complete is primarily a metadata transaction.

## Goals

1. Implement core lifecycle:
   - `CreateMultipartUpload`
   - `UploadPart`
   - `CompleteMultipartUpload`
   - `AbortMultipartUpload`
   - `ListParts`
   - `ListMultipartUploads`
2. Preserve correctness under concurrency and crash/restart.
3. Avoid full-object materialization and full-object rewrite during complete.
4. Keep existing non-multipart object paths working unchanged.

## Non-goals (core phase)

1. `UploadPartCopy`
2. Multipart checksum helper APIs (`x-amz-checksum-*` multipart variants)
3. SSE multipart variants
4. Lifecycle expiration of incomplete multipart uploads
5. Object lock multipart edge cases

## Data Model

### Objects table extension

Extend per-PG `objects` metadata with layout discriminator:

- `data_layout`:
  - `0 = InlineLegacy` (current single-payload model)
  - `1 = MultipartManifest`
- `parts_count` (nullable)
- `metadata_blob` (nullable BLOB for serialized user metadata headers)

Notes:

- Existing objects remain `InlineLegacy`.
- Multipart-completed objects are `MultipartManifest`.
- `size` remains user-visible object size (sum of part sizes).
- `total_size` remains meaningful for legacy objects; multipart path can set it
  equal to `size` for compatibility.

### In-progress multipart tables

```sql
CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id        TEXT PRIMARY KEY,
    bucket           TEXT NOT NULL,
    key              TEXT NOT NULL,
    initiated_at     INTEGER NOT NULL,
    state            INTEGER NOT NULL DEFAULT 0,   -- 0 InProgress, 1 Completing, 2 Aborting
    metadata_blob    BLOB NOT NULL,
    owner_principal  TEXT
);

CREATE INDEX IF NOT EXISTS idx_mpu_bucket_key
    ON multipart_uploads (bucket, key, initiated_at, upload_id);

CREATE TABLE IF NOT EXISTS multipart_parts (
    upload_id        TEXT NOT NULL,
    part_number      INTEGER NOT NULL,
    generation       INTEGER NOT NULL,
    size             INTEGER NOT NULL,
    etag             BLOB NOT NULL,
    etag_kind        INTEGER NOT NULL,
    part_okh         BLOB NOT NULL,                -- 16-byte object key hash used in shard keys
    part_vid         INTEGER NOT NULL,             -- per-part shard key version field
    ec_k             INTEGER NOT NULL,
    ec_m             INTEGER NOT NULL,
    last_modified    INTEGER NOT NULL,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
);
```

### Completed multipart manifest table

```sql
CREATE TABLE IF NOT EXISTS object_parts (
    bucket           TEXT NOT NULL,
    key              TEXT NOT NULL,
    version_id       INTEGER NOT NULL,
    part_number      INTEGER NOT NULL,
    size             INTEGER NOT NULL,
    etag             BLOB NOT NULL,
    etag_kind        INTEGER NOT NULL,
    part_okh         BLOB NOT NULL,
    part_vid         INTEGER NOT NULL,
    ec_k             INTEGER NOT NULL,
    ec_m             INTEGER NOT NULL,
    PRIMARY KEY (bucket, key, version_id, part_number)
);

CREATE INDEX IF NOT EXISTS idx_object_parts_lookup
    ON object_parts (bucket, key, version_id, part_number);
```

Rationale:

- `multipart_parts` tracks mutable in-progress state.
- `object_parts` is immutable committed manifest for reads.
- No data copy needed at complete; metadata rows are copied/committed.

## Placement and Part Identity

Each uploaded part becomes an independent shard set.

Part identity fields:

- `upload_id`
- `part_number`
- `generation` (increments on part overwrite/re-upload)

Deterministic derivations:

- `part_okh = hash16("mpu/" + upload_id + "/" + part_number + "/" + generation)`
- `part_vid = generation` (or another deterministic u64)

Shard keys continue using existing format:

- `ShardKey(part_okh, part_vid, shard_index)`

This reuses current shard CRC verification and avoids shard key format changes.

## API and Coordinator Design

Add coordinator methods:

- `create_multipart_upload`
- `upload_part`
- `complete_multipart_upload`
- `abort_multipart_upload`
- `list_parts`
- `list_multipart_uploads`

### CreateMultipartUpload

1. Validate bucket/auth.
2. Lock metadata PG for `(bucket,key)`.
3. Insert `multipart_uploads` row with `InProgress` and stored metadata blob.
4. Return `upload_id`.

### UploadPart

1. Validate `partNumber` in `[1,10000]`.
2. Resolve upload row and require `InProgress`.
3. Determine next `generation` for this part number.
4. EC-encode uploaded part bytes and write shard set using part identity.
5. Upsert `multipart_parts` row atomically to new generation.
6. Best-effort delete prior generation shard set for that part number.
7. Return part ETag.

### CompleteMultipartUpload

1. Parse XML part list; enforce strictly increasing part numbers.
2. Lock metadata PG and transition upload `InProgress -> Completing`.
3. Validate each listed part exists and ETag matches.
4. Enforce S3 part-size rule (`>= 5 MiB` for all non-final parts).
5. Allocate destination object `version_id` using existing versioning rules.
6. Insert/replace final `objects` row with `data_layout = MultipartManifest`.
7. Materialize committed manifest rows into `object_parts`.
8. Delete `multipart_uploads` + `multipart_parts` rows.
9. Return completion response.

Important:

- No full-byte concatenation.
- No final-object EC rewrite.

### AbortMultipartUpload

1. Resolve upload row; if missing return `NoSuchUpload`.
2. Transition to `Aborting`.
3. Delete all shard sets referenced by `multipart_parts` rows.
4. Delete `multipart_uploads` + `multipart_parts` rows.
5. Return success (idempotent cleanup behavior where possible).

### List APIs

- `ListParts`: page through `multipart_parts` by `part_number`.
- `ListMultipartUploads`: page through `multipart_uploads` by `(key, upload_id)`.

## Read Path for Multipart Objects

`get_object`, `head_object`, `get_object_range`, and copy-source reads must branch by
`data_layout`.

### Legacy objects (`InlineLegacy`)

Keep current behavior.

### Multipart objects (`MultipartManifest`)

1. Read object metadata row and manifest rows from `object_parts`.
2. Resolve requested byte range to subset of parts.
3. For each needed part, read/reconstruct bytes from that part's shard set.
4. Concatenate only the requested slices.
5. Verify integrity:
   - per-shard CRC already enforced by `read_shard`
   - optionally verify reconstructed part CRC against stored part ETag

For full GET/copy-source-full, verify composite checksum/etag semantics defined below.

## ETag and Checksum Semantics

Part ETag:

- Keep current CRC64 byte-based representation for part uploads.

Completed object ETag:

- Use composite multipart ETag scheme already anticipated in design notes:
  checksum derived from ordered part checksums plus `-part_count` suffix in HTTP
  representation.

Introduce explicit `etag_kind` value for multipart-composite CRC64.

## HTTP/Router/XML Work

Add router operations before catch-all object routes:

- `POST /bucket/key?uploads` -> initiate
- `PUT /bucket/key?partNumber=N&uploadId=...` -> upload part
- `POST /bucket/key?uploadId=...` -> complete
- `DELETE /bucket/key?uploadId=...` -> abort
- `GET /bucket?uploads` -> list multipart uploads
- `GET /bucket/key?uploadId=...` -> list parts

Add XML parse/serialize helpers:

- Complete request body parser
- Initiate/complete/list-parts/list-uploads responses

Add multipart-specific errors:

- `NoSuchUpload`
- `InvalidPart`
- `InvalidPartOrder`
- `EntityTooSmall`

## Concurrency Invariants

1. Multipart control-plane state lives in one metadata PG per object key.
2. Upload state transitions are monotonic:
   - `InProgress -> Completing`
   - `InProgress -> Aborting`
3. `UploadPart` rejected once not `InProgress`.
4. Part overwrite is atomic at metadata level (new generation becomes visible only
   after full shard write succeeds).
5. Complete never exposes partial manifests.

## Crash/Recovery Behavior

On startup:

1. Existing temp-file cleanup remains.
2. Multipart sweeper scans uploads stuck in `Completing/Aborting` and re-drives
   cleanup to convergence.
3. If crash occurs after object manifest commit but before in-progress row cleanup,
   completed object remains valid; sweeper can finalize cleanup of MPU rows.

## Implementation Phases

### Phase A: Storage schema + APIs

Files:

- `crates/storage/src/schema.rs`
- `crates/storage/src/types.rs`
- `crates/storage/src/traits.rs`
- `crates/storage/src/pg_store.rs`

Output:

- multipart tables and committed manifest table
- CRUD/list methods for uploads/parts/manifests

### Phase B: Coordinator lifecycle

Files:

- `crates/server/src/coordinator.rs`
- `crates/server/src/error.rs`

Output:

- create/upload/complete/abort/list core logic
- generation-safe part overwrite logic

### Phase C: Read-path multipart support

Files:

- `crates/server/src/coordinator.rs`

Output:

- multipart-aware get/head/range/copy-source behavior

### Phase D: HTTP, router, XML

Files:

- `crates/server/src/http/router.rs`
- `crates/server/src/http/mod.rs`
- `crates/server/src/http/xml.rs`
- `crates/server/src/http/response.rs`

### Phase E: Integration tests

Files:

- add `crates/s3-tests/tests/multipart.rs`
- unignore multipart-dependent tests incrementally

Order:

1. core lifecycle tests
2. resend/overwrite part tests
3. size/etag validation tests
4. atomic multipart write test
5. versioning multipart basic tests

## Step-by-Step Execution Plan

Each step is intended to be one reviewable PR/commit slice.

### Step 1: Storage schema migration (no behavior change)

Files:

- `crates/storage/src/schema.rs`

Tasks:

1. Add `objects.data_layout`, `objects.parts_count`, `objects.metadata_blob`.
2. Add `multipart_uploads`, `multipart_parts`, and `object_parts` tables.
3. Add indexes for lookup/list paths.

Acceptance:

1. Existing tests still pass unchanged.
2. New schema is idempotent (`CREATE TABLE/INDEX IF NOT EXISTS`).

### Step 2: Storage types and trait contracts

Files:

- `crates/storage/src/types.rs`
- `crates/storage/src/traits.rs`
- `crates/storage/src/error.rs` (if new metadata errors are needed)

Tasks:

1. Add multipart type structs:
   - upload row info
   - part row info
   - list request/response structs
2. Extend `PgMetadataStore` with multipart CRUD/list methods.
3. Add explicit multipart metadata errors (e.g. `NoSuchUpload`, invalid part state).

Acceptance:

1. Compiles with method stubs in implementations.
2. Error mapping remains explicit and testable.

### Step 3: PgStore multipart metadata implementation

Files:

- `crates/storage/src/pg_store.rs`

Tasks:

1. Implement multipart table CRUD/list methods.
2. Implement atomic upsert of part rows by `(upload_id, part_number)`.
3. Implement transition-safe state updates for upload rows.
4. Implement manifest row commit for completed objects (`object_parts`).

Acceptance:

1. New storage unit tests for:
   - create/get/delete upload
   - upsert/overwrite part
   - list parts pagination
   - list uploads pagination
   - manifest insert/read/delete

### Step 3b: PgStore multipart metadata hardening

Files:

- `crates/storage/src/pg_store.rs`
- `crates/storage/src/tests/metadata_tests.rs`

Tasks:

1. Add deterministic rollback handling tests around transaction failures (including commit failure paths).
2. Add concurrency stress tests for multipart metadata operations.

Acceptance:

1. Deterministic commit-failure rollback leaves connection usable and state consistent.
2. Concurrent upserts/state transitions preserve invariants and explicit error mapping.

### Step 4: Coordinator data-layout plumbing

Files:

- `crates/server/src/coordinator.rs`

Tasks:

1. Add internal object data layout enum/constants.
2. Update existing single-part `put_object` metadata writes to set
   `data_layout=InlineLegacy`.
3. Keep existing read/write behavior unchanged for legacy objects.

Acceptance:

1. No external behavior change.
2. Existing coordinator tests pass.

### Step 5: CreateMultipartUpload and ListMultipartUploads

Files:

- `crates/server/src/coordinator.rs`

Tasks:

1. Implement `create_multipart_upload`.
2. Implement `list_multipart_uploads`.
3. Ensure metadata PG lock usage follows current lock invariants.

Acceptance:

1. Coordinator unit tests for initiate + list ordering/pagination.

### Step 6: UploadPart (core write path for parts)

Files:

- `crates/server/src/coordinator.rs`

Tasks:

1. Implement part-number validation.
2. Implement generation increment and deterministic part identity derivation.
3. EC-encode uploaded part bytes and write part shard set.
4. Upsert `multipart_parts` row and cleanup old generation best-effort.

Acceptance:

1. Coordinator tests for:
   - first upload
   - re-upload same part number
   - concurrent uploads for same part number
   - invalid part number bounds

### Step 7: CompleteMultipartUpload (manifest commit)

Files:

- `crates/server/src/coordinator.rs`
- `crates/server/src/error.rs` (multipart-specific errors)

Tasks:

1. Parse/validate ordered part list input (from coordinator API boundary).
2. Transition upload state to `Completing`.
3. Validate all parts present and ETags match.
4. Validate part-size constraints (all but last >= 5 MiB).
5. Allocate final object `version_id` using existing rules.
6. Write final object metadata row with `MultipartManifest`.
7. Commit immutable rows into `object_parts`.
8. Delete in-progress rows.

Acceptance:

1. Coordinator tests for:
   - happy path complete
   - missing part
   - wrong ETag
   - invalid order
   - too-small non-final part
   - retry semantics

### Step 7b: Multipart fault-injection test strategy (deferred)

Files:

- `plans/fault-injection.md` (design and test methodology)
- `crates/storage/src/test_util/faults.rs` (or successor fault hooks)
- multipart coordinator/storage tests

Tasks:

1. Define deterministic failure points for multipart write paths:
   - `UploadPart`: fail after shard write set but before metadata upsert.
   - `CompleteMultipartUpload`: fail after each stage (`set_upload_state`,
     `put_object_meta`, `commit_object_parts`) to exercise partial-progress recovery.
2. Add test harness controls so failures can be injected by operation/phase
   without timing-based races.
3. Add end-to-end tests that assert post-failure invariants:
   - no leaked visible metadata state
   - no leaked current-generation part shards on failed upsert
   - retry behavior converges to a valid completed or aborted state
4. Add storage-level rollback tests for `complete_multipart_commit` so failures
   during finalize (object row write, manifest insert, upload-row delete, and
   commit-time failures) prove transaction atomicity.
5. Document expected recovery semantics for each injected failure point.

Acceptance:

1. Deterministic tests reproduce and validate cleanup/retry behavior for the
   two known risk areas above.
2. `complete_multipart_commit` failure-path tests prove no partial finalize
   state is committed.
3. Test docs clearly map each fault point to expected system state.

### Step 8: AbortMultipartUpload and ListParts

Files:

- `crates/server/src/coordinator.rs`

Tasks:

1. Implement abort state transition + shard cleanup for all staged parts.
2. Implement list parts pagination/markers.
3. Enforce no uploads after completing/aborting states.

Acceptance:

1. Coordinator tests for:
   - abort success
   - abort missing upload
   - abort idempotence behavior
   - list parts correctness

### Step 9: Multipart-aware object reads

Files:

- `crates/server/src/coordinator.rs`

Tasks:

1. Branch `get_object`, `head_object`, `get_object_range`, copy-source full read
   by `data_layout`.
2. Implement part-manifest traversal and byte-range-to-parts mapping.
3. Preserve legacy path unchanged.

Acceptance:

1. Coordinator tests for:
   - full GET of multipart object
   - range GET spanning part boundaries
   - HEAD of multipart object
   - copy-source read of multipart object

### Step 10: Router operations and dispatch wiring

Files:

- `crates/server/src/http/router.rs`
- `crates/server/src/http/mod.rs`

Tasks:

1. Add multipart operations to router enum.
2. Add query-param precedence rules so multipart routes match before catch-all
   object operations.
3. Add handler dispatch to new coordinator methods.

Acceptance:

1. Router unit tests for all multipart route patterns.
2. HTTP handler unit tests for method/query validation.

### Step 11: XML parsing and response rendering

Files:

- `crates/server/src/http/xml.rs`
- `crates/server/src/http/response.rs`

Tasks:

1. Add complete request XML parser.
2. Add initiate/complete/list-parts/list-uploads XML responses.
3. Add multipart-specific error XML coverage.

Acceptance:

1. XML unit tests for request/response bodies and edge cases.

### Step 12: Core integration tests in `s3-tests`

Files:

- `crates/s3-tests/tests/multipart.rs` (new)
- existing ignored tests in:
  - `atomic.rs`
  - `versioning.rs`
  - `copy_object.rs`
  - `checksums.rs`
  - `object_attributes.rs`

Tasks:

1. Port/enable core multipart lifecycle tests first.
2. Unignore only tests supported by current scope.
3. Keep out-of-scope tests ignored with explicit reasons.

Acceptance:

1. `cargo test -p s3-tests --test multipart` passes.
2. `cargo test -p s3-tests` passes with expected ignored set.

## Suggested Commit Plan

1. `storage: add multipart schema primitives`
2. `storage: add multipart metadata APIs and pg_store impl`
3. `server: add coordinator multipart create/upload/list primitives`
4. `server: add coordinator complete/abort multipart`
5. `server: add multipart manifest read path`
6. `server: wire multipart router/http/xml`
7. `s3-tests: add core multipart integration tests`

## Verification

1. `cargo fmt`
2. `cargo clippy --workspace -- -D warnings`
3. `cargo test -p storage`
4. `cargo test -p server --lib`
5. `cargo test -p s3-tests --test multipart` (once added)
6. `cargo test -p s3-tests`

## Follow-on

1. `UploadPartCopy`
2. Multipart checksum helper compatibility
3. `GetObjectAttributes` `ObjectParts` support
4. SSE multipart variants
5. Lifecycle `AbortIncompleteMultipartUpload`
