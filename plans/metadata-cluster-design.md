# Metadata Cluster — Design Document

## Scope

This document covers the design of the metadata cluster (subsystem 4 in the build
sequence). The metadata cluster is responsible for the bucket/object namespace: it
tracks which objects exist, their versions, and enough information to route requests
to the right storage nodes. It is the authoritative index for the system.

The metadata cluster does NOT store:
- Shard data (that's the storage nodes)
- User-defined metadata / content-type / S3 headers (those are stored in the shard
  data as a prepended metadata blob — see storage-node-design.md, Architecture C2)
- Raw checksums of shard data (those are per-storage-node)

It stores the minimum needed for namespace operations (ListObjects, routing) and
object-level bookkeeping (versioning, lifecycle).

---

## Relationship to Storage Node Design

The storage node design doc (storage-node-design.md) establishes several decisions
that constrain the metadata cluster:

- **Architecture C (preferred)**: The metadata cluster is the primary source for
  namespace operations. Object metadata (content-type, x-amz-meta-*) is also embedded
  in shard data for disaster recovery. The metadata cluster does NOT store user-defined
  metadata.
- **Shards are fully immutable (write-once)**: CopyObject-to-self with metadata
  changes creates entirely new shards. The metadata cluster updates its record to point
  to the new shards.
- **Shard identity**: The metadata cluster must track the mapping from
  (bucket, key, version) → shard placement information.
- **HeadObject for user metadata**: Served from storage nodes (reading the metadata
  prefix from shard data), NOT from the metadata cluster. The metadata cluster provides
  routing information (which nodes hold the shards) so the frontend can fetch the
  metadata from the right place.

---

## Role in the Stack

```
HTTP Frontend
     │
     ├─── ListObjects, HeadBucket ─────► Metadata Cluster (direct response)
     │
     ├─── PutObject ──► Metadata Cluster (namespace entry)
     │                  + Storage Nodes (shard data)
     │
     ├─── GetObject ──► Metadata Cluster (routing info)
     │                  + Storage Nodes (shard data, includes user metadata)
     │
     ├─── HeadObject ─► Metadata Cluster (size, etag, last-modified)
     │                  + Storage Node shard 0 (user metadata, content-type)
     │
     └─── DeleteObject ► Metadata Cluster (remove namespace entry)
                         + Storage Nodes (delete shards, async)
```

The metadata cluster is on the critical path for every S3 operation except scrub and
repair. Its latency and availability directly determine system performance.

---

## What the Metadata Cluster Stores

### Per-bucket record

```
bucket_name (primary key, string, max 63 chars)
  - owner_id (u64 or access key reference)
  - created_at (u64, unix millis)
  - versioning_status (Disabled | Enabled | Suspended)
  - region (u16, maps to a region name via config — fixed enum, not a string)
```

Buckets are a flat namespace — no nesting. Bucket names are globally unique.

### Per-object record

This is the core record. One record per (bucket, key, version).

```
(bucket, key, version_id) → ObjectRecord
  - size (u64, original object size in bytes — NOT including prepended metadata)
  - etag (fixed-size binary, max 64 bytes — see etag format below)
  - last_modified (u64, unix millis)
  - storage_class (u8, initially just STANDARD)
  - ec_k (u8)
  - ec_m (u8)
  - shard_placement_key (bytes, the key used for placement — may be object key hash,
    PG ID, or composite)
  - status (u8: Live, DeleteMarker, PendingDelete)
```

### What is NOT stored here

- **content-type** — in shard data (metadata prefix)
- **x-amz-meta-*** — in shard data (metadata prefix)
- **content-encoding, content-disposition, cache-control, expires** — in shard data
- **per-shard checksums** — in each storage node's local metadata DB
- **shard data** — on storage nodes

The metadata cluster record is intentionally small and fixed-size. Variable-length
user metadata (up to 2KB) does not flow through Raft consensus. This keeps write
transactions fast and replication costs predictable.

### Open question: Placement generation and the rebalance problem

Since the placement layer is deterministic (given a key and cluster map, any node
can compute shard locations), the metadata cluster may not need to store explicit
shard locations. But this breaks down during topology changes.

**The problem**: When the cluster map changes (node added, removed, reweighted),
there's a gap between "where the new placement says shards should be" and "where
shards actually are" (still at old locations until the repair subsystem migrates them).

Example: Object placed under cluster map V1 → shards on nodes [A, B, C, D, E, F].
Map changes to V2 (node D removed, replaced by G). V2 placement says shards are on
[A, B, C, G, E, F]. But shard 3 is still on D (or missing, if D failed) and hasn't
been migrated to G yet. A GetObject using V2 placement goes to G and finds nothing.

#### Approach 1: Store placement generation per object

Each object metadata record includes the cluster map version (generation) it was
placed with. Reads use that version's cluster map to compute placement. The system
keeps a history of recent cluster maps.

After the repair subsystem migrates a shard to its new location, it updates the
object's generation in the metadata cluster.

- Pro: Reads always go to the correct location.
- Pro: Simple, explicit — no guessing or fallback.
- Con: **Requires updating metadata for every affected object during rebalance.**
  A single node addition can affect O(shards/N) objects — potentially millions of
  metadata writes through Raft consensus. This is expensive and slow.
- Con: Must keep old cluster maps around (though they're small).

#### Approach 2: Placement groups (PGs)

Instead of per-object placement, hash objects into a fixed number of placement groups
(e.g. 1024-8192 PGs). Each PG maps to a set of nodes. The metadata cluster stores
which PG an object belongs to (just `pg_id = hash(key) % pg_count`). Migration status
is tracked per PG, not per object.

```
Object → PG (via hash) → nodes (via placement)
```

On topology change:
- Compute new PG → node mapping.
- For each PG whose node set changed, migrate shards.
- Track migration status per PG: {pending, migrating, complete}.
- Reads for a PG in "migrating" state try both old and new locations.
- Once all shards for a PG are migrated, mark it complete.

- Pro: **Only ~1000-8000 PGs to track**, not millions of objects. Migration status
  is manageable.
- Pro: This is the proven approach (Ceph, virtually all distributed storage).
- Pro: Rebalance is bounded: you know exactly how many PGs are affected and can
  track progress.
- Con: Adds an indirection layer. One more concept to understand.
- Con: PG count must be chosen at cluster creation time (or requires a split/merge
  mechanism later). Too few PGs → uneven distribution. Too many PGs → more state to
  track.
- Con: PG-level granularity means all objects in a PG move together, even if only
  some are affected by the topology change (minor — the placement is deterministic
  so this is correct).

#### Approach 3: Try old and new placement (no generation stored)

Compute placement from both the current and previous cluster map. Try current first,
fall back to previous.

- Pro: No per-object metadata change needed.
- Pro: Simple to implement for single topology changes.
- Con: "Previous" only handles one transition. Multiple successive topology changes
  (e.g. two nodes fail in sequence) require tracking N old maps and trying N
  fallback placements — O(N × k) reads in the worst case.
- Con: No clear termination — how does the system know when all shards have been
  migrated and the old map can be discarded?

#### Approach 4: Two-phase cluster map

Don't activate a new cluster map until all affected shards have been migrated. The
old map stays active during the entire migration. Reads always use the current
(old) map and are always correct.

- Pro: Simplest read path — no fallback, no generation.
- Pro: No per-object metadata update needed.
- Con: Topology changes are slow. Adding a node means waiting for all affected shards
  to migrate before the node is "live" in the placement. This could take hours or days
  for large clusters.
- Con: A failed node stays in the active map during migration, meaning the system
  operates in degraded mode until migration completes. (But repair can still happen —
  reconstruct missing shards to new locations under the old map, then switch.)
- Con: Multiple concurrent topology changes are complex (must serialize them).

#### Discussion

This question is closely related to whether we use placement groups. Without PGs:
- Per-object generation (Approach 1) requires millions of metadata updates on
  rebalance — doesn't scale.
- Try-both (Approach 3) gets messy with successive topology changes.
- Two-phase (Approach 4) makes topology changes very slow.

**With PGs (Approach 2)**, migration state is tracked per PG (~1000-8000 entries).
This is the standard approach for a reason — it bounds the management overhead
regardless of object count.

**Open question**: Do we adopt PGs? The placement crate already supports them as a
caller concern (`placer.place(&pg_id.to_le_bytes(), ...)`). The question is whether
the metadata cluster and repair subsystem are designed around PG-level tracking.

If PGs are adopted:
- The metadata record doesn't need a placement generation. It stores `placement_key`
  (which may be the PG ID or the raw object key).
- A separate PG state table tracks migration status per PG.
- The cluster map is versioned. Each PG knows which cluster map version its current
  placement was computed from.

If PGs are not adopted:
- We likely need Approach 4 (two-phase map) for simplicity, accepting that topology
  changes are slow. Or Approach 1 (per-object generation), accepting that rebalance
  writes many metadata records.

**Recommendation**: Adopt placement groups. The per-PG migration tracking is far
more manageable than per-object metadata updates or multi-fallback read paths. PG
count should be configurable at cluster creation time, with a sensible default (e.g.
1024 for small clusters, 4096+ for larger ones).

This means the metadata per-object record does NOT need a placement generation. The
per-object record stores the `placement_key` (from which the PG is derived). PG
migration state is tracked in a separate, small table.

---

## Consistency Model

The territory map says "Strong preferred." For an S3-compatible system, this means:

- **Read-after-write consistency**: A successful PutObject is immediately visible to
  subsequent GetObject and ListObjects calls. AWS S3 provides this as of December 2020.
- **Read-after-delete consistency**: A successful DeleteObject is immediately reflected.
- **List consistency**: ListObjects reflects all completed writes and deletes.

### Implications

Strong consistency requires that all metadata operations go through a single
serialized log (Raft) or equivalent. Eventual consistency would allow stale reads
but would violate S3's current consistency guarantees.

This means:
- All writes (Put, Delete) go through Raft leader.
- Reads (Get, Head, List) can be served from:
  - **Leader only**: Simplest. Consistent. But leader becomes a bottleneck.
  - **Any replica with linearizable read (ReadIndex)**: Raft ReadIndex protocol
    confirms the leader is still the leader, then serves from local state. Lower
    latency, distributed read load. One extra round-trip to leader per read.
  - **Lease-based reads**: Leader grants time-based leases; replicas serve reads
    within the lease. Fastest, but depends on clock synchronization.

**Recommendation**: Start with leader reads (simplest). Move to ReadIndex when read
throughput becomes a bottleneck. Lease-based reads are an optimization for later.

---

## Consensus and Replication

### Raft-replicated SQLite

The territory map suggests "Raft-replicated SQLite (Litestream-style)." This is the
starting point.

The idea: run SQLite as the metadata store on each Raft replica. The Raft log entries
are SQL transactions (or deterministic state machine commands). When a log entry is
committed, every replica applies the same transaction to its local SQLite instance.

**This is not Litestream.** Litestream replicates the WAL to S3 for backup — it's
single-node, not multi-node consensus. What we want is closer to:
- **rqlite**: Raft consensus with SQLite as the state machine. Proven, simple.
- **dqlite**: Canonical's Raft + SQLite (C library). Less mature.
- **LiteFS**: Fly.io's approach — single writer, replicated reads. Not true Raft.

### Implementation approaches

#### Approach 1: Application-level state machine over SQLite

The Raft log contains application-level commands (not raw SQL):

```
enum MetadataCommand {
    CreateBucket { name, owner, ... },
    PutObjectMeta { bucket, key, version, size, etag, ... },
    DeleteObjectMeta { bucket, key, version },
    SetBucketVersioning { bucket, status },
    ...
}
```

Each replica applies commands to its local SQLite database. The commands are
deterministic — same command on same state produces same result.

- Pro: Commands are compact (no SQL overhead in the Raft log).
- Pro: Full control over what's replicated.
- Pro: SQLite schema can change without changing the Raft protocol (commands are
  abstract).
- Pro: Can validate commands before proposing to Raft.
- Con: Must implement a command for every operation. More code.
- Con: Must ensure command application is exactly deterministic (no floating-point,
  no timestamps derived from wall clock during apply — use leader's timestamp in the
  command).

#### Approach 2: Replicate SQLite WAL

Replicate the SQLite WAL (Write-Ahead Log) bytes through Raft. Each Raft log entry
is a WAL frame. Replicas apply WAL frames to their SQLite database.

- Pro: SQLite handles all the complex query logic.
- Pro: Can use arbitrary SQL for reads without defining commands.
- Con: WAL frames are opaque binary — hard to debug, version, or validate.
- Con: SQLite WAL format may change between versions (unlikely but possible).
- Con: Must ensure all replicas run the same SQLite version with the same compile
  options.
- Con: WAL frames can be large (contain full page images).

#### Approach 3: Use an existing Raft + SQLite implementation

Embed or depend on rqlite (Go), dqlite (C), or a Rust equivalent.

- Pro: Battle-tested Raft + SQLite integration.
- Con: rqlite is Go (language mismatch). dqlite is C with a less mature Rust wrapper.
- Con: Dependency on external project's design decisions and limitations.
- Con: May not support our exact needs (e.g., custom snapshot strategy, fine-grained
  control over replication).

**Recommendation**: Approach 1 (application-level state machine). It gives the most
control, keeps the Raft log entries small, and avoids coupling to SQLite's internal
WAL format. Use a Rust Raft library (e.g., `openraft`) for the consensus layer and
SQLite for local storage on each replica.

### Open question: Raft library

Rust Raft libraries:
- **openraft**: Active development, async, well-documented. Used by Databend.
- **raft-rs** (tikv/raft-rs): Port of etcd's Raft. Lower-level, more manual. Used by
  TiKV.
- **Custom**: Write our own. Maximum control, significant effort.

**Leaning**: `openraft` — active, async, Rust-native. But needs evaluation for our
needs (especially snapshot and membership change support).

---

## Schema Design

### SQLite schema (per replica)

```sql
-- Bucket table
CREATE TABLE buckets (
    name          TEXT PRIMARY KEY,
    owner_id      INTEGER NOT NULL,
    created_at    INTEGER NOT NULL,  -- unix millis
    versioning    INTEGER NOT NULL DEFAULT 0  -- 0=Disabled, 1=Enabled, 2=Suspended
);

-- Object table: one row per (bucket, key, version)
-- For unversioned buckets, version_id is a fixed sentinel (e.g. "null")
CREATE TABLE objects (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    version_id    TEXT NOT NULL,
    size          INTEGER NOT NULL,
    etag          BLOB NOT NULL,     -- fixed-size, max 64 bytes (512 bits)
    etag_kind     INTEGER NOT NULL,  -- 0=CRC64-NVME, 1=MD5, 2=SHA256, ...
    last_modified INTEGER NOT NULL,  -- unix millis
    storage_class INTEGER NOT NULL DEFAULT 0,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    placement_key BLOB NOT NULL,
    status        INTEGER NOT NULL DEFAULT 0,  -- 0=Live, 1=DeleteMarker, 2=PendingDelete
    PRIMARY KEY (bucket, key, version_id)
);

-- Index for ListObjectsV2: prefix scan by (bucket, key)
CREATE INDEX idx_objects_list ON objects (bucket, key);

-- Index for ListObjectVersions: all versions of an object
CREATE INDEX idx_objects_versions ON objects (bucket, key, last_modified DESC);

-- Multipart uploads (in-progress)
CREATE TABLE multipart_uploads (
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    upload_id     TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    ec_k          INTEGER NOT NULL,
    ec_m          INTEGER NOT NULL,
    PRIMARY KEY (upload_id)
);

CREATE INDEX idx_multipart_bucket ON multipart_uploads (bucket, key);

-- Multipart parts (completed parts for an in-progress upload)
CREATE TABLE multipart_parts (
    upload_id     TEXT NOT NULL,
    part_number   INTEGER NOT NULL,
    size          INTEGER NOT NULL,
    etag          BLOB NOT NULL,
    etag_kind     INTEGER NOT NULL,
    placement_key BLOB NOT NULL,
    PRIMARY KEY (upload_id, part_number)
);
```

### Notes on the schema

- **version_id**: For unversioned buckets, all objects use a sentinel version ID
  (e.g. the empty string or "null"). A PutObject overwrites the existing record.
  For versioned buckets, each PutObject creates a new record with a unique version_id.

- **etag**: Stored as fixed-size binary (BLOB), max 64 bytes (512 bits). An
  `etag_kind` discriminator (u8 stored as INTEGER) identifies the hash algorithm:
  CRC64-NVME (8 bytes), MD5 (16 bytes), SHA-256 (32 bytes), etc. The S3 API returns
  etags as hex strings in quotes (e.g. `"d41d8cd98f00b204e9800998ecf8427e"`), so the
  frontend hex-encodes the binary on output. This keeps the metadata record fixed-size
  and avoids storing hex-encoded strings that are 2× the binary size.

  For multipart uploads, S3's etag format is `MD5_of_part_MD5s-partcount`. We can
  either replicate this convention or use our own (e.g. CRC64-NVME of the combined
  stream). The etag_kind field distinguishes these cases.

  512 bits (64 bytes) is sufficient for any current or foreseeable hash, including
  SHA-512. If we default to CRC64-NVME for internal etags, that's only 8 bytes per
  object — very compact.

- **placement_key**: The key used for rendezvous hashing placement. This may be a hash
  of the object key, or a PG ID, or a composite. The placement layer computes shard
  locations from this key + the cluster map. Not interpreted by the metadata cluster.

- **status**: Live objects are returned by ListObjects. DeleteMarkers are only visible
  in ListObjectVersions. PendingDelete objects are being garbage collected (shards not
  yet fully deleted).

- **Multipart tables**: Track in-progress multipart uploads. On
  CompleteMultipartUpload, the parts are assembled into a final object record and the
  multipart records are deleted. On AbortMultipartUpload, all records and staged shards
  are deleted.

---

## Operations

### PutObject

```
1. Frontend receives object data + headers
2. Frontend prepends user metadata to object data (metadata blob)
3. Frontend EC-encodes the combined stream into k+m shards
4. Placement layer determines shard → node assignments
5. Frontend writes shards to storage nodes (parallel)
6. Once sufficient shards are durably written (quorum or all):
   7. Frontend proposes PutObjectMeta command to Raft leader
   8. Raft commits the command
   9. Leader applies: INSERT or UPDATE objects record
   10. Return success to client
```

**Write ordering**: Shard data is written BEFORE the metadata record. If the frontend
crashes between step 5 and step 7, orphan shards exist on storage nodes but the
metadata cluster has no record. These orphans are cleaned up by the repair subsystem
(see Garbage Collection below). This is safer than metadata-first, which would leave
a metadata record pointing to missing shards.

**Atomicity**: The PutObject is not atomic across the metadata cluster and storage
nodes. There is a window between shard writes and metadata commit where:
- Shards exist but are not reachable (no metadata record yet).
- A crash loses the metadata write — orphan shards need cleanup.

For versioned buckets, the new version becomes visible atomically when the metadata
record is committed. The old version remains accessible until explicitly deleted.

For unversioned buckets, the PUT replaces the previous record. The old shards become
orphans and need garbage collection.

### GetObject

```
1. Frontend queries metadata cluster: SELECT ... WHERE bucket=? AND key=?
   (for latest live version)
2. Metadata cluster returns: size, etag, ec_k, ec_m, placement_key, version_id
3. Frontend computes shard locations from placement_key + cluster map
4. Frontend reads k shards from storage nodes (parallel)
5. Frontend reconstructs the original byte stream
6. Frontend strips the metadata prefix, returns object data + headers to client
```

For range requests, the frontend computes which shard bytes correspond to the
requested byte range (accounting for the metadata prefix offset) and reads only the
necessary portions.

### HeadObject

```
1. Frontend queries metadata cluster: SELECT size, etag, last_modified, ...
   WHERE bucket=? AND key=? (latest live version)
2. Metadata cluster returns the fixed fields (size, etag, last-modified, etc.)
3. For user-defined metadata (content-type, x-amz-meta-*):
   a. Frontend computes shard 0 location from placement_key + cluster map
   b. Reads metadata prefix from shard 0 (or reconstructs from k shards if
      shard 0 is unavailable or object is very small)
   c. Parses user metadata from the prefix
4. Returns combined response (metadata cluster fields + shard metadata fields)
```

HeadObject is two round-trips: one to the metadata cluster (fast), one to a storage
node (small read). For large objects, step 3 is a partial read of shard 0. For small
objects, it's a full k-shard reconstruction (but the shards are tiny).

### ListObjectsV2

```
1. Frontend queries metadata cluster:
   SELECT key, size, etag, last_modified, storage_class
   FROM objects
   WHERE bucket = ?
     AND key > ?                    -- ContinuationToken (exclusive start)
     AND key LIKE ? || '%'          -- prefix filter (if specified)
     AND status = 0                 -- Live only
   ORDER BY key
   LIMIT ?                          -- MaxKeys (default 1000)
2. If delimiter is specified, apply common-prefix grouping logic
3. Return results to client
```

ListObjects is served entirely from the metadata cluster. No storage node I/O. No
user-defined metadata is returned (S3 spec does not include it in list responses).

This is the most performance-sensitive metadata operation for many workloads (e.g.
tools that enumerate large buckets). SQLite B-tree index on (bucket, key) gives
efficient prefix scans.

### DeleteObject

```
Unversioned bucket:
1. Frontend proposes DeleteObjectMeta to Raft leader
2. Raft commits
3. Leader applies: UPDATE objects SET status = PendingDelete WHERE ...
4. Return success to client (object no longer visible in List/Get)
5. Background: garbage collector reads PendingDelete records, deletes shards
   from storage nodes, then DELETE the metadata record

Versioned bucket:
1. Frontend proposes InsertDeleteMarker to Raft leader
2. Raft commits
3. Leader applies: INSERT objects (status = DeleteMarker, ...)
4. Return success + new version_id to client
5. Previous version's shards are NOT deleted (still accessible by version_id)
```

Delete is two-phase: metadata marks the object as deleted (fast, synchronous), then
shard deletion happens asynchronously. This keeps the delete response fast and
prevents the metadata cluster from blocking on storage node I/O.

### DeleteBucket

```
1. Check: SELECT COUNT(*) FROM objects WHERE bucket = ? AND status = 0 LIMIT 1
2. If not empty → return BucketNotEmpty error
3. Propose DeleteBucket to Raft leader
4. Raft commits
5. Leader applies: DELETE FROM buckets WHERE name = ?
```

### CopyObject

```
1. Read source object (GetObject path)
2. If metadata directive is REPLACE: construct new metadata blob with new headers
3. EC-encode with new (or same) metadata prepended
4. Write shards to storage nodes
5. Propose PutObjectMeta for destination (bucket, key)
6. If source != destination: no cleanup needed (source remains)
7. If source == destination (metadata-only change): old shards become orphans,
   garbage collected
```

CopyObject-to-self always rewrites shards (resolved in storage-node-design.md). This
maintains the invariant that shard metadata is always accurate.

---

## Versioning

### Unversioned buckets (default)

Each (bucket, key) has at most one object record with status = Live. PutObject
replaces the existing record (the old one becomes PendingDelete). DeleteObject marks
the record as PendingDelete.

### Versioned buckets

Each (bucket, key) can have many versions. PutObject creates a new version_id.
GetObject without a version_id returns the latest Live version. GetObject with a
version_id returns that specific version.

DeleteObject (without version_id) inserts a DeleteMarker. The previous version is not
deleted. DeleteObject (with version_id) permanently deletes that version.

### Version ID generation

AWS S3 uses opaque version IDs. We need version IDs that:
- Are globally unique (across all objects in the system)
- Are sortable by creation time (for "latest version" queries)
- Are URL-safe (used in S3 request parameters)

Options:
- **ULID**: 128-bit, lexicographically sortable, URL-safe base32. 48 bits of
  timestamp + 80 bits of randomness.
- **UUIDv7**: 128-bit, time-ordered, standard UUID format. hex + hyphens.
- **Custom**: Timestamp + node_id + sequence number.

**Recommendation**: ULID. Lexicographic sort = chronological sort, which means
"latest version" is just `ORDER BY version_id DESC LIMIT 1`. URL-safe encoding
avoids escaping issues. Small dependency.

---

## Garbage Collection

When an object is overwritten or deleted, its shards become orphans. The metadata
cluster marks the old record as PendingDelete. A background garbage collector:

```
1. Query metadata cluster for PendingDelete records (batched, rate-limited)
2. For each PendingDelete record:
   a. Compute shard locations from placement_key + cluster map
   b. Send DeleteShard to each storage node
   c. Once all shards confirmed deleted (or confirmed absent):
      d. Delete the metadata record from the metadata cluster
3. If a storage node is unreachable, retry later (do not delete the metadata
   record until all shards are confirmed gone)
```

The garbage collector runs on the Raft leader (or a designated node). It must be
idempotent — deleting a shard that's already gone returns success.

### Open question: GC timing

- **Immediate**: Start GC as soon as a record is marked PendingDelete. Lowest orphan
  accumulation. Higher load on storage nodes during write bursts.
- **Batched**: GC runs periodically (e.g. every 60 seconds), processes a batch of
  PendingDelete records. Amortizes overhead. Orphans accumulate briefly.
- **Rate-limited**: GC processes at most N shards/second. Prevents GC from starving
  client I/O on storage nodes.

**Recommendation**: Batched + rate-limited. GC is background work and must not
interfere with client I/O.

---

## Cluster Membership and Availability

### Raft cluster size

- **3 replicas**: Tolerates 1 failure. Minimum for production.
- **5 replicas**: Tolerates 2 failures. Better for larger deployments.
- **1 replica**: Development only. No fault tolerance.

### Leader election and failover

Raft handles leader election automatically. Typical election timeout: 1-5 seconds.
During leader election, writes are blocked. Reads may also be blocked depending on
the read strategy (leader reads are blocked; ReadIndex reads can proceed if a follower
confirms the leader was recently alive).

### Metadata cluster vs storage node count

The metadata cluster is a small fixed-size Raft group (3-5 nodes). The storage node
count can be much larger (tens to hundreds). These are independent — adding storage
nodes does not affect the metadata cluster.

The metadata cluster nodes can be co-located with storage nodes (same physical
machines) or run on dedicated hardware. Co-location is simpler; dedicated hardware
gives better isolation.

### Open question: Metadata cluster scaling

For very large deployments (millions of objects, high request rate), a single Raft
group may become a bottleneck. Options:

1. **Vertical scaling**: Faster SSD, more RAM for SQLite page cache. Goes far for
   metadata-only workloads.
2. **Read replicas**: Serve reads from followers (with ReadIndex). Distributes read
   load. Writes still go through leader.
3. **Sharded metadata**: Partition the namespace across multiple Raft groups (e.g. by
   bucket hash or key prefix). Adds significant complexity (cross-shard operations
   like ListBuckets, bucket rename).

**Recommendation**: Start with a single Raft group. Vertical scaling + read replicas
should handle most workloads. Sharding is deferred — if needed, it's a major
architectural change that should be designed separately.

---

## Snapshot and Recovery

### Raft snapshots

The Raft log grows unboundedly. Periodically, the leader takes a snapshot of the
current state (the SQLite database) and truncates the log. Followers that fall too
far behind receive the snapshot instead of replaying individual log entries.

For our application-level state machine:
- **Snapshot = SQLite database file** (or a consistent copy).
- Take a snapshot when the log exceeds a configurable size (e.g. 10,000 entries).
- Use `sqlite3_backup_init` / `sqlite3_backup_step` for a consistent online backup.

### Backup strategy

Beyond Raft replication (which protects against node failures), periodic external
backups protect against correlated failures or bugs:

- **SQLite backup to object storage**: Periodically copy the SQLite database to an
  external location (another S3-compatible store, local disk, etc.).
- **Raft log archival**: Archive old Raft log segments for point-in-time recovery.

This is operational infrastructure, not part of the core design. But the system should
make it easy (expose a backup API or CLI command).

### Disaster recovery from shard data

If the metadata cluster is completely lost and no backups exist, Architecture C
(storage-node-design.md) allows reconstruction:

1. Scan all shards on all storage nodes.
2. For C2 (prepend): reconstruct objects from k shards each, extract the metadata
   prefix.
3. Rebuild the metadata cluster from the extracted metadata.
4. This recovers: object keys, sizes, user metadata, content types, EC parameters.
5. This does NOT recover: exact timestamps, version ordering (unless embedded in the
   metadata prefix), bucket-level configuration.

This is a last-resort recovery path. Normal operations depend on Raft replication
and backups.

---

## Interface to Other Subsystems

### To HTTP Frontend

The metadata cluster exposes an internal API (trait-based, like the storage node):

```rust
#[async_trait]
pub trait MetadataStore {
    // Bucket operations
    async fn create_bucket(&self, req: CreateBucketReq) -> Result<(), MetadataError>;
    async fn delete_bucket(&self, bucket: &str) -> Result<(), MetadataError>;
    async fn head_bucket(&self, bucket: &str) -> Result<BucketInfo, MetadataError>;
    async fn list_buckets(&self, owner: u64) -> Result<Vec<BucketInfo>, MetadataError>;

    // Object operations (namespace only — no data)
    async fn put_object_meta(&self, req: PutObjectMetaReq) -> Result<PutObjectMetaResp, MetadataError>;
    async fn get_object_meta(&self, bucket: &str, key: &str, version: Option<&str>)
        -> Result<ObjectRecord, MetadataError>;
    async fn delete_object_meta(&self, bucket: &str, key: &str, version: Option<&str>)
        -> Result<DeleteResult, MetadataError>;
    async fn list_objects(&self, req: ListObjectsReq) -> Result<ListObjectsResp, MetadataError>;
    async fn list_object_versions(&self, req: ListVersionsReq) -> Result<ListVersionsResp, MetadataError>;

    // Multipart operations
    async fn create_multipart(&self, req: CreateMultipartReq) -> Result<String, MetadataError>;
    async fn put_part_meta(&self, req: PutPartMetaReq) -> Result<(), MetadataError>;
    async fn complete_multipart(&self, req: CompleteMultipartReq) -> Result<ObjectRecord, MetadataError>;
    async fn abort_multipart(&self, upload_id: &str) -> Result<(), MetadataError>;
    async fn list_multipart_uploads(&self, req: ListMultipartReq) -> Result<ListMultipartResp, MetadataError>;
    async fn list_parts(&self, upload_id: &str) -> Result<Vec<PartInfo>, MetadataError>;
}
```

### To Placement Layer

The metadata cluster does NOT call the placement layer directly. It stores the
`placement_key` per object. The frontend uses the placement layer to compute shard
locations from the placement_key. This keeps the metadata cluster decoupled from
placement — it just stores and retrieves records.

### To Storage Nodes

The metadata cluster does NOT communicate directly with storage nodes. The frontend
orchestrates shard reads/writes. The only exception is the garbage collector, which
sends delete commands to storage nodes for orphan cleanup.

### To Repair Subsystem

The repair subsystem queries the metadata cluster for:
- PendingDelete records (garbage collection)
- All live objects (for scrub verification — checking that expected shards exist)
- Cluster map changes (for rebalance planning)

---

## Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("bucket not found: {name}")]
    BucketNotFound { name: &'static str },  // or a bounded inline string

    #[error("bucket already exists")]
    BucketAlreadyExists,

    #[error("bucket not empty")]
    BucketNotEmpty,

    #[error("object not found")]
    ObjectNotFound,

    #[error("version not found: {version_id}")]
    VersionNotFound { version_id: &'static str },

    #[error("upload not found: {upload_id}")]
    UploadNotFound { upload_id: &'static str },

    #[error("invalid key: {reason}")]
    InvalidKey { reason: &'static str },

    #[error("raft consensus error: {reason}")]
    Consensus { reason: &'static str },

    #[error("not leader: leader is node {leader_id}")]
    NotLeader { leader_id: u64 },

    #[error("metadata store unavailable")]
    Unavailable,
}
```

Note: error types need more thought. The `&'static str` fields are placeholders —
real bucket names and version IDs are dynamic. May need bounded inline strings or
just carry the relevant numeric/hash identifiers. Must not allocate on the hot path
per the memory policy.

---

## Configuration

```rust
pub struct MetadataClusterConfig {
    /// Raft node ID for this replica.
    pub node_id: u64,

    /// Peer addresses for Raft cluster members.
    pub peers: Vec<(u64, SocketAddr)>,

    /// Path to local SQLite database.
    pub db_path: PathBuf,

    /// Path to Raft log storage.
    pub raft_log_path: PathBuf,

    /// Raft snapshot interval (number of log entries).
    pub snapshot_interval: u64,

    /// Raft election timeout range (millis).
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,

    /// Raft heartbeat interval (millis).
    pub heartbeat_interval: u64,

    /// Maximum concurrent read queries.
    pub max_concurrent_reads: usize,

    /// SQLite page cache size (pages).
    pub sqlite_cache_size: i64,
}
```

---

## Observability

Metrics to expose:

- `metadata_write_duration_seconds` (histogram): Raft write latency (propose → commit).
- `metadata_read_duration_seconds` (histogram): Read query latency.
- `metadata_objects_total` (gauge): Total object count.
- `metadata_buckets_total` (gauge): Total bucket count.
- `metadata_pending_deletes` (gauge): Objects awaiting GC.
- `raft_term` (gauge): Current Raft term.
- `raft_commit_index` (gauge): Latest committed log index.
- `raft_applied_index` (gauge): Latest applied log index.
- `raft_leader` (gauge): Current leader node ID.
- `raft_proposals_total` (counter): Total Raft proposals.
- `raft_proposal_failures_total` (counter): Failed proposals.
- `gc_shards_deleted_total` (counter): Shards deleted by GC.
- `gc_records_cleaned_total` (counter): Metadata records removed by GC.

---

## Build Sequence

1. **SQLite schema and local operations**: Define the schema. Implement
   bucket/object CRUD against a local SQLite database (no Raft). This can be tested
   immediately.
2. **MetadataStore trait + in-memory implementation**: Define the trait. Implement
   `MemoryMetadataStore` for testing the frontend without persistence.
3. **Raft integration**: Add openraft (or chosen library). Wire the application-level
   state machine to SQLite. Test leader election, write replication, read consistency.
4. **Snapshot and recovery**: Implement SQLite-based snapshots. Test follower catch-up
   from snapshot.
5. **Versioning**: Add version_id generation, delete markers, ListObjectVersions.
6. **Multipart tracking**: Add multipart upload tables and operations.
7. **Garbage collector**: Background process for PendingDelete cleanup.
8. **Integration tests**: End-to-end with HTTP frontend and storage nodes. Simulate
   leader failover during writes. Verify consistency under concurrent operations.

---

## Summary of Open Questions

| # | Question | Current Leaning | Alternatives |
|---|---|---|---|
| 1 | Placement generation / rebalance tracking | Placement groups (PGs) with per-PG migration state | Per-object generation, two-phase map, try-both fallback |
| 2 | Raft read strategy | Leader reads (simplest) | ReadIndex, lease-based |
| 3 | State machine approach | Application-level commands (Approach 1) | WAL replication (Approach 2), existing library (Approach 3) |
| 4 | Raft library | openraft | raft-rs (tikv), custom |
| 5 | Version ID format | ULID | UUIDv7, custom |
| 6 | GC timing | Batched + rate-limited | Immediate |
| 7 | Metadata cluster scaling | Single Raft group + vertical | Sharded metadata (deferred) |
| 8 | ~~etag storage format~~ | **Resolved: Binary BLOB, max 64 bytes (512 bits) + etag_kind discriminator** | |
| 9 | Error types — dynamic strings | Needs design | Bounded inline strings, numeric identifiers only |

---

## Cross-references

- **Storage Node Design**: `plans/storage-node-design.md` — Architecture C (embedded
  metadata), shard immutability, C2 prepend design.
- **Territory Map**: `plans/territory-map.md` — subsystem 4 definition, build sequence.
- **Placement API**: `plans/placement-api.md` — rendezvous hashing, ClusterMap,
  deterministic placement.
- **EC Engine API**: `plans/ec-engine-api.md` — systematic encoding, stripe size.
