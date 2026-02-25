# Metadata Service — Design Document

## Scope

This document covers the design of the metadata service (subsystem 4 in the build
sequence). The metadata service is responsible for the bucket/object namespace: it
tracks which objects exist, their versions, and enough information to route requests
to the right storage nodes. It is the authoritative index for the system.

**Architecture**: The current leaning is **per-PG metadata (Architecture E)**, where
object metadata lives on the same storage nodes that hold the PG's shards. A small
**global service** (3-5 node Raft group) handles bucket operations, cluster topology,
and PG state. There is no separate centralized metadata cluster for object records.

The metadata service does NOT store:
- Shard data (that's the storage node's shard store)
- User-defined metadata / content-type / S3 headers (those are stored in the shard
  data as a prepended metadata blob — see storage-node-design.md, Architecture C2)
- Raw checksums of shard data (those are per-storage-node)

It stores the minimum needed for namespace operations (ListObjects, routing) and
object-level bookkeeping (versioning, lifecycle).

---

## Relationship to Storage Node Design

The storage node design doc (storage-node-design.md) establishes several decisions
that constrain the metadata service:

- **Architecture C2 (preferred)**: User metadata (content-type, x-amz-meta-*) is
  prepended to object data before EC encoding. The metadata service does NOT store
  user-defined metadata — it stores only the fields needed for namespace operations.
- **Shards are fully immutable (write-once)**: CopyObject-to-self with metadata
  changes creates entirely new shards. The metadata record is updated to reflect
  the new version.
- **Shard identity**: The metadata service tracks (bucket, key, version). Shard
  locations are derived from PG placement, not stored per-object.
- **HeadObject for user metadata**: Served from storage nodes (reading the metadata
  prefix from shard data), NOT from the metadata service. The metadata service provides
  the fixed fields (size, etag, last-modified); the frontend reads user metadata from
  the PG's storage nodes.
- **Per-PG co-location**: With Architecture E, the metadata index and shard data
  are on the same nodes. The PG primary handles both metadata writes and shard I/O.

---

## Role in the Stack

```
HTTP Frontend
     │
     ├─── CreateBucket, ListBuckets ──► Global Service (Raft, 3-5 nodes)
     │
     ├─── PutObject ──► PG Primary (object record + shard data on same nodes)
     │
     ├─── GetObject ──► PG Primary/Replica (object record + shard data)
     │
     ├─── HeadObject ─► PG Primary (object record: size, etag, last-modified)
     │                  + shard 0 (user metadata, content-type)
     │
     ├─── ListObjects ► Fan-out to all PG primaries, merge-sort
     │
     └─── DeleteObject ► PG Primary (mark PendingDelete + delete shards async)
```

With per-PG metadata, most S3 operations go directly to PG nodes — no separate
metadata cluster in the request path. Only bucket-level operations and cluster
topology changes go to the global service. This means per-object latency depends
on PG primary availability, not on a central metadata cluster.

---

## What the Metadata Service Stores

### Per-bucket record (global service)

```
bucket_name (primary key, string, max 63 chars)
  - owner_id (u64 or access key reference)
  - created_at (u64, unix millis)
  - versioning_status (Disabled | Enabled | Suspended)
  - region (u16, maps to a region name via config — fixed enum, not a string)
```

Buckets are a flat namespace — no nesting. Bucket names are globally unique.

### Per-object record (per-PG, on PG's storage nodes)

This is the core record. One record per (bucket, key, version).

```
(bucket, key, version_id) → ObjectRecord
  - size (u64, original object size in bytes — NOT including prepended metadata)
  - etag (fixed-size binary, max 64 bytes — see etag format below)
  - etag_kind (u8)
  - last_modified (u64, unix millis)
  - storage_class (u8, initially just STANDARD)
  - ec_k (u8)
  - ec_m (u8)
  - status (u8: Live, DeleteMarker, PendingDelete)
```

Note: no placement_key or shard locations. The PG is derived from the object key:
`pg_id = hash(bucket + "/" + key) % pg_count`. Shard locations are derived from the
PG's current node mapping. This keeps the per-object record small and fixed-size.

### What is NOT stored here

- **content-type** — in shard data (metadata prefix)
- **x-amz-meta-*** — in shard data (metadata prefix)
- **content-encoding, content-disposition, cache-control, expires** — in shard data
- **per-shard checksums** — in each storage node's local metadata DB
- **shard data** — on storage nodes

The per-object record is intentionally small and fixed-size. Variable-length
user metadata (up to 2KB) is in shard data, not in the metadata index. This keeps
per-PG replication fast and predictable.

### Open question: Placement generation and the rebalance problem

Since the placement layer is deterministic (given a key and cluster map, any node
can compute shard locations), the metadata service may not need to store explicit
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
object's generation in the metadata service.

- Pro: Reads always go to the correct location.
- Pro: Simple, explicit — no guessing or fallback.
- Con: **Requires updating metadata for every affected object during rebalance.**
  A single node addition can affect O(shards/N) objects — potentially millions of
  metadata writes through Raft consensus. This is expensive and slow.
- Con: Must keep old cluster maps around (though they're small).

#### Approach 2: Placement groups (PGs)

Instead of per-object placement, hash objects into a fixed number of placement groups
(e.g. 1024-8192 PGs). Each PG maps to a set of nodes. The global service stores
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

**Resolved: Adopt placement groups.** The per-PG migration tracking is far more
manageable than per-object metadata updates or multi-fallback read paths.

PG count is configurable at cluster creation time, with a sensible default (e.g.
1024 for small clusters, 4096+ for larger ones). PG count cannot change after
creation without a data migration (PG split/merge), which is out of scope for v1.

The per-object metadata record does NOT need a placement generation. It does not
even need to store a placement_key — the PG is derived from the object key:

```
pg_id = hash(bucket + "/" + key) % pg_count
```

PG migration state is tracked in a separate, small table (see PG State Table below).

---

## Open question: Centralized vs per-PG metadata

The rest of this document describes a **centralized** metadata cluster (a single Raft
group that holds all object records). But there is a fundamentally different
architecture: **per-PG metadata**, where each PG's storage nodes also own the object
metadata for that PG. This eliminates the separate metadata cluster entirely.

### Architecture E: Per-PG metadata (Ceph model)

Instead of a central metadata service, each PG's nodes hold both shards and object
metadata. There is no separate metadata cluster for object records.

```
Global service (tiny, 3-5 nodes):
  - Bucket table (create/delete/list buckets, versioning config)
  - Cluster map + PG state (node membership, PG migration)
  - PG count
  ~ a few hundred records total

Per-PG (on PG's storage nodes):
  - Object records for all objects in this PG
  - Shard data
  - Per-object user metadata (in shard prefix, C2)
```

**Object operations** go directly to the PG's nodes:

```
PutObject:
  1. hash(bucket/key) % pg_count → PG
  2. PG → nodes (via placement)
  3. Prepend user metadata, EC-encode, write shards to PG's nodes
  4. PG's primary node commits the object record to PG-local metadata
  5. Done — one set of nodes, one round-trip for metadata

GetObject:
  1. hash(bucket/key) → PG → nodes
  2. Read object record from any PG node (or primary)
  3. Read k shards from PG's nodes
  4. Reconstruct, strip metadata prefix, return

HeadObject:
  1. hash(bucket/key) → PG → nodes
  2. Read object record from PG node (size, etag, etc.)
  3. Read shard prefix for user metadata (same nodes)
  4. Return
```

**Per-PG consensus**: Each PG needs its own mechanism for consistent metadata writes.
The key question is whether this requires full Raft per PG or something lighter.

#### Option 1: Per-PG Raft

Each PG has a Raft group formed by its k+m nodes. With (4,2) and 6 nodes per PG, a
quorum is 4.

- Pro: Self-contained strong consistency per PG.
- Con: 1024-4096 Raft groups running simultaneously — each with its own leader
  election, log, term management, and snapshots. Enormous implementation and
  operational complexity.

**Not recommended** — too heavyweight for this use case.

#### Option 2: Primary-based with epoch fencing (Ceph model)

This is how Ceph handles PG consensus without per-PG Raft:

1. **The global service** (a small Raft/Paxos group, 3-5 nodes) maintains the
   cluster map with a monotonically increasing **epoch** number. This is the single
   source of truth for cluster membership.

2. **PG primary is determined by placement**, not election. The first node in the
   placement output for that PG is the primary. No per-PG leader election needed —
   primary is a deterministic function of (PG ID, cluster map).

3. **Writes go to the primary**. The primary replicates to secondaries synchronously.
   Once replicas acknowledge, the primary responds to the client. The primary
   serializes all writes for this PG — giving per-key linearizability.

4. **On failure**: The global service detects the failure (heartbeats), increments
   the epoch, publishes a new cluster map. The new primary for affected PGs is
   determined by placement (next eligible node in the list). No election.

5. **Epoch fencing prevents split-brain**: If the old primary tries to accept writes
   with a stale epoch, replicas reject it — they've seen a newer epoch. The epoch
   acts as a fencing token. This is the key correctness mechanism.

6. **Peering**: When a new primary takes over after a failure, it contacts the other
   replicas to reconcile state ("what's the latest version of each object in this
   PG?"). They exchange short PG operation logs and agree on the authoritative state.
   This is simpler than full Raft log replay — it only happens after failures, not
   during normal operation.

- Pro: **No per-PG Raft.** No leader election, no log, no term management per PG.
  The global service (one small Raft group) handles all the hard distributed systems
  work. PGs are just primary-backup replication with epoch fencing.
- Pro: Proven at massive scale (Ceph runs millions of PGs this way).
- Pro: Primary determination is deterministic from the cluster map — instant
  "election" on failure (just recompute placement with the new map).
- Pro: Normal-case write path is simple: client → primary → replicate to secondaries
  → ack. No consensus protocol in the write path.
- Con: Relies on the global service for failure detection and epoch management. If the
  global service is unavailable, PGs cannot handle failures (though existing primaries
  continue serving).
- Con: Peering after failures adds latency before a PG is available again (must
  reconcile state with replicas).
- Con: Must implement the peering protocol correctly — this is where correctness bugs
  would live.

#### Option 3: Leverage shard writes (C2 prepend, no separate metadata replication)

With C2 (metadata prepended to data before EC encoding), writing k+m shards already
replicates the metadata across all PG nodes as part of the shard data. The PG-local
SQLite index is derived state — it can be rebuilt from the shards. Metadata
consistency follows from shard write consistency. The local index is updated after
shard writes succeed and serves as a cache/index.

- Pro: No separate metadata replication mechanism at all.
- Pro: The shard write path IS the metadata replication path.
- Con: Conditional PUT still needs a serialization point — two concurrent PUTs to the
  same key must be ordered. This requires a primary or lock, which brings us back to
  Option 2.
- Con: Rebuilding the index from shards after a failure is slow (must read all shards).

**Option 3 works for the data path but not for conditional operations.** It can be
combined with Option 2: the primary serializes writes (for conditional PUT ordering),
the shard write replicates the data + metadata, and the local index is derived state.

#### Recommended: Option 2 (primary-based with epoch fencing)

This gives us per-key linearizability (the primary serializes writes per PG) without
the overhead of running thousands of Raft groups. The global service handles the
hard distributed systems work (failure detection, epoch management, map distribution).
Combined with C2 (metadata in shard data), the local per-node metadata index is
derived state that can be rebuilt from shards if needed.

**Pros of per-PG metadata**:
- **No separate metadata cluster to build and operate.** Eliminates an entire
  subsystem. Storage nodes are the metadata nodes.
- **Natural horizontal scaling.** Write throughput scales with PG count (each PG has
  independent consensus). No single Raft leader bottleneck.
- **Co-locality.** Metadata and data are on the same nodes. Fewer network round-trips
  for PutObject and GetObject.
- **PG migration moves everything together.** No split-brain between a metadata
  cluster and storage nodes during topology changes.
- **Disaster recovery is simpler.** Reconstruct k shards → get both data and metadata.
  No separate metadata backup to manage.

**Cons of per-PG metadata**:
- **ListObjects requires fan-out.** Must query all PGs (or all nodes), merge-sort
  results. With 1024 PGs, that's 1024 parallel queries. Bounded but expensive.
  Options:
  1. **Fan-out and merge**: Query all PG primaries, merge-sort. O(pg_count) queries
     per LIST. Each query is cheap (local SQLite index scan). Can be parallelized.
  2. **Global LIST index**: Maintain a secondary index for listing, updated
     asynchronously after PG-level writes. LIST queries the global index. Slightly
     stale — eventually consistent for LIST (AWS S3 was eventually consistent for
     LIST until 2020). Strongly consistent for GET/PUT/DELETE.
  3. **Hybrid**: Fan-out for small result sets, global index for large prefix scans.
- **Bucket-level operations still need a global service.** CreateBucket, DeleteBucket,
  ListBuckets, versioning config. This service is tiny but still needs consensus.
- **Many small Raft groups.** If using per-PG Raft: 1024+ simultaneous Raft groups.
  Each has its own leader election, log, snapshots. Implementation and operational
  complexity.
- **Strong LIST consistency is harder.** With a centralized metadata cluster,
  ListObjects is a single query — trivially consistent. With per-PG metadata, LIST
  must assemble results from multiple PGs, and a concurrent PutObject to one PG
  might not be visible in the LIST if the query already passed that PG. Achieving
  strong LIST consistency requires either a global snapshot or a two-phase LIST
  protocol.

### Centralized (current design) vs per-PG metadata

| Property | Centralized metadata | Per-PG metadata |
|---|---|---|
| Write throughput | Single Raft leader (bottleneck) | Scales with PG count |
| Read (Get/Head) | Two hops (metadata cluster + storage) | One hop (PG nodes) |
| ListObjects | Single query, trivially consistent | Fan-out to all PGs, eventually consistent for v1 |
| Operational complexity | Two systems (metadata cluster + storage) | One system (storage nodes do both) |
| Failure blast radius | Metadata cluster failure → all ops fail | One PG failure → only that PG's objects |
| Scaling model | Vertical (bigger metadata node) | Horizontal (more nodes = more PGs) |
| Per-PG consensus | N/A (one Raft group) | Primary-based with epoch fencing (lightweight) |
| Strong LIST consistency | Easy | Hard (requires coordination across PGs) |

### Discussion

Three key decisions have shifted the balance toward per-PG metadata:

1. **Eventually consistent LIST is acceptable for v1.** AWS S3 was eventually
   consistent for LIST for 14 years (2006-2020). This removes the strongest
   argument for centralized metadata (trivially consistent LIST). Per-key strong
   consistency (conditional PUT, read-after-write) is still mandatory and is
   naturally provided by per-PG consensus.

2. **Per-PG consensus does not require per-PG Raft.** Primary-based consensus with
   epoch fencing (Option 2, Ceph model) eliminates the implementation complexity of
   running thousands of Raft groups. The global service (one small Raft group) handles
   failure detection and epoch management. PGs use simple primary-backup replication.

3. **Per-PG metadata eliminates an entire subsystem.** No separate metadata cluster
   to build, operate, or keep consistent with storage nodes. PG migration moves data
   and metadata together. Disaster recovery is simpler (reconstruct shards → get both).

**Current leaning: per-PG metadata (Architecture E) with primary-based consensus.**

The centralized design remains a valid fallback if per-PG proves too complex during
implementation. The key insurance policy: keep the per-object schema and operations
identical regardless of where they live. If centralized is needed, it's the same
schema in a single Raft group instead of distributed across PGs.

Remaining concerns with per-PG:
- **ListObjects fan-out**: 1024 parallel queries, merge-sort. Bounded but adds
  latency. Can be mitigated with a global LIST index (secondary, eventually
  consistent) for large prefix scans.
- **Peering protocol correctness**: The primary-based consensus model requires a
  correct peering implementation for failure recovery. This is where bugs would live.
- **Global service as single point of failure for topology changes**: If the global
  service is down, PGs cannot handle failures (though existing primaries continue
  serving reads and writes).

---

## Consistency Model

### Per-key consistency (priority)

The most important consistency guarantee is **per-key strong consistency**:

- **Read-after-write**: A successful PutObject is immediately visible to subsequent
  GetObject and HeadObject calls for that key.
- **Read-after-delete**: A successful DeleteObject is immediately reflected for that
  key.
- **Conditional PUT**: `If-None-Match: *` (create-only) and `If-Match` (update-only)
  must be linearizable — two concurrent conditional PUTs to the same key must not
  both succeed.

Per-key consistency is critical for correctness and is what most applications depend
on. It is naturally provided by per-PG consensus — all operations on a given key go
to the same PG, which has a single serialization point.

### LIST consistency (lower priority)

- **Strongly consistent LIST**: ListObjects reflects all completed writes and deletes
  at the time the LIST is issued. AWS S3 provides this as of December 2020.
- **Eventually consistent LIST**: ListObjects may not immediately reflect very recent
  writes or deletes. A second LIST shortly after will see them.

AWS S3 was eventually consistent for LIST for its first ~14 years (2006-2020). Many
applications tolerate eventually consistent LIST. Making LIST strongly consistent is
significantly harder in a distributed architecture.

### How AWS S3 achieved strong consistency

Reference: Werner Vogels, "Diving Deep on S3 Consistency" (April 2021).
https://www.allthingsdistributed.com/2021/04/s3-strong-consistency.html

S3's architecture has three components relevant to consistency:

1. **Persistence tier**: The authoritative metadata store. Eventually consistent on
   its own (replication lag across nodes).
2. **Cache layer**: Serves most reads. Fast but potentially stale.
3. **Witness**: A lightweight in-memory service that tracks recent writes. On every
   read, the cache checks with the witness: "has this object been modified since my
   cached version?" If stale, the cache refreshes from the persistence tier.

The witness is the key insight: it's small, fast (in-memory, no disk I/O), and only
tracks staleness — not the actual metadata. It acts as a "read barrier" that prevents
stale reads without requiring the persistence tier itself to be strongly consistent.

This approach is relevant to our design because it decouples per-key consistency
(handled by the witness + cache) from the persistence tier's replication model.

### Implications for our architecture

**With centralized metadata cluster**: All operations go through a single Raft group.
Both per-key and LIST consistency are trivial — Raft serializes everything.

**With per-PG metadata (Architecture E)**:
- Per-key consistency is natural — each PG has its own consensus point.
- LIST consistency requires coordination across PGs. Options:
  1. **Accept eventually consistent LIST** for v1. Most applications tolerate this.
     LIST results may briefly miss very recent writes to other PGs. Per-key operations
     (Get, Put, Delete, conditional Put) are still strongly consistent.
  2. **Global LIST index with witness**: Maintain a secondary index for LIST queries,
     with a witness mechanism to detect staleness. Adds complexity but achieves strong
     LIST consistency.
  3. **Fan-out with snapshot barrier**: Issue LIST to all PGs at a consistent point in
     time (logical clock or barrier protocol). Complex to implement correctly.

**Recommendation**: Per-key strong consistency is mandatory. Eventually consistent
LIST is acceptable for v1 — it was good enough for S3 for 14 years. Strong LIST
consistency can be added later via a witness-style mechanism or global index if
needed.

---

## Consensus and Replication

### Scope note

This section describes the Raft-replicated SQLite approach. In the per-PG architecture
(Architecture E, current leaning), this applies **only to the global service** (bucket
table, cluster map, PG state — a few hundred records). Per-PG object metadata uses
primary-based consensus with epoch fencing (see "Per-PG consensus" section above).

In the centralized architecture, this applies to all metadata (millions of object
records in a single Raft group).

The design below is written for the general case and applies to both.

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

With per-PG metadata (Architecture E), there are two schemas:
- **Global service** (Raft-replicated, 3-5 nodes): buckets, pg_state, cluster_maps
- **Per-PG** (on each PG's storage nodes): objects, multipart_uploads, multipart_parts

### Global service schema (Raft-replicated SQLite)

```sql
-- Bucket table
CREATE TABLE buckets (
    name          TEXT PRIMARY KEY,
    owner_id      INTEGER NOT NULL,
    created_at    INTEGER NOT NULL,  -- unix millis
    region        INTEGER NOT NULL DEFAULT 0,  -- u16 enum, maps to region name via config
    versioning    INTEGER NOT NULL DEFAULT 0  -- 0=Disabled, 1=Enabled, 2=Suspended
);

-- Placement group state table
-- Tracks PG → node mapping and migration status during topology changes.
-- pg_count is a cluster-level constant set at creation time.
CREATE TABLE pg_state (
    pg_id             INTEGER PRIMARY KEY,  -- 0..pg_count-1
    map_version       INTEGER NOT NULL,     -- cluster map version this PG was placed with
    status            INTEGER NOT NULL DEFAULT 0,  -- 0=Stable, 1=Migrating, 2=Splitting (future)
    -- node assignments are derived from placement(pg_id, cluster_map[map_version])
    -- during migration, both map_version and map_version+1 assignments are valid
    migration_started INTEGER              -- unix millis, NULL if not migrating
);

-- Cluster map history (recent versions only — old versions pruned after all PGs migrate)
CREATE TABLE cluster_maps (
    version       INTEGER PRIMARY KEY,
    map_data      BLOB NOT NULL,          -- serialized ClusterMap (nodes, weights, topology)
    created_at    INTEGER NOT NULL         -- unix millis
);
```

### Per-PG schema (per storage node, one SQLite DB per PG)

Each storage node has one SQLite database per PG it participates in. The PG primary
replicates writes to secondaries. This schema is derived state — it can be rebuilt
from shard data (C2 prepend) if needed.

```sql
-- Object table: one row per (bucket, key, version)
-- For unversioned buckets, version_id is a fixed sentinel (e.g. "null")
-- Objects are in this DB because hash(bucket/key) % pg_count = this PG's ID.
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
    status        INTEGER NOT NULL DEFAULT 0,  -- 0=Live, 1=DeleteMarker, 2=PendingDelete
    PRIMARY KEY (bucket, key, version_id)
);

-- Index for ListObjectsV2: prefix scan by (bucket, key)
CREATE INDEX idx_objects_list ON objects (bucket, key);

-- Index for ListObjectVersions: all versions of an object
CREATE INDEX idx_objects_versions ON objects (bucket, key, last_modified DESC);

-- PG operation log (for peering after failures)
-- Recent operations, retained for a configurable window.
-- During peering, new primary exchanges oplog with replicas to reconcile state.
CREATE TABLE pg_oplog (
    seq           INTEGER PRIMARY KEY AUTOINCREMENT,
    op_type       INTEGER NOT NULL,  -- 0=Put, 1=Delete, 2=DeleteMarker
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    version_id    TEXT NOT NULL,
    epoch         INTEGER NOT NULL,  -- cluster map epoch when op was committed
    timestamp     INTEGER NOT NULL   -- unix millis
);

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
-- No placement_key: PG is derived from the parent object's key
CREATE TABLE multipart_parts (
    upload_id     TEXT NOT NULL,
    part_number   INTEGER NOT NULL,
    size          INTEGER NOT NULL,
    etag          BLOB NOT NULL,
    etag_kind     INTEGER NOT NULL,
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

- **No placement_key in object records**: The PG is derived deterministically from
  the object key: `pg_id = hash(bucket + "/" + key) % pg_count`. Shard locations are
  derived from the PG's node mapping (looked up in `pg_state` + `cluster_maps`). This
  keeps the per-object record minimal — no variable-length placement data.

- **pg_state**: Tracks migration status per PG. In steady state, all PGs have
  `status = Stable` and their `map_version` matches the current cluster map. During a
  topology change, affected PGs transition to `Migrating` — reads try both old and new
  node assignments. Once all shards for a PG are migrated, it returns to `Stable` with
  the new `map_version`.

- **cluster_maps**: History of recent cluster map versions. Old versions are retained
  as long as any PG references them (i.e. has a `map_version` pointing to that
  version). Once all PGs have migrated past a version, it can be pruned.

- **status**: Live objects are returned by ListObjects. DeleteMarkers are only visible
  in ListObjectVersions. PendingDelete objects are being garbage collected (shards not
  yet fully deleted).

- **Multipart tables**: Track in-progress multipart uploads. On
  CompleteMultipartUpload, the parts are assembled into a final object record and the
  multipart records are deleted. On AbortMultipartUpload, all records and staged shards
  are deleted.

---

## Operations

With per-PG metadata (Architecture E), object operations go to PG nodes. The PG
primary serializes writes and replicates to secondaries using epoch-fenced
primary-backup replication.

### PutObject

```
1. Frontend receives object data + headers
2. Frontend prepends user metadata to object data (metadata blob)
3. Frontend EC-encodes the combined stream into k+m shards
4. Frontend derives PG: pg_id = hash(bucket + "/" + key) % pg_count
5. Frontend looks up PG's current node mapping (from pg_state + cluster_map)
6. Placement layer determines shard → node assignments within the PG's node set
7. Frontend writes shards to storage nodes (parallel)
8. Once sufficient shards are durably written (quorum or all):
   9. PG primary commits the object record (replicates to secondaries)
   10. Primary confirms write
   11. Return success to client
```

**Write ordering**: Shard data is written BEFORE the metadata record. If the frontend
crashes between steps 7 and 9, orphan shards exist on storage nodes but no metadata
record exists. These orphans are cleaned up by the repair subsystem (see Garbage
Collection below). This is safer than metadata-first, which would leave a metadata
record pointing to missing shards.

**Atomicity**: The PutObject is not fully atomic. There is a window between shard
writes and metadata commit where:
- Shards exist but are not reachable (no metadata record yet).
- A crash loses the metadata write — orphan shards need cleanup.

For versioned buckets, the new version becomes visible atomically when the metadata
record is committed. The old version remains accessible until explicitly deleted.

For unversioned buckets, the PUT replaces the previous record. The old shards become
orphans and need garbage collection.

### GetObject

```
1. Frontend derives PG: pg_id = hash(bucket + "/" + key) % pg_count
2. Frontend looks up PG's node mapping from cluster map
   (if PG is Migrating, try new mapping first, fall back to old)
3. Frontend queries PG primary (or any replica for reads):
   SELECT size, etag, ec_k, ec_m, version_id FROM objects WHERE ...
4. PG node returns the object record
5. Frontend reads k shards from PG's storage nodes (parallel)
6. Frontend reconstructs the original byte stream
7. Frontend strips the metadata prefix, returns object data + headers to client
```

With per-PG metadata, steps 3-4 and 5-6 go to the same set of nodes. The metadata
query and shard reads can be pipelined or combined.

For range requests, the frontend computes which shard bytes correspond to the
requested byte range (accounting for the metadata prefix offset) and reads only the
necessary portions.

### HeadObject

```
1. Frontend derives PG, looks up node mapping
2. Frontend queries PG node for object record (size, etag, last-modified, etc.)
3. For user-defined metadata (content-type, x-amz-meta-*):
   a. Reads metadata prefix from shard 0 on same PG nodes (or reconstructs
      from k shards if shard 0 is unavailable or object is very small)
   b. Parses user metadata from the prefix
4. Returns combined response (object record fields + shard metadata fields)
```

With per-PG co-location, HeadObject goes to the same set of nodes for both the
metadata record and the shard prefix read. For large objects, step 3 is a small
partial read. For small objects, it's a full k-shard reconstruction (but tiny).

### ListObjectsV2

```
1. Frontend fans out to all PG primaries:
   SELECT key, size, etag, last_modified, storage_class
   FROM objects
   WHERE bucket = ?
     AND key > ?                    -- ContinuationToken (exclusive start)
     AND key LIKE ? || '%'          -- prefix filter (if specified)
     AND status = 0                 -- Live only
   ORDER BY key
   LIMIT ?                          -- MaxKeys (default 1000)
2. Frontend merge-sorts results from all PGs
3. If delimiter is specified, apply common-prefix grouping logic
4. Return results to client
```

ListObjects requires fan-out to all PG primaries (or all nodes, each serving its
PGs). This is the most expensive operation in the per-PG model. With 1024 PGs, it's
1024 parallel queries — each cheap (local SQLite index scan), but the fan-out adds
latency. No user-defined metadata is returned (S3 spec does not include it).

SQLite B-tree index on (bucket, key) gives efficient prefix scans per PG. The
merge-sort overhead is bounded by pg_count × result_limit.

Eventually consistent for v1: a concurrent PutObject to one PG may not appear in a
LIST that has already passed that PG. Per-key operations (Get, Put, Delete) are
still strongly consistent.

### DeleteObject

```
Unversioned bucket:
1. Frontend derives PG, sends to PG primary
2. PG primary: UPDATE objects SET status = PendingDelete WHERE ...
3. Primary replicates to secondaries
4. Return success to client (object no longer visible in List/Get)
5. Background: PG primary's garbage collector deletes shards from PG's nodes,
   then DELETEs the metadata record

Versioned bucket:
1. Frontend derives PG, sends to PG primary
2. PG primary: INSERT objects (status = DeleteMarker, ...)
3. Primary replicates to secondaries
4. Return success + new version_id to client
5. Previous version's shards are NOT deleted (still accessible by version_id)
```

Delete is two-phase: metadata marks the object as deleted (fast, synchronous), then
shard deletion happens asynchronously. This keeps the delete response fast.

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
1. PG primary queries its local PG database for PendingDelete records (batched,
   rate-limited)
2. For each PendingDelete record:
   a. Send DeleteShard to each node in this PG's node set
   b. Once all shards confirmed deleted (or confirmed absent):
      c. DELETE the metadata record from the PG database
      d. Replicate the deletion to secondaries
3. If a node is unreachable, retry later (do not delete the metadata record
   until all shards are confirmed gone)
```

With per-PG metadata, the garbage collector runs on each PG primary — it only needs
to clean up shards for its own PG's objects, on its own PG's nodes. It must be
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

### Global service (Raft cluster)

The global service is a small Raft group responsible for bucket metadata, cluster maps,
and PG state. With per-PG metadata (Architecture E), this is the only Raft group in
the system.

**Raft cluster size**:
- **3 replicas**: Tolerates 1 failure. Minimum for production.
- **5 replicas**: Tolerates 2 failures. Better for larger deployments.
- **1 replica**: Development only. No fault tolerance.

**Leader election and failover**: Raft handles leader election automatically. Typical
election timeout: 1-5 seconds. During leader election, bucket operations and cluster
map updates are blocked. Per-PG object operations (Get, Put, Delete) are NOT blocked
— they go to PG primaries, not the global service.

**Scaling**: The global service handles only bucket-level operations and cluster
topology changes. This is low throughput — a single Raft group is sufficient even for
very large deployments. The per-object write throughput scales with PG count (each PG
has an independent primary).

### Per-PG availability

Each PG's availability depends on its node set:
- **Normal operation**: The PG primary serves reads and writes. Secondaries replicate.
- **Primary failure**: The global service detects the failure, increments the epoch,
  publishes a new cluster map. The new primary (next eligible node) takes over after
  peering with replicas.
- **Secondary failure**: The PG continues operating in degraded mode. Writes go to
  remaining secondaries. Repair subsystem reconstructs missing shards on a new node.
- **PG unavailability**: If the primary fails and no secondary can take over (below
  quorum), the PG's objects are unavailable until recovery. Other PGs are unaffected.

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

With per-PG metadata, the per-PG SQLite databases are co-located with shard data.
If a node's PG database is lost, it can be rebuilt from the shards:

1. Scan all shards on the node.
2. For C2 (prepend): reconstruct objects from k shards each, extract the metadata
   prefix.
3. Rebuild the PG database from the extracted metadata.
4. This recovers: object keys, sizes, user metadata, content types, EC parameters.
5. This does NOT recover: exact timestamps, version ordering (unless embedded in the
   metadata prefix).

For global service recovery (bucket table, cluster maps, PG state), standard Raft
replication and backups apply. The global service data is small and changes
infrequently.

---

## Interface to Other Subsystems

### To HTTP Frontend

With per-PG metadata, the frontend interacts with two services:

**Global service** (for bucket operations and cluster topology):

```rust
#[async_trait]
pub trait GlobalService {
    // Bucket operations
    async fn create_bucket(&self, req: CreateBucketReq) -> Result<(), MetadataError>;
    async fn delete_bucket(&self, bucket: &str) -> Result<(), MetadataError>;
    async fn head_bucket(&self, bucket: &str) -> Result<BucketInfo, MetadataError>;
    async fn list_buckets(&self, owner: u64) -> Result<Vec<BucketInfo>, MetadataError>;

    // Cluster topology
    async fn get_cluster_map(&self) -> Result<ClusterMap, MetadataError>;
    async fn get_pg_state(&self, pg_id: u32) -> Result<PgState, MetadataError>;
}
```

**PG primary** (for object operations — same node that stores shards):

```rust
#[async_trait]
pub trait PgMetadataStore {
    // Object operations (namespace only — no data)
    async fn put_object_meta(&self, req: PutObjectMetaReq) -> Result<PutObjectMetaResp, MetadataError>;
    async fn get_object_meta(&self, bucket: &str, key: &str, version: Option<&str>)
        -> Result<ObjectRecord, MetadataError>;
    async fn delete_object_meta(&self, bucket: &str, key: &str, version: Option<&str>)
        -> Result<DeleteResult, MetadataError>;
    async fn list_pg_objects(&self, req: ListPgObjectsReq) -> Result<ListPgObjectsResp, MetadataError>;
    async fn list_object_versions(&self, req: ListVersionsReq) -> Result<ListVersionsResp, MetadataError>;

    // Multipart operations
    async fn create_multipart(&self, req: CreateMultipartReq) -> Result<String, MetadataError>;
    async fn put_part_meta(&self, req: PutPartMetaReq) -> Result<(), MetadataError>;
    async fn complete_multipart(&self, req: CompleteMultipartReq) -> Result<ObjectRecord, MetadataError>;
    async fn abort_multipart(&self, upload_id: &str) -> Result<(), MetadataError>;
    async fn list_parts(&self, upload_id: &str) -> Result<Vec<PartInfo>, MetadataError>;
}
```

**ListObjects** is assembled by the frontend: fan-out `list_pg_objects` to all PG
primaries (or a subset based on prefix → PG mapping if deterministic), merge-sort.

For testing and the centralized fallback, a unified `MetadataStore` trait can wrap
both interfaces (global service + PG metadata) into a single abstraction.

### To Placement Layer

The global service stores cluster maps and PG state. The frontend derives the PG from
the object key (`hash(bucket/key) % pg_count`), then uses the placement layer to
compute the PG's node set from the cluster map. The PG primary is the first node in
the placement output.

### To Storage Nodes

With per-PG metadata, the PG primary IS a storage node. Object metadata operations
and shard I/O are co-located. The frontend sends both shard writes and metadata
commits to the same set of nodes.

The garbage collector runs on PG primaries — each PG primary cleans up PendingDelete
records for its own PG by deleting shards from its node set.

### To Repair Subsystem

The repair subsystem interacts with PG primaries for:
- PendingDelete records (garbage collection per PG)
- All live objects in a PG (for scrub verification)

And with the global service for:
- PG migration state (which PGs need shard migration after topology changes)
- Cluster map history (old and new maps for computing migration plans)

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

    #[error("not pg primary: primary is node {primary_id}")]
    NotPgPrimary { primary_id: u64 },

    #[error("stale epoch: have {have}, current {current}")]
    StaleEpoch { have: u64, current: u64 },

    #[error("pg unavailable: pg {pg_id} is peering")]
    PgPeering { pg_id: u32 },

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

### Global service configuration

```rust
pub struct GlobalServiceConfig {
    /// Raft node ID for this replica.
    pub node_id: u64,

    /// Peer addresses for Raft cluster members.
    pub peers: Vec<(u64, SocketAddr)>,

    /// Path to local SQLite database (bucket table, cluster maps, PG state).
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

    /// Failure detection interval for storage nodes (millis).
    pub heartbeat_check_interval: u64,

    /// SQLite page cache size (pages).
    pub sqlite_cache_size: i64,
}
```

### Per-node PG metadata configuration

```rust
pub struct PgMetadataConfig {
    /// Path to per-PG SQLite databases (one per PG this node participates in).
    /// Directory structure: {pg_db_dir}/{pg_id}.db
    pub pg_db_dir: PathBuf,

    /// Maximum concurrent read queries per PG.
    pub max_concurrent_reads_per_pg: usize,

    /// SQLite page cache size per PG database (pages).
    pub sqlite_cache_size_per_pg: i64,

    /// PG operation log retention (entries, for peering after failures).
    pub pg_oplog_retention: u64,
}
```

---

## Observability

### Global service metrics

- `global_raft_term` (gauge): Current Raft term.
- `global_raft_commit_index` (gauge): Latest committed log index.
- `global_raft_leader` (gauge): Current leader node ID.
- `global_raft_proposals_total` (counter): Total Raft proposals.
- `global_raft_proposal_failures_total` (counter): Failed proposals.
- `global_buckets_total` (gauge): Total bucket count.
- `global_cluster_map_epoch` (gauge): Current cluster map epoch.
- `global_pgs_migrating` (gauge): PGs currently in migration.

### Per-PG metrics (per storage node)

- `pg_write_duration_seconds` (histogram, labels: pg_id): Write latency (primary commit + replication).
- `pg_read_duration_seconds` (histogram, labels: pg_id): Read query latency.
- `pg_objects_total` (gauge, labels: pg_id): Object count per PG.
- `pg_pending_deletes` (gauge, labels: pg_id): Objects awaiting GC per PG.
- `pg_is_primary` (gauge, labels: pg_id): 1 if this node is primary for the PG.
- `pg_epoch` (gauge, labels: pg_id): Latest epoch seen by this PG.
- `pg_replication_lag_entries` (gauge, labels: pg_id, replica): Replication lag to secondaries.
- `pg_peering_duration_seconds` (histogram): Time spent in peering after primary failover.
- `gc_shards_deleted_total` (counter): Shards deleted by GC.
- `gc_records_cleaned_total` (counter): Metadata records removed by GC.

### Aggregate metrics (computed from per-PG)

- `metadata_objects_total` (gauge): Total object count (sum across all PGs on this node).
- `list_fanout_duration_seconds` (histogram): ListObjects fan-out latency.

---

## Build Sequence

### Phase 1: Local metadata operations (no distribution)

1. **SQLite schema and local operations**: Define the schema. Implement
   bucket/object CRUD against a local SQLite database. This can be tested
   immediately with unit tests.
2. **MetadataStore trait + in-memory implementation**: Define the trait. Implement
   `MemoryMetadataStore` for testing the frontend without persistence.
3. **Versioning**: Add version_id generation (ULID), delete markers,
   ListObjectVersions.
4. **Multipart tracking**: Add multipart upload tables and operations.

### Phase 2: Global service (Raft)

5. **Raft integration for global service**: Add openraft (or chosen library). Wire
   the application-level state machine to SQLite for bucket table, cluster map, and
   PG state. Test leader election, write replication, read consistency.
6. **Snapshot and recovery**: Implement SQLite-based snapshots for the global
   service. Test follower catch-up from snapshot.
7. **Cluster map distribution**: Global service publishes cluster maps to all
   storage nodes. Nodes subscribe and receive epoch updates.

### Phase 3: Per-PG metadata (primary-based consensus)

8. **PG primary determination**: Implement primary selection from placement output.
   Primary receives all writes for its PGs.
9. **Primary-backup replication**: Primary replicates object records to secondaries.
   Epoch fencing prevents stale primaries from accepting writes.
10. **Peering protocol**: After primary failure, new primary reconciles state with
    replicas. Exchange PG operation logs, agree on authoritative state.
11. **Per-PG SQLite index**: Each storage node maintains a SQLite index for its PGs'
    objects. Derived state — can be rebuilt from shards if needed.

### Phase 4: Integration

12. **Garbage collector**: Background process for PendingDelete cleanup.
13. **ListObjects fan-out**: Frontend queries all PG primaries, merge-sorts results.
    Eventually consistent for v1.
14. **Integration tests**: End-to-end with HTTP frontend and storage nodes. Simulate
    primary failover during writes. Verify per-key consistency under concurrent
    operations. Test PG migration during topology changes.

---

## Summary of Open Questions

| # | Question | Current Leaning | Alternatives |
|---|---|---|---|
| 1 | ~~Placement generation / rebalance tracking~~ | **Resolved: Placement groups (PGs)** with per-PG migration state. PG count set at cluster creation. | |
| 2 | Raft read strategy (global service) | Leader reads (simplest) | ReadIndex, lease-based |
| 3 | State machine approach (global service) | Application-level commands (Approach 1) | WAL replication (Approach 2), existing library (Approach 3) |
| 4 | Raft library (global service) | openraft | raft-rs (tikv), custom |
| 5 | Version ID format | ULID | UUIDv7, custom |
| 6 | GC timing | Batched + rate-limited | Immediate |
| 7 | Centralized vs per-PG metadata | **Leaning per-PG (Architecture E)**: primary-based consensus + relaxed LIST consistency removes main barriers | Centralized fallback if per-PG proves too complex |
| 8 | ~~etag storage format~~ | **Resolved: Binary BLOB, max 64 bytes (512 bits) + etag_kind discriminator** | |
| 9 | Error types — dynamic strings | Needs design | Bounded inline strings, numeric identifiers only |
| 10 | Peering protocol design | Ceph-style PG log exchange | TBD — correctness-critical, needs careful design |
| 11 | PG write quorum | All k+m secondaries ack (strongest) | Majority of replicas (faster, tolerates slow nodes) |

---

## Cross-references

- **Storage Node Design**: `plans/storage-node-design.md` — Architecture C (embedded
  metadata), shard immutability, C2 prepend design.
- **Territory Map**: `plans/territory-map.md` — subsystem 4 definition, build sequence.
- **Placement API**: `plans/placement-api.md` — rendezvous hashing, ClusterMap,
  deterministic placement.
- **EC Engine API**: `plans/ec-engine-api.md` — systematic encoding, stripe size.
