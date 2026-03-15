# Storage Node — Design Document

> Status: Superseded historical design note.
>
> The implemented storage backend kept many of the shard-store decisions from this
> document: per-PG directories, per-PG SQLite `metadata.db`, file-per-shard data,
> temp-file writes with `fsync` + rename, CRC64-NVME shard verification, and
> `PgStore` / `SharedStorageNode` as the main local storage abstractions.
>
> The active architecture no longer follows the metadata-embedding and separate
> storage-daemon design options discussed here. User/object metadata now lives in
> the current per-PG SQLite schema, payload layout has been unified around fixed
> segments, and read retention/reclaim semantics were designed later.
>
> Treat this document as background only. The current source of truth is:
> - `crates/storage/src/lib.rs`
> - `crates/storage/src/pg_store.rs`
> - `crates/storage/src/node.rs`
> - `plans/completed/pg-serialization-and-crc-verification.md`
> - `plans/completed/unified-segment-payload-redesign.md`
> - `plans/completed/segment-integrity-checks-for-bounded-reads.md`

## Scope

This document covers the design of the per-node storage daemon (subsystem 3 in the
build sequence). The storage node is responsible for storing erasure-coded shards on
local disk, maintaining per-shard integrity checksums, and exposing a simple internal
API for shard read/write/delete/verify operations.

The storage node does NOT handle:
- Object-level S3 semantics (bucket policy, versioning logic)
- Placement decisions (that's the placement layer)
- Erasure coding (that's the EC engine, used by the coordinator/frontend)
- S3 protocol (that's the HTTP frontend)

It is a shard-level key-value store with integrity guarantees. With per-PG metadata
(Architecture E), it also hosts the per-PG object metadata index — but the storage
node's core responsibility is still shard I/O and integrity.

---

## Role in the Stack

```
HTTP Frontend
     │
     ▼
Metadata ──────► Placement
     │                │
     ▼                ▼
  ┌──────────────────────┐
  │   Storage Node (×N)  │  ◄── this document
  │                      │
  │  shard I/O + local   │
  │  metadata + checksums│
  └──────────────────────┘
         │
         ▼
      Local Disk
```

The frontend (or a coordinator component) erasure-codes an object into k+m shards,
uses the placement layer to determine which nodes hold which shards, then sends each
shard to its assigned storage node via internal RPC. On read, the process reverses:
fetch shards from storage nodes, reconstruct if any are missing.

Each storage node is autonomous — it knows nothing about which object a shard belongs
to or how it relates to other shards. It simply stores bytes keyed by a shard
identifier and ensures integrity.

---

## Open question: Where does S3 object metadata live?

S3 objects carry metadata beyond the raw bytes: user-defined metadata (`x-amz-meta-*`
headers, up to 2KB total), system metadata (content-type, content-encoding,
cache-control, content-disposition, content-language, expires), ETags, and optionally
tags. The question is whether this metadata is stored only in the metadata cluster, or
also (or exclusively) at the storage node level alongside shard data.

### Architecture A: Metadata cluster only (current implicit design)

Storage nodes store raw shard bytes. All object-level metadata lives in the metadata
cluster (subsystem 4).

```
Storage Node:  shard_key → shard_bytes + CRC64
Metadata DB:   bucket/key/version → {content-type, user-meta, etag, size, shard_keys, ...}
```

- Pro: Clean separation. Storage nodes are simple shard KV stores.
- Pro: HeadObject and ListObjects are pure metadata operations — no storage node I/O.
- Pro: Metadata updates (e.g. CopyObject that changes content-type without rewriting
  data) only touch the metadata cluster.
- Pro: Metadata cluster can be optimized independently (Raft, caching, indexing).
- **Con: The metadata cluster is a critical single point of truth.** Lose it and you
  have raw shards with no way to know which object they belong to, what their
  content-type is, or what user metadata was attached. Recovery requires either
  restoring the metadata DB from backup or a full re-index (which can't recover user
  metadata at all — those bytes aren't in the shards).
- Con: Every object read requires a metadata lookup + storage node reads — two round
  trips.

### Architecture B: Metadata alongside shards (MinIO approach)

MinIO stores an `xl.meta` file alongside the erasure-coded data parts on each node.
This file contains the full object metadata: content-type, user-defined metadata,
part info, EC parameters, checksums, etc. There is no separate metadata cluster —
the namespace is reconstructed by scanning the data nodes.

```
Storage Node:  /data/<bucket>/<object>/
                  xl.meta       ← full object metadata (replicated across nodes)
                  part.1        ← shard data
```

- **Pro: Objects are self-describing.** Any k nodes can reconstruct both the data and
  the metadata. No separate metadata service to lose.
- Pro: Simpler overall architecture — no metadata cluster to build and operate.
- Pro: Metadata consistency is guaranteed by the same EC/replication that protects data.
- **Con: ListObjects requires scanning all nodes** (or maintaining a separate index
  that is effectively a metadata cluster anyway). MinIO handles this but it's slow
  for large buckets.
- Con: HeadObject must contact storage nodes (at least one).
- Con: Metadata updates require writing to k+m nodes (same cost as a full object PUT).
- Con: Storage node is no longer a simple shard KV — it understands object structure.
- Con: Bucket-level operations (list, policy) need a separate mechanism.

### Architecture C: Hybrid — metadata cluster as primary, embedded metadata for DR

The metadata cluster is the primary source for namespace operations (list, head). But
object metadata is also preserved alongside shard data so that disaster recovery can
reconstruct the full namespace — including user-defined metadata, content-type, etc. —
by scanning shards alone.

There are several sub-options for how the metadata is embedded:

#### C1: Replicate metadata header in every shard file

Each shard file contains a small header (a copy of the full object metadata), followed
by the shard data.

```
Shard file on disk:
  [header_len: u32][header_crc: u64][metadata_header: bytes][shard_data: bytes]

metadata_header contains:
  - object key (or hash)
  - content-type
  - user-defined metadata (x-amz-meta-*)
  - EC parameters (k, m, shard_index)
  - original object size
  - object-level checksum
  - version id
```

- Pro: Any single shard is self-describing — a DR tool can identify the object from
  one shard alone without needing k shards.
- Pro: Metadata has its own integrity check (header_crc), independent of the shard
  data CRC. A corrupt metadata header is detectable even if the shard data is fine.
- Con: Metadata is replicated k+m times. For (4,2), that's 6 copies of ~200-500
  bytes = ~1.2-3KB total per object. Negligible in absolute terms.
- Con: Header adds complexity to the shard file format — write and read paths must
  account for it. Storage node must skip header bytes when serving shard data.

#### C2: Prepend metadata to object data before EC encoding

Instead of storing a header per shard file, prepend the metadata to the object byte
stream before splitting into k data shards and erasure-coding. The metadata becomes
part of the EC-protected data — it's encoded into the shards the same way as the
object bytes.

```
Byte stream fed to EC encoder:
  [metadata_len: u32][metadata: bytes][object_data: bytes]

metadata contains:
  - format version (u8)
  - object key (or hash)
  - content-type
  - user-defined metadata (x-amz-meta-*)
  - content-encoding, content-disposition, cache-control, expires
  - EC parameters (k, m)
  - original object size
  - object-level checksum
  - version id
  - CRC64-NVME over the metadata blob itself
```

**Key property: systematic EC codes preserve data shards verbatim.** ISA-L uses a
systematic encoding — the k data shards are the original data split into k equal
chunks, and the m parity shards are computed from them. Data shards are NOT scrambled.

However, the shard size is `ceil(total_stream_length / k)`. Whether the metadata fits
entirely within shard 0 depends on the object size:

- **Large objects** (shard_size >> metadata_size): The metadata prefix sits entirely
  within shard 0 as plain readable bytes. HeadObject = small read from shard 0.
  For k=4 and ~500 bytes of metadata, this holds for objects larger than ~2KB.
- **Small objects** (shard_size comparable to metadata_size): The metadata is striped
  across multiple data shards. Reading it requires fetching k shards and
  reconstructing — same cost as a full GetObject.
- **Empty objects** (0 data bytes): The entire stream is just metadata (~300-500
  bytes). With k=4, shard_size ≈ 75-125 bytes. Metadata is split across all k shards.

**Implication for HeadObject**: For large objects, HeadObject can be served by reading
just the prefix of shard 0 — no EC reconstruction needed. For small objects, HeadObject
requires full reconstruction. This is a significant split in behaviour.

**HeadObject strategy**: HeadObject always reads user metadata from storage nodes, not
the metadata cluster. For large objects this means reading the prefix of shard 0. For
small/empty objects this means reconstructing from k shards. The cost is acceptable:

- **Small objects**: k network round-trips, but each shard is tiny (tens to hundreds
  of bytes). Total I/O is negligible. The latency cost is the k parallel round-trips,
  not the data volume.
- **Large objects**: Single small read from shard 0.
- **GetObject** (more common than HeadObject): Reads all k shards anyway, so metadata
  comes for free at the front of the reconstructed byte stream. Zero extra cost.
- **ListObjects** (most common): Served entirely from the metadata cluster. No storage
  node I/O. Not affected.

**Implication for the metadata cluster**: The metadata cluster stores only the minimum
needed for namespace operations and routing:

```
Metadata cluster record:
  - bucket + key (namespace index)
  - size
  - etag
  - last-modified
  - storage class
  - version id
  - EC parameters (k, m) and placement info
```

User-defined metadata (content-type, x-amz-meta-*, content-encoding, cache-control,
etc.) is NOT stored in the metadata cluster. It lives only in the shard data, prepended
to the object bytes. This is analogous to a macOS file fork — the user metadata is a
separate data stream associated with the object, stored alongside it but not part of
the core namespace index.

This keeps metadata cluster transactions minimal: small, fixed-size records. Every
PutObject writes a small record to Raft consensus. No variable-size user metadata
blobs flowing through the consensus path. This matters for write throughput — Raft
replication cost is proportional to record size, and user metadata can be up to 2KB
per object.

**Implication for S3 API**: S3 has no API that searches or filters by user-defined
metadata. ListObjectsV2 returns only key/size/etag/last-modified/storage-class —
never content-type or x-amz-meta-*. User metadata is always accessed by exact object
key (HeadObject or GetObject), which means placement is always known and we can
always route to the right storage nodes.

- **Pro: Zero mechanism needed at the storage layer.** The EC engine, storage node,
  and shard file format are completely unaware of metadata. It's just bytes. No header
  format at the storage layer.
- **Pro: Metadata gets the same EC protection as data** — same fault tolerance,
  same reconstruction.
- **Pro: Metadata integrity can include a CRC within the metadata blob itself**
  (verifiable after reconstruction).
- **Pro: DR recovery is clean** — reconstruct k shards and the metadata is right
  there at the front of the byte stream.
- Con: HeadObject requires storage node I/O (shard 0 for large objects, k shards for
  small objects). Adds latency compared to a pure metadata cluster lookup. Acceptable
  trade-off for keeping metadata cluster transactions minimal.
- Con: The layer that EC-encodes (coordinator/frontend) must know the metadata format
  and prepend it. The EC engine stays generic.
- Con: Changing the metadata format changes the encoded bytes, which could complicate
  format versioning. A version byte at the start of the metadata blob handles this.

#### C3: EC-encode metadata separately

Treat the metadata as a small separate "object" with its own EC pass. Store the
resulting metadata shards alongside the data shards.

- Pro: Metadata and data are independently recoverable.
- Pro: Could use different EC parameters for metadata (e.g. more redundancy).
- Con: Two EC passes per object. Extra complexity for minimal benefit.
- Con: Need to track which shards are metadata vs data.

**Not recommended** — the complexity isn't justified. C1 or C2 are simpler.

#### C1 vs C2

| Property | C1 (replicated header) | C2 (prepend to data) |
|---|---|---|
| Single shard self-describing | Yes (any shard) | No — need k shards for small objects; shard 0 only for large objects |
| HeadObject for user metadata | Read header from any shard | Large objects: shard 0. Small/empty objects: need k shards or metadata cluster cache |
| Metadata in metadata cluster | All metadata | Depends on HeadObject strategy (see mitigation options above) |
| Storage node awareness | Must know header format | None — opaque bytes |
| Space overhead | k+m copies (~1-3KB total) | 1× metadata within EC overhead |
| Integrity check | Separate header CRC per shard | CRC in metadata blob (verifiable after reconstruction) |
| Implementation complexity | Header in shard file format | Metadata prepend at coordinator |
| Format versioning | Header version in shard format | Version byte in metadata blob |
| Degraded HeadObject | Read header from any other shard | Must reconstruct shard 0 from k other shards |

**C2 is simpler for the storage node** — it stays a pure shard KV with no knowledge of
metadata. All the metadata handling lives in the coordinator/frontend layer.

**C1 is more resilient for HeadObject** — works from any single shard regardless of
object size. No behavioural split between small and large objects.

**C1 is better for DR tooling** — any single shard identifies its object without
needing to gather k shards. Also allows the storage node to independently verify that
a shard belongs to the right object (cross-check header vs shard_key).

**C2 keeps the metadata cluster minimal.** User-defined metadata is not stored in the
metadata cluster at all — it lives only in the shard data (analogous to a file fork).
The metadata cluster transacts only the fixed-size fields needed for ListObjects and
routing. HeadObject pays the cost of reading from storage nodes (shard 0 for large
objects, k shards for small objects), but this keeps write throughput on the Raft
consensus path as high as possible.

### Metadata integrity

Regardless of C1 vs C2, object metadata should have its own integrity check:

- **C1**: A CRC64-NVME over the metadata header, stored in the header itself. This
  catches metadata corruption independently of shard data corruption. The S3 protocol
  doesn't specify metadata integrity, but we should — especially since this is a DR
  recovery source.
- **C2**: The metadata is part of the shard data, so the shard CRC covers it. No
  separate check needed (or possible — metadata is mixed into EC-encoded bytes).

If metadata integrity is important independently of data integrity (e.g. detecting
"metadata corrupted but data fine"), that's an argument for C1.

### Architecture D: Replicated metadata blob, separate from EC shards

Instead of embedding metadata in each shard, store a separate small metadata blob
(replicated, not erasure-coded) on a few nodes. This is closer to how some systems
separate the "inode" from the "data blocks."

- Pro: Metadata blob is small, so simple 3-way replication is cheap and fast.
- Pro: No header overhead in shard files.
- Con: Another replication mechanism to implement.
- Con: Still need the metadata cluster for namespace operations.

---

**Resolved: Shards are fully immutable (write-once).** S3 objects are immutable — the
only way to change object metadata (content-type, x-amz-meta-* headers) is
`CopyObject` to the same key with `x-amz-metadata-directive: REPLACE`. We implement
this as a full shard rewrite: CopyObject-to-self creates entirely new shards (with
new metadata). Old shards are deleted (or become a prior version if versioning is
enabled).

This means embedded metadata is always accurate — it is written once at shard creation
and the shard is never modified, only eventually deleted. The cost is that a
metadata-only change on a large object rewrites all shards (e.g. changing content-type
on a 5GB object re-encodes ~7.5GB). This is an acceptable trade-off: metadata-only
CopyObject-to-self is a rare operation, and the invariant "shards are write-once,
metadata is always authoritative" is far more valuable than optimizing that edge case.

---

### Preferred: Architecture C with per-PG metadata (C + E)

**Current leaning**: Architecture C2 (prepend metadata to data before EC), combined
with per-PG metadata (Architecture E from metadata-cluster-design.md). Object metadata
is embedded in shard data (C2) and also stored in the per-PG SQLite database on each
PG's nodes. C1 (replicated header per shard) vs C2 is still an open sub-question,
but C2 is favoured for storage node simplicity.

The storage node is a shard store with a per-PG metadata index. The metadata index
stores the fixed-size object records (size, etag, last-modified, ec params, status).
User-defined metadata (content-type, x-amz-meta-*) lives only in the shard data (C2
prepend) — it is not in the per-PG metadata index.

Normal operation:
- HeadObject → PG node (fixed fields from per-PG metadata + user metadata from shards).
- ListObjects → Fan-out to all PG primaries, merge-sort.
- GetObject → PG node (metadata record + shard data, same nodes).
- PutObject → PG primary (shards + metadata record, same nodes).

Disaster recovery:
- Reconstruct k shards → get both data and metadata. Rebuild per-PG SQLite from shards.

Architecture B (MinIO-style) is not preferred because it replicates full metadata to
every node (k+m copies), does not integrity-check it, and couples the storage node
to object-level semantics.

---

## Shard Identity

A shard must be uniquely addressable. The key question is what the shard identifier
looks like.

### Option A: Opaque shard ID assigned by metadata layer

The metadata cluster assigns a unique ID (e.g. UUID or u128) when creating a shard.
The storage node treats this as an opaque key.

- Pro: Simple. Storage node has no knowledge of object structure.
- Pro: Easy to reason about — one flat namespace.
- Con: Requires the metadata layer to track the mapping from object → shard IDs.
- Con: UUID generation adds a dependency (though minor).

### Option B: Composite key (object_key_hash, shard_index)

The shard is identified by a hash of the object key plus the shard index (0..k+m).
The storage node still treats the key as opaque bytes, but the structure is defined
by convention.

- Pro: Shard ID is derivable from the object key — no metadata lookup needed for
  placement verification or repair.
- Pro: Natural fit with rendezvous hashing (placement already hashes the object key).
- Con: Multipart uploads and object versioning complicate the "one object = one set of
  shards" model.

### Option C: Content-addressed (hash of shard data)

Shard key = hash(shard_bytes). Enables deduplication.

- Pro: Free deduplication.
- Con: Enormous complexity (reference counting, garbage collection, partial writes).
- Con: Erasure-coded shards have high entropy — dedup ratio is near zero.
- Con: Not worth the complexity for this system.

**Recommendation**: Option A or B. Lean toward B for repairability. With per-PG
directories, the shard_key only needs to be unique within a PG — the pg_id provides
the outer namespace. The storage node just needs `&[u8]` as a key within a PG.

### Shard key format (if using Option B)

```
shard_key = object_key_hash (16 bytes) || version_id (8 bytes) || shard_index (1 byte)
```

25 bytes total. Fixed-size, no parsing needed. The storage node treats this as opaque.

---

## On-Disk Layout

This is the most consequential design decision. Options from simplest to most complex:

### Top-level structure: per-PG directories

With per-PG metadata (Architecture E in metadata-cluster-design.md), the on-disk
layout is organized by placement group. Each PG is a self-contained unit on disk:

```
/data/
  pg-0000/
    shards/
      <prefix>/<shard_key_hex>      # shard data files
    metadata.db                      # per-PG SQLite (object records, oplog)
    tmp/                             # temp files for atomic writes
  pg-0001/
    shards/
      ...
    metadata.db
    tmp/
  ...
  pg-1023/
    ...
  node.db                            # node-level metadata (node_id, disk_id, format_version)
```

**Why per-PG directories:**
- **GC is PG-scoped.** The PG primary can scan its own `metadata.db` for
  PendingDelete records and delete shards from the same directory. No cross-PG
  coordination needed.
- **Migration moves a whole PG.** To migrate PG-42 to a new node, copy (or
  reconstruct) the entire `pg-0042/` directory. Shards, metadata, and oplog move
  together as a unit.
- **Scrub is PG-scoped.** Each PG can be scrubbed independently. The scrub process
  walks the PG's shard directory and cross-references against `metadata.db`.
- **Peering after failure.** When a new primary takes over, it reconciles by
  exchanging oplog entries. The oplog is in the PG's `metadata.db`, so peering is
  self-contained.
- **Startup recovery is PG-scoped.** Each PG directory can reconcile its
  `metadata.db` against its shard files independently. Faster parallel recovery.
- **Deletion of a PG.** If a PG migrates away entirely, `rm -rf pg-NNNN/` cleans
  up everything. No scattered shards to find.

A node participates in many PGs (typically `pg_count * (k+m) / node_count` PGs).
With 1024 PGs and 6 nodes, each node hosts ~1024 PGs. With more nodes, fewer PGs
per node.

### Option 1: One file per shard (filesystem-managed)

Within each PG directory, shards are stored as individual files:

```
/data/pg-0042/shards/<prefix>/<shard_key_hex>
```

Where `<prefix>` is the first 2 hex chars of the shard key (fanout to avoid large
directories within a PG).

- Example: `/data/pg-0042/shards/a3/a3b7f2...89_03` (shard index 3)
- Shard data is the file contents.
- CRC64-NVME stored in the PG's `metadata.db` (not inline with data).

**Write path**: write to temp file in PG's tmp/ dir → fsync → rename into shards/
→ fsync parent dir. Atomic on POSIX filesystems.

**Pros**:
- Simplest possible implementation. Easy to debug (ls, hexdump, du).
- Filesystem handles space allocation, directory indexing, free space.
- Works on any POSIX filesystem.
- cp, rsync, standard tools all work for manual recovery.
- Per-PG directories naturally limit directory size — even with millions of objects,
  each PG holds only `total_objects / pg_count` shards.

**Cons**:
- Inode overhead: each shard consumes an inode. For small objects with (4,2) EC, a
  1KB object creates 6 files of ~170 bytes each. Inode overhead dominates.
- Metadata overhead: stat() per shard on read. Filesystem metadata operations are not
  free.
- Fragmentation: many small files fragment sequential read patterns.

**Mitigation**: XFS handles large file counts well (B-tree directories, efficient
extent allocation). Set `mkfs.xfs -n ftype=1` and use directory fanout. With per-PG
directories, each PG's shard/ tree is much smaller than the total — for 10M shards
across 1024 PGs, that's ~10K files per PG directory. Easily within XFS's comfortable
range.

### Option 2: Packed shard files (append-log per directory bucket)

Group shards into larger "pack" files to reduce inode count and improve sequential
I/O:

```
/data/<bucket_id>/pack-<seq>.dat     # concatenated shard data
/data/<bucket_id>/pack-<seq>.idx     # offset index
```

Each pack file contains many shards appended sequentially. An index file maps
shard_key → (offset, length) within the pack.

- Write: append shard to current open pack file. Write index entry. fsync both.
- Read: look up (offset, length) in index, pread from pack file.
- Delete: mark as deleted in index. Reclaim space via compaction.

**Pros**:
- Far fewer inodes (one per pack file, not per shard).
- Better sequential write throughput (large sequential writes).
- Natural batching of fsyncs (group commit).

**Cons**:
- Space reclamation requires compaction (copy live shards to new pack, delete old).
- Compaction is a background process that competes with client I/O.
- Index must be crash-consistent with data — this is the hard part.
- More complex implementation.
- Tools can't inspect individual shards easily.

### Option 3: Direct block device (BlueStore-style)

Bypass the filesystem entirely. Manage block allocation directly on a raw partition.

- Pro: Maximum control over layout, alignment, write amplification.
- Pro: No double-write (filesystem journal + application fsync).
- Con: Enormous implementation effort. Ceph spent years on BlueStore.
- Con: Lose all filesystem tooling. Custom mkfs, fsck, recovery tools needed.

**Out of scope for v1.** The filesystem-based implementation comes first. A raw block
device backend may be added later as an alternative `ShardStore` implementation.
Switching a node from filesystem to block device would require rebuilding that node's
data (migrating shards), which is acceptable — it's the same process as replacing a
disk. The `ShardStore` trait boundary ensures the rest of the system is unaffected by
which backend a given node uses. A cluster could run a mix of filesystem-backed and
block-device-backed nodes simultaneously.

### Option 4: Hybrid — data files on XFS, metadata in SQLite on separate volume

Keep shard data as files (Option 1 or 2), but store the per-PG SQLite databases on
a separate filesystem or volume.

This is the approach described in the territory map ("XFS recommended for data,
BTRFS/ZFS for metadata"). The separate filesystem for metadata allows:
- Using BTRFS/ZFS checksumming for the per-PG SQLite databases (defense in depth).
- Separate I/O scheduling for metadata vs data.
- Smaller metadata volume that benefits from being on SSD even if data is on HDD.

With per-PG layout, the structure would be:
```
/data/pg-NNNN/shards/...          # XFS data volume
/meta/pg-NNNN/metadata.db         # BTRFS/ZFS metadata volume (or SSD)
```

The PG directory is split across volumes, but logically remains a single unit.

**This is the recommended starting point** — simple file-per-shard for data (Option
1), per-PG SQLite for metadata, potentially on separate volumes.

---

## Open question: Small object optimization

For a 1KB object with (4,2) EC, each shard is ~170 bytes. Six files of 170 bytes is
wasteful — filesystem block size (typically 4KB) means each shard wastes ~3.8KB, for
a total of ~24KB used to store 1KB of data. That's 24x overhead.

Options:
1. **Accept the overhead.** Simple, and storage is cheap. Most S3 workloads are
   dominated by large objects in total bytes stored.
2. **Pack small shards.** Use Option 2 (packed files) only for shards below a size
   threshold (e.g. 64KB). Large shards use individual files.
3. **Inline small shards in the metadata DB.** Store shards below a threshold
   directly in SQLite (as BLOBs). This eliminates file overhead entirely for small
   objects.
4. **Defer.** Start with Option 1, measure in practice, optimize if it matters.

**Recommendation**: Start with Option 1 (file per shard) and measure. If small object
performance becomes a concern, Option 3 (inline in SQLite) is the simplest
optimization and avoids the compaction complexity of Option 2.

---

## Local Metadata Database

Each storage node maintains per-PG SQLite databases. With per-PG metadata
(Architecture E), each PG's `metadata.db` stores both shard-level information (CRC,
status) and object-level metadata records (as described in metadata-cluster-design.md).

### What each per-PG metadata DB stores

**Shard tracking** (shard-level, local to this node):
- `shard_key` (primary key, variable-length bytes)
- `data_size` (u64, bytes)
- `crc64_nvme` (u64, computed at write time)
- `created_at` (u64, unix timestamp or monotonic)
- `last_verified` (u64, timestamp of last scrub pass)
- `status` (u8: Live, Deleting, Quarantined)
- `data_path` (optional, if not derivable from shard_key — for Option 2/3)

**Object records** (per-PG metadata, replicated across PG nodes):
- See metadata-cluster-design.md Per-PG schema (objects table, pg_oplog, multipart
  tables).

**Node-level metadata** (separate `node.db`, not per-PG):
- `node_id` (u32, this node's identity)
- `disk_id` (u32, if multiple disks per node)
- `format_version` (u16)
- List of PGs this node participates in and its role (primary/secondary)
- Current cluster map epoch

### SQLite vs LMDB

| Property | SQLite (WAL mode) | LMDB |
|---|---|---|
| Crash safety | Excellent (WAL + checkpoints) | Good if used correctly; fragile if not |
| Read concurrency | Unlimited readers (WAL mode) | Unlimited readers (MVCC) |
| Write concurrency | Single writer (fine for us) | Single writer |
| Read latency | ~1-5 us for key lookup | ~0.5-1 us for key lookup |
| Write latency | ~50-200 us (WAL + fsync) | ~10-50 us |
| Crash failure modes | Well-studied; recovery is automatic | mmap-based; corrupt data on partial writes if WRITEMAP used; database can grow indefinitely on crash without cleanup |
| Tooling | sqlite3 CLI, extensive ecosystem | Limited tooling |
| Dependency complexity | Single C file (amalgamation) | Single C file |
| Max DB size | Unlimited (file-based) | Must be pre-configured (mmap) |
| Space reclamation | VACUUM or auto_vacuum | Never shrinks (copy to reclaim) |

**Additional SQLite considerations:**
- WAL mode with `PRAGMA synchronous=NORMAL` gives crash safety with good performance.
  `NORMAL` means WAL writes are not fsync'd on every commit, but checkpoints are. This
  means you can lose the last few transactions on a power failure, but the database
  won't be corrupted. For our use case, losing a metadata entry means the shard data
  file exists but isn't tracked — recoverable by scanning the data directory.
- `PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;` for maximum safety at the cost
  of ~2x write latency.
- Connection pooling: one writer connection, multiple reader connections.

**Additional LMDB considerations:**
- LMDB's `MDB_WRITEMAP` flag is where most of the fragility lives. Without it, LMDB
  is fairly safe but slower.
- LMDB databases can only grow, never shrink. Over time, deletes accumulate free space
  internally but the file on disk never gets smaller. The only remedy is copying to a
  fresh database.
- LMDB requires the maximal database size to be configured at open time (mmap).
  Misjudging this wastes virtual address space or requires a reopen.

### Open question: RocksDB?

RocksDB is another option, used by TiKV, CockroachDB, and many storage systems.

- Pro: Excellent write throughput (LSM tree, batched writes).
- Pro: Good compression support (reduces metadata storage).
- Pro: Built-in bloom filters for existence checks.
- Con: Large dependency (C++ library, complex build).
- Con: Compaction storms can spike latency.
- Con: Tuning complexity (dozens of options that matter).
- Con: Conflicts with the project's goal of minimal dependencies.

**Not recommended** unless write throughput to the metadata DB becomes a bottleneck,
which is unlikely since metadata operations are lightweight compared to shard I/O.

### Recommendation

**SQLite in WAL mode.** Crash-safe, well-understood, single-file dependency, excellent
tooling. Use `synchronous=NORMAL` as default, `synchronous=FULL` as a configuration
option for operators who want maximum durability.

---

## Integrity: CRC64-NVME

Every shard is protected by a CRC64-NVME checksum.

### Why CRC64-NVME specifically?

- AWS S3 now supports CRC64-NVME as a checksum algorithm — aligning with the
  ecosystem.
- NVMe spec uses CRC64 for end-to-end data protection (T10-DIF / PI).
- CRC64 provides stronger error detection than CRC32C (which Ceph uses) for large
  blocks — relevant for multi-MB shards.
- Hardware acceleration: available on ARM (CRC instructions) and expected on x86 in
  future CPUs. Software implementation is ~2-4 GB/s on modern CPUs, sufficient
  for disk-speed I/O.

### When checksums are computed

1. **On write**: CRC64 computed over shard data as it's written. Stored in metadata DB.
2. **On read**: CRC64 recomputed and compared against stored value. Mismatch → return
   error to caller, mark shard as quarantined.
3. **On scrub**: Background process reads every shard and verifies CRC64. Reports
   mismatches to the repair subsystem.

### Open question: Checksum storage location

**Option A**: Metadata DB only. CRC is looked up from SQLite on read.
- Pro: Single source of truth. Simple.
- Con: If the metadata DB is lost/corrupted, checksums are lost.

**Option B**: Inline with data file. Append or prepend CRC to the shard file.
- Pro: Checksum travels with the data. Survives metadata DB loss.
- Con: Shard file is no longer pure data — readers must know the format.
- Con: Slight complexity in read/write path.

**Option C**: Separate checksum file (sidecar). `<shard>.crc` alongside `<shard>.dat`.
- Pro: Survives metadata DB loss. Data file is pure.
- Con: More files (doubles inode count).
- Con: Consistency between data and checksum file.

**Option D**: Both metadata DB and inline.
- Pro: Defense in depth. Can verify DB checksum against inline checksum.
- Con: Must keep two copies in sync.

**Recommendation**: Option A (metadata DB only) for simplicity. If the metadata DB is
lost, checksums can be recomputed by re-reading the data files. The metadata DB itself
should be on a checksumming filesystem (BTRFS/ZFS) for its own protection.

### CRC64-NVME implementation

**Resolved**: Use ISA-L's `crc64_rocksoft_refl()`. CRC-64/Rocksoft and CRC-64/NVME are
the same algorithm (polynomial `0xAD93D23594C93659`, reflected, init/xorout
`0xFFFFFFFFFFFFFFFF`). ISA-L is already a dependency for the EC engine. The function
auto-selects the fastest implementation at runtime (CLMUL/AVX-512 on x86_64).

Add the FFI binding to the ec-sys crate alongside the existing erasure coding bindings.

Test vectors:
- `"123456789"` → `0xAE8B14860A799888`
- `"hello world!"` → `0xD9160D1FA8E418E3`
- 32 zero bytes → `0xCF3473434D4ECF3B`

---

## Write Path

### Single shard write

```
1. Receive shard data + shard_key + pg_id over RPC
2. Validate: shard_key length, data size within limits, PG is owned by this node
3. Compute CRC64-NVME over shard data
4. Write shard data to temp file in pg-{pg_id}/tmp/ (O_WRONLY | O_CREAT | O_EXCL)
5. fsync temp file
6. Rename temp file to pg-{pg_id}/shards/<prefix>/<shard_key_hex>
7. fsync parent directory
8. Insert shard record into pg-{pg_id}/metadata.db (shard_key, size, crc64, timestamp)
9. Return success + crc64 to caller
```

### Failure modes

- **Crash between step 5 and 6**: Temp file exists in PG's tmp/ dir. On startup,
  clean all PG tmp/ directories.
- **Crash between step 6 and 8**: Shard file exists but no metadata record. On
  startup, scan each PG's shard directory for files not in that PG's metadata DB —
  either add them (recompute CRC) or delete them, depending on policy.
- **Crash during step 8**: SQLite WAL handles this. Transaction either committed or not.

### Open question: Write ordering — data first or metadata first?

**Data first (recommended)**: Write the shard file, then update metadata DB.
- On crash: orphaned data files can be detected and cleaned up or re-indexed.
- Data files are self-describing in the sense that the shard key is the filename.

**Metadata first**: Insert metadata record, then write data file.
- On crash: metadata points to a missing file. Must handle "metadata exists, file
  doesn't" as an error state.
- This is strictly worse — better to have data without metadata (recoverable) than
  metadata without data (error-prone).

### Open question: fsync strategy

Every fsync costs ~1-10ms on SSD (flash commit), ~10-50ms on HDD (platter rotation).
Fsync-per-shard is safe but potentially slow under high write throughput.

Options:
1. **fsync per shard**: Maximum safety. ~100-1000 writes/sec per disk.
2. **Group commit**: Batch multiple shard writes, fsync once per batch. Higher
   throughput but a batch of shards can be lost together on crash. Since shards are
   erasure-coded across nodes, losing a batch on one node is recoverable as long as
   we don't lose corresponding shards on other nodes simultaneously.
3. **fdatasync instead of fsync**: Skip metadata (mtime, atime) sync. Slightly faster,
   still ensures data durability.
4. **O_DSYNC on open**: Kernel syncs every write. Simpler but can't batch.

**Recommendation**: fdatasync per shard as default (option 3). Add optional group
commit (option 2) behind a configuration flag for high-throughput deployments. The
rename step still needs fsync of the parent directory.

### Open question: O_DIRECT

O_DIRECT bypasses the page cache. Pros: avoids polluting the page cache with
write-once shard data (especially relevant for large shards that would evict hot
metadata from cache). Cons: requires aligned buffers (typically 512-byte or 4K
aligned), more complex buffer management.

Options:
1. **Don't use O_DIRECT**: Simple. Let the kernel manage caching. Use `posix_fadvise(DONTNEED)` after write to hint that the page cache should be freed.
2. **Use O_DIRECT for writes only**: Avoids cache pollution on ingest. Reads go through
   page cache (useful for recent reads — temporal locality).
3. **Use O_DIRECT for both reads and writes**: Full control. Requires application-level
   read cache (or relies on the caller not re-reading recently-written data).

**Recommendation**: Start without O_DIRECT (option 1), use `posix_fadvise(DONTNEED)`
after writing large shards. Add O_DIRECT support later if cache pollution is measured
as a problem. O_DIRECT requires aligned allocations, which adds complexity to the
buffer management — worth it only when needed.

---

## Read Path

```
1. Receive shard_key over RPC
2. Look up metadata record in SQLite (get crc64, size, status)
3. If status != Live → return NotFound or appropriate error
4. Open data file, read shard data
5. Compute CRC64-NVME over read data
6. Compare against stored CRC64:
   - Match → return shard data to caller
   - Mismatch → mark shard as Quarantined, return IntegrityError
7. Return shard data + crc64 to caller
```

### Open question: Always verify on read?

CRC64 verification on every read adds CPU cost. For a 4MB shard at ~3 GB/s CRC
throughput, that's ~1.3ms — modest but not zero.

Options:
1. **Always verify**: Maximum safety. Every read is integrity-checked.
2. **Verify on scrub only**: Skip CRC on normal reads. Background scrub catches
   corruption. Faster reads, but corrupt data could be served to clients.
3. **Configurable**: Default to always-verify, allow operators to disable for
   throughput-sensitive workloads.
4. **Sample-based**: Verify a random percentage of reads (e.g. 10%).

**Recommendation**: Always verify (option 1). The CPU cost is negligible compared to
disk I/O. Serving corrupt data silently is much worse than a small CPU overhead. The
EC layer above can reconstruct from other shards if we detect corruption early.

---

## Delete Path

```
1. Receive pg_id + shard_key over RPC
2. Update pg-{pg_id}/metadata.db: shard status = Deleting
3. unlink shard file from pg-{pg_id}/shards/...
4. Delete shard record from pg-{pg_id}/metadata.db
5. Return success
```

Deletes are idempotent: deleting a non-existent shard returns success.

### Open question: Lazy vs eager deletion

**Eager** (above): Delete immediately. Simple.

**Lazy**: Mark as deleted in metadata, background process unlinks files. Allows
undeletion within a grace period. Adds complexity.

**Recommendation**: Eager deletion for now. The per-PG metadata handles versioning
and soft-delete semantics at the object level. By the time a shard delete reaches
the storage node, it should be final.

---

## Scrub (Background Verification)

A background process that reads every shard and verifies its CRC64. This catches
silent data corruption (bit rot) that wouldn't be detected until a client reads the
shard. Scrub operates per PG:

```
for each PG this node participates in:
    for each shard in pg-{pg_id}/metadata.db where status == Live:
        read shard data from pg-{pg_id}/shards/...
        compute CRC64-NVME
        if mismatch:
            mark shard as Quarantined
            report to repair subsystem
        else:
            update last_verified timestamp
        sleep/yield to avoid starving client I/O
```

### Design parameters

- **Scrub interval**: Configurable. Default: complete full scrub within 30 days.
  For 10TB of data at ~50 MB/s background read rate, that's ~2.3 days continuous.
  Spread over 30 days → ~2 hours/day of scrub I/O.
- **I/O priority**: Use `ionice` (CFQ) or cgroup I/O limits. Scrub must not degrade
  client latency.
- **Ordering**: Sequential scan of each PG's shard directory for sequential disk
  reads. Process one PG at a time (or interleave at a coarse granularity). Don't
  randomize (random I/O on HDD is catastrophic for scrub throughput).
- **PG-scoped progress**: Scrub progress is tracked per PG. A PG migration doesn't
  reset scrub progress for unrelated PGs.

---

## Internal API

The storage node exposes a simple RPC API. This is NOT the S3 API — it's an internal
protocol between the coordinator/frontend and the storage nodes.

### Operations

All shard operations are PG-scoped:

```
WriteShard(pg_id: u32, shard_key: bytes, data: bytes) → Result<WriteAck, Error>
    WriteAck { crc64: u64, stored_size: u64 }

ReadShard(pg_id: u32, shard_key: bytes) → Result<ShardData, Error>
    ShardData { data: bytes, crc64: u64 }

DeleteShard(pg_id: u32, shard_key: bytes) → Result<(), Error>

StatShard(pg_id: u32, shard_key: bytes) → Result<ShardStat, Error>
    ShardStat { size: u64, crc64: u64, created_at: u64, last_verified: u64 }

ListShards(pg_id: u32, prefix: bytes, cursor: bytes, limit: u32) → Result<ShardList, Error>
    ShardList { keys: Vec<bytes>, next_cursor: Option<bytes> }
```

### Open question: Streaming vs buffered for large shards

For large shards (multi-MB), should the API stream data or buffer the entire shard?

**Buffered**: Receive entire shard into memory, then write. Simple. Memory cost = max
shard size per concurrent write.

**Streaming**: Write data to disk as it arrives over the network. Lower memory
footprint. More complex (partial write cleanup on failure).

**Recommendation**: Start buffered. With (4,2) EC and 4MB stripe size, a shard is at
most 4MB. Even 100 concurrent writes = 400MB of buffers — manageable. Add streaming
later if memory pressure demands it.

### Open question: RPC protocol

The territory map lists "HTTP/2 + simple binary" as the current leaning for internal
wire protocol.

Options:
1. **gRPC (tonic)**: Well-known, good streaming support, code generation from proto
   files. But: large dependency, HTTP/2 framing overhead for small messages.
2. **HTTP/2 with a binary format (e.g. postcard/bincode)**: Lighter than gRPC,
   still gets multiplexing. But: need to build request routing ourselves.
3. **Custom TCP protocol**: Minimal overhead. But: need to implement framing,
   multiplexing, flow control.
4. **QUIC (quinn)**: UDP-based, good for lossy networks. Overkill for datacenter.

**Recommendation**: Defer this decision. The storage node can be developed with an
in-process API first (function calls), which is how it'll be tested anyway. The RPC
layer is a transport concern that can be added when integrating with the frontend.
Design the internal API as a trait; implement the trait in-process first, add RPC
later.

```rust
/// Per-PG shard store. Each PG on this node has its own ShardStore instance
/// backed by a PG directory and PG-local SQLite database.
/// v1-minimal: synchronous API (no async runtime).
pub trait ShardStore {
    fn write_shard(&self, key: &[u8], data: &[u8]) -> Result<WriteAck, StoreError>;
    fn read_shard(&self, key: &[u8]) -> Result<ShardData, StoreError>;
    fn delete_shard(&self, key: &[u8]) -> Result<(), StoreError>;
    fn stat_shard(&self, key: &[u8]) -> Result<ShardStat, StoreError>;
}

/// The storage node daemon manages multiple PG ShardStore instances.
/// It routes requests to the correct PG based on the pg_id in the request.
pub trait StorageNode {
    fn get_pg_store(&self, pg_id: u32) -> Result<&dyn ShardStore, StoreError>;
}
```

---

## Concurrency Model

### IO model

**v1-minimal: Synchronous IO throughout.** No async runtime, no Tokio. Plain blocking
file IO and blocking SQLite calls. The HTTP server uses thread-per-connection or a
simple thread pool. This eliminates async complexity and makes the codebase much
simpler for initial development.

**Post-v1-minimal**: When adding distribution (RPC, multi-node), revisit the IO model.
Options at that point: Tokio for network + spawn_blocking for disk, or io_uring for
true async disk IO. But for single-node v1-minimal, sync is the right choice.

### Disk-level concurrency

Each physical disk should have its own I/O queue/thread. Concurrent writes to the
same disk should be limited (SSDs handle ~32-128 concurrent ops well; HDDs should
serialize writes).

A storage node may manage multiple disks. Each disk is essentially an independent
shard store. The node daemon multiplexes across them. PG directories should ideally
be spread across disks for balanced I/O, but a single PG's shards should be on one
disk (simplifies the per-PG directory structure).

### PG-level concurrency

With per-PG metadata, each PG's SQLite database has its own write serialization
(SQLite single-writer). PGs on the same node are independent — writes to different
PGs don't contend on metadata locks. This gives natural per-PG write concurrency.

The PG primary serializes writes for conditional correctness (per-key
linearizability). This is per-PG, not per-node — different PGs on the same node
can accept writes concurrently.

---

## Memory Management

Following `guides/rust_memory_policy_strict.md`:

### ZONE_INIT
- Open per-PG SQLite connections (one per PG this node participates in).
- Pre-allocate shard I/O buffers (buffer pool).
- Per-PG startup recovery: scan each PG's directories for orphaned temp files.

### ZONE_HOT (shard read/write)
- Borrow a buffer from the pool (fixed-size, e.g. 4MB + header room).
- Read/write shard data into that buffer.
- Compute CRC64 over the buffer.
- Return buffer to pool on completion.
- No per-request heap allocation.

### Buffer pool design

Pre-allocate N buffers of size S at startup. N and S are configurable.
- N = max concurrent shard I/O operations.
- S = max shard size (default 4MB, matching EC stripe size).

If the pool is exhausted, new requests block (backpressure) rather than allocating
more buffers. This bounds memory usage regardless of load.

For O_DIRECT (if added later), buffers must be aligned to the filesystem block size
(typically 4KB). Use `std::alloc::Layout` with alignment.

---

## Filesystem Considerations

### XFS for data

- B-tree directories handle millions of files without performance degradation.
- Extent-based allocation minimizes fragmentation for large files.
- Delayed allocation + speculative preallocation for sequential writes.
- `mkfs.xfs` recommended options: `-n ftype=1` (d_type support for readdir filtering).
- Mount options: `noatime,nodiratime` (avoid metadata updates on read).

### Metadata volume

The SQLite database can be on the same filesystem or a separate one.

**Separate volume benefits**:
- Can use BTRFS or ZFS for the metadata volume — their checksumming protects the
  SQLite database file itself.
- SSD for metadata, HDD for data (if applicable).
- Separate I/O scheduling — metadata reads don't compete with data reads.

**Same volume**:
- Simpler operations. One disk to manage.
- Fine for all-SSD deployments.

**Recommendation**: Support both. Default to same volume. Document the separate-volume
setup for operators who want defense-in-depth.

---

## Startup and Recovery

On startup, the storage node reconciles each PG's metadata DB with its shard files.
This is done per PG, which enables parallel recovery across PGs:

For each PG directory:
1. **Delete orphaned temp files**: Any file in the PG's `tmp/` directory is removed.
2. **Detect orphaned shard files**: Scan PG's `shards/` directory for files not in
   the PG's `metadata.db`. Policy: delete them (they're from incomplete writes) or
   re-index them (compute CRC, add to metadata DB). Deleting is safer — if the
   metadata was never committed, the coordinator never acknowledged the write, so the
   caller will retry.
3. **Detect missing shard files**: Query PG's `metadata.db` for shards that should
   exist but whose data files are missing. Mark as Quarantined. Report to repair
   subsystem.
4. **Verify metadata DB integrity**: `PRAGMA integrity_check` on each PG's SQLite DB.
5. **Check epoch**: Compare this node's last-seen epoch against the global service's
   current epoch. If behind, fetch the latest cluster map to determine if any PG
   role changes have occurred (new primary, PG migration).

This scan should be fast (just readdir + metadata DB queries per PG, no data reads).
PGs can be recovered in parallel across threads. Full CRC verification is the scrub's
job, not startup's.

---

## Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("shard not found: key length {key_len}")]
    NotFound { key_len: usize },

    #[error("integrity error: expected CRC {expected:#018x}, got {actual:#018x}")]
    IntegrityError { expected: u64, actual: u64 },

    #[error("shard already exists: key length {key_len}")]
    AlreadyExists { key_len: usize },

    #[error("shard too large: {size} bytes exceeds limit {limit}")]
    ShardTooLarge { size: u64, limit: u64 },

    #[error("disk full: {available} bytes available, need {required}")]
    DiskFull { available: u64, required: u64 },

    #[error("I/O error on {operation}: {errno}")]
    Io { operation: &'static str, errno: i32 },

    #[error("metadata database error: {reason}")]
    MetadataDb { reason: &'static str },

    #[error("buffer pool exhausted")]
    Backpressure,

    #[error("node is shutting down")]
    ShuttingDown,
}
```

Note: no `String` fields. `Io` carries the raw errno rather than a `std::io::Error`
(which contains a heap-allocated message). The `operation` field is `&'static str`
("write", "read", "fsync", etc.).

---

## Configuration

```rust
pub struct StorageNodeConfig {
    /// Path to the data directory (PG directories with shard files).
    /// Layout: {data_dir}/pg-{NNNN}/shards/...
    pub data_dir: PathBuf,

    /// Optional separate path for per-PG metadata databases.
    /// If set, PG metadata lives at {metadata_dir}/pg-{NNNN}/metadata.db
    /// If None, metadata is at {data_dir}/pg-{NNNN}/metadata.db
    pub metadata_dir: Option<PathBuf>,

    /// Maximum shard size in bytes. Default: 4 * 1024 * 1024 (4MB).
    pub max_shard_size: u64,

    /// Number of pre-allocated I/O buffers. Default: 64.
    pub buffer_pool_size: usize,

    /// Size of each I/O buffer. Must be >= max_shard_size.
    pub buffer_size: usize,

    /// SQLite synchronous mode. Default: Normal.
    pub sync_mode: SyncMode, // Normal | Full

    /// Directory fanout depth (hex prefix) within PG shard dirs. Default: 1 (16 subdirs).
    pub fanout_depth: u8,

    /// Target scrub interval in days. Default: 30.
    pub scrub_interval_days: u32,

    /// Maximum concurrent shard I/O operations per disk.
    pub max_concurrent_io: usize,

    /// Whether to verify CRC on every read. Default: true.
    pub verify_on_read: bool,
}
```

---

## Observability

Metrics to expose (for Prometheus or similar):

- `shards_total` (gauge): Total shard count.
- `shards_bytes_total` (gauge): Total shard data bytes.
- `shard_write_duration_seconds` (histogram): Write latency.
- `shard_read_duration_seconds` (histogram): Read latency.
- `shard_write_bytes_total` (counter): Total bytes written.
- `shard_read_bytes_total` (counter): Total bytes read.
- `integrity_errors_total` (counter): CRC mismatches detected.
- `scrub_shards_verified` (counter): Shards verified by scrub.
- `scrub_errors_found` (counter): Corruption found by scrub.
- `buffer_pool_available` (gauge): Available buffers.
- `buffer_pool_waits_total` (counter): Times a request had to wait for a buffer.
- `disk_free_bytes` (gauge): Free space on data volume.

---

## Build Sequence for the Storage Node

1. **CRC64-NVME binding**: Add `crc64_rocksoft_refl` FFI binding to ec-sys crate,
   with a safe Rust wrapper. Verify with NVMe test vectors. This is a leaf dependency.
2. **ShardStore trait + in-memory implementation**: Define the per-PG API trait.
   Implement a `MemoryShardStore` for testing the layers above without disk I/O.
3. **Per-PG file store (file-per-shard)**: Implement `FileShardStore` backed by a PG
   directory. Write path (temp file → fsync → rename), read path (with CRC
   verification), delete. Each instance manages one PG directory.
4. **Per-PG SQLite metadata layer**: Integrate SQLite for the per-PG shard index
   and object records. Migration schema.
5. **StorageNode daemon**: Multiplexes across PG stores. Routes requests by pg_id.
   Manages PG lifecycle (create PG directory when assigned, clean up when migrated).
6. **Startup recovery**: Per-PG orphan cleanup, consistency check, epoch reconciliation.
7. **Scrub**: Background verification process per PG with I/O throttling.
8. **Buffer pool**: Pre-allocated buffer management for ZONE_HOT compliance.
9. **Integration tests**: End-to-end write → read → verify → delete cycles per PG,
   crash recovery simulation, concurrent operations across PGs, PG migration
   (directory lifecycle).

---

## Summary of Open Questions

| # | Question | Current Leaning | Alternatives |
|---|---|---|---|
| 0a | Where does S3 object metadata live? | Preferred: Architecture C2 + E (per-PG metadata index + embedded in shard data) | Centralized metadata cluster (Arch A/C), MinIO-style (Arch B) |
| 0b | How is metadata embedded? | Open: C1 (replicated header per shard) vs C2 (prepend to data before EC) | C1 gives single-shard self-description + independent integrity check; C2 is simpler for storage node |
| 1 | Shard identity model | Composite key (Option B) | Opaque UUID (Option A) |
| 2 | On-disk layout | Per-PG directories, file per shard (Option 1) | Packed files (Option 2) |
| 3 | Small object optimization | Defer; measure first | Inline in SQLite |
| 4 | Metadata DB | SQLite WAL mode | LMDB, RocksDB |
| 5 | Checksum storage | Metadata DB only | Inline with data |
| 6 | fsync strategy | fdatasync per shard | Group commit |
| 7 | O_DIRECT | No (use fadvise DONTNEED) | O_DIRECT for writes |
| 8 | CRC verify on read | Always | Configurable / sample |
| 9 | Deletion model | Eager | Lazy with grace period |
| 10 | RPC protocol | Defer (trait-based API first) | gRPC, HTTP/2, custom |
| 11 | IO model (v1-minimal) | Synchronous (no async runtime) | Tokio, io_uring (post-v1-minimal) |
| 12 | Streaming vs buffered writes | Buffered | Streaming for large shards |
