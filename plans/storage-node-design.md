# Storage Node — Design Document

## Scope

This document covers the design of the per-node storage daemon (subsystem 3 in the
build sequence). The storage node is responsible for storing erasure-coded shards on
local disk, maintaining per-shard integrity checksums, and exposing a simple internal
API for shard read/write/delete/verify operations.

The storage node does NOT handle:
- Object-level logic (that's the metadata cluster)
- Placement decisions (that's the placement layer)
- Erasure coding (that's the EC engine, used by the coordinator/frontend)
- S3 protocol (that's the HTTP frontend)

It is a shard-level key-value store with integrity guarantees.

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

### Preferred: Architecture C (hybrid)

**Current leaning**: Architecture C — metadata cluster as the primary source for
namespace operations, with object metadata also preserved alongside shard data for
disaster recovery. C1 (replicated header per shard) vs C2 (prepend to data before EC)
is an open sub-question. Not yet a final decision.

The storage node remains a simple shard store for normal operations. Only a disaster
recovery tool needs to parse metadata from shards.

Normal operation:
- HeadObject, ListObjects → metadata cluster only (fast).
- GetObject → metadata cluster for routing, then storage nodes for shard data.
- PutObject → storage nodes receive shards, metadata cluster gets the namespace entry.

Disaster recovery:
- Scan shards on all nodes, extract/reconstruct metadata, rebuild the metadata cluster.

**Arguments for Architecture C over A**: Losing the metadata cluster would mean
permanent loss of user-defined metadata, content-types, and the namespace — even
though the raw data bytes survive in the shards.

**Arguments for Architecture A over C**: Simpler storage node (especially vs C1). If
the metadata cluster has its own robust replication and backup strategy
(Raft-replicated SQLite with periodic snapshots), the risk of total metadata loss may
be low enough that the extra complexity isn't justified.

Architecture B (MinIO-style, no metadata cluster) is not preferred because it makes
ListObjects expensive and couples the storage node to object-level semantics. MinIO
also replicates the full metadata to every node (k+m copies), does not integrity-check
it, and requires every node to act as a metadata server — none of which we want.

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

**Recommendation**: Option A or B. Lean toward B for repairability, but this depends
on how the metadata cluster tracks versions and multipart uploads. Final decision can
be deferred until the metadata cluster design is done — the storage node just needs
`&[u8]` as a key.

### Shard key format (if using Option B)

```
shard_key = object_key_hash (16 bytes) || version_id (8 bytes) || shard_index (1 byte)
```

25 bytes total. Fixed-size, no parsing needed. The storage node treats this as opaque.

---

## On-Disk Layout

This is the most consequential design decision. Options from simplest to most complex:

### Option 1: One file per shard (filesystem-managed)

```
/data/<prefix>/<shard_key_hex>
```

Where `<prefix>` is the first 2-4 hex chars of the shard key (fanout to avoid large
directories).

- Example: `/data/a3/b7/a3b7f2...89_03` (shard index 3 of object a3b7f2...89)
- Shard data is the file contents.
- CRC64-NVME stored in the local metadata DB (not inline with data).

**Write path**: write to temp file in same directory → fsync → rename → fsync parent
dir. Atomic on POSIX filesystems.

**Pros**:
- Simplest possible implementation. Easy to debug (ls, hexdump, du).
- Filesystem handles space allocation, directory indexing, free space.
- Works on any POSIX filesystem.
- cp, rsync, standard tools all work for manual recovery.

**Cons**:
- Inode overhead: each shard consumes an inode. For small objects with (4,2) EC, a
  1KB object creates 6 files of ~170 bytes each. Inode overhead dominates.
- Directory scaling: even with fanout, millions of files stress the filesystem's
  directory implementation (XFS B-tree handles this better than ext4 HTree).
- Metadata overhead: stat() per shard on read. Filesystem metadata operations are not
  free.
- Fragmentation: many small files fragment sequential read patterns.

**Mitigation**: XFS handles large file counts well (B-tree directories, efficient
extent allocation). Set `mkfs.xfs -n ftype=1` and use directory fanout. For a node
with 10TB and average shard size of 1MB, that's ~10M files — well within XFS's
comfortable range.

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

Keep shard data as files (Option 1 or 2), but store the local metadata (shard
index, checksums, status) in a SQLite database on a separate filesystem or volume.

This is the approach described in the territory map ("XFS recommended for data,
BTRFS/ZFS for metadata"). The separate filesystem for metadata allows:
- Using BTRFS/ZFS checksumming for the metadata DB itself (defense in depth).
- Separate I/O scheduling for metadata vs data.
- Smaller metadata volume that benefits from being on SSD even if data is on HDD.

**This is the recommended starting point** — simple file-per-shard for data (Option
1), SQLite for metadata, potentially on separate volumes.

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

Each storage node maintains a local database tracking which shards it holds and their
integrity state.

### What the metadata DB stores

Per shard:
- `shard_key` (primary key, variable-length bytes)
- `data_size` (u64, bytes)
- `crc64_nvme` (u64, computed at write time)
- `created_at` (u64, unix timestamp or monotonic)
- `last_verified` (u64, timestamp of last scrub pass)
- `status` (u8: Live, Deleting, Quarantined)
- `data_path` (optional, if not derivable from shard_key — for Option 2/3)

Node-level:
- `node_id` (u32, this node's identity)
- `disk_id` (u32, if multiple disks per node)
- `format_version` (u16)
- Total shard count, total bytes (derived, cached)

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

Need a Rust CRC64-NVME implementation. Options:
- `crc64fast` crate: CRC64-ECMA, not CRC64-NVME. Wrong polynomial.
- Roll our own using the NVMe polynomial (0xAD93D23594C93659, reflected).
- Use a crate that supports the NVMe polynomial if one exists.
- Hardware acceleration via `crc` intrinsics on ARM, table-based on x86 for now.

This needs investigation. The polynomial and table generation are straightforward but
correctness matters — need known test vectors from the NVMe spec.

---

## Write Path

### Single shard write

```
1. Receive shard data + shard_key over RPC
2. Validate: shard_key length, data size within limits
3. Compute CRC64-NVME over shard data
4. Write shard data to temp file (O_WRONLY | O_CREAT | O_EXCL)
5. fsync temp file
6. Rename temp file to final path
7. fsync parent directory
8. Insert metadata record into SQLite (shard_key, size, crc64, timestamp)
9. Return success + crc64 to caller
```

### Failure modes

- **Crash between step 5 and 6**: Temp file exists on disk. On startup, scan for and
  delete orphaned temp files.
- **Crash between step 6 and 8**: Data file exists but no metadata record. On startup,
  scan data directory for files not in the metadata DB — either add them (recompute
  CRC) or delete them, depending on policy.
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
1. Receive shard_key over RPC
2. Update metadata: status = Deleting
3. unlink data file
4. Delete metadata record
5. Return success
```

Deletes are idempotent: deleting a non-existent shard returns success.

### Open question: Lazy vs eager deletion

**Eager** (above): Delete immediately. Simple.

**Lazy**: Mark as deleted in metadata, background process unlinks files. Allows
undeletion within a grace period. Adds complexity.

**Recommendation**: Eager deletion for now. The metadata cluster handles versioning
and soft-delete semantics at the object level. By the time a delete reaches the
storage node, it should be final.

---

## Scrub (Background Verification)

A background process that reads every shard and verifies its CRC64. This catches
silent data corruption (bit rot) that wouldn't be detected until a client reads the
shard.

```
for each shard in metadata DB where status == Live:
    read shard data from disk
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
- **Ordering**: Sequential scan of data directory for sequential disk reads. Don't
  randomize (random I/O on HDD is catastrophic for scrub throughput).

---

## Internal API

The storage node exposes a simple RPC API. This is NOT the S3 API — it's an internal
protocol between the coordinator/frontend and the storage nodes.

### Operations

```
WriteShard(shard_key: bytes, data: bytes) → Result<WriteAck, Error>
    WriteAck { crc64: u64, stored_size: u64 }

ReadShard(shard_key: bytes) → Result<ShardData, Error>
    ShardData { data: bytes, crc64: u64 }

DeleteShard(shard_key: bytes) → Result<(), Error>

StatShard(shard_key: bytes) → Result<ShardStat, Error>
    ShardStat { size: u64, crc64: u64, created_at: u64, last_verified: u64 }

ListShards(prefix: bytes, cursor: bytes, limit: u32) → Result<ShardList, Error>
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
#[async_trait]
pub trait ShardStore {
    async fn write_shard(&self, key: &[u8], data: &[u8]) -> Result<WriteAck, StoreError>;
    async fn read_shard(&self, key: &[u8]) -> Result<ShardData, StoreError>;
    async fn delete_shard(&self, key: &[u8]) -> Result<(), StoreError>;
    async fn stat_shard(&self, key: &[u8]) -> Result<ShardStat, StoreError>;
}
```

---

## Concurrency Model

### Open question: Async runtime

The EC engine is deliberately sync (pure CPU work). The storage node is I/O-bound
(disk reads/writes, network). An async runtime is natural here.

Options:
1. **Tokio**: De facto standard. Excellent ecosystem. Large dependency.
2. **Thread pool + blocking I/O**: Simpler. One thread per disk. No async complexity.
   But: less efficient for the network side (need separate threads for network I/O).
3. **io_uring (tokio-uring or glommio)**: Maximum I/O efficiency. Linux-only. Less
   mature ecosystem.

**Consideration**: The storage node will eventually need network I/O (RPC) and disk
I/O. Tokio handles both. However, disk I/O on Linux is not truly async — `tokio::fs`
uses a thread pool internally. `io_uring` is the only way to get real async disk I/O
on Linux.

**Recommendation**: Tokio for the network layer. For disk I/O, use
`tokio::task::spawn_blocking` with a bounded thread pool (one thread per disk is the
simplest model). Consider io_uring later if disk I/O scheduling becomes a bottleneck.

### Disk-level concurrency

Each physical disk should have its own I/O queue/thread. Concurrent writes to the
same disk should be limited (SSDs handle ~32-128 concurrent ops well; HDDs should
serialize writes).

A storage node may manage multiple disks. Each disk is essentially an independent
shard store. The node daemon multiplexes across them.

---

## Memory Management

Following `guides/rust_memory_policy_strict.md`:

### ZONE_INIT
- Open SQLite connections.
- Pre-allocate shard I/O buffers (buffer pool).
- Scan data directory for orphaned temp files.

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

On startup, the storage node must reconcile its metadata DB with the actual files on
disk:

1. **Delete orphaned temp files**: Any file matching the temp pattern (e.g.
   `.shard.tmp.*`) is removed.
2. **Detect orphaned data files**: Scan data directory for files not in the metadata
   DB. Policy: delete them (they're from incomplete writes) or re-index them (compute
   CRC, add to metadata DB). Deleting is safer — if the metadata was never committed,
   the coordinator never acknowledged the write, so the caller will retry.
3. **Detect missing data files**: Query metadata DB for shards that should exist but
   whose data files are missing. Mark as Quarantined. Report to repair subsystem.
4. **Verify metadata DB integrity**: `PRAGMA integrity_check` on SQLite.

This scan should be fast (just readdir + metadata DB queries, no data reads). Full
CRC verification is the scrub's job, not startup's.

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
    /// Path to the data directory (shard files).
    pub data_dir: PathBuf,

    /// Path to the metadata database. Defaults to data_dir/metadata.db.
    pub metadata_db_path: Option<PathBuf>,

    /// Maximum shard size in bytes. Default: 4 * 1024 * 1024 (4MB).
    pub max_shard_size: u64,

    /// Number of pre-allocated I/O buffers. Default: 64.
    pub buffer_pool_size: usize,

    /// Size of each I/O buffer. Must be >= max_shard_size.
    pub buffer_size: usize,

    /// SQLite synchronous mode. Default: Normal.
    pub sync_mode: SyncMode, // Normal | Full

    /// Directory fanout depth (hex prefix). Default: 2 (256 subdirectories).
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

1. **CRC64-NVME implementation**: Find or write a correct CRC64-NVME implementation
   with test vectors from the NVMe spec. This is a leaf dependency.
2. **ShardStore trait + in-memory implementation**: Define the API trait. Implement a
   `MemoryShardStore` for testing the layers above without disk I/O.
3. **Local file store (file-per-shard)**: Implement `FileShardStore` with the write
   path (temp file → fsync → rename), read path (with CRC verification), delete.
4. **SQLite metadata layer**: Integrate SQLite for the shard index. Migration schema.
5. **Startup recovery**: Orphan cleanup, consistency check.
6. **Scrub**: Background verification process with I/O throttling.
7. **Buffer pool**: Pre-allocated buffer management for ZONE_HOT compliance.
8. **Integration tests**: End-to-end write → read → verify → delete cycles, crash
   recovery simulation, concurrent operations.

---

## Summary of Open Questions

| # | Question | Current Leaning | Alternatives |
|---|---|---|---|
| 0a | Where does S3 object metadata live? | Preferred: Architecture C (metadata cluster + embedded metadata for DR) | Pure shard KV (Arch A), MinIO-style (Arch B) |
| 0b | How is metadata embedded? | Open: C1 (replicated header per shard) vs C2 (prepend to data before EC) | C1 gives single-shard self-description + independent integrity check; C2 is simpler for storage node |
| 1 | Shard identity model | Composite key (Option B) | Opaque UUID (Option A) |
| 2 | On-disk layout | File per shard (Option 1) | Packed files (Option 2) |
| 3 | Small object optimization | Defer; measure first | Inline in SQLite |
| 4 | Metadata DB | SQLite WAL mode | LMDB, RocksDB |
| 5 | Checksum storage | Metadata DB only | Inline with data |
| 6 | fsync strategy | fdatasync per shard | Group commit |
| 7 | O_DIRECT | No (use fadvise DONTNEED) | O_DIRECT for writes |
| 8 | CRC verify on read | Always | Configurable / sample |
| 9 | Deletion model | Eager | Lazy with grace period |
| 10 | RPC protocol | Defer (trait-based API first) | gRPC, HTTP/2, custom |
| 11 | Async runtime | Tokio + spawn_blocking | Thread pool, io_uring |
| 12 | Streaming vs buffered writes | Buffered | Streaming for large shards |
