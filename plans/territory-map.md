# Territory Map: S3-Compatible Object Store (argmin)

## Context

The goal is to build a full, high-quality, reliable, easy-to-operate S3-compatible object storage system. Before writing any code, we need to map the full scope so we can identify clear, independently buildable subsystems. This document is a territory map — not an implementation plan — intended to subdivide the problem into pieces that can be tackled in order.

---

## Layer 1: S3 API Surface (User-Facing)

### Priority 1 — Core Object Operations (MVP)
| Operation | Notes |
|---|---|
| `PutObject` | Single-part, up to 5GB; checksums optional |
| `GetObject` | Range requests, conditional (If-Match, If-None-Match) |
| `DeleteObject` | Single object delete |
| `HeadObject` | Metadata only, no body |
| `ListObjectsV2` | Prefix/delimiter, pagination (ContinuationToken) |
| `CreateBucket` | Bucket creation |
| `DeleteBucket` | Must be empty |
| `ListBuckets` | Per-user bucket list |

### Priority 2 — Multipart Upload (needed for large objects and most real clients)
| Operation | Notes |
|---|---|
| `CreateMultipartUpload` | Returns UploadId |
| `UploadPart` | Min 5MB parts, max 10,000 parts |
| `CompleteMultipartUpload` | Assembles parts into object |
| `AbortMultipartUpload` | Cleans up staged parts |
| `ListMultipartUploads` | Lists in-progress uploads |
| `ListParts` | Lists parts for a given UploadId |

### Priority 3 — Versioning & Conditional Ops
| Operation | Notes |
|---|---|
| `PutBucketVersioning` | Enable/suspend versioning |
| `GetBucketVersioning` | |
| `ListObjectVersions` | List all versions |
| `DeleteObjects` (bulk) | Up to 1000 objects at once |
| Conditional PUT | `If-None-Match: *` for create-only semantics |

### Priority 4 — Nice to Have / Deferred
- Lifecycle policies
- Bucket notifications
- CORS
- Static website hosting
- Replication (cross-region)
- Object tagging
- ACLs (can start with owner-only or policy-based)
- Presigned URLs (important for clients, but straightforward)

### Auth Layer
- AWS Signature V4 (mandatory — all AWS SDKs use this)
- HMAC-SHA256, canonical request, credential scope
- Virtual-hosted-style vs. path-style bucket addressing

### Checksum Support
- AWS SDK sends CRC32, CRC32C, SHA1, SHA256, or CRC64-NVME
- CRC64-NVME as internal integrity checksum (attached automatically)
- Per-part and full-object checksums for multipart

---

## Layer 2: Internal Architecture

The system decomposes into these major internal layers:

```
┌─────────────────────────────────────────┐
│         HTTP Frontend (S3 API)          │  ← request parsing, auth, routing
├──────────────────┬──────────────────────┤
│  Global Service  │   Placement Layer    │  ← buckets, cluster map, PG state
│  (Raft, 3-5 n.)  │                      │
├──────────────────┴──────────────────────┤
│         Erasure Coding Layer            │  ← RS encode/decode, stripe assembly
├─────────────────────────────────────────┤
│    Storage Node Layer (per machine)     │  ← per-PG: shard I/O + object
│      per-PG metadata + shard data       │    metadata index + checksums
├─────────────────────────────────────────┤
│         Repair / Background Jobs        │  ← heal, rebalance, scrub, PG migration
└─────────────────────────────────────────┘
```

---

## Subsystem Breakdown (Proposed Work Packages)

### 1. Erasure Coding Engine
**What**: Pure library for Reed-Solomon encoding/decoding over GF(2^8), using Cauchy matrices.
**In**: `k` data shards + configurable `m` parity shards; arbitrary byte slices
**Out**: encoded shards; reconstruct from any `k` of `k+m` shards
**Dependencies**: none
**Notes**: Standalone, testable in isolation. Many existing RS libs (klauspost/reedsolomon in Go, reed-solomon-erasure in Rust) — evaluate reuse vs. write-from-scratch.

### 2. Placement / Topology
**What**: Given an object key and cluster topology, deterministically select which nodes hold which shards.
**In**: object key (or placement group ID), cluster map (nodes with weights/failure domains)
**Out**: ordered list of `k+m` node assignments, one per shard
**Dependencies**: none
**Notes**: CRUSH-inspired: pseudo-random, no per-object map needed. Rendezvous hashing (`score = -ln(U) / w`) as simpler alternative. Must handle node add/remove with minimal data movement and respect failure domain constraints (rack, machine, disk).

### 3. Storage Node — Shard I/O
**What**: Per-node shard storage layer: file-per-shard on XFS, CRC64-NVME integrity, per-PG directory layout.
**In**: shard writes/reads/deletes scoped by PG
**Out**: stored bytes, per-shard checksums
**Dependencies**: Erasure Coding Engine
**Notes**: Per-PG directories on disk (`pg-NNNN/shards/`, `pg-NNNN/metadata.db`, `pg-NNNN/tmp/`). Atomic writes (temp → fsync → rename). CRC64-NVME per shard. SQLite in WAL mode for per-PG shard index. ShardStore trait per PG, StorageNode multiplexes across PGs. See `plans/storage-node-design.md`.

### 4. Global Service (Raft)
**What**: Small Raft-replicated cluster (3-5 nodes) that owns global state: bucket table, cluster map with epoch, PG state.
**In**: bucket CRUD, cluster topology changes (node add/remove/reweight), PG migration lifecycle
**Out**: bucket metadata, cluster map (with monotonic epoch), PG-to-node mapping and migration status
**Dependencies**: Placement layer
**Notes**: Raft-replicated SQLite with application-level state machine (not WAL replication). Handles only global operations — not per-object metadata. Tables: `buckets`, `pg_state`, `cluster_maps`. Publishes cluster map epochs to all storage nodes. This is the only Raft group in the system. Tiny write volume (bucket ops + topology changes are rare). See `plans/metadata-cluster-design.md`.

### 5. Per-PG Metadata (on Storage Nodes)
**What**: Object metadata index co-located with shard data on each PG's storage nodes. Primary-based consensus with epoch fencing.
**In**: object metadata operations (put/get/delete/list) routed by PG
**Out**: object records (size, etag, last-modified, EC params, status), operation log for peering
**Dependencies**: Storage Node shard I/O, Global Service (for epoch, PG assignments)
**Notes**: PG primary (first node in placement output) serializes writes, replicates to secondaries. Epoch fencing prevents split-brain (replicas reject stale-epoch writes). Per-PG SQLite stores object records + oplog. Peering after primary failure reconciles state. User metadata (content-type, x-amz-meta-*) is NOT here — it's in shard data (C2 prepend). ListObjects = fan-out to all PG primaries, merge-sort (eventually consistent for v1). See `plans/metadata-cluster-design.md`.

### 6. Auth / IAM (minimal)
**What**: Credential management and request authentication.
**In**: `Authorization` header, `X-Amz-*` headers
**Out**: validated identity or 403/401 error
**Dependencies**: none (pure crypto)
**Notes**: Start with static access-key/secret pairs (sufficient for most use cases). AWS Signature V4 is HMAC-SHA256 over canonical request. Presigned URLs use same mechanism. Skip full IAM policies initially — owner-only or simple allow-all per key.

### 7. HTTP Frontend
**What**: S3-compatible HTTP/1.1 and HTTP/2 server.
**In**: raw HTTP requests
**Out**: properly formatted S3 XML responses
**Dependencies**: Auth layer, Global Service, Per-PG Metadata, Storage Nodes
**Notes**: Parse AWS Signature V4. Route bucket operations to Global Service. Route object operations by deriving PG from key, then to PG primary. Handle virtual-hosted-style (`bucket.host`) and path-style (`/bucket/key`). Return correct error XML (NoSuchKey, AccessDenied, etc.). Chunked transfer for large GET/PUT. ListObjects fans out to all PG primaries and merge-sorts.

### 8. Multipart Staging
**What**: Temporary storage for in-progress multipart uploads.
**In**: UploadId + PartNumber + data
**Out**: staged parts retrievable by UploadId
**Dependencies**: Storage Nodes, Per-PG Metadata
**Notes**: Parts use the same per-PG storage infrastructure as objects (tracked in per-PG metadata). On Complete, assemble into final EC-encoded object. On Abort, delete staged shards. Multipart upload records live in the PG of the target object key.

### 9. Repair / Background Maintenance
**What**: Detect and repair missing/corrupted shards; rebalance on node add/remove; periodic scrub; PG migration.
**In**: cluster health events, periodic triggers, PG migration state from Global Service
**Out**: healed shards, rebalanced data, migrated PGs
**Dependencies**: Placement, Erasure Coding, Storage Nodes, Global Service
**Notes**: Prioritize repairs that restore stripes to full width. Delay rebuilds during correlated outages (avoid burning parity unnecessarily). Bandwidth-limited repair to avoid starving client IO. PG migration: copy shard data + metadata from old nodes to new nodes per cluster map change, then update PG state in Global Service.

---

## Milestone: v1-minimal (single-node, in-process)

The first milestone is a single-process S3-compatible server that links all subsystems
together without distribution. No Raft, no RPC, no multi-node. This validates the
full data path end to end and allows testing with real S3 clients (aws-cli, boto3).

### What v1-minimal includes

- **S3 API**: Priority 1 operations — PutObject (single-part), GetObject (with range
  requests), DeleteObject, HeadObject, ListObjectsV2, CreateBucket, DeleteBucket,
  ListBuckets. AWS Signature V4 auth with static access key/secret pairs.
- **EC engine**: Encode/decode with ISA-L, default (4,2).
- **Placement**: Rendezvous hashing, but with a single-node cluster map (all shards
  go to the local node). PGs with fixed modulo (hash % pg_count).
- **Storage node**: Per-PG directories, file-per-shard on local disk, CRC64-NVME
  integrity, SQLite WAL for per-PG metadata (shard index + object records).
- **Metadata**: Per-PG SQLite databases hold object records directly. No Raft, no
  replication — single writer. Bucket table in a separate SQLite DB (placeholder
  for the future global service).
- **Sync IO**: Plain synchronous IO throughout. No async runtime (Tokio deferred to
  post-v1-minimal). Thread-per-connection or simple thread pool for HTTP. Sync file
  IO, sync SQLite. Simplifies the entire codebase for initial development.
- **All in one process**: HTTP server → EC encode → place (locally) → write shards
  to disk → commit metadata to per-PG SQLite. No RPC — just trait method calls.

### What v1-minimal defers

- Multi-node / distribution (Raft, RPC, replication, epoch fencing)
- Primary-based consensus and peering protocol
- Multipart uploads (Priority 2 — fast follow after v1-minimal works)
- Versioning (Priority 3)
- Scrub / repair / background maintenance
- PG migration and rebalancing
- Conditional PUT (If-None-Match)

### Why v1-minimal first

- Tests the full S3 data path with real clients before tackling distribution.
- Validates the per-PG on-disk layout, shard format, metadata schema, EC integration.
- Every trait (`ShardStore`, `PgMetadataStore`, `GlobalService`) gets an in-process
  implementation that becomes the test harness for multi-node later.
- Catches design mistakes early — easier to fix the shard format or schema now than
  after building replication on top.
- Usable immediately for development and testing of higher layers.

---

## Build Sequence

### v1-minimal build order

```
Phase 1 — leaf libraries (no deps, parallelizable):             [COMPLETE]
  1. Erasure Coding Engine      (pure math, ISA-L wrapper)        [done]
  2. Placement / Topology       (pure math, rendezvous hashing)   [done]
  3. CRC64-NVME                 (ISA-L crc64_rocksoft_refl)       [done]

Phase 2 — storage layer:                                         [COMPLETE]
  4. ShardStore trait + FileShardStore  (per-PG file I/O, CRC, SQLite shard index) [done]
  5. Per-PG metadata (local)           (object records in per-PG SQLite, no replication) [done]
  6. Bucket metadata (local)           (bucket table in SQLite, no Raft) [done]

Phase 3 — S3 server:                                            [COMPLETE]
  7. Auth / SigV4               (SigV4 verification with ring)             [done]
  8. HTTP Frontend              (tiny_http, S3 routing, XML responses)      [done]
  9. Coordinator                (S3 op → EC → PG → store → metadata)       [done]
```

### Phase 3 implementation notes

Deviations from the original plan discovered during implementation:

- **Placement crate not used**: v1-minimal PG derivation is just `rapidhash(bucket/key) %
  pg_count` — the full rendezvous hashing placer is for the distributed case (assigning
  shards to different nodes). Server depends on `rapidhash` directly, not the `placement`
  crate.
- **`PutObjectMetaReq` has no `last_modified` field**: The storage layer's `put_object_meta`
  upsert sets `last_modified` automatically. The coordinator doesn't need to compute it.
- **`HeadObject` calls `get_object` internally**: Rather than a separate flow, it just calls
  `get_object` and drops the body. Simpler and correct for v1-minimal where all shards are
  local. Optimization (read only shard 0 prefix) still deferred.
- **`GetObject` reads all k+m shards eagerly**: Rather than reading k data shards first and
  falling back to parity, the implementation reads all shards up front and tracks which
  succeed. Simpler for single-node where all shards are local.
- **`request.rs` returns `(S3Request, tiny_http::Request)` tuple**: `tiny_http::Request`
  consumes self in `respond()`, so the parsed request and the original request must both be
  returned. The plan didn't account for this ownership constraint.
- **ListObjectsV2 continuation token is just the last key**: Real S3 uses opaque
  base64-encoded tokens. The current implementation passes the last key as `start_after` to
  the storage layer. Will need fixing when real clients interact with the server.
- **`delete_bucket` emptiness check is duplicated**: The coordinator fans out to all PGs to
  check emptiness, but `SqliteBucketDb::delete_bucket` also has its own check. Only the
  coordinator check matters since per-PG metadata is separate from the bucket DB.
- **SigV4 signing key test vector was from `iam` service, not `s3`**: The standalone
  `derive_signing_key` test had a wrong expected value. The e2e signature verification tests
  (which use actual AWS-documented S3 signatures) pass, confirming correctness.

### Post-v1-minimal (distributed)

```
Phase 4 — distribution:
  10. Global Service (Raft)      (bucket DB, cluster map, PG state — replicated)
  11. Internal RPC protocol      (node-to-node shard I/O and metadata replication)
  12. Per-PG replication         (primary-based consensus, epoch fencing)
  13. Peering protocol           (primary failover recovery)

Phase 5 — completeness:
  14. Multipart uploads
  15. Versioning + conditional PUT
  16. Repair / scrub / background maintenance
  17. PG migration / rebalancing
```

### Notes on build order

- **Phase 1 items 1 and 2 are already implemented.** CRC64-NVME just needs an FFI
  binding added to ec-sys (ISA-L's `crc64_rocksoft_refl` is the same algorithm).
- **Phase 2 is the core of v1-minimal.** The storage layer with per-PG SQLite is the
  foundation everything else builds on. The same SQLite schema and ShardStore trait
  will be used in the distributed version — we're just skipping replication for now.
- **Phase 3 wires it all together.** The HTTP frontend and coordinator are the
  integration layer. The coordinator orchestrates: parse S3 request → prepend
  metadata (C2) → EC encode → derive PG → write shards → commit object record.
- **Phase 4 adds distribution.** The in-process trait implementations from v1-minimal
  become the "local" implementations. RPC adds remote variants. Raft adds the global
  service. Per-PG replication adds primary-backup consensus.

---

## Key Design Decisions

### Resolved

| Decision | Resolution |
|---|---|
| Language | **Rust** |
| Metadata architecture | **Per-PG metadata** (Ceph model). Global service for buckets + cluster map only |
| Per-PG consensus | **Primary-based with epoch fencing** (no per-PG Raft). Deferred to post-v1-minimal |
| Global service | **Raft-replicated SQLite** with application-level state machine. Deferred to post-v1-minimal |
| Placement algorithm | **Rendezvous hashing** (implemented) |
| PG count (v1-minimal) | **Fixed modulo** (hash % pg_count). Dynamic rendezvous later |
| EC parameters | Configurable, default **(4,2)**. **ISA-L** for encoding (implemented) |
| User metadata storage | **C2: prepend to data before EC**. Storage node sees opaque bytes |
| Metadata embedding | **C2 over C1**. Single-shard self-description (C1) not worth the storage node complexity |
| Shard identity | **Composite key**: object_key_hash ‖ version_id ‖ shard_index. Fixed-size, opaque to storage node |
| On-disk layout | **Per-PG directories, file per shard**. Raw block device out of scope |
| Local metadata DB | **SQLite WAL mode** (synchronous=NORMAL default) |
| Checksum storage | **Per-PG metadata DB only** (recomputable from data) |
| fsync strategy | **fdatasync per shard** |
| O_DIRECT | **No** — use posix_fadvise(DONTNEED) for large shards |
| CRC verify on read | **Always** |
| Deletion model | **Eager** (shard delete is final by the time it reaches storage node) |
| Raft state machine | **Application-level commands** (not WAL replication) |
| Raft library | **openraft** (needs evaluation for snapshot/membership support) |
| Version ID format | **ULID** (lexicographic sort = chronological) |
| GC timing | **Batched + rate-limited** |
| IO model (v1-minimal) | **Synchronous** (plain sync IO, no async runtime). Async deferred to post-v1-minimal |
| Streaming vs buffered | **Buffered** (4MB max shard, manageable memory) |
| Global service reads | **Leader reads** (simplest, low traffic on global service) |
| Etag calculation | **CRC64-NVME** for single-part uploads. Not MD5 (deprecated). Multipart composite etag TBD |
| Etag storage | **Binary BLOB, max 64 bytes** + etag_kind discriminator (u8) |
| Consistency model | **Per-key strong** (mandatory). Eventually consistent LIST (acceptable for v1) |
| Wire protocol (internal) | **Trait-based API first**. In-process for v1-minimal. RPC added post-v1-minimal |
| Encryption at rest | **LUKS at filesystem level** first. Per-object encryption deferred |

### Still open (needed before v1-minimal coding starts)

| Decision | Options | Notes |
|---|---|---|
| Shard key byte format | Exact encoding of composite key | Hash width, version field size, byte order. Small decision |
| PG write quorum (distributed) | All replicas / majority | Deferred to post-v1-minimal. Single-node has no quorum |
| v1-minimal S3 scope | Priority 1 only / include multipart | Multipart is a fast follow but most clients need it |

---

## Out of Scope (for now)

- Cross-region replication
- Lifecycle policies
- Object tagging
- Bucket notifications / SNS/SQS
- Static website hosting
- Full IAM policies (roles, resource policies)
- Object Lock / WORM
- Intelligent tiering / auto-tiering (design for it, don't build yet)
