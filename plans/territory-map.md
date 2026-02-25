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

## Dependency Order (Suggested Build Sequence)

```
Phase 1 — leaf libraries (no deps, parallelizable):
  1. Erasure Coding Engine      (pure math, ISA-L wrapper)
  2. Placement / Topology       (pure math, rendezvous hashing)
  3. Auth / IAM                 (pure crypto, SigV4)

Phase 2 — core services (deps on Phase 1, parallelizable):
  4a. Storage Node shard I/O    (deps: EC Engine)
  4b. Global Service (Raft)     (deps: Placement)

Phase 3 — integrated metadata:
  5. Per-PG Metadata            (deps: 4a + 4b — integrates shard I/O with
                                 epoch fencing and PG assignments from Global Service)

Phase 4 — user-facing:
  6. HTTP Frontend              (deps: Auth, Global Service, Per-PG Metadata, Storage)
  7. Multipart Staging          (deps: Storage, Per-PG Metadata)

Phase 5 — background:
  8. Repair / Background        (deps: all of the above)
```

### Notes on build order

- **4a and 4b are independent and can be built in parallel.** The storage node shard
  I/O (file operations, CRC, SQLite shard index) doesn't need the global service.
  The global service (Raft, bucket DB, cluster map) doesn't need shard I/O.
- **Phase 3 integrates them**: the per-PG metadata layer adds primary-based consensus,
  epoch fencing, object records, and the peering protocol on top of the shard store,
  using epochs and PG assignments from the global service.
- **The global service is small but foundational.** It's the single source of truth for
  cluster topology. Everything else derives PG assignments and epochs from it.

---

## Key Design Decisions (Resolved or Leaning)

| Decision | Options | Status |
|---|---|---|
| Language | Rust / Go | **Rust** |
| Metadata architecture | Centralized Raft cluster / per-PG metadata | **Leaning per-PG** (Ceph model). Global service for buckets + cluster map only |
| Per-PG consensus | Per-PG Raft / primary-based with epoch fencing | **Primary-based with epoch fencing** (no per-PG Raft) |
| Global service | SQLite+Raft | **Raft-replicated SQLite** with application-level state machine |
| Placement algorithm | CRUSH / Rendezvous hashing | **Rendezvous hashing** (implemented) |
| Placement groups | Yes / No | **Yes** — dynamic PG count via rendezvous hashing over PG set (~100-200 PGs/node) |
| EC parameters | (k, m) — e.g., (4,2), (6,3), (8,4) | Configurable, default (4,2). **ISA-L** for encoding |
| User metadata storage | In metadata index / prepended to shard data | **C2: prepend to data before EC** (metadata cluster stays lean) |
| Consistency model | Strong / eventual | **Per-key strong** (mandatory). Eventually consistent LIST (v1) |
| Etag format | Hex string / binary BLOB | **Binary BLOB, max 64 bytes** + etag_kind discriminator |
| Wire protocol (internal) | gRPC / HTTP/2 / custom | Defer — trait-based API first, RPC later |
| Encryption at rest | LUKS / per-object SSE-C | LUKS at filesystem level first |

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
