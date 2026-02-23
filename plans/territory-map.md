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
├─────────────────────────────────────────┤
│          Metadata Layer                 │  ← bucket/object namespace, versions
├────────────────────┬────────────────────┤
│   Placement Layer  │  Multipart Staging │  ← where does this data live?
├────────────────────┴────────────────────┤
│         Erasure Coding Layer            │  ← RS encode/decode, stripe assembly
├─────────────────────────────────────────┤
│    Storage Node Layer (per machine)     │  ← local reads/writes, checksums
├─────────────────────────────────────────┤
│         Repair / Background Jobs        │  ← heal, rebalance, scrub
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

### 3. Storage Node (local)
**What**: Per-node daemon that stores shard data and its own metadata.
**In**: shard writes/reads over internal RPC
**Out**: stored bytes, per-shard checksums
**Dependencies**: Erasure Coding Engine
**Notes**: File layout on disk (XFS recommended for data, BTRFS/ZFS for metadata). Atomic writes. CRC64-NVME attached to every shard. Local metadata DB (SQLite or LMDB — LMDB fragile on crash, SQLite safer). Expose simple internal API (not S3).

### 4. Metadata Cluster
**What**: Distributed store of bucket/object namespace: bucket→objects, object→version list, version→shard placement.
**In**: namespace operations (put, get, delete, list, version-list)
**Out**: object metadata (etag, size, checksum, shard locations, content-type, user-metadata)
**Dependencies**: Placement layer
**Notes**: Options: Raft-replicated SQLite (Litestream-style), co-located quorum per shard group, or simple leader-replicated. For simplicity, start with Raft/replicated SQLite. Separation of metadata from data is key for small-object performance.

### 5. HTTP Frontend
**What**: S3-compatible HTTP/1.1 and HTTP/2 server.
**In**: raw HTTP requests
**Out**: properly formatted S3 XML responses
**Dependencies**: Auth layer, Metadata Cluster, Storage Nodes
**Notes**: Parse AWS Signature V4. Route to correct bucket/object operations. Handle virtual-hosted-style (`bucket.host`) and path-style (`/bucket/key`). Return correct error XML (NoSuchKey, AccessDenied, etc.). Chunked transfer for large GET/PUT.

### 6. Auth / IAM (minimal)
**What**: Credential management and request authentication.
**In**: `Authorization` header, `X-Amz-*` headers
**Out**: validated identity or 403/401 error
**Dependencies**: none (pure crypto)
**Notes**: Start with static access-key/secret pairs (sufficient for most use cases). AWS Signature V4 is HMAC-SHA256 over canonical request. Presigned URLs use same mechanism. Skip full IAM policies initially — owner-only or simple allow-all per key.

### 7. Multipart Staging
**What**: Temporary storage for in-progress multipart uploads.
**In**: UploadId + PartNumber + data
**Out**: staged parts retrievable by UploadId
**Dependencies**: Storage Node layer
**Notes**: Parts can use the same storage infrastructure as objects (just different namespace). Track via metadata. On Complete, assemble into final EC-encoded object. On Abort, delete staged shards.

### 8. Repair / Background Maintenance
**What**: Detect and repair missing/corrupted shards; rebalance on node add/remove; periodic scrub.
**In**: cluster health events, periodic triggers
**Out**: healed shards, rebalanced data
**Dependencies**: Placement, Erasure Coding, Storage Nodes
**Notes**: Prioritize repairs that restore stripes to full width. Delay rebuilds during correlated outages (avoid burning parity unnecessarily). Bandwidth-limited repair to avoid starving client IO.

---

## Dependency Order (Suggested Build Sequence)

```
1. Erasure Coding Engine      (no deps, pure math)
2. Placement / Topology       (no deps, pure math)
3. Auth / IAM                 (no deps, pure crypto)
4. Storage Node               (deps: EC Engine)
5. Metadata Cluster           (deps: Placement)
6. Multipart Staging          (deps: Storage Node, Metadata)
7. HTTP Frontend              (deps: Auth, Metadata, Storage Nodes)
8. Repair / Background        (deps: all of the above)
```

---

## Key Design Decisions to Resolve Early

| Decision | Options | Current Leaning |
|---|---|---|
| Language | Rust / Go | Notes reference both; Go for simplicity |
| Metadata store | SQLite+Raft / LMDB / distributed | Replicated SQLite (simpler) |
| Placement algorithm | CRUSH / Rendezvous hashing | Rendezvous for simplicity |
| EC parameters | (k, m) — e.g., (4,2), (6,3), (8,4) | Configurable, default (4,2) |
| Consistency model | Strong / eventual | Strong preferred |
| Wire protocol (internal) | gRPC / HTTP/2 / custom | HTTP/2 + simple binary |
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
