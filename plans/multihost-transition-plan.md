# Multihost Storage Transition Plan

Status: active

## Context

The current storage stack has been refactored enough that the coordinator mostly
talks to storage through higher-level request operations instead of composing raw
PG access directly. That makes this the right point to start moving from the
single-process, single-disk model toward a multihost model.

This plan is deliberately staged. The first goal is not to make a production
distributed system in one jump. The first goal is to introduce the right
cluster-shaped internal boundaries while preserving current S3 behavior and test
coverage.

The immediate direction is:

1. keep one externally visible S3 service
2. introduce multiple independent local storage nodes first
3. make placement and shard routing node-aware
4. add epoch fencing and PG ownership before real failures
5. only then move to local multi-process and remote internal connections

This avoids mixing storage layout changes, distributed coordination, transport,
failure handling, and internal auth in the same step.

## Constraints

1. AWS-visible S3 behavior must not change except for separately justified
   compatibility fixes.
2. Bucket metadata remains PG-sharded unless there is a deliberate later redesign.
3. Object metadata remains per-PG metadata, not a centralized object metadata
   service.
4. Data becomes multihost by placing individual EC shards on storage nodes.
5. No two processes may share the same PG directory or local SQLite metadata DB.
6. The system must not rely on process-local locks once more than one process can
   act on the same logical PG.
7. The configured cluster must have enough eligible nodes for the configured EC
   shape before node-aware shard placement is enabled.
8. Every phase must have clear exit criteria and must keep the full local S3 test
   suite passing.
9. New production dependencies, especially for RPC or consensus, need a separate
   decision before adoption.

## Current Model

The implemented model is still a local cluster collapsed into one process.

1. `argmin-s3` opens one `StorageCluster` and shares it across all worker
   coordinators.
2. The local cluster opens one `SharedStorageNode` per configured local node
   under `node-<id>` data directories, with node 0 acting as the initial
   metadata primary.
3. `PgTopology` is a sorted list of local PG IDs with hash-modulo routing.
4. Bucket metadata, object metadata, multipart state, stream sessions, shard
   rows, and reclaim rows live in per-PG SQLite databases.
5. Per-PG `Mutex<PgStore>` guards serialize local metadata mutations.
6. The write path already has the right visibility order:
   - write shard files durably
   - then publish metadata rows
7. The current EC data layout is still interim:
   - one segment is encoded into `k + m` shards
   - all shards for that segment are written as one local batch to one shard PG
8. Reads use an object metadata snapshot plus process-local payload generation
   leases to protect in-flight reads from physical cleanup.
9. Reclaim rows are durable, but the active reclaim queue and execution leases are
   process-local.
10. The placement crate is used to compute shard locations, but payload IO does
    not yet dispatch shards to those locations.

This model is good for single-host correctness work, but the process boundary is
currently part of the correctness story. That is the main thing this plan has to
remove.

## Main Gaps

### Cluster State

There is no cluster map, node membership table, PG acting set, PG primary, PG
state, or monotonic cluster epoch.

Existing generation concepts are not substitutes:

- object `GenerationId` is an object payload generation
- bucket execution generations are cache and reservation freshness mechanisms
- neither one can fence stale cluster operations

The multihost model needs a distinct `ClusterEpoch`.

### Placement

Current PG routing is local hash-modulo routing. The placement crate provides a
weighted rendezvous node-placement primitive, but it is not connected to storage.

The current segment placement also needs correction before it becomes the
distributed data layout. Segment data placement should follow the bounded
object-local PG-set direction from `plans/segment-pg-placement-tradeoffs.md`,
rather than scattering based on transient stream/session details.

### Metadata Replication

Per-PG SQLite metadata exists only on the local process that owns the current
`PgStore`. There is no PG command log, replica apply path, peering state, stale
epoch rejection, or recovery protocol.

This is the hardest correctness step. Until it exists, only one local owner may
mutate a logical PG.

### Metadata Integrity

Shard data has explicit checksums, but metadata does not yet have an equivalent
end-to-end integrity model. SQLite can detect some local database corruption, but
that is not enough for a replicated PG. The system also needs to detect semantic
replica divergence, for example:

1. a replica has a corrupted row that still satisfies SQLite constraints
2. a replica missed or reordered a metadata command
3. two replicas return different object or bucket metadata for the same key
4. a stale replica appears current after restart

The metadata replication design must therefore include command integrity and
state comparison. The safe default is fail-closed:

- if metadata replicas disagree in a way that cannot be resolved from an
  authoritative command log or quorum decision, the PG should enter an
  inconsistent or peering state rather than serving conflicting metadata

### Temporary Node Unavailability

The plan needs an explicit policy for nodes that are temporarily unavailable but
have not yet been declared permanently failed or removed from the acting set.

Reads are comparatively straightforward: if enough shards are reachable to
reconstruct the object, reads can proceed, subject to the PG being in a state
that allows reads.

Writes are a harder design choice. Some systems allow degraded writes that commit
an object while one or more target shards are missing. Other systems reject writes
until all target shards can be written, or write missing shards to temporary
handoff locations.

The early multihost phases should use the conservative rule:

- writes require all target data shards, metadata replicas, and required commit
  records to be durable before success

That rule can be relaxed later, but only after the metadata format, repair
priority, and failure semantics explicitly support it.

### Process-Local Coordination

These are currently correctness mechanisms and therefore must be moved behind
cluster-owned or PG-primary-owned mechanisms:

1. PG mutexes
2. ordered two-PG locking helpers
3. bucket write drain waits
4. multipart completion locks
5. object payload generation leases
6. in-memory reclaim work queue
7. bucket cache invalidation and freshness paths
8. any tests that inspect or mutate raw local PG state as if it were global

### Background Work

Background workers currently run as local node work. In a multihost system, most
background work must be owned by the relevant PG primary or by a durable claimant
protocol:

1. reclaim
2. lifecycle expiration
3. multipart cleanup
4. shard repair
5. PG backfill and migration
6. scrub

Durable rows should be the source of truth; in-memory queues may only be
optimizations.

## Target Architecture

The target shape introduces a cluster boundary below the coordinator.

```text
S3 coordinator
    |
    v
StorageCluster
    |
    +-- ControlPlane
    |     - cluster epoch
    |     - node map
    |     - PG count
    |     - PG state
    |     - PG acting sets
    |
    +-- PgRouter
    |     - bucket PG derivation
    |     - object metadata PG derivation
    |     - data PG derivation
    |     - route to PG primary
    |
    +-- PgPrimaryService
    |     - serialized metadata commands for one PG
    |     - command replication
    |     - command checksums and state digests
    |     - epoch fencing
    |     - peering state
    |
    +-- ShardPlacement
    |     - map data PG and shard index to NodeId
    |     - use placement crate
    |
    +-- ShardNodeClient
          - write shard
          - read shard
          - delete shard
          - local implementation first, RPC later
```

The initial implementation can keep these as in-process Rust calls. The important
step is to make the boundary explicit and shaped like the eventual distributed
system.

## Core Types

The distributed boundary should make these concepts explicit and hard to confuse:

1. `NodeId`
2. `ClusterEpoch`
3. `PgId`
4. `BucketPgId`
5. `ObjectMetadataPgId`
6. `DataPgId`
7. `PgActingSet`
8. `PgPrimary`
9. `PgState`
10. `ShardIndex`
11. `ShardLocation`

`GenerationId` should remain object-payload terminology. It should not be reused
for cluster-map versions.

Phase 0 adds the storage-layer newtypes that are useful before behavior changes:
`ClusterEpoch`, `PgId`, `BucketPgId`, `ObjectMetadataPgId`, `DataPgId`, and
`ShardIndex`. `NodeId` already exists in the placement crate. `ShardLocation` is
deferred until node-aware shard placement can include the actual `NodeId` and
epoch/acting-set information it needs.

## Placement Model

The target placement model should keep these rules:

1. bucket metadata PG is derived from bucket name
2. object metadata PG is derived from bucket name and object key
3. data PGs are derived from object generation identity using a bounded
   object-local PG set
4. segment bands map onto that bounded data PG set
5. each data PG maps to a node acting set under the current cluster map
6. each EC shard index maps to one node in that acting set
7. request fan-out for one object must not scale with total cluster PG count

The segment metadata should continue to identify the data PG for a segment or
band. Reads and migration should use PG state and cluster-map history to find old
or new shard locations. Avoid adding a per-object placement generation as the
main routing mechanism; that pushes rebalance work into object metadata updates
and defeats the PG model.

## Required Invariants

1. Object visibility is still controlled by metadata rows, not by shard files.
2. A successful write publishes metadata only after required shard writes are
   durable.
3. A stale cluster epoch cannot mutate PG metadata or shard state.
4. A logical PG has one primary for a given epoch.
5. Replicas reject commands from non-primary or stale-epoch senders.
6. `ListBuckets`, `ListObjects`, and related fanout operations either use a
   coherent enough set of active PG primaries or fail; they must not silently
   return partial results.
7. Metadata replicas must not serve or acknowledge state that fails command-log
   integrity or replica-state validation.
8. Metadata replica divergence must move the PG into a repair or peering state
   unless an authoritative log or quorum rule can resolve it safely.
9. Physical cleanup must not remove shards that can still be read by an
   in-flight valid reader.
10. Reclaim and repair are idempotent and can resume from durable state.
11. No implementation phase may require multiple processes to share one local PG
   directory.

## Phase 0: Plan And Naming Cleanup

Document the intended terminology and decide the names before changing behavior.

Work items:

1. add the core type wrappers or aliases where they clarify illegal states
2. decide which existing `PgId` uses mean metadata PG versus data PG
3. decide the first static cluster config format for local development
4. document that bucket metadata remains PG-sharded
5. document that `ClusterEpoch` is distinct from object `GenerationId`
6. identify tests that currently depend on raw PG internals

Exit criteria:

1. terminology is documented in this plan or a follow-up design note
2. no runtime behavior change
3. existing tests still pass if code is touched

Phase 0 implementation notes:

1. `ClusterEpoch` is a nonzero storage-layer newtype.
   - `ClusterEpoch::INITIAL` is epoch `1`
   - it is distinct from `GenerationId`
   - `GenerationId` remains only an object payload generation identifier
2. `PgId` is the raw PG identifier.
   - existing single-host internals may keep using raw `u32` temporarily
   - new cluster-boundary APIs should use typed PG roles
3. `BucketPgId` means the PG that owns bucket metadata and bucket subresources.
   - derived from bucket name
   - bucket metadata remains PG-sharded
   - this is not a global-service table
4. `ObjectMetadataPgId` means the PG that owns object namespace metadata for a
   `(bucket, key)` pair.
   - derived from bucket name and object key
   - this PG controls object visibility
5. `DataPgId` means a PG used for payload shard placement.
   - current code derives this with bounded object-generation data PG selection
   - Phase 4 maps each data PG and shard index onto node placement
6. `ShardIndex` means an EC shard position within one stripe.
   - it is not a node ID
   - Phase 4 maps `(DataPgId, ShardIndex)` to a storage `NodeId`
7. `NodeId` already exists in the placement crate.
   - do not introduce a separate incompatible storage `NodeId`
   - wire the existing placement type into storage when Phase 2/4 needs it
8. `ShardLocation` is deferred until node-aware placement exists.
   - it should include at least `DataPgId`, `ShardIndex`, and `NodeId`
   - it may also need `ClusterEpoch` or a PG acting-set reference

First static local cluster config decision:

1. the first multihost harness should be configured as a static cluster map at
   process startup
2. the default local cluster shape should derive from the configured EC shape
   and include at least one local node per shard
3. local multi-node mode should use explicit node records with:
   - `NodeId`
   - data directory
   - weight
   - topology key, initially at least rack and machine
4. if a compact environment format is used for early development, prefer one
   value containing node records over a spread of loosely coupled variables
5. each local node store must have a distinct directory below or beside
   `ARGMIN_DATA_DIR`
6. the cluster epoch is static `ClusterEpoch::INITIAL` until Phase 5
7. the parser for this config should live near server startup at first, but the
   validated cluster-map representation should be storage/control-plane shaped

Raw PG test dependency inventory:

1. storage unit and integration tests call `SharedStorageNode::get_pg` and
   `lock_two_pgs` directly
2. coordinator tests use `test_bucket_pg_id_for`, `test_object_pg_id_for`,
   `test_data_pg_id_for`, and `test_lock_bucket_pg`
3. coordinator reclaim and multipart tests use `test_shard_exists`,
   `test_read_shard_raw`, `test_delete_shards`, and `test_register_written_shards`
4. several tests deliberately find keys where bucket PG and object PG are equal
   or different to exercise lock ordering and deadlock behavior
5. sparse PG topology tests depend on explicit PG ID fanout behavior
6. these tests are valid for the current single-process implementation, but they
   should move behind cluster-shaped deterministic helpers as Phase 1 and Phase 2
   hide raw PG access from coordinator-facing APIs

## Phase 1: Introduce The Storage Cluster Boundary

Introduce a `StorageCluster`-shaped boundary without changing behavior.

Work items:

1. keep the current `SharedStorageNode` behavior as the initial local-node
   implementation
2. make coordinator request paths depend on the cluster boundary rather than on
   local-node implementation details
3. keep PG locks and local stores hidden inside storage
4. preserve current S3 behavior exactly
5. keep tests focused on behavior rather than raw local PG implementation where
   possible
6. replace the temporary `Deref<Target = SharedStorageNode>` delegation with
   explicit `StorageCluster` request APIs before Phase 1 is complete
7. narrow the temporary one-node helper constructors to tests and local
   transition code, then remove them as soon as routing and placement have
   cluster-owned entry points

Exit criteria:

1. existing local S3 and storage tests pass
2. coordinator request paths use explicit cluster-shaped APIs rather than
   relying on transparent local-node delegation
3. no distributed behavior is claimed yet

Phase 1 implementation notes:

1. `storage::StorageCluster` is the first cluster-shaped storage handle.
   - it still contains temporary metadata-primary forwarding to
     `SharedStorageNode`
   - production startup no longer uses the old root data directory as a
     compatibility layout
   - transparent `Deref` delegation to `SharedStorageNode` has been removed
   - process-local worker and cache registries use a cluster-owned local
     registry key that still maps to the backing local node until a real cluster
     identity exists
   - this is still a single-process metadata-primary implementation, but
     coordinator production paths no longer hold raw single-node handles for
     background workers
2. `Coordinator` now owns an `Arc<StorageCluster>` internally.
   - old public/test harness constructors that accepted `Arc<SharedStorageNode>`
     have been removed
   - new cluster-shaped constructors accept `Arc<StorageCluster>`
   - read runtime, reclaim workers, lifecycle registries, and bucket fast-path
     watchers all carry cluster handles
   - cache and lifecycle registries still intentionally share state for handles
     backed by the same local node until a real opaque cluster identity exists
3. `argmin-s3` now builds one explicit `StorageCluster` and shares that across
   worker coordinators.
4. explicit request APIs on `StorageCluster` now cover the coordinator
   production request surface:
   - direct PUT segment shard writes and metadata commit
   - stream segment shard writes and append commit
   - stream upload session load, abort, and stale-session listing
   - read-side segment payload reconstruction
   - default payload EC shape lookup
   - bucket and object metadata operations
   - authorization snapshot loaders
   - multipart state transitions
   - lifecycle and reclaim operations
5. cluster compatibility wrappers also expose existing coordinator test hooks
   with `test_*` names so tests no longer depend on transparent local-node
   delegation. These helpers should be narrowed or replaced with
   behavior-shaped cluster test utilities as multi-node coverage is added.
6. No request routing, placement, PG ownership, or distributed behavior has
   changed yet.

## Phase 2: In-Process Multi-Node Harness

Create several independent local storage nodes inside one process.

This is the first multihost-shaped milestone, but still without transport,
failure detection, or internal auth.

Work items:

1. create `LocalNodeStore` instances with distinct data directories
2. assign stable `NodeId`s to those local stores
3. add a static in-memory cluster map with epoch `1`
4. route shard operations through a local `ShardNodeClient`
5. keep metadata ownership single-primary and static
6. ensure no local node store shares SQLite or shard directories with another
   node store

Exit criteria:

1. one process can run with multiple local node stores
2. current behavior passes with a local static cluster map
3. multi-node mode can be enabled in local tests without RPC
4. no code path depends on shared process state between node stores except the
   explicit test harness and static control plane

Phase 2 implementation notes:

1. `storage::LocalClusterMap` is the initial static in-process control-plane
   shape.
   - it carries `ClusterEpoch::INITIAL`
   - it assigns stable placement `NodeId`s to local stores
   - node 0 is the default metadata primary for the initial harness
2. `StorageCluster::open_local_nodes` opens one independent
   `SharedStorageNode` per local node under `node-<id>` subdirectories.
   - duplicate node IDs are rejected
   - duplicate/canonical-equivalent data directories are rejected
   - every local node gets its own SQLite and shard directories
3. The temporary one-node constructors have been removed.
   - tests that need direct local-node internals use explicit metadata-primary
     test hooks
   - broad coordinator, server-http, and s3-tests harnesses open the local
     cluster layout
4. `argmin-s3` opens the Phase 2 local cluster harness below the configured
   data directory by default.
5. Request routing is still metadata-primary/local-node forwarding for this
   slice.
   `ShardNodeClient` and node-aware shard writes/reads remain the next Phase 2
   step.

## Phase 3: Data PG Selection

Status: complete.

Fix the object-to-data-PG mapping before relying on the distributed layout.

Work items:

1. implement bounded object-local data PG sets
2. add segment banding within the bounded set
3. use object generation identity, not transient session identity, as the stable
   data placement input
4. make direct PUT and streaming PUT use the same placement model
5. keep object metadata placement unchanged

Exit criteria:

1. one object's data PG fan-out is bounded by configuration
2. sequential segment bands have predictable locality
3. placement tests cover determinism, fan-out bounds, and stability
4. reclaim remains rooted in object generation identity

Phase 3 implementation notes:

1. `PgTopology` now has a bounded object-generation data-PG selection primitive.
   - default object-local data PG-set width is `4`, clamped by topology size
   - default segment band size is `16` segments
   - the selected set is derived from `(bucket, key, generation_id)` using a
     deterministic ranked hash over the configured PG IDs
   - segment data PG selection maps `segment_index / band_size` over that
     bounded set
2. The storage crate owns PG placement and shard-key hash derivation.
   `server-core` reuses `storage::PgTopology` and storage hash helpers instead
   of carrying duplicate coordinator-side implementations.
3. PutObject writes now reserve an object payload generation before writing
   shard payloads.
   - direct PUT uses the reserved generation for the segment shard key and data
     PG selection before metadata commit
   - streaming PutObject sessions reserve the final object generation at session
     creation, and staged segments use generation-derived segment hashes and
     bounded object-local data PG selection
   - streaming staged segments keep a per-append `segment_vid` so concurrent
     duplicate appends do not collide on identical shard file names before the
     metadata conflict is detected
   - reservations are included in `next_generation_id`, so concurrent in-flight
     PutObject writes cannot collide on the same generation-derived shard keys
   - successful commit, abort, and direct PUT precondition failure release the
     reservation
4. Multipart uploads now reserve the final object payload generation when the
   upload is created.
   - the reservation is stored on the multipart upload row and included in
     `next_generation_id`, so concurrent multipart uploads for the same key do
     not collide
   - streamed UploadPart segment data PG selection uses the reserved object
     generation plus `(part_number, segment_index)` banding over the bounded
     object-local PG set
   - streamed UploadPart shard keys remain session-unique while staging, so
     concurrent reuploads of the same part do not overwrite one another before
     metadata commit decides the winning part generation
   - CompleteMultipartUpload publishes the reserved generation as the final
     object generation and releases the reservation in the same metadata
     transaction
   - CompleteMultipartUpload reparents only selected streamed part segment rows
     and removes omitted streamed part metadata rows, so completing a subset of
     uploaded parts does not leave unreachable metadata
   - AbortMultipartUpload deletes the staged part metadata and releases the
     reservation with the upload row
5. Data PG terminology is now reflected in storage metadata, schema columns,
   request structs, test helpers, and diagnostics as `data_pg_id`.
   - the previous ambiguous data-placement PG names were removed from active code
   - shard-specific names remain only for actual EC shard indexes, shard keys,
     and shard files

Phase 3 completion notes:

1. all Phase 3 exit criteria are met for the current local PG model
2. physical EC shard placement across nodes starts in Phase 4
3. crash-durable cleanup of unreferenced shard files remains Phase 7 scavenger
   work, not Phase 3 data placement work

## Phase 4: Node-Aware Shard Placement

Status: planned.

Change data writes so EC shards are distributed across nodes.

Work items:

1. use the placement crate to map each data PG acting set to nodes
2. map each EC shard index to a node in the data PG acting set
3. replace all-shards-to-one-local-PG writes with per-shard node writes
4. publish metadata only after the required shard writes complete
5. update reads to fetch shards from their assigned nodes
6. validate that the active cluster map can place all `k + m` shards for the
   configured EC shape on distinct eligible nodes
7. reject startup or configuration changes that would make the current EC shape
   unplaceable
8. preserve existing read reconstruction and checksum behavior

Implementation order:

- [x] 4.1 Validate local placement shape before opening node stores.
  - build the local placement view from configured node IDs and EC shape
  - reject `k + m` shapes that cannot be placed on distinct active nodes
  - add startup/config tests for too few local nodes and the first valid shape
- [x] 4.2 Add a cluster-level shard placement helper.
  - map `(data_pg_id, shard index, EC shape, stable placement key)` to `NodeId`
  - keep this in `StorageCluster`/`LocalClusterMap`, not coordinator code
  - test deterministic, distinct-node placement for each shard in a stripe
- [x] 4.3 Add cluster-owned local shard IO dispatch.
  - write, read, and delete one shard on its assigned local node
  - keep the API shaped like the later remote-node boundary
  - leave metadata operations on the current metadata primary for this phase
- [x] 4.4 Move direct PutObject payload writes to per-shard node placement.
  - publish object metadata only after all required shard writes complete
  - clean up already-written shards on pre-commit errors
  - test shard files landing under multiple `node-XXXX` stores
- [x] 4.5 Finish placed reads for segment producers.
  - standard segment metadata records carry an explicit payload storage marker,
    so reads route to either metadata-primary shard files or placed shard files
    without speculative fallback
  - direct PutObject reads now fetch from assigned nodes using metadata-primary
    shard ack rows as a Phase 4 bridge
  - streaming PutObject segment records remain marked as metadata-primary until
    Step 4.6 moves their writes
  - multipart payload records remain metadata-primary until Step 4.7 moves
    direct and streamed UploadPart writes
  - keep missing shard reconstruction and corrupt shard handling covered in
    local multi-node mode
- [x] 4.6 Move streaming PutObject segment writes and commit cleanup.
  - write staged segment shards to assigned nodes
  - register/publish metadata only after the required shard writes complete
  - test failed commit cleanup across local nodes
- [ ] 4.7 Move multipart direct and streamed part payloads.
  - direct UploadPart should follow the direct PutObject shape
  - streamed UploadPart should reuse the streaming segment placement path
  - test abort, part reupload, subset completion, and omitted part cleanup in
    multi-node mode
- [ ] 4.8 Move delete and reclaim cleanup to placement-aware shard deletion.
  - recompute per-shard node placement from stored `data_pg_id`, EC shape, and
    shard identity
  - keep crash-durable scavenger work in the later scavenger phase
- [ ] 4.9 Do a Phase 4 boundary sweep.
  - active payload reads/writes/deletes should no longer assume all EC shards live
    in one local PG store
  - keep S3-visible behavior unchanged
  - run the full suite and clippy before marking Phase 4 complete

Phase 4 implementation notes:

1. Step 4.1 rejects local multihost configs that cannot place all EC shards on
   distinct local nodes.
   - the production binary now always opens the local cluster layout under
     `node-<id>` directories
   - `ARGMIN_LOCAL_NODE_COUNT` defaults to `ARGMIN_EC_K + ARGMIN_EC_M`
   - configured values must be at least `ARGMIN_EC_K + ARGMIN_EC_M`
   - `LocalClusterMap` performs the same validation before preparing local node
     directories or opening stores, so storage APIs cannot bypass startup checks
2. Step 4.2 adds cluster-owned payload shard placement.
   - `StorageCluster` and `LocalClusterMap` expose placement helpers that return
     `ShardLocation` values for each EC shard in a stripe
   - `ShardLocation` carries the static cluster epoch, data PG, shard index, and
     assigned `NodeId`
   - the placement key is domain-separated and derived from the data PG plus the
     caller-provided stable payload key
   - payload IO still uses the existing local-node path until Step 4.3/4.4 moves
     writes and reads onto these locations
3. The post-4.2 boundary cleanup removed the old single-node coordinator
   constructors and broad S3 test harness startup paths.
   - `s3-tests` and `server-http` tests now open the same local cluster layout as
     production startup
   - direct `SharedStorageNode` construction remains for storage-node internals
     and narrow metadata-primary test hooks only
4. Step 4.3 adds cluster-owned local shard IO dispatch.
   - `StorageCluster` exposes placed payload-shard write, read, read-into, and
     delete operations that dispatch to the `NodeId` carried by `ShardLocation`
   - shard IO rejects stale location epochs and unknown local nodes before
     reaching the local store
   - placed shard IO rejects mismatches between `ShardLocation::shard_index()`
     and the shard index embedded in the `ShardKey`, so callers cannot write,
     read, or delete a shard through another shard's node placement
   - shard reads take the expected `WriteAck` and validate stored size and
     CRC64 at the cluster shard boundary
   - the implementation is still in-process and file-backed, but coordinator
     request paths can now move to a cluster-shaped shard boundary in Steps
     4.4-4.8
5. Step 4.4 moves direct buffered PutObject payloads onto placed shard IO.
   - direct PutObject shards are encoded through the existing storage EC helper
     and written one shard at a time through `StorageCluster` placed shard
     dispatch
   - object metadata and shard ack rows are still published through the
     metadata-primary bridge; direct placed reads use those ack rows for
     per-shard CRC and size verification until metadata PG routing is moved in
     Phase 6
   - failed direct writes, failed direct commits, the test-only pre-commit
     probe, and object payload reclaim clean up placed shard files
     best-effort
   - object payload reclaim also deletes metadata-primary shard rows until
     Phase 6 removes the metadata-primary bridge
   - Step 4.8 still owns the later boundary sweep for remaining cleanup paths
     introduced by streaming and multipart placement work
   - request-path tests assert direct PutObject shard files land under distinct
     `node-XXXX` stores, and EC fault-injection tests now manipulate the placed
     shard file paths
6. Step 4.5 makes standard segment reads explicit about payload shard storage.
   - `stream_upload_segments`, `object_segments`, and
     `object_segment_reclaim_segments` record whether shard files live under the
     metadata-primary PG store or are placed through the cluster map
   - direct buffered PutObject records are marked as placed, while existing
     streaming standard object records remain metadata-primary until Step 4.6
   - read routing uses that stored marker and no longer tries placed IO before
     falling back to the metadata-primary node
   - the old local-node direct PUT shard writer was removed, and the
     local-node direct commit helper is crate-internal bridge code behind
     `StorageCluster`
   - standard object reclaim uses the stored marker to delete placed direct PUT
     shard files before removing metadata-primary bridge rows; multipart reclaim
     remains metadata-primary until Step 4.7
7. Step 4.6 moves streaming PutObject staged segment files onto placed shard IO.
   - the cluster wrapper marks `StreamUploadTarget::PutObject` segment records as
     placed after the metadata-primary prepare step allocates their segment ID
   - streamed UploadPart segment records remain metadata-primary until Step 4.7
   - stream segment writes use the same placed shard writer as direct PutObject,
     while metadata-primary shard ack rows remain the Phase 4 bridge for reads
     and recovery
   - abort cleanup uses the exact staged segment rows removed by the
     metadata-primary abort operation, so a segment that commits during the
     abort window cannot lose its metadata row while keeping placed shard files
   - append-commit failure cleanup deletes placed staged segment files after the
     metadata-primary bridge removes shard ack rows
   - tests cover placed stream shard fanout, abort cleanup, abort racing with a
     segment commit, and a post-write commit failure that would otherwise orphan
     placed staged shard files
8. The test harness uses a split topology while metadata routing is still a
   Phase 4 bridge.
   - broad S3, HTTP, auth, and model tests use one metadata PG with the default
     `k=4,m=2` six-node local cluster, preserving the local cluster shape
     without opening unused metadata PG stores
   - those broad request-path tests still use the Phase 4 metadata-primary
     bridge for metadata; direct buffered PutObject and streaming PutObject
     payload IO now use placed shard files, while multipart payloads and
     remaining cleanup continue to move in Steps 4.7-4.8
   - tests that specifically exercise metadata PG fanout, merge, pagination, or
     bucket/object PG separation opt into two PGs
   - sparse topology tests keep explicit PG sets such as `[0, 2, 5]`
   - placement and shard IO tests use one PG unless the test checks
     PG-dependent placement
9. Metadata operations still delegate through the metadata-primary
   `SharedStorageNode` in Phase 4.
   - only payload shard placement and IO are node-aware at this point
   - the other local node stores are not metadata owners or metadata replicas
     yet
   - Phase 6 must remove this bridge by routing metadata operations through
     cluster-owned PG primaries/replica sets instead of
     `StorageCluster::single_node`

Exit criteria:

1. a direct put writes shard files to multiple local node stores
2. streaming put writes segment shards to multiple local node stores
3. get/head/list/delete behavior remains unchanged from the S3 API perspective
4. tests cover missing shard, corrupt shard, and reconstruction behavior in
   multi-node local mode
5. tests cover too-few-node startup/configuration rejection for the configured EC
   shape

## Phase 5: Cluster Epoch And PG State

Add epoch-fenced APIs before adding real failure behavior.

Work items:

1. introduce `ClusterEpoch`
2. make metadata and shard operations carry an epoch
3. add static PG acting sets and primary identity to the local control plane
4. reject stale-epoch operations
5. add PG state values even if most remain unused initially:
   - active
   - peering
   - degraded
   - backfilling
6. make logs and errors include node, PG, and epoch information

Exit criteria:

1. stale epoch tests fail closed
2. active PG operations continue to pass the existing suite
3. no operation can silently mutate metadata or shard state without an epoch at
   the cluster boundary

## Phase 6: PG Metadata Replication

Turn per-PG metadata mutation into primary-owned replicated commands.

Initial replication can require every configured replica to be available. That
keeps the first version simple and avoids claiming degraded write availability
before peering and recovery are real.

Work items:

1. replace the Phase 4 metadata-primary bridge with cluster-owned metadata PG
   routing
2. define PG metadata commands for bucket, object, multipart, stream, and reclaim
   mutations
3. apply commands through the PG primary
4. replicate commands to PG replicas before acknowledging success
5. encode commands canonically so all replicas hash the same logical operation
6. add command checksums and a chained log or equivalent replay state
7. record each replica's applied log index and state digest
8. make replicas reject non-primary or stale-epoch commands
9. make reads use primary-owned metadata until replica-read semantics are
   specified
10. add peering placeholders but keep failure handling disabled initially

Exit criteria:

1. mutating PG operations are command-shaped
2. metadata replicas converge in no-failure tests
3. primary and replica SQLite state can be compared in deterministic tests
4. writes fail closed when a required metadata replica is unavailable
5. command-log checksum mismatch prevents replica acknowledgement
6. applied-state digest mismatch prevents the PG from being considered clean
7. no active cluster metadata path mutates or reads PG state by bypassing
   cluster PG routing through `StorageCluster::single_node`

## Phase 6a: Metadata Integrity And Divergence Policy

Define how metadata corruption and replica disagreement are detected and handled.

This phase should not rely only on SQLite page or constraint checks. Those are
useful local signals, but replicated correctness needs PG-level integrity.

Work items:

1. define canonical encodings for metadata commands and, where useful, metadata
   rows or table ranges
2. define the PG command-log hash chain:
   - epoch
   - PG ID
   - monotonically increasing log index
   - previous log hash
   - command checksum
3. define a durable replica state table for:
   - current epoch
   - applied log index
   - applied log hash
   - optional state digest or Merkle root
4. decide how periodic metadata scrub compares replicas:
   - full PG digest for small PGs
   - table or key-range digests for larger PGs
   - targeted row comparison after a digest mismatch
5. define the resolution rules:
   - replay from a valid command log when possible
   - rebuild a bad replica from a clean peer or snapshot when needed
   - enter inconsistent or peering state when no safe authoritative source exists
6. decide whether record-level checksums are needed for high-value rows, or
   whether command-log plus state-digest verification is sufficient initially

Exit criteria:

1. a corrupted metadata row can be detected by scrub or read validation
2. a replica with a missing, reordered, or modified command is detected
3. a divergent replica is excluded from clean PG state until repaired
4. tests cover primary corruption, replica corruption, stale replica restart, and
   digest mismatch repair
5. the system never resolves divergent metadata by silently choosing an arbitrary
   replica

## Phase 6b: Temporary Failure Write Policy

Decide the policy for writes while one or more target nodes are temporarily
unavailable.

This is a design checkpoint before implementing degraded availability. The
options are:

1. strict writes
   - require every target shard and metadata replica before acknowledging success
   - simplest correctness model
   - lower write availability during transient node failures
2. degraded writes
   - allow success with fewer than `k + m` shards written, but at least enough
     durable shards to reconstruct
   - requires per-shard present/missing metadata, urgent repair, and clear
     durability policy for how much redundancy must exist before success
   - increases availability but can acknowledge writes with reduced failure
     tolerance
3. handoff writes
   - write temporarily unavailable shards to alternate eligible nodes
   - requires placement exceptions, handoff metadata, and later migration back to
     the intended acting set
   - preserves shard count at commit time but makes placement and repair more
     complex

Default policy until this phase is completed:

- strict writes only
- degraded reads are allowed when the PG state permits reads and at least `k`
  valid shards are reachable
- do not acknowledge a write that leaves the committed generation below the full
  intended shard count

Exit criteria:

1. the chosen policy is documented with explicit success and failure conditions
2. metadata can represent the chosen policy without ambiguity
3. repair/backfill behavior is defined for every committed write state
4. tests cover transient target-node outage during direct put, streaming put, and
   multipart complete
5. tests cover a second failure before repair for any policy that acknowledges
   writes below full redundancy

## Phase 7: Replace Process-Local Coordination

Remove process-local mechanisms from logical correctness.

Work items:

1. replace multipart completion locks with PG-primary serialization
2. replace bucket write drain waits with durable reservation or primary-owned
   state
3. replace object payload generation leases with cluster-visible read pins or a
   durable expiring lease table
4. make reclaim work claiming durable and idempotent
5. add a physical shard scavenger for unreferenced shard files that can be left
   by crashes or persistent delete failures after metadata has already stopped
   referencing a payload, including omitted multipart part shards cleaned up
   after CompleteMultipartUpload
6. make bucket cache freshness depend on PG or cluster notifications rather than
   local invalidation alone
7. audit tests for hidden single-process assumptions

Exit criteria:

1. a second process would not be required to see local mutexes or condition
   variables to preserve correctness
2. reclaim can resume after process restart from durable rows
3. multipart complete remains serialized for one upload and one destination
4. in-flight reads are protected from physical cleanup across process boundaries
5. unreferenced shard files left by best-effort cleanup failures are eventually
   detected and removed without consulting process-local state

## Phase 8: Local Multi-Process RPC

Move from in-process multi-node to local multi-process nodes.

This phase should not change the logical API. It should only replace local
`ShardNodeClient` and PG-primary calls with internal connections.

Work items:

1. run one process per storage node with a distinct data directory
2. expose internal shard and PG-primary APIs over a local transport
3. keep internal auth disabled or static for this phase
4. keep the cluster map static at startup
5. ensure node restart does not corrupt local metadata or shard files
6. add integration tests that start multiple local processes

Exit criteria:

1. no PG directory is shared between processes
2. the same S3 suite passes against local multi-process mode
3. killing a non-critical process fails closed with clear errors
4. restarting a process preserves its local shard and metadata state

## Phase 9: Failure, Peering, Repair, And Migration

Add real distributed behavior after the normal path is already shaped correctly.

Work items:

1. add heartbeat and failure detection to the control plane
2. bump cluster epoch on membership or PG acting-set changes
3. elect or assign new PG primaries
4. implement PG peering from the durable PG command log
5. define read and write availability rules for peering and degraded PGs
6. implement shard repair for missing or corrupt shards
7. implement PG backfill and migration for changed acting sets
8. add cluster-map history retention and pruning

Exit criteria:

1. stale primaries cannot accept writes after an epoch change
2. PGs enter peering before serving unsafe requests
3. repair restores missing shards from available EC data
4. migration can move a PG acting set without per-object metadata rewrites
5. failure-injection tests cover primary loss, replica loss, restart, and repair

## Phase 10: Replicated Control Plane

Replace the static in-memory control plane with a real replicated control plane.

Because bucket metadata is now PG-sharded, this control plane should remain
small. Its initial scope should be:

1. cluster map
2. node membership
3. cluster epoch
4. PG count
5. PG state
6. PG acting sets

Bucket rows should not move back into the global service unless there is a
separate design decision to reverse the bucket-metadata-sharding model.

Exit criteria:

1. control-plane state survives node restart
2. epoch changes are linearized
3. storage nodes reject stale control-plane state
4. local multi-process tests no longer depend on static startup-only cluster
   membership

## Testing Strategy

Each phase needs tests at the smallest level that can prove the new invariant,
plus the existing S3 integration tests.

Required test areas:

1. placement determinism and fan-out bounds
2. per-shard node distribution
3. stale epoch rejection
4. PG-primary command serialization
5. metadata replica convergence
6. metadata command-log checksum and state-digest mismatch detection
7. metadata corruption and divergent-replica repair
8. process restart persistence
9. reclaim idempotence
10. in-flight read protection across cleanup
11. local multi-process startup and shutdown
12. failure injection for primary loss, replica loss, and stale sender writes

`./scripts/coverage` remains the main integration coverage signal. The full test
suite should be run before committing each completed implementation slice.

## Main Risks

### Metadata Replication

This is the largest correctness risk. Object and bucket metadata are the S3
visibility boundary, so replication bugs can create externally visible
inconsistency. The first version should prefer fail-closed all-replica writes
over premature degraded availability.

### Metadata Corruption And Divergence

Shard checksums are not enough to protect the namespace. Metadata corruption can
make the system expose the wrong version, lose a delete marker, resurrect an old
object, or route reads to the wrong shard set. Replica comparison must therefore
be explicit. If command logs, state digests, or row checks disagree, the PG must
fail closed or enter peering rather than returning whichever replica answered
first.

### Process-Local State

The current code intentionally uses local mutexes, condition variables, caches,
and queues. These must be treated as single-process optimizations only after this
transition. Any one of them left as a correctness dependency can produce a
multi-process race.

### Placement Drift

Once shard placement is multihost, placement determinism becomes a correctness
property, not just a balancing property. The deterministic-log placement plan
should be resolved before placement decisions are relied on across heterogeneous
nodes or long-lived persisted cluster maps.

### Unplaceable EC Shapes

The single-host implementation accepts any EC shape because every shard is local.
That is no longer valid once each shard must be placed on a distinct eligible
node. Phase 4 must reject cluster maps and configuration changes where the
configured `k + m` shard count cannot be placed under the current node and
failure-domain constraints.

### Reclaim Safety

Physical shard deletion is intentionally decoupled from S3 metadata visibility.
That model should remain, but read pins and reclaim claims must become
cluster-visible before cleanup can run on multiple nodes.

### LIST Semantics Under Failure

Bucket and object listing are fanout operations. During PG failure, peering, or
epoch change, partial results must not be returned as complete listings.

## First Concrete Milestone

The first implementation milestone should be:

1. one process
2. multiple independent local node stores
3. static cluster map
4. static epoch `1`
5. bounded object-local data PG selection
6. placement-driven per-shard node assignment
7. no failures
8. no internal RPC
9. existing S3 behavior unchanged

That milestone forces the storage layout and API boundaries to become
multihost-shaped without also introducing remote auth, network transport,
failure detection, peering, and repair.
