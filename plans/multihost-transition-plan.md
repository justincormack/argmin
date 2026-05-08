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
3. crash-durable cleanup of unreferenced shard files remains Phase 9 scavenger
   work, not Phase 3 data placement work

## Phase 4: Node-Aware Shard Placement

Status: complete.

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
- [x] 4.7 Move multipart part payloads.
  - the active UploadPart path is streaming-based and now reuses the placed
    streaming segment path
  - multipart part, object-part, segment, and reclaim metadata records all refer
    to placed payload shards; there is no metadata-primary payload fallback
  - abort, part reupload, subset completion, and omitted part cleanup are
    placement-aware in multi-node mode
- [x] 4.8 Move delete and reclaim cleanup to placement-aware shard deletion.
  - recompute per-shard node placement from stored `data_pg_id`, EC shape, and
    shard identity
  - sweep remaining delete/reclaim paths now that multipart reclaim records also
    use placed payload shard identities
  - keep crash-durable scavenger work in the later scavenger phase
- [x] 4.9 Do a Phase 4 boundary sweep.
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
   - Step 4.8 centralizes the later cleanup paths introduced by streaming and
     multipart placement work
   - request-path tests assert direct PutObject shard files land under distinct
     `node-XXXX` stores, and EC fault-injection tests now manipulate the placed
     shard file paths
6. Step 4.5 moves standard segment reads to the cluster payload boundary.
   - direct buffered PutObject records now refer to placed shard files; the
     metadata-primary PG still stores the temporary shard ack rows used for
     per-shard CRC and size verification
   - read routing uses placed shard IO through `StorageCluster` instead of
     trying placed IO before falling back to the metadata-primary node
   - the old local-node direct PUT shard writer was removed, and the
     local-node direct commit helper is crate-internal bridge code behind
     `StorageCluster`
   - standard object reclaim deletes placed direct PUT shard files before
     removing metadata-primary bridge rows; multipart reclaim remains
     metadata-primary until Step 4.7
7. Step 4.6 moves streaming PutObject staged segment files onto placed shard IO.
   - the cluster wrapper writes `StreamUploadTarget::PutObject` segment payload
     files through placed shard IO after the metadata-primary prepare step
     allocates their segment ID
   - streamed UploadPart segment records remain on the metadata-primary payload
     path until Step 4.7
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
     bridge for metadata; direct buffered PutObject, streaming PutObject, and
     multipart payload IO now use placed shard files
   - tests that specifically exercise metadata PG fanout, merge, pagination, or
     bucket/object PG separation opt into two PGs
9. Step 4.7 moves streamed UploadPart payload files onto placed shard IO.
   - `StorageCluster` now writes both standard stream PutObject segments and
     streamed UploadPart segments through placed shard IO after the
     metadata-primary prepare step allocates the segment payload ID
   - multipart manifest, staged-part, object-part, and reclaim rows now refer to
     placed payload files; the metadata-primary shard rows are only the Phase 4
     ack bridge
   - multipart reads route direct part shard sets and streamed part segments
     through placed shard IO
   - abort, reupload replacement cleanup, and completion cleanup for omitted
     streamed parts delete placed shard files and metadata-primary bridge rows
     best-effort
   - there is no separate direct UploadPart request path at this point; the
     public/test UploadPart helper already uses begin, append, and finalize
     streaming calls, while the direct multipart shard-set metadata shape is
     ready if that path is introduced later
   - sparse topology tests keep explicit PG sets such as `[0, 2, 5]`
   - placement and shard IO tests use one PG unless the test checks
     PG-dependent placement
10. Step 4.8 centralizes delete and reclaim cleanup at the cluster boundary.
   - `StorageCluster` has one payload shard-set deletion helper for strict
     reclaim and a best-effort variant for abort/commit cleanup paths
   - placed cleanup recomputes each shard's local node from the stored data PG,
     EC shape, stable payload identity, and shard index before deleting the file
   - metadata-primary shard rows remain the Phase 4 ack bridge and are deleted by
     the same helper after placed shard deletion, so cleanup paths do not need
     separate local-node assumptions
   - the public single-node reclaim entry point was removed; object payload
     reclaim must go through `StorageCluster` so placed payloads cannot bypass
     node-aware deletion
   - crash-durable cleanup for files that survive process death or delete
     failures remains deferred to the later scavenger phase
11. Metadata operations still delegate through the metadata-primary
   `SharedStorageNode` in Phase 4.
   - only payload shard placement and IO are node-aware at this point
   - the other local node stores are not metadata owners or metadata replicas
     yet
   - Phase 6 must remove this bridge by routing metadata operations through
     cluster-owned PG primaries/replica sets instead of
     `StorageCluster::single_node`
12. Step 4.9 removes the remaining broad single-node payload escape hatches.
   - `StorageCluster::test_metadata_storage_node` was removed; coordinator
     tests either use cluster-level hooks for explicit metadata bridge checks or
     the same request paths as production
   - the coordinator test helper that hand-wrote stream shard files through the
     metadata-primary node was removed with its legacy test
   - server-core reclaim tests now seed placed payload records instead of
     metadata-primary records
   - raw local shard file IO remains crate-internal storage bridge code used by
     placed shard dispatch, while the single-node stream shard writer and
     single-node segment payload reader were removed
   - `SharedStorageNode` no longer deletes payload shard keys during direct PUT,
     stream append, stream abort, multipart completion omitted-part cleanup,
     streamed-part replacement, or multipart abort paths; placed file deletion
     and temporary shard ack cleanup are owned by `StorageCluster`
   - completed multipart upload pruning now runs after cluster-owned omitted-part
     cleanup, so post-commit prune failures cannot hide cleanup records from the
     placement boundary
   - direct PutObject commit errors are split into pre-publish and post-publish
     failures so `StorageCluster` only deletes newly written placed shards when
     object metadata was not published
   - the transitional `PayloadShardStorage` type and `payload_storage` schema
     columns were removed once every active payload path used placed shard IO;
     omitted test or internal rows cannot silently recreate metadata-primary
     payload placement
   - metadata-primary shard rows remain only as the explicit Phase 4 ack bridge;
     Phase 6 removes that bridge with cluster-owned metadata PG routing

Exit criteria:

1. a direct put writes shard files to multiple local node stores
2. streaming put writes segment shards to multiple local node stores
3. get/head/list/delete behavior remains unchanged from the S3 API perspective
4. tests cover missing shard, corrupt shard, and reconstruction behavior in
   multi-node local mode
5. tests cover too-few-node startup/configuration rejection for the configured EC
   shape

## Phase 5: Cluster Epoch And PG State

Status: complete.

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
   - inconsistent
6. make logs and errors include node, PG, and epoch information

Exit criteria:

1. stale epoch tests fail closed
2. active PG operations continue to pass the existing suite
3. no operation can silently mutate metadata or shard state without an epoch at
   the cluster boundary

Phase 5 implementation notes:

1. Step 5.1 adds an explicit static PG route table to `LocalClusterMap`.
   - every configured PG has a `LocalPgRoute` with `ClusterEpoch::INITIAL`, a
     primary node, an acting set, and `PgState::Active`
   - the initial local acting set is all configured local nodes, with the
     existing metadata-primary node as the static primary
   - `PgState` includes `active`, `peering`, `degraded`, `backfilling`, and
     `inconsistent` even though only active routes are constructed initially
   - placed shard placement and IO now reject unknown or non-active PGs through
     the route table before touching a node store
   - local cluster startup rejects empty and duplicate PG sets before preparing
     node directories or opening SQLite stores
   - shard IO errors include node, PG, and epoch context so stale or misrouted
     operations fail with enough information for later peering and repair work
   - placed-segment reads and EC recovery treat only physical shard absence,
     checksum failure, or shard length corruption as recoverable shard loss; PG
     route errors such as peering, stale epoch, or misrouted nodes propagate
     instead of collapsing to object-not-found behavior
2. Step 5.2 makes operation epochs explicit on the local payload boundary.
   - local map payload placement, node lookup, shard read, shard write, and
     shard delete APIs now require the caller's operation epoch instead of
     silently using the current cluster epoch
   - `StorageCluster` carries an operation epoch on the handle; production
     constructors bind it to the current static local map epoch, tests can
     construct a stale handle to verify fail-closed behavior, and cluster-level
     payload APIs always use the handle epoch instead of accepting an
     independent epoch argument
   - stale operation epochs fail before placement or node-store IO, distinct
     from stale shard locations that were produced by an older route table
   - current production paths still bind the handle to the static epoch at
     construction; Step 5.3 extends the same stale-handle check to the
     metadata-primary bridge before Phase 6 replaces that bridge
   - tests cover stale-epoch rejection for both placement and shard mutation
     without creating or modifying shard files
3. Step 5.3 fences metadata-primary bridge calls by the `StorageCluster`
   operation epoch.
   - stale metadata-primary bridge errors are surfaced through
     `StoreError::StaleMetadataPrimaryBridge` so they remain distinct from
     payload placement, shard IO, and underlying metadata/SQLite failures
   - result-returning bucket, object, multipart, stream, reclaim, and lifecycle
     metadata bridge methods check the cluster handle epoch before forwarding to
     the metadata-primary `SharedStorageNode`
   - best-effort queue/worker bridge helpers no-op on stale handles rather than
     mutating process-local metadata queues
   - direct PUT and stream append commit helpers reject stale handles before the
     metadata publish path; they do not try to perform current-epoch payload
     cleanup for an operation that may have been written under an older route
     table
   - payload lease acquisition is fenced as new work, but a successful acquire
     returns an active lease token that releases against the metadata-primary
     node where it was recorded even if the acquiring cluster handle later
     becomes stale
   - tests cover stale bucket metadata creation and object generation
     reservation attempts failing before metadata mutation, plus lease release
     after an epoch transition
4. Step 5.4 audits and closes remaining broad `SharedStorageNode` escape
   hatches at the cluster boundary.
   - production `StorageCluster` paths may only touch the metadata-primary node
     through an epoch-checked bridge helper, an explicitly best-effort stale
     no-op helper, or a read-only topology/config helper
   - remaining direct metadata-primary shard-row access is documented as the
     Phase 4 ack bridge and is reachable only after the operation epoch has
     been checked
   - test-only helpers are either epoch checked, read-only topology helpers, or
     explicitly named as lock/contention hooks that do not model production
     routing
5. Step 5.5 tightens stale/control-plane error context.
   - storage errors for stale handles and inactive/misrouted PGs should carry
     enough node, PG, and epoch context to diagnose a failed route
   - metadata-primary bridge errors should make clear whether they are
     operation-epoch failures or underlying metadata/SQLite failures
   - logs should not collapse stale epoch, inactive PG, or misroute conditions
     into object-not-found or best-effort silence except for explicitly
     best-effort worker queues
   - typed storage errors now preserve metadata-primary stale bridge failures
     separately from `StalePayloadPlacement` and `StalePayloadOperation`,
     include data PG context for stale payload operations, and preserve
     inactive PG, missing PG, stale shard operation, stale shard location,
     missing node, acting-set, shard-index mismatch, and node-local shard store
     errors when shard IO is converted to higher-level storage results
   - coordinator segment reads, including zero-size segments, route through the
     storage payload API so stale-handle fencing is not bypassed above the
     storage layer
   - best-effort payload cleanup still suppresses cleanup failures, but emits a
     trace event with the typed storage error when request tracing is active
6. Step 5.6 is a bounded stabilization gate before Phase 6.
   Phase 4 and Phase 5 exposed enough review issues around routing, stale
   handles, payload cleanup, and partial publish failures that the current
   bridge needs an explicit confidence pass before metadata replication
   multiplies the same state space.

   5.6 work items:

   1. Operation classification matrix.
      - classify every public `StorageCluster` method as one of:
        epoch-fenced metadata bridge, payload placement/read/write/delete,
        best-effort cleanup or worker queue, read-only topology/config, active
        token release, or test hook
      - document the expected stale-handle behavior for each class
      - add representative stale-handle tests for each class, including
        metadata bridge calls, payload writes, payload reads, zero-size reads,
        best-effort cleanup, reclaim queues, and active lease-token release

   2. Stateful local-cluster model.
      - add a small proptest trace model around local cluster handles and PG
        routes, not full S3 semantics
      - model: create a handle at epoch N, advance the map epoch, change PG
        state between active/peering/degraded/backfilling/inconsistent,
        place/write/read/delete payload shards, call metadata bridge methods,
        acquire/release payload leases, and run best-effort cleanup
      - invariants: stale handles never start new metadata or payload work,
        stale payload reads fail before returning data including zero-size
        payloads, active lease tokens release after epoch change, route and
        control-plane errors never become `NotFound`, and physical shard loss is
        recoverable only in the EC read paths that explicitly allow it

   3. Failure-injection coverage around cleanup and publish boundaries.
      - inject failures around placed shard delete, metadata-primary ack delete,
        multipart omitted-part cleanup, stream abort cleanup, and reclaim cleanup
      - cover pre-publish and post-publish failures for direct PUT, stream
        append, and multipart completion where hooks already exist or can be
        added narrowly
      - assert committed data remains readable when publish succeeded and
        successful cleanup removes payload files plus metadata-primary ack rows
      - for injected best-effort cleanup failures, assert the failure emits
        typed trace context when tracing is active and leaves only the explicitly
        allowed orphan state for later scavenger/reclaim work

   4. Mechanical boundary checks.
      - add rg-based scripts under `scripts/` with no new dependencies for the
        review patterns we keep checking manually
      - checks should reject broad `self.single_node` use outside the approved
        helper area, route/control-plane errors converted through generic
        `StoreError::Io { source: std::io::Error::other(...) }`, direct
        production shard read/write/delete bypassing `StorageCluster` placed IO,
        and reintroduction of metadata-primary payload write paths
      - keep the scripts specific and auditable; they are guardrails, not a
        substitute for tests

   5. Error taxonomy tests.
      - assert exact variants for stale metadata bridge, stale payload
        placement, stale payload operation, stale shard operation, stale shard
        location, inactive PG, missing PG route, node not in acting set,
        shard-index mismatch, and node-local store failure wrapped with route
        context
      - include at least one coordinator-level test proving route/control-plane
        storage errors do not become `ObjectNotFound`

   6. Plan-level and guide-level invariants.
      - add either a short guide or a dedicated plan section that states the
        rules reviewers should enforce: the metadata-primary bridge is
        temporary and epoch-fenced, payload IO is always placed IO,
        metadata-primary shard rows are ack bridge only, best-effort cleanup may
        suppress but must trace, token release is not new work, and
        crash-durable orphan cleanup belongs to a later scavenger/reclaim phase

   5.6 exit criteria:

   - operation classification matrix exists and all public `StorageCluster`
     methods are accounted for
   - stateful local-cluster model covers stale handles, route state changes,
     lease release, best-effort cleanup, and recoverable physical shard loss
   - failure-injection tests cover the known publish/cleanup boundaries from
     Phases 4 and 5
   - mechanical boundary checks are wired into the normal verification path:
     either CI or the documented pre-commit/full-suite gate
   - error taxonomy regressions pin the typed stale/route/control-plane errors
   - Phase 5 is marked complete only after these checks pass with the full test
     suite

   5.6 implementation notes:

   - `guides/storage-cluster-invariants.md` now defines the operation classes,
     bridge-era invariants, and the public `StorageCluster` method matrix
   - `scripts/check-storage-cluster-boundaries` now guards the recurring review
     patterns around `self.single_node`, direct shard IO bypasses, legacy
     metadata-primary payload writes, and generic route/control-plane IO
     conversions
   - `guides/testing.md` includes the boundary script in broad local
     verification so the check is part of the documented full-suite gate
   - `prop_local_cluster_trace_preserves_epoch_route_and_cleanup_invariants`
     now runs randomized local-cluster traces covering map epoch changes, PG
     active/peering/degraded/backfilling/inconsistent transitions, current and
     stale placed shard IO, stale metadata bridge calls, zero-size stale payload
     reads, best-effort reclaim queue suppression, lease release after epoch
     changes, and EC recovery after physical shard loss
   - payload cleanup fault-injection hooks now cover placed shard delete,
     metadata-primary ack delete, and typed best-effort cleanup error
     observation under request trace context
   - placed payload shard deletion now unlinks only placed files; the
     metadata-primary ack bridge owns only shard-row deletion so colocated
     placed shards cannot lose ack rows before ack cleanup runs
   - server-core regressions now cover stream append cleanup failures that
     trace typed placed-delete and ack-delete errors while leaving only the
     allowed orphan state, plus reclaim placed-delete failure that returns an
     error, preserves retryable reclaim metadata and payload, and succeeds on a
     later retry
   - error taxonomy regressions now pin stale shard locations, acting-set
     violations, shard-index mismatches, and coordinator GET behavior so typed
     route/control-plane errors do not become `ObjectNotFound`
   - closeout audit confirmed that every public `StorageCluster` method in
     `cluster.rs` and `cluster/request_ops.rs` is listed in
     `guides/storage-cluster-invariants.md`
   - Phase 5.6 and Phase 5 are complete after the documented broad verification
     gate passed with the boundary script, clippy, and full nextest suite

## Phase 6: PG Metadata Replication

Turn per-PG metadata mutation into primary-owned replicated commands.

Initial replication can require every configured replica to be available. That
keeps the first version simple and avoids claiming degraded write availability
before peering and recovery are real.

Work items:

1. Phase 6.1: metadata PG routing boundary.
   - replace the Phase 4 metadata-primary bridge with cluster-owned metadata PG
     routing
   - keep a single primary for each PG and keep strict fail-closed behavior
   - route metadata reads through the PG primary; replica-read semantics stay
     out of scope for Phase 6
   - exit when no active metadata path mutates or reads PG state through the old
     metadata-primary bridge or single-node shortcut
   - first slice:
     - add a local-map `metadata_pg_primary_node` lookup that validates handle
       epoch, route presence, PG active state, primary membership in the acting
       set, and primary node presence before returning a storage node
     - move obvious single-PG bucket/object metadata calls, direct PUT metadata,
       stream metadata, multipart object metadata, reclaim metadata, and payload
       shard ack rows through routed metadata PG primaries; payload shard acks
       route through the data PG primary, while object/stream publication routes
       through the object PG primary
     - keep composite/global scans, multi-PG bucket cleanup/finalization, and
       lease bookkeeping/worker queues on the temporary bridge until they are
       split into explicit per-PG fanout in the next 6.1 slice
     - keep payload lease release and worker enqueue on the bridge, but route
       the release-time reclaim-existence check through the object PG primary so
       a deferred reclaim row published off-bridge is requeued after the last
       read lease drops
     - use `StoreError::StaleMetadataOperation` for routed metadata stale-handle
       failures; keep `StoreError::StaleMetadataPrimaryBridge` only for the
       temporary bridge surfaces that remain during 6.1
   - second slice:
     - move read-only composite bucket/object/multipart listing scans from the
       metadata-primary bridge to explicit fanout over routed metadata PG
       primaries, preserving the existing in-process merge and pagination rules
     - include owner bucket listing, lifecycle sweep bucket discovery, full
       lifecycle object/version/upload scans, ListObjects/ListObjectVersions,
       and ListMultipartUploads
     - keep bucket snapshot-pair locking, bucket delete/finalization, completed
       multipart prune, lease bookkeeping, and worker queues on the bridge until
       they are split into explicit per-PG or coordination paths
   - third slice:
     - move completed multipart tombstone pruning from the metadata-primary
       bridge to explicit fanout over routed metadata PG primaries
     - preserve the existing global newest-`keep` ordering across PGs before
       deleting older tombstones through a command-owned acting-set mutation on
       their owning PG
     - keep bucket snapshot-pair locking, bucket delete/finalization, lease
       bookkeeping, and worker queues on the bridge until they are split into
       explicit per-PG or coordination paths
   - fourth slice:
     - move bucket snapshot-pair loading from the metadata-primary bridge to
       routed bucket PG primaries
     - preserve same-bucket request merging and deterministic PG lock ordering
       for distinct buckets that share a local primary node
     - keep bucket delete/finalization, lease bookkeeping, and worker queues on
       the bridge until they are split into explicit per-PG or coordination
       paths
   - fifth slice:
     - move bucket delete begin/finalize metadata checks from the
       metadata-primary bridge to routed metadata PG primaries
     - preserve bucket write-drain behavior on the bucket PG primary and fan out
       bucket emptiness, reclaim-root, and completed-upload cleanup checks
       across routed PG primaries
     - keep finalization's lease-count checks and reclaim/finalize worker queues
       on the bridge until queue ownership is split into explicit coordination
       paths
   - sixth slice:
     - move payload lease bookkeeping and reclaim/finalize worker queues from
       the metadata-primary bridge into local-cluster runtime state shared by
       all handles for the same local map
     - fence lease acquisition as routed object-PG metadata work, but keep
       release of an already-acquired lease token independent of the current
       cluster epoch
     - make release-time reclaim enqueue use the captured local runtime state so
       a final lease release after an epoch transition remains visible to the
       current worker handle
     - move best-effort stream upload session sweeping from the bridge to routed
       metadata PG fanout
     - after this slice, the metadata-primary bridge remains only for
       current-handle test hooks; active production metadata paths either route
       through PG primaries, use read-only topology/config helpers, or use the
       local runtime coordination state
   - closeout:
     - Phase 6.1 is complete: active production metadata reads and mutations no
       longer use the metadata-primary bridge or direct single-node shortcuts
     - the remaining `metadata_primary_bridge_node` callers are
       `#[cfg(any(test, feature = "test-hooks"))]` test helpers only
     - `metadata_primary_topology_node` remains as a read-only topology/config
       helper for PG mapping and default EC shape until cluster topology becomes
       first-class state
     - payload lease bookkeeping and reclaim/finalize queues are now
       local-cluster runtime coordination state; this is intentionally
       single-process and must become durable/replicated in a later
       coordination/scavenger phase
     - closeout verification used `cargo clippy --all-targets --all-features
       -- -D warnings`, `./scripts/check-storage-cluster-boundaries`, and full
       `cargo nextest run`
2. Phase 6.2: command substrate vertical slice.
   - define the command envelope, command identity, canonical encoding, command
     checksum, and log-index shape
   - implement one small metadata mutation end to end through command apply,
     preferably bucket create or a bucket config update
   - compare primary and replica SQLite state in deterministic tests for that
     first command
   - first slice:
     - add `MetadataCommandId`, per-PG log-index shape, canonical command
       encoding, and CRC64 command checksum verification
     - migrate `create_bucket_with_config_and_load_info` onto the command path
       for local clusters
     - keep command log indexes separate from bucket execution generations:
       create commands reserve a bucket execution generation from the routed
       primary so delete/recreate still invalidates fast-path cache state after
       earlier non-command bucket mutations
     - keep an in-process pending create-bucket command keyed by bucket PG and
       bucket name until the command fully applies, so retries after partial
       replica apply reuse the same command identity, timestamp, and execution
       generation instead of poisoning already-updated replicas
     - apply the create-bucket command to every active acting node for the
       bucket PG and apply the routed primary last, so strict-replica failures
       happen before primary publication where possible
     - keep bucket-row finalization consistent with replicated create by
       deleting finalized bucket rows from every active acting node, also
       primary last; Phase 6.3 will move the rest of bucket lifecycle mutation
       onto explicit command apply
     - include deterministic command-encoding coverage and a routed local
       cluster tests that compare created bucket metadata across primary and
       replica SQLite stores, prove delete-then-recreate removes stale replica
       bucket rows, and prove retry after partial replica apply converges on
       one identical command result
     - this is still an in-process substrate: command indexes are local runtime
       state, command logs are not durable, and later Phase 6 work must add
       durable ordering, origin/epoch checks, hash chaining, and recovery
3. Phase 6.3: bucket command migration. Complete.
   - migrate bucket mutations to command apply
   - keep reads primary-owned and epoch-fenced
   - first slice:
     - migrate `put_bucket_versioning_and_load_info` onto the metadata command
       path as the first non-create bucket mutation
     - reserve bucket execution generations on the routed bucket PG primary,
       then carry the explicit generation through command apply so every
       active acting node converges on the same cache-invalidation generation
     - keep one in-process pending bucket metadata command per bucket command
       stream until full apply succeeds, matching the create-bucket retry
       behavior for partial replica apply
     - include deterministic command encoding coverage, primary/replica
       convergence tests, and a partial-replica retry regression
   - second slice:
     - migrate `put_bucket_acl_and_load_info` onto the metadata command path
       using the same explicit bucket execution generation and pending-command
       retry model as versioning
     - serialize pending bucket mutations across command kinds: a partially
       applied versioning command blocks a later ACL command until the
       versioning retry converges, rather than allowing a newer generation to
       make the earlier command stale on already-updated replicas
     - reject stale or same-generation divergent ACL commands during command
       apply rather than letting an older command lower or overwrite the bucket
       incarnation state
     - include deterministic command encoding coverage, primary/replica
       convergence tests, partial-replica retry regression, and a cross-command
       ordering regression
   - third slice:
     - migrate row-backed bucket property mutations onto a
       `PutBucketProperty` metadata command:
       `put_bucket_object_lock_and_load_info`,
       `put_bucket_encryption_and_load_info`,
       `put_bucket_public_access_block_and_load_info`,
       `delete_bucket_public_access_block_and_load_info`,
       `put_bucket_ownership_controls_and_load_info`,
       `delete_bucket_ownership_controls_and_load_info`, and
       `put_bucket_abac_enabled_and_load_info`
     - use the same explicit bucket execution generation, per-bucket pending
       command stream, and primary-last acting-set apply model as versioning
       and ACL commands
     - keep exact stored-state idempotence for bucket encryption commands, not
       only effective encryption equivalence, so same-generation divergent
       commands fail closed
     - include deterministic command encoding coverage, primary/replica
       convergence tests for every row-backed property, and a partial-replica
       retry regression
   - fourth slice:
     - migrate opaque bucket subresource mutations onto a
       `PutBucketSubresource` metadata command:
       `put_bucket_subresource_and_load_info` and
       `delete_bucket_subresource_and_load_info`
     - cover policy, tagging, lifecycle, and CORS bodies, including policy
       public-summary metadata and policy/lifecycle generation mirrors
     - preserve per-bucket command-stream serialization, explicit bucket
       execution generation, and primary-last acting-set apply
     - make retry idempotence prevent duplicate subresource-generation
       increments on replicas that already applied a partial command
     - include deterministic command encoding coverage, primary/replica
       convergence tests, a partial-replica retry regression, and stale or
       same-generation divergent command rejection coverage
4. Phase 6.4: object, stream, multipart, and reclaim command migration. Complete.
   - migrate ordinary object mutations, including generation
     reservation/release, direct PUT publish, delete, tag, ACL, retention, and
     legal-hold changes
   - migrate stream session creation, segment append/finalize/abort metadata,
     multipart create/part/finalize/abort metadata, omitted-part cleanup
     metadata, and reclaim rows
   - keep multi-step workflows serialized by the PG primary command path
   - completed:
     - add `ReserveObjectGeneration`, `ReleaseObjectGeneration`, and
       `CommitDirectPutObject` metadata command payloads with deterministic
       canonical encoding and CRC64 coverage
     - route object generation reservation/release through the object metadata
       PG acting set, with the routed primary applied last and in-process
       pending-command retry for partial replica apply
     - drain any pending object metadata command at direct PUT reservation
       entry before allocating a new generation, so a normal client retry after
       partial direct PUT publish converges the prior command stream before
       starting the replacement write
     - keep per-key object version allocation monotonic through explicit
       `object_version_counters`, reserve numbered versions through the object
       metadata command stream before publish/delete-marker commands consume
       them, and remove bucket-owned counter rows on bucket delete
     - route direct PUT object publication through a prepared command carrying
       the explicit live-object row, segment rows, write sequence, last-modified
       timestamp, reservation ID, and any stale payload reclaim rows needed by
       unversioned overwrite
     - keep direct PUT payload shard placement and data-PG ack rows on the
       Phase 4/5 bridge for now, but only register ack rows after the object-PG
       command stream has accepted or prepared the matching direct PUT command
     - remove the pending direct PUT command as soon as object metadata has
       been published to every active acting node, before any post-publish
       fallible work, because the object is already visible at that point
     - include primary/replica convergence tests for reservation/release and
       direct PUT publish, plus a partial-primary-apply retry regression proving
       the same pending command is reused and replicas converge
     - add `DeleteObjectVersion` and `InsertDeleteMarker` command payloads
       carrying exact version IDs, owner/timestamp/write-sequence data, and
       object-payload reclaim metadata for live-version deletes
     - route ordinary current-object delete, version-specific delete, and
       current delete-marker insertion through the object metadata PG acting
       set with pending-command retry for partial replica apply
     - include acting-set convergence tests for live object delete and delete
       marker insertion, plus a partial-primary-apply retry regression for
       object delete
     - route stream PutObject generation reservation through the object
       metadata PG acting set and publish stream/copy finalization through the
       same replicated standard-object command used by direct PUT
     - add `CreateStreamUpload`, `AppendStreamSegment`, and `AbortStreamUpload`
       command payloads for PutObject stream staging metadata, with canonical
       encoding coverage
     - route PutObject stream session creation, segment append, and abort
       through the object metadata PG acting set; prepare drains pending object
       commands before allocating the next staged segment generation
     - harden stream append cleanup so zero-apply append command failures
       remove the just-written payload shards and ack rows, while partial
       replica apply keeps payload in place with the pending command for retry
     - include acting-set convergence coverage for PutObject stream session
       creation, append, and abort, plus a partial-primary-apply append retry
       regression proving payload remains readable until the pending command
       converges
     - add `CommitMultipartObject` command payloads as the actual completed
       multipart object publication path, carrying the exact live object,
       manifest parts, selected streamed part segment rows, completed-upload
       idempotence row data, omitted-part cleanup refs, active streamed
       UploadPart session cleanup refs, deterministic object write sequence,
       last modified timestamp, and stale payload reclaim rows needed to
       converge replicas after completion
     - initially advanced each applying node's bucket-PG completed multipart
       upload sequence to at least the command's completion order before
       publishing the object-PG command; Phase 7.3 replaces that cross-PG side
       effect with an explicit bucket-PG sequence command
     - serialize completed-upload order allocation on the bucket-PG primary
       before constructing the object-PG completion command, so concurrent
       completions for the same bucket on different object PG primaries cannot
       allocate duplicate pruning orders
     - retain pending multipart completion commands even when a hook or node
       failure occurs before the first acting node applies the command, because
       the command is now the convergence record for the completion attempt
     - include convergence regressions for streamed UploadPart completion across
       acting nodes, deterministic write sequence on a replica with divergent
       local object history, concurrent completion order allocation across
       different object PG primaries, replica bucket sequence advancement, and
       zero-apply command failure followed by retry
     - add a `PutObjectMetadata` command payload for ordinary per-version
       object metadata updates: object tags, tag deletion, object ACL,
       retention, and legal-hold changes
     - route those object metadata mutations through the object metadata PG
       acting set with the same pending-command drain/retry behavior used by
       direct PUT and delete commands
     - include deterministic canonical encoding coverage, acting-set
       convergence coverage for every mutation kind, and a partial-primary
       apply retry regression for object tag updates
     - route lifecycle current-object expiration, noncurrent live-version
       expiration, and expired delete-marker removal through the object
       metadata PG command stream
     - extend null-version delete-marker commands to carry optional stale
       payload reclaim metadata, so suspended-versioning lifecycle expiration
       converges object replacement, segment-row cleanup, and reclaim rows on
       every acting object-PG node
     - include convergence coverage for disabled current expiration, suspended
       null-current expiration, noncurrent live-version expiration, and expired
       delete-marker removal
     - add a `CreateMultipartUpload` command payload carrying the exact upload
       row, initiated timestamp, and reserved object generation
     - route multipart upload creation through the object metadata PG acting set
       with pending-command retry for partial replica apply, while preserving
       the raw upload-id generation reservation used by completion/abort
     - include acting-set convergence coverage for multipart create and a
       partial-primary-apply retry regression proving the same pending command
       converges before the retry returns
     - add an `AbortMultipartUpload` command payload so explicit abort deletes
       the multipart upload row, generation reservation, part rows, and streamed
       part staging metadata from every acting object-PG node
     - keep payload shard deletion cluster-owned; abort preparation snapshots
       the primary cleanup rows under the object-PG bucket lock and carries
       them in the abort command so partial-apply retries can still clean
       uploaded part and active streamed UploadPart payloads after metadata
       converges
     - include regression coverage for create -> abort -> fresh create on the
       same key, proving abort does not leave replica-local generation
       reservations that poison the next create
     - include partial-apply abort retry coverage with uploaded part payload
       shards, proving the pending command keeps enough cleanup refs to delete
       placed payload files after retry
     - route lifecycle-driven MPU abort through the same command path after the
       bucket-lifecycle recheck, and cover it with acting-set convergence plus
       placed payload cleanup assertions
     - add a `CommitStreamPart` command payload carrying the exact multipart
       upload row, committed part row, selected staged segment rows, previous
       part row, and displaced staged segment rows needed for replacement
       cleanup
     - route streamed UploadPart session creation, segment append, finalization,
       and abort through the object metadata PG acting set; append and abort now
       use the same command path for PutObject and UploadPart stream sessions
     - make stream-part command apply validate the explicit part generation,
       upload/session binding, staged segment snapshot, and displaced replacement
       segment snapshot before deleting stream staging rows
     - return the committed command timestamp from streamed UploadPart
       finalization, so partial-apply retry converges to the accepted metadata
       timestamp instead of a retry-local timestamp
     - include acting-set convergence coverage for streamed UploadPart staging
       and finalization, asserting every active object-PG node has the part and
       staged segment rows and no residual stream session after finalize
     - make `CreateStreamUpload` command apply revalidate `UploadPart` targets
       against the current multipart upload row on each acting node, so an
       abort/complete that wins after preflight cannot leave a new stream
       session for a non-in-progress upload
     - remove the obsolete `SharedStorageNode` streamed UploadPart create and
       finalize helpers, leaving streamed UploadPart metadata mutation on the
       cluster command path
     - add a `DeleteObjectPayloadReclaim` command payload carrying the exact
       reclaim row being removed after placed payload cleanup, so reclaim worker
       metadata transitions converge across the object-PG acting set and
       partial apply leaves a retryable pending command
     - include acting-set convergence and partial-apply retry coverage for
       reclaim-row deletion after payload cleanup
     - audit and remove the remaining dead node-local metadata mutation
       bypasses for direct object delete, streamed PutObject mutation, and
       test-only multipart create; retained node-local multipart methods are
       read-only/preflight helpers, while active mutation goes through the
       cluster command path
   - remaining slices: none; Phase 6.4 is complete.
5. Phase 6.5: replica command acceptance hardening. Complete.
   - keep the Phase 6.4 invariant that metadata commands are applied to every
     required acting-set replica before acknowledging success
   - add replica-side validation so commands are rejected when they come from a
     non-primary origin, stale epoch, wrong PG, or a conflicting duplicate log
     position
   - make fail-closed replica rejection explicit: writes fail when any required
     metadata replica rejects or cannot validate the command
   - cover accepted-command convergence and rejected-command non-mutation in
     no-failure and injected-rejection tests
   - completed:
     - replica acceptance now validates command origin, command epoch, target
       PG, active acting-set membership, and per-replica log position before
       mutating the local `PgStore`
     - exact duplicate delivery of an already accepted command is treated as an
       already-applied no-op, so partial-apply retry remains idempotent without
       re-running local mutation
     - same-index divergent commands fail closed with a typed `StoreError`
       variant and do not mutate replica state
     - unseen out-of-allocation-order log indexes are accepted in this
       in-memory phase, because command indexes are allocated before the
       apply lock and different buckets on the same PG can validly apply in a
       different order until Phase 6.6 introduces a durable PG command log
     - contiguous durable log-chain enforcement, hash chaining, and state
       digests remain Phase 6.6 work
     - tests cover invalid origin/epoch/PG/acting-set context, accepted
       convergence, duplicate retry no-op, conflicting duplicate rejection,
       valid out-of-allocation-order application, and non-mutation on rejection
6. Phase 6.6: log chain and state digest. Complete for the local command-owned
   metadata surface.
   - add a durable command-log hash chain or equivalent replay state:
     epoch, PG ID, monotonically increasing log index, previous log hash, and
     command checksum
   - record each replica's applied log index, applied log hash, and state digest
     or equivalent comparison value
   - command-log checksum mismatch prevents replica acknowledgement
   - applied-state digest mismatch prevents the PG from being considered clean
   - completed:
     - each per-PG SQLite store now has a durable `metadata_command_log` table
       and `metadata_command_replica_state` row
     - command acceptance reads durable replica state, so duplicate retry and
       checksum-conflict handling survive local cluster reopen
     - local log-index allocation is seeded from each PG's durable command log
       during local cluster open, so post-restart commands allocate after the
       persisted maximum index instead of reusing index 1
     - each replica applies the metadata mutation and records the durable
       command-log/state advancement in one SQLite transaction; a local record
       failure rolls back the metadata mutation instead of leaving an
       unconvergeable digest mismatch
     - zero-apply commands that are abandoned now record durable no-op/tombstone
       log entries on the acting set before clearing pending state, preserving
       the contiguous hash-chain prefix for later commands; this covers both
       shared request-op publishers and cluster-level object publishers such as
       generation reservation, generation release, stream append, and direct PUT
       commit
     - tombstone recording is idempotent across partial abandon attempts: if one
       replica records the abandoned log row and another fails, a later pending
       retry finishes recording matching tombstones instead of treating the
       abandoned row as a command-log conflict
     - pending command replay now distinguishes `Applied` from `Abandoned`;
       matching idempotent retries do not report success for a command that was
       deliberately skipped, and bucket/object entry points either rebuild a
       fresh command or fail closed rather than synthesizing a result from the
       abandoned command payload
     - abandoned PUT-object stream creation releases its pre-reserved generation
       when pending drain later completes the tombstone, without recursively
       entering the public bucket-locking release path
     - required reservation cleanup triggered by abandoned commands is no longer
       best-effort: if the compensating release command cannot be durably
       applied it remains pending and the drain returns an error for a later
       retry, instead of tombstoning or dropping the release
     - sparse out-of-allocation-order command entries remain valid: the
       contiguous hash-chain prefix advances when missing earlier entries arrive
     - the Phase 6.6 state digest initially covered committed command-owned
       metadata tables: `buckets`, `bucket_subresources`, `objects`,
       `object_parts`, `object_segments`, and committed rows in
       `multipart_part_segments`; Phase 7.3 expands this into the broader
       canonical binary inventory
     - the `buckets` digest uses an explicit committed-metadata column allowlist;
       transient write-drain counters such as `write_reservations_blocked` and
       `active_write_reservations` are excluded; at the Phase 6.6 boundary the
       local completed-MPU order allocator column
       `completed_multipart_upload_sequence` was also still excluded
     - transient bridge/staging/reclaim/allocator tables are intentionally
       outside this digest until those paths are fully command-owned; examples
       include payload shard ack rows, stream session/control rows, raw stream
       segment staging rows, multipart upload/control rows, multipart part
       staging rows, reclaim rows, and local allocator counters
     - the online full-state digest gate remains bounded to early/small local
       command streams to avoid quadratic command execution on large traces; if
       a local command stream crosses that bound, restart validation treats the
       unverified sentinel as a mandatory full-digest recompute point, persists
       the recomputed digest, and only then admits the PG as locally clean
     - tests cover atomic apply-plus-record rollback, durable duplicate retry
       after reopen, post-reopen allocation past persisted log entries,
       zero-apply tombstone convergence, partial tombstone retry, abandoned
       create-bucket retry, abandoned generation reservation retry, abandoned
       stream-create reservation cleanup, failed abandoned-cleanup retry,
       checksum conflict rejection, out-of-order sparse log convergence, and
       committed metadata/manifest digest mismatch rejection
   - remaining slices: none; Phase 6.6 is complete.
7. Phase 6.7: peering placeholders and closeout. Complete.
   - add explicit peering/backfill/inconsistent placeholders needed by later
     repair work, but keep failure handling disabled initially
   - keep strict writes as the only acknowledged write mode
   - close out with boundary checks proving active metadata paths route through
     cluster-owned PG routing
   - proposed slices:
     1. add the missing `PgState::Inconsistent` placeholder and make every
        existing PG-route path treat all non-`Active` states as fail-closed
        route/control-plane errors with typed state context
     2. pin the non-active PG behavior matrix for metadata primary lookup,
        acting-set fanout, command apply/abandon, payload placement, shard
        read/write/delete, and placed segment recovery; Phase 6.7 must not add
        real peering, repair, or degraded availability behavior
     3. reassert strict writes: successful writes still require every required
        metadata replica and every required payload shard; no quorum
        acknowledgement, degraded writes, or missing-shard writes are accepted
     4. close the production boundary: remaining metadata-primary bridge access
        is test-hook-only, active production metadata paths cannot reach
        `StorageCluster::single_node`, and direct shard IO / metadata-primary
        payload paths cannot reappear
     5. update the invariant guide, public `StorageCluster` method matrix,
        boundary-check script, and local-cluster trace/model tests so the final
        Phase 6 routing boundary is executable rather than review-only
   - completed:
     - `PgState::Inconsistent` is now part of the local PG state model; metadata
       primary lookup, acting-set fanout, command apply/abandon validation,
       payload placement, shard IO, and placed segment recovery all reject
       every non-`Active` state before reading or mutating metadata/payload
       state
     - the local-cluster trace strategy now includes `Inconsistent`, so the
       existing randomized route/epoch/cleanup model exercises it alongside
       peering, degraded, and backfilling states
     - strict-write behavior is pinned with deterministic tests: a metadata
       write with a missing required acting-set replica fails closed without
       publishing bucket metadata and keeps the command pending for retry, and a
       direct PUT payload write fails if any placed shard target is outside the
       active data-PG acting set while cleaning up any shards written before the
       failure
     - the invariant guide now explicitly states that Phase 6 writes require
       every metadata replica and every required payload shard; quorum,
       degraded, and missing-shard writes remain out of scope
     - `scripts/check-storage-cluster-boundaries` now fails if
       `self.single_node` is used outside the two approved
       metadata-primary helper methods or if `metadata_primary_bridge_node()`
       is called outside a cfg-gated test hook, in addition to the existing
       placed-shard and legacy metadata-primary payload checks
     - the public `StorageCluster` method matrix has been refreshed with the
       metadata-command test hooks added during Phase 6
     - exit criteria are satisfied by the Phase 6.2-6.7 command-path,
       convergence, checksum/digest, strict-write, non-active route, and
       boundary-check coverage; Phase 6.7 closeout verification used
       `cargo nextest run`, `cargo clippy --all-targets --all-features --
       -D warnings`, `./scripts/check-storage-cluster-boundaries`, and
       `git diff --check`
   - remaining slices: none; Phase 6.7 is complete.
   - explicitly out of scope for Phase 6.7:
     - heartbeat or failure detection
     - cluster-epoch changes for membership or acting-set updates
     - primary election or promotion
     - real peering/backfill/repair/migration
     - degraded read/write availability policy implementation
     - metadata command-log retention/compaction implementation

Exit criteria:

1. mutating PG operations are command-shaped
2. metadata replicas converge in no-failure tests
3. primary and replica SQLite state can be compared in deterministic tests
4. writes fail closed when a required metadata replica is unavailable
5. command-log checksum mismatch prevents replica acknowledgement
6. applied-state digest mismatch prevents the PG from being considered clean
7. no active cluster metadata path mutates or reads PG state by bypassing
   cluster PG routing through `StorageCluster::single_node`

## Phase 7: Metadata Model, Integrity, And Divergence Policy

Define the metadata model before adding repair, degraded availability, or
multi-process coordination.

Phase 6 created a working replicated command path, but it deliberately followed
the existing request-shaped code. That leaves some foundations too implicit:

- the command log and SQLite tables currently duplicate much of the same state
  until the materialized tables overwrite or delete old rows
- the system has a command-to-database apply path, but not a clearly documented
  command/state/checkpoint model
- many command payloads still mirror exact S3 operations rather than a smaller
  storage-level mutation language
- compaction and repair cannot be made safe until the relationship between log
  history, materialized state, and checkpoints is explicit

The intended model is:

- the metadata command log is the ordered mutation history for a PG
- SQLite metadata tables are the materialized serving view of that history
- a checkpoint or snapshot is a compact proof of materialized state at a log
  index
- object payload bytes are not part of the metadata command log, canonical
  metadata state, checkpoint, or scrub digest; the metadata records only payload
  descriptors such as size, segment/part layout, placement references, and the
  payload CRC64 values needed to verify object data
- replay is `checkpoint + retained log tail -> materialized state`
- deterministic apply is `canonical command + prior canonical state -> next
  canonical state`
- scrub and repair compare canonical state, not accidental SQLite row layout
- the system must never resolve disagreement by silently choosing whichever
  replica answered first

This phase is intentionally early. If the command representation, state digest,
checkpoint shape, or replay model needs rework, doing it now is cheaper and
safer than after temporary availability policy, real peering, or multi-process
RPC add more states.

Work items:

1. document the source-of-truth split:
   - command log as mutation history
   - SQLite tables as materialized serving view
   - checkpoints/snapshots as compact equivalence points
   - pending commands as unclosed log intents, not independent state
2. define canonical metadata state independent of SQLite layout:
   - bucket records and subresources
   - object versions and delete markers
   - segment manifests and object parts, including payload sizes and CRC64
     values, but not object payload bytes
   - multipart uploads, uploaded parts, and streamed part segments
   - reclaim rows, write sequences, bucket execution generations, and other
     allocators/counters
3. define canonical row, table-range, and full-PG encodings for digest and
   repair comparison:
   - stable field ordering and integer/string encoding
   - explicit handling of optional fields and deleted/missing rows
   - table/range boundaries that can scale beyond small test PGs
4. decide whether to keep request-shaped commands or refactor toward
   storage-shaped commands before moving on:
   - current examples are AWS-shaped commands like put tags, put ACL, complete
     multipart upload, and lifecycle expiry
   - possible storage-shaped commands include `PutBucketRecord`,
     `PutBucketSubresource`, `PutObjectVersion`, `DeleteObjectVersion`,
     `PutSegmentManifest`, `PutMultipartUpload`, `PutMultipartPart`,
     `DeleteMultipartUpload`, and `DeleteReclaimRecord`
   - S3 semantics should ideally produce storage mutations; replication,
     replay, digest, and repair should reason about storage mutations
5. define the command/state mapping:
   - every command has deterministic preconditions over canonical state
   - every accepted command produces deterministic canonical state changes
   - replay does not require reconstructing an AWS request from current
     database state
   - reverse mapping is not required after overwrites/deletes, but replay and
     checkpoint validation must be unambiguous
6. define integrity and scrub policy:
   - use the Phase 6 command-log hash chain and replica state table as the
     baseline integrity source for every replicated metadata PG
   - every persisted binary command-log entry must carry a per-entry checksum
     in addition to the hash-chain link, so disk bitrot or torn writes are
     detected before replay, compaction, peering, or repair trusts that entry
   - every persisted checkpoint, snapshot, canonical row block, or table-range
     block introduced by this phase must also carry a checksum over its
     canonical binary encoding
   - use canonical row/range/full-PG digests where scrub or targeted repair
     needs comparison beyond the log
   - SQLite page and constraint checks are useful diagnostics, but they are not
     the integrity boundary for replicated metadata
7. define divergence resolution rules:
   - replay from a valid command log when possible
   - rebuild a bad replica from a clean peer or snapshot when needed
   - enter inconsistent or peering state when no safe authoritative source
     exists
8. define metadata command-log retention and compaction policy:
   - compact only after a durable checkpoint, state digest, or equivalent
     replica comparison point has been established
   - preserve enough log history for pending command retry, replica restart,
     peering, and targeted repair
   - define how compacted replicas prove equivalence and still reject stale,
     divergent, or conflicting commands
9. implement the chosen immediate rework before Phase 8 if it affects command
   payloads, canonical encodings, digest/checkpoint shape, or repair semantics
10. add model and corruption tests:
   - deterministic command apply over canonical state
   - replay from checkpoint plus log tail
   - row corruption detected by canonical digest
   - missing, reordered, modified, or conflicting log entries detected
   - divergent replicas excluded from clean PG state until repaired

Proposed slices:

1. Phase 7.1: model and current digest inventory.
   - add a guide that defines the command-log/materialized-view/checkpoint
     relationship, payload-byte boundary, canonical-state target, command
     mapping rules, and divergence policy
   - make the current committed-serving-view digest inventory explicit in code,
     so included tables and intentional Phase 7 gaps are reviewable without
     relying on SQLite schema discovery
   - keep the existing SQL-row digest as an interim online gate; this slice does
     not yet introduce the final binary canonical row/range/checkpoint encoding
2. Phase 7.2: storage-shaped command decision and command mapping.
   - decide which existing S3-shaped command payloads should be retained for
     now and which should be converted to storage-shaped mutations before Phase
     8
   - define deterministic preconditions and state transitions for the retained
     command set over canonical metadata state
   - add a command mapping matrix to
     [metadata-model.md](../guides/metadata-model.md) covering current command
     shape, canonical state read/write sets, deterministic preconditions, and
     retry/idempotence behavior
   - make the rule explicit for future commands: coordinator/auth code handles
     AWS request semantics, while storage commands carry post-validation storage
     effects that replay can apply without reconstructing an AWS request
   - convert the clearest request-shaped payloads first:
     `CreateMultipartUploadCommand` should carry the exact multipart upload row,
     and `CreateStreamUploadCommand` should carry the exact stream session row
     and target storage state, rather than embedding the request structs
   - rename or replace request-oriented retry comparison helpers where touched,
     so storage pending-command retry compares command intent/effect rather than
     AWS request semantics
3. Phase 7.3: canonical binary state encoding. Complete.
   - define row, table-range, and full-PG encodings that do not depend on
     SQLite row formatting
   - include payload descriptors and payload CRC64 values, but not object
     payload bytes
   - first convert the online committed-serving-view digest from SQLite
     `quote(...)` text to typed canonical binary table-range encoding, then
     expand the inventory to the remaining command-owned in-progress,
     allocator, and reclaim tables
4. Phase 7.4: log checksum and restart replay validation. Complete.
   - inventory persisted binary and integrity records before changing formats:
     `metadata_command_log`, command canonical bytes, command-log hash-chain
     state, full-PG canonical digest inputs, and any checkpoint/range block
     format introduced in this phase
   - add or update persisted binary formats so every log entry and every new
     checkpoint/range block has a checksum over its canonical encoding; the
     current log row stores command checksums and hash links, but not the
     canonical command bytes needed for standalone replay
   - define whether Phase 7.4 introduces persisted checkpoints/range blocks or
     only proves log-tail replay against the current full-PG canonical digest;
     if a checkpoint/range block is introduced, it must carry kind/version, PG,
     epoch, range identity, row count/range metadata, canonical bytes or digest,
     and a checksum over that block
   - add a storage-level replay harness that creates state through
     `StorageCluster`, reopens stores, validates or replays the durable command
     history, compares canonical state before and after, and uses direct
     metadata mutation only for explicit corruption/divergence injection
   - add corruption tests for modified command payload/checksum, modified
     command id/log index, modified previous hash, missing middle log entry,
     duplicate log index with conflicting payload, abandoned entries, and
     corrupted canonical metadata rows
   - chose not to introduce persisted checkpoints/range blocks in Phase 7.4:
     checkpoint-only replay, checkpoint plus valid tail, corrupted checkpoint,
     corrupted tail, stale checkpoint handling, checkpoint-based repair, and
     log compaction remain Phase 7.5/Phase 10 work
   - add divergence tests for primary row corruption, replica row corruption,
     missing/extra/reordered replica log entries, same materialized state with
     different history, and matching history with corrupted materialized state
   - close out by updating [metadata-model.md](../guides/metadata-model.md)
     with the implemented formats, checksum coverage, replay guarantees, and
     any retention/compaction items deferred to Phase 7.5
5. Phase 7.5: retention and compaction policy.
   - define the pre-checkpoint retention contract:
     - retain every accepted command-log row in the applied prefix
       `1..=applied_log_index`
     - retain abandoned/tombstone rows exactly like applied rows until a durable
       checkpoint or equivalent compact proof covers them
     - retain sparse tail rows beyond the applied prefix because a missing
       earlier command can later arrive and advance the prefix
     - retain pending commands and any rows needed for retry/idempotence until
       the pending operation converges or is durably abandoned
     - retain enough log history for restart validation and acting-set
       agreement; a materialized state digest alone is not a compaction
       authority
   - add mechanical guardrails before implementing compaction:
     - expose command-log retention stats per PG: min/max log index, applied
       index, retained count, abandoned count, sparse tail count, and missing
       applied-prefix entries
     - make the compaction entry point explicitly return
       `UnsupportedUntilCheckpoint` while no durable checkpoint/range block
       exists
     - add tests proving unsupported compaction is a no-op and retained applied
       rows remain required for restart validation
   - specify the future checkpoint equivalence shape without building it yet:
     - checkpoint covers PG, epoch, applied log index, applied log hash, and
       canonical state digest or canonical row/range blocks
     - every checkpoint and row/range block carries a CRC64 over its canonical
       bytes
     - compaction may delete log rows only at or below a checkpoint that all
       required acting-set replicas agree on
     - sparse tail rows and pending commands beyond the checkpoint remain
       retained
   - close the phase as retention policy plus guardrails; checkpoint-backed
     compaction, repair authority, and peering use of checkpoints remain Phase
     10 work unless Phase 7.5 explicitly expands scope

Completed:

- Phase 7.1:
  - added [metadata-model.md](../guides/metadata-model.md) with the
    source-of-truth split, payload-byte boundary, target canonical state,
    command-mapping direction, integrity requirements, and divergence rules
  - replaced SQLite schema discovery in the online metadata state digest with an
    explicit committed-serving-view table descriptor list:
    `bucket_subresources`, `buckets`, committed `multipart_part_segments`,
    `object_parts`, `object_segments`, and `objects`
  - pinned that interim digest inventory in a unit test; the missing tables
    remain visible Phase 7 gaps rather than hidden schema-scan exclusions
- Phase 7.2 first slice:
  - added the command mapping inventory to
    [metadata-model.md](../guides/metadata-model.md), including current command
    shape, canonical read/write sets, and retry behavior
  - converted `CreateStreamUploadCommand` from an embedded
    `CreateStreamUploadReq` to a storage-shaped stream session row
  - converted `CreateMultipartUploadCommand` from an embedded
    `CreateMultipartUploadReq` plus separate timestamps/generation fields to a
    storage-shaped multipart upload row
  - updated canonical command encoding coverage so stream/MPU creation command
    checksums cover the storage row state
  - tightened stream/MPU creation retry matching so an existing row is accepted
    only when it exactly matches the row from an applied pending command; same
    request fields with different row-only fields now fail closed
  - included all then-durable stream session row fields in
    `StreamUploadRecord` and `CreateStreamUploadCommand` checksums so stream
    create row matching covers the full storage row
- Phase 7.2 `PutObjectMetadata` slice:
  - converted the durable `PutObjectMetadataCommand` from request-shaped
    "put tags/delete tags/put ACL/put retention/put legal hold" mutations to a
    storage-shaped post-mutation `LiveObjectRecord`
  - kept the mutation enum as an internal constructor detail at the storage API
    boundary, where coordinator-approved AWS semantics are turned into a
    concrete metadata row image
  - changed command encoding so object metadata updates checksum the live object
    post-image, including ACL/public-read, tags, object-lock state, payload
    identity, timestamps, layout, metadata blobs, and encryption state
  - tightened pending-command retry so a same AWS mutation is accepted only when
    the pending command's post-image exactly matches the row the retry would
    produce
- Phase 7.2 bucket-command closeout:
  - converted `CreateBucketCommand` to carry the exact bucket-table row image
    rather than reconstructing a row from `CreateBucketConfig` during replay
  - converted `PutBucketVersioningCommand`, `PutBucketAclCommand`, and
    `PutBucketPropertyCommand` to carry bucket-table post-images; bucket
    property commands retain only a storage property-effect discriminator so
    replay can validate the intended field group changed
  - added an explicit bucket-row command type with raw storage columns,
    including raw encryption settings, so same-effective but different-stored
    rows do not compare equal
  - tightened bucket pending-command retry so retries compare the row image
    produced by the current request against the pending command post-image, not
    just the original AWS request fields
  - updated canonical command encoding coverage and retry regressions for
    bucket row-image matching
  - Phase 7.2 is complete: all current metadata command payloads are classified
    as storage-shaped before Phase 7.3 canonical binary state encoding
- Phase 7.3 first slices:
  - replaced the online committed-serving-view digest's SQLite `quote(...)`
    row text with a canonical binary full-PG encoding over the explicit Phase
    7 inventory
  - the full-PG encoding now starts with a stable domain/version header; each
    encoded table range includes table name, filter identity, ordered column
    list, row boundaries, and typed SQL values
  - canonical value encoding distinguishes `NULL`, integers, text, blobs, and
    real values before hashing, so metadata bitrot checks no longer depend on
    SQLite text formatting
  - expanded the online digest inventory to command-owned durable upload,
    stream, reclaim, and reservation rows:
    `multipart_uploads`, `multipart_parts`, all `multipart_part_segments`
    including staging rows, `completed_multipart_uploads`, `stream_uploads`,
    `stream_upload_segments`, `object_generation_reservations`,
    `object_segments_reclaims`, `object_segment_reclaim_segments`,
    `multipart_reclaims`, `multipart_reclaim_parts`, and
    `multipart_reclaim_part_segments`
  - changed digest-covered test setup hooks, including reclaim-row seeding and
    stream-session timestamp forcing, to update every object-PG acting node and
    refresh each replica's state digest, so test-only setup does not hide real
    digest divergence
  - added `DeleteCompletedMultipartUpload` as a row-shaped metadata command
    for completed-MPU tombstone pruning and bucket-finalization cleanup; direct
    primary-local deletion is not valid now that completed tombstones are
    covered by the canonical metadata digest
  - removed durable `stream_uploads.next_segment_vid`; stream segment payload
    generation allocation is now local runtime state used only to keep
    concurrent uncommitted appends on distinct placed shard keys, while the
    command-owned `stream_upload_segments.segment_vid` value remains in the
    canonical digest
  - moved `object_version_counters` behind object metadata command ownership:
    `next_version_id` is now a read-only candidate allocator, and versioned
    object writers apply an explicit `ReserveObjectVersion` command before
    publish/delete-marker commands consume the reserved ID; object publish
    commands still carry the exact version ID and keep the counter monotonic
    during apply, while the counter table is included in the canonical digest
  - hardened `ReserveObjectVersion` after partial apply plus local-cluster
    reopen: new reservations choose the maximum next-version candidate across
    the acting set, and replicas whose local counter is lower advance forward
    under the command; this can leave gaps in opaque version IDs, but prevents
    a lost in-memory pending reservation from making a versioned key
    permanently unwritable before Phase 7.4 adds durable replay/checkpoint
    repair
  - narrowed the legacy `PgMetadataStore::put_object_meta` object-row seeder to
    `cfg(test)`, so production code cannot mutate command-owned object rows or
    version counters outside metadata command apply
  - made `PgMetadataStore` crate-private and removed its public re-export; the
    remaining old direct command-owned bucket/object/multipart/stream mutators
    are gated to tests or explicit test hooks, while production mutations
    enter through `StorageCluster` command APIs and private `PgStore` command
    apply helpers
  - renamed the remaining production bucket-row delete hook to
    `PgMetadataStore::delete_finalized_bucket` and documented it as the explicit
    finalization exception: it is only used by the cluster acting-set fanout
    after `MarkBucketDeleting`, write drain, visible-data checks, and reclaim
    checks have completed; each local delete fails closed unless the replica row
    is already `Deleting`
  - moved `buckets.completed_multipart_upload_sequence` behind an explicit
    bucket-PG `AdvanceCompletedMultipartUploadSequence` metadata command:
    CompleteMultipartUpload first reserves/advances the bucket sequence through
    the bucket command stream, then uses that reserved order in the object-PG
    `CommitMultipartObject` command; object-PG command apply no longer mutates
    bucket-PG state as a side effect
  - included `buckets.completed_multipart_upload_sequence` in the canonical
    bucket-row digest and stopped treating it as local runtime state in bucket
    command row projections
  - moved `pg_counters.next_bucket_execution_generation` under bucket metadata
    command ownership: command construction now reads a candidate execution
    generation without mutating the durable counter, and command apply advances
    `pg_counters` in the same transaction as the bucket row/subresource
    mutation
  - moved DeleteBucket begin onto a row-shaped `MarkBucketDeleting` bucket
    metadata command, so the Active-to-Deleting state transition and execution
    generation advance are replicated through the command stream rather than
    written through the old metadata-primary bucket path
  - narrowed old `SharedStorageNode` direct bucket mutation helpers to
    `cfg(test)`, leaving production bucket state/subresource/counter mutation
    behind `StorageCluster` command APIs
  - included `pg_counters` in the canonical full-PG digest so counter
    corruption or off-command allocator mutation is detected before later
    command apply
  - included `multipart_uploads.state` in the canonical full-PG digest and
    removed the abort-prepare path's off-command `Aborting` state write; abort
    and complete commands now carry the exact terminal cleanup snapshot they
    observed under the object-PG bucket lock and delete the upload row through
    metadata command apply
  - narrowed the raw multipart upload state setter to test/test-hook builds, so
    production code cannot change digest-covered upload state outside command
    apply
  - retained explicit exclusions for state that is still local-only or still
    mutates outside its own command stream:
    - bucket write-drain counters (`write_reservations_blocked` and
      `active_write_reservations`) are bucket-delete/read-write fencing state,
      not canonical S3 metadata; they remain outside the metadata digest until
      the Phase 9 lease/fence model decides whether this state should stay
      process-local or become a replicated fence
  - added a mechanical storage-cluster boundary check that fails closed if new
    ungated production `PgMetadataStore` methods are added outside the explicit
    read-only/finalized-delete/write-drain allowlist
  - documented the test setup rule that normal cluster/coordinator metadata
    state should be created through production `StorageCluster` or
    `Coordinator` APIs; direct `PgMetadataStore` mutation is reserved for
    PgStore unit coverage, replica-local assertions, and explicit
    fault-injection/divergence setup with a comment
  - Phase 7.3 is complete: canonical binary state encoding covers every current
    command-owned metadata table, production direct mutators for digest-covered
    metadata are gated or private, and bucket write-drain counters are
    explicitly deferred to the Phase 9 fence/lease model
- Phase 7.4 step 1:
  - added the persisted integrity record inventory to
    [metadata-model.md](../guides/metadata-model.md), covering current
    `metadata_command_log` rows, in-memory canonical command bytes,
    `metadata_command_replica_state`, full-PG canonical digest input,
    checkpoint/range block gap, and the object-payload boundary
  - made the then-current replay gap explicit: command-log rows persisted the
    command checksum and hash-chain links, but not the canonical command bytes
    needed for standalone log replay
- Phase 7.4 step 2:
  - persisted canonical command bytes in `metadata_command_log.command_bytes`
    for applied commands
  - replaced the abandoned-command zero checksum sentinel with a distinct
    canonical tombstone encoding whose checksum is tied to the original command
    checksum
  - verified persisted command bytes against `command_checksum` before
    idempotence, abandon, and hash-prefix advancement decisions
  - added command-log header decoding so stored bytes must agree with the SQL
    row's epoch, PG, log index, and applied-vs-tombstone kind before prefix
    advancement treats the row as valid
  - tightened applied-row validation so checksum-consistent bytes must also
    decode as a known command payload and consume the full command byte slice
  - wired the applied-row verifier into the stable command-encoding matrix so
    every maintained command payload encoding must also be accepted by the
    persisted-log validator, including representative branch variants
  - added regressions for stored checksum corruption, stored command-byte
    corruption, malformed-but-checksummed applied bytes, abandoned tombstone
    identity, row-key/kind mismatch, and conflict rollback with a
    valid-but-different command-log row
- Phase 7.4 step 3:
  - chose not to introduce persisted checkpoint/range blocks in this slice;
    checkpoint semantics remain deferred to Phase 7.5/Phase 10, and the current
    replay boundary is retained command-log prefix plus materialized canonical
    state digest
  - added local-cluster restart validation that walks every opened PG's applied
    command-log prefix, verifies persisted command bytes/checksums,
    applied-vs-abandoned identity, and hash-chain links, then compares the
    materialized metadata state against the stored canonical digest
  - added restart regressions for a missing applied log entry, missing
    replica-state row on a nonempty PG, reordered row identity, corrupted hash
    link, corrupted materialized metadata row, a large prefix whose state
    digest is explicitly unverified and must be recomputed/persisted during
    open, and a stale-but-internally-coherent replica whose validated command
    prefix differs from peers; incoherent durable/materialized state or
    acting-set disagreement still fails cluster open rather than admitting the
    replica as clean
- Phase 7.4 step 4:
  - expanded restart divergence coverage for non-reference replica row
    corruption and for replicas with the same materialized metadata digest but
    different accepted command-log history; both fail cluster open rather than
    resolving divergence by choosing an arbitrary replica
- Phase 7.4 step 5:
  - added a positive local-cluster replay/open harness that builds mixed bucket
    and object metadata through `StorageCluster`, captures each opened
    node/PG's accepted log index, log hash, state digest, and durable max log
    index, reopens the stores, and verifies those replay-state snapshots and
    materialized object rows are preserved
- Phase 7.4 closeout:
  - kept persisted checkpoint/range blocks out of Phase 7.4 deliberately; the
    implemented replay boundary is retained command-log prefix plus canonical
    materialized metadata digest, validated during local cluster open
  - documented that checkpoint replay, checkpoint corruption handling,
    digest-mismatch repair, and command-log compaction are deferred to Phase
    7.5/Phase 10 rather than partially implemented here
  - Phase 7.4 is complete: every persisted command-log row stores canonical
    bytes with CRC64 coverage, restart validation verifies command bytes,
    tombstone identity, hash-chain links, materialized state digest, and
    acting-set agreement, and tests cover corruption/divergence without
    silently choosing an arbitrary replica
- Phase 7.5 first slice:
  - documented the pre-checkpoint retention contract: applied-prefix rows,
    abandoned tombstones, sparse tail rows, pending retry rows, and any rows
    needed for restart validation and acting-set agreement are retained until a
    durable checkpoint or equivalent compact proof exists
  - added `PgStore` command-log introspection for retained rows, abandoned
    rows, sparse tail rows, missing applied-prefix rows, min/max log index, and
    the current applied index
  - added an explicit compaction guard that returns
    `UnsupportedUntilCheckpoint` and leaves rows untouched while checkpoint
    persistence is not implemented
  - added PgStore regressions proving pre-checkpoint compaction is a no-op and
    stats expose both retained applied-prefix rows and sparse tail rows

Exit criteria:

1. the log/materialized-view/checkpoint relationship is documented and encoded
   in tests, with checkpoint persistence explicitly deferred
2. every command-owned metadata table has a canonical state representation
3. the project has either committed to request-shaped commands for now with
   explicit limits, or refactored the core command payloads toward
   storage-shaped mutations
4. a corrupted metadata row is detected by restart validation against the
   canonical materialized metadata digest; read-time scrub remains future work
5. a replica with a missing, reordered, or modified command is detected
6. every persisted binary command-log entry and every persisted
   checkpoint/snapshot/range block introduced by this phase has a checksum over
   its canonical encoding; no checkpoint/snapshot/range block is introduced in
   this phase
7. a divergent replica is excluded from clean PG state until repaired
8. tests cover primary corruption, replica corruption, stale replica restart,
   digest mismatch detection, checksum failure, and retained log-prefix plus
   materialized digest validation
9. command-log compaction is not implemented in this phase; Phase 7.5 must
   define retention so compaction cannot remove entries still needed for
   pending retry, peering, restart recovery, or repair validation
10. the system never resolves divergent metadata by silently choosing an
   arbitrary replica

## Phase 8: Temporary Failure Write Policy

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

## Phase 9: Replace Process-Local Coordination

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

## Phase 10: Local Multi-Process RPC

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

## Phase 11: Failure, Peering, Repair, And Migration

Add real distributed behavior after the normal path is already shaped correctly.

Work items:

1. add heartbeat and failure detection to the control plane
2. bump cluster epoch on membership or PG acting-set changes
3. elect or assign new PG primaries
4. implement PG peering from the durable PG command log
5. define read and write availability rules for peering and degraded PGs
6. implement shard repair for missing or corrupt shards
7. implement PG backfill and migration for changed acting sets
8. implement metadata command-log retention and compaction using the Phase 7
   policy
9. add cluster-map history retention and pruning

Exit criteria:

1. stale primaries cannot accept writes after an epoch change
2. PGs enter peering before serving unsafe requests
3. repair restores missing shards from available EC data
4. migration can move a PG acting set without per-object metadata rewrites
5. command-log retention bounds long-running disk growth without breaking
   restart, peering, or repair correctness
6. failure-injection tests cover primary loss, replica loss, restart, and repair

## Phase 12: Replicated Control Plane

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
