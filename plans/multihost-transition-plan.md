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

These process-local mechanisms either had to move behind cluster-owned or
PG-primary-owned mechanisms, or had to be made explicitly test-only before
multihost mode could be considered correct:

1. PG mutexes: local-mode serialization remains local, but remote metadata
   command install/reissue/fanout uses storage-node-owned PG-primary critical
   sections.
2. ordered two-PG locking helpers: retired in Phase 10.8; bucket snapshot pairs
   now use routed operation-shaped snapshot reads.
3. bucket write drain waits: replaced by durable bucket write
   reservations/drains plus restartable polling.
4. multipart completion locks: retired; completion ordering and object
   publication use durable PG-primary command serialization.
5. coordinator-local object payload generation leases: replaced by
   command-owned reservation/allocation paths where correctness depends on
   durable metadata.
6. in-memory reclaim work queue: wake hint only; durable reclaim/finalizer roots
   are rescanned before blocking.
7. bucket cache invalidation and freshness paths: correctness-neutral
   performance state guarded by durable execution/incarnation identity checks.
8. any tests that inspect or mutate raw local PG state as if it were global:
   must remain explicit test hooks and must not be production authority.

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

1. storage unit and integration tests still call `SharedStorageNode::get_pg`
   directly in storage-owned setup/assertion helpers; the old two-PG lock helper
   has been removed
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
       local payload lease drops
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
     - keep finalization's reclaim-root checks and reclaim/finalize worker
       queues on the bridge until queue ownership is split into explicit
       coordination paths
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
     - PG mapping and default EC shape are now held by `LocalClusterMap`, not by
       the metadata-primary bridge
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
       primary cleanup rows through the object-PG storage-side snapshot/build
       boundary and carries them in the abort command so partial-apply retries
       can still clean uploaded part and active streamed UploadPart payloads
       after metadata converges. Phase 10.8 later removed the process-local
       object-PG bucket lock from this path.
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
       at the Phase 6.6 boundary, then-transient write-drain counters were
       excluded and the local completed-MPU order allocator column
       `completed_multipart_upload_sequence` was also still excluded
     - transient bridge/staging/reclaim/allocator tables are intentionally
       outside this digest until those paths are fully command-owned; examples
       include payload shard ack rows, stream session/control rows, raw stream
       segment staging rows, multipart upload/control rows, multipart part
       staging rows, reclaim rows, and local allocator counters
     - command apply keeps a per-table digest cache: it verifies the
       command-owned tables touched by the command, refreshes those table
       digests after apply, and persists the composed PG state digest;
       restart validation still recomputes the digest from materialized rows
       and must not derive new trust from those rows alone
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
5. Phase 7.5: retention and compaction policy. Complete.
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
   - note for later hardening: metadata digest maintenance must remain
     independent of request-shaped command table maps. Digest updates are
     maintained from SQLite row changes; before checkpoint-backed compaction or
     repair trusts checkpoints, add coverage that exercises representative
     command variants and asserts the cached PG digest still matches a full
     materialized recompute.
6. Phase 7.6: metadata-command performance stabilisation.
   - add a bounded optimisation phase before Phase 8 because the Phase 7
     command-log and digest hardening increased full-suite runtime from roughly
     122s at `b553724a13da5979971435bfac4e5f4d706cc609` to roughly 177s after
     Phase 7.5 closeout
   - keep this phase focused on current-path optimisation: reduce overhead
     without changing request semantics, storage APIs, command granularity, or
     coordinator operation ordering
   - semantic batching is deliberately split out of this phase. The main
     regression has been recovered by current-path work, and batching requires
     separate analysis: it only helps when a request contains multiple objects
     routed to the same metadata PG, and it needs a command-log, retry,
     partial-failure, and AWS per-object error design before implementation.
     Track that work in
     [delete-objects-batch-command-plan.md](delete-objects-batch-command-plan.md).
   - measure every slice before and after:
     - full `cargo nextest run` wall time
     - summed libtest per-test seconds grouped by server-core authz model,
       server-core coordinator, storage, server-http, and external S3 tests
     - representative isolated tests:
       `authz_model_phase2_get_object_acl_existing_matrix`,
       `authz_model_phase4_put_object_write_matrix`,
       `list_object_versions_clamps_oversized_max_keys`,
       `test_versioning_list_object_versions_oversized_max_keys_returns_at_most_1000_entries`,
       and `prop_storage_version_listing_pagination_roundtrip`
     - one `perf record/report` sample for
       `list_object_versions_clamps_oversized_max_keys`, tracking total cycles,
       SQLite prepare/planning cost, command-log/digest maintenance cost, and
       CRC/hash cost
   - Phase 7.6.1 current-path optimisation candidates:
     - replace hot repeated `PgStore` statement preparation with
       `prepare_cached` or an explicit `PgStore` statement-cache pattern where
       lifetimes and transactions remain clear
     - reduce redundant command-log state loads and prefix-advance queries
       inside a single command transaction without weakening restart validation
       or idempotence
     - keep incremental metadata digest triggers and bootstrap validation, but
       make sure common write paths do not re-run full table scans
     - verify that simple PUT/GET remains flat and that improvements land in
       metadata-heavy object/bucket mutation and listing paths
   - exit criteria:
     - current-path optimisation measurements identify which overheads were
       reduced and which remain
     - any remaining major slowdown has an explicit owner: statement
       preparation, command-log transaction volume, replica fanout, digest
       maintenance, or coordinator semantic batching
     - no batching API is introduced in Phase 7.6
     - the plan records that Phase 8 can proceed with the current metadata
       command cost; storage-level batching is a later standalone project

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
  - blob values in canonical table digests are encoded as byte length plus
    CRC64 of the blob contents; object payload bytes remain outside the command
    log, while payload descriptor rows include payload CRC64 values
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
  - removed digest-covered `stream_uploads.next_segment_vid` from the Phase 7.3
    command-owned metadata model; Phase 9.2 later reintroduces this as
    non-digest durable PG-primary allocator state, while the command-owned
    `stream_upload_segments.segment_vid` value remains in the canonical digest
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
    observed through the storage-side object-PG snapshot/build boundary and
    delete the upload row through metadata command apply. Phase 10.8 later
    removed the process-local object-PG bucket lock from this path.
  - narrowed the raw multipart upload state setter to test/test-hook builds, so
    production code cannot change digest-covered upload state outside command
    apply
  - retained explicit exclusions for state that was still local-only or still
    mutated outside its own command stream at this phase:
    - bucket write-drain counters were bucket-delete/read-write fencing state,
      not canonical S3 metadata; Phase 9.4 replaced them with durable
      bucket-PG-primary coordination rows, and Phase 9.4.6 removed the old
      bucket-row counter fields
  - added a mechanical storage-cluster boundary check that fails closed if new
    ungated production `PgMetadataStore` methods are added outside the explicit
    read-only/finalized-delete/write-drain allowlist
  - documented the test setup rule that normal cluster/coordinator metadata
    state should be created through production `StorageCluster` or
    `Coordinator` APIs; direct `PgMetadataStore` mutation is reserved for
    PgStore unit coverage, replica-local assertions, and explicit
    fault-injection/divergence setup with a comment
  - Phase 7.3 is complete: canonical binary state encoding covers every current
    command-owned metadata table, and production direct mutators for
    digest-covered metadata are gated or private. The bucket write-drain counter
    deferral was closed by Phase 9.4 durable coordination rows and Phase 9.4.6
    counter removal.
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
  - added a per-table digest cache for the command-apply path; online apply
    verifies and refreshes only the command-owned tables touched by the command,
    while restart validation still recomputes the full materialized digest from
    SQLite rows
  - added restart regressions for a missing applied log entry, missing
    replica-state row on a nonempty PG, reordered row identity, corrupted hash
    link, corrupted materialized metadata row, large-prefix materialized
    metadata tampering with a previously verified digest, and a
    stale-but-internally-coherent replica whose validated command prefix differs
    from peers; incoherent durable/materialized state or acting-set disagreement
    still fails cluster open rather than admitting the replica as clean
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
- Phase 7.5 closeout:
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
  - explicitly kept checkpoint-backed compaction, checkpoint repair authority,
    and peering use of checkpoints deferred to Phase 10
  - replaced command affected-table digest refresh with SQLite-maintained
    digest stats so common object PUT/delete paths do not scan growing metadata
    tables on every applied command
  - guarded digest trigger/bootstrap repair with a durable completion marker
    written after the full cache refresh, so partial trigger installation or
    interrupted refresh is retried on the next open
  - Phase 7.5 is complete: retention policy is fail-closed before checkpoints,
    compaction is an explicit no-op, and the exposed stats make retained log
    state inspectable without deleting rows
- Phase 7.6 setup:
  - profiling against `b553724a13da5979971435bfac4e5f4d706cc609` showed the
    remaining slowdown is concentrated in metadata-heavy server-core/storage
    paths, while the external S3 harness is approximately flat overall
  - representative current-vs-baseline isolated tests show roughly 1.7x-2.1x
    overhead on authz/model/listing/versioning paths, while simple PUT/GET is
    effectively unchanged
  - `perf` on `list_object_versions_clamps_oversized_max_keys` points at
    repeated SQLite statement preparation/planning and extra command-log/digest
    metadata work; CRC64 hashing is not a material hotspot
  - Phase 7.6 must measure each optimisation slice independently rather than
    hiding the cost with broad test harness shape changes
- Phase 7.6.1 first current-path optimisation slice:
  - increased the per-PG SQLite prepared-statement cache and routed hot
    command-log, metadata digest state, object-version counter, direct object
    write, delete-marker, object metadata update, and object-version delete SQL
    through cached statements
  - collapsed cached PG metadata digest calculation from one query per
    digest-covered table to one cached query over `metadata_table_digests`
  - representative selected server-core measurements:
    - before: 67.563 summed test-seconds, 34.755s wall
    - after command/digest helper caching: 49.436 summed test-seconds, 25.406s
      wall
    - after object-write/delete helper caching: 45.294 summed test-seconds,
      23.653s wall
  - `list_object_versions_clamps_oversized_max_keys` improved from 11.839s to
    6.801s in the selected nextest run; the focused `perf` sample dropped from
    roughly 53.4B cycles before Phase 7.6.1 to roughly 31.4B cycles, with
    SQLite prepare/planning samples dropping from roughly 47.9% to roughly
    25.5%
  - `test_versioning_list_object_versions_oversized_max_keys_returns_at_most_1000_entries`
    improved from 25.540s before this slice to 16.881s in the isolated S3
    harness run
  - simple PUT/GET remained effectively flat, which matches the goal of
    reducing metadata-command overhead rather than changing payload behavior
- Phase 7.6.1 second current-path optimisation slice:
  - routed additional hot `PgStore` prepared statements through SQLite's
    statement cache, including dynamic listing/query shapes that repeat across
    authz/model and version-listing tests; some production `.prepare()` sites
    remain for lower-volume shard, part, and segment helpers and can be
    considered in later current-path optimisation slices if perf points there
  - avoided one redundant command-log row reload on the common path where a
    newly inserted command is the next contiguous applied-prefix entry; conflict
    and retry paths still reload and validate the durable row
  - tried combining digest trigger maintenance into a single trigger `UPDATE`,
    measured it as slower, and reverted that part
  - representative selected server-core measurements improved from the prior
    committed 45.294 summed test-seconds / 23.653s wall to 44.010 summed
    test-seconds / 22.992s wall
  - `list_object_versions_clamps_oversized_max_keys` improved from 6.801s to
    6.550s in the selected nextest run; the isolated perf sample was roughly
    30.5B cycles with SQLite prepare/planning still around 23%
  - the isolated S3 oversized versioning test improved from 16.881s to 16.130s
- Phase 7.6.1 full-suite checkpoint after the first two current-path
  optimisation slices:
  - clean `cargo nextest run` completed 4525 tests in 137.108s, down from the
    Phase 7.5 closeout run's 177.927s and closer to the
    `b553724a13da5979971435bfac4e5f4d706cc609` baseline of 123.266s
  - summed libtest per-test time improved from 2721.7s to 2080.4s; the b553
    baseline was 1923.7s
  - remaining grouped deltas versus b553 are concentrated in:
    server-core authz/model at 1044.8s versus 929.7s, server-core coordinator
    non-model at 285.6s versus 234.8s, and storage unit/integration at 114.6s
    versus 65.1s
  - the external S3 harness is no longer the main regression in this sample:
    it measured 575.3s summed test time versus 642.0s at b553
  - the next current-path profiling targets are
    `authz_model_phase2_get_object_acl_existing_matrix`,
    `prop_storage_timestamp_tie_invariance`, and a representative
    multipart/reclaim trace property; these cover the largest remaining
    authz/model, storage property, and coordinator trace costs without changing
    request semantics
- Phase 7.6.1 remaining-hotspot profiling after the full-suite checkpoint:
  - isolated `perf record` timings were 22.416s for
    `authz_model_phase2_get_object_acl_existing_matrix`, 9.105s for
    `prop_storage_timestamp_tie_invariance`, and 6.810s for
    `prop_reclaim_queue_trace_matches_model`; isolated timings are lower than
    full-suite timings because they avoid nextest contention
  - flat perf samples still show SQLite as the largest remaining cost class:
    about 46% sqlite-ish symbols in the authz ACL matrix, about 58% in the
    storage timestamp property, and about 55% in the reclaim trace property
  - SQLite prepare/parser/tokenizer/trigger-construction symbols remain a
    material part of that cost, especially in the storage and reclaim property
    tests where `sqlite3GetToken`, `yy_reduce`, `yy_find_shift_action`,
    `sqlite3Parser`, `sqlite3RunParser`, `triggerSpanDup`, and
    `sqlite3_str_vappendf` are prominent
  - CRC/hash work is not the dominant remaining issue in the storage/reclaim
    properties, but it is visible in the authz ACL matrix where policy/model
    hash work and metadata digest CRC work are mixed with SQLite execution
  - the next low-risk current-path work should focus on eliminating remaining
    repeated SQLite parse sites that are still on hot metadata/property paths,
    then separately decide whether repeated per-case PgStore/schema/bootstrap
    setup in property/model tests should be reduced as test harness overhead
    rather than production request-path overhead
- Phase 7.6.1 SQLite cache follow-up:
  - converted the remaining explicit `PgStore` `.prepare(...)` call sites to
    `.prepare_cached(...)`, added metadata-error cached query helpers, and
    routed additional hot fixed statement shapes through them: bucket execution
    generation reads/advances, test bucket ACL/versioning/property mutations,
    object write/version/generation allocation reads, object metadata
    put/get/version reads, object ACL/tags/retention/legal-hold mutations,
    generation reservation reads/inserts, completed-MPU sequence reads, and
    reclaim-root/existence lookups
  - increased the per-PG SQLite prepared statement cache from 256 to 1024
    entries after measuring 2048 as no better on the representative sample
  - representative isolated timings:
    - after only the broad `.prepare_cached(...)` sweep:
      `authz_model_phase2_get_object_acl_existing_matrix` 22.183s,
      `prop_storage_timestamp_tie_invariance` 9.063s, and
      `prop_reclaim_queue_trace_matches_model` 6.770s
    - after the metadata cached-helper/cache-capacity follow-up:
      `authz_model_phase2_get_object_acl_existing_matrix` 21.611s,
      `prop_storage_timestamp_tie_invariance` 8.753s, and
      `prop_reclaim_queue_trace_matches_model` 6.759s
    - with a 2048-entry statement cache:
      `authz_model_phase2_get_object_acl_existing_matrix` 21.655s,
      `prop_storage_timestamp_tie_invariance` 8.730s, and
      `prop_reclaim_queue_trace_matches_model` 6.932s, so 1024 remains the
      current choice
  - the concrete external oversized versioning regression case,
    `test_versioning_list_object_versions_oversized_max_keys_returns_at_most_1000_entries`,
    measured 15.073s in an isolated current run without the temporary delete
    chunking workaround
  - new flat `perf` samples still show SQLite execution and parse/name lookup
    as the largest remaining cost class: storage timestamp is led by
    `sqlite3GetToken`, `sqlite3VdbeExec`, `yy_reduce`,
    `yy_find_shift_action`, `sqlite3_str_vappendf`, `sqlite3Parser`, and
    `sqlite3RunParser`; authz ACL is led by `sqlite3VdbeExec`,
    `sqlite3StrICmp`, `lookupName`, `exprDup`, WAL checksum work, and
    application CRC/hash work
  - the cache follow-up is therefore useful but modest; remaining Phase 7.6.1
    work should look for high-cardinality dynamic SQL, repeated schema/store
    bootstrap in model/property harnesses, and command-log transaction/query
    volume rather than expecting fixed-statement caching alone to close the
    full gap
  - added opt-in SQLite statement profiling with
    `ARGMIN_SQLITE_PROFILE_MS=<milliseconds>`; it uses
    `sqlite3_trace_v2(SQLITE_TRACE_PROFILE)` through `rusqlite` and logs
    per-statement elapsed time plus normalized SQL text
  - statement-profile aggregation shows the storage timestamp property has
    about 206k SQLite profile events; top total-time buckets are repeated
    schema/migration/bootstrap statements across temporary PgStores, including
    `ALTER TABLE ... ADD COLUMN ...`, `SELECT 1 FROM sqlite_master WHERE type =
    'trigger'`, digest trigger creation, `COMMIT`, and object insert/listing
    statements
	  - statement-profile aggregation for
	    `authz_model_phase2_get_object_acl_existing_matrix` shows about 791k SQLite
	    profile events; top total-time buckets are command-log and digest
	    transaction volume rather than isolated long queries: `COMMIT`,
	    `SELECT table_name, table_digest FROM metadata_table_digests`, command-log
	    row lookups, replica-state loads, bucket info loads, and bucket/object
	    mutation fanout
	  - in both samples SQLite's profile callback reported a 1ms maximum per
	    statement, so the visible slowdown is many repeated SQLite statements and
	    transaction boundaries rather than a small number of individually long
	    queries
  - Phase 7.6.1 production-path command/digest optimisation follow-up:
    - consolidated bucket-policy subresource mutation so a `PutBucketPolicy` or
      delete-policy command writes the subresource row with `RETURNING
      generation`, advances the bucket execution counter, and updates
      `bucket_policy_public`, `bucket_policy_generation`, and
      `bucket_execution_generation` in one bucket-row update instead of three
      separate bucket updates
    - changed metadata digest triggers to update digest stats and `table_digest`
      in one tuple assignment while computing the changed row digest once; this
      keeps the same digest format but reduces per-row trigger work for command
      owned tables
    - added a durable `metadata_digest_revision` row bumped by the same digest
      triggers; command acceptance still recomputes and compares the digest after
      dirty/off-log writes, including writes from another SQLite connection to
      the same PG, but skips the `metadata_table_digests` scan when this handle's
      last-clean revision still matches the durable revision
    - representative isolated
      `authz_model_phase2_get_object_acl_existing_matrix` timings improved from
      21.611s after the statement-cache follow-up to 17.515s after bucket
      subresource consolidation, 16.923s after tuple digest triggers, and
      16.373s after the durable digest-revision check
    - the statement-profile event count for that authz case fell from about
      791k before these command/digest optimisations to about 766k; the
      `SELECT table_name, table_digest FROM metadata_table_digests` count fell
      from 88,944 to 48,700
    - remaining production-shaped hotspots are still mostly command-log
      volume/transaction count, bucket info loads, object insert side effects,
      and object listing/read statement shapes; test-only schema/bootstrap
      overhead should stay lower priority unless it masks production costs
  - Phase 7.6.1 current-path bucket/version/log follow-up:
    - clean full-suite baseline after the previous command/digest slice was
      `cargo nextest run` passing 4528 tests in 118.100s
    - added a one-row command-log prefix fast path for the common case where a
      newly inserted command is exactly `applied_log_index + 1` and there is no
      durable tail row to fold in; conflict, retry, gap-filling, and
      malformed-row paths still use the full durable row validation path
    - changed hot bucket-by-name and bucket-record-by-name lookups to fixed
      cached statements and reused the `BucketInfo` already loaded by bucket
      write-reservation acquisition when constructing write snapshots
    - combined the object-version allocator's `MAX(version_id)` and
      `object_version_counters.next_version_id` reads into one cached statement,
      preserving the same max-of-materialized-state-and-counter allocation rule
    - tried a digest clean-revision micro-optimisation that replaced the
      combined state/revision read with a revision-only read on the clean path;
      statement profiling showed it doubled revision reads without improving the
      representative path, so it was reverted
    - representative isolated
      `authz_model_phase2_get_object_acl_existing_matrix` timing moved from
      16.161s at the start of this follow-up to 14.724s after fixed cached
      bucket lookups, 14.540s after write-snapshot bucket-row reuse, and
      14.747s after the final combined-version/log-fast-path slice in the
      latest sample
    - statement-profile counts for that authz case show bucket-info loads fell
      from about 39.7k to about 34.4k; the old separate
      `MAX(version_id)`/counter reads became one combined allocator read
    - full-suite validation after the bucket/version/log changes was
      `cargo nextest run` passing 4528 tests in 112.784s; after the
      current-object bookkeeping rewrite, deterministic fast-path coverage, and
      re-verification it was 4529 passing in 114.522s; after the final
      command-record digest-revision optimisation it was 4529 passing in
      114.171s
    - collapsed current-object bookkeeping for object writes/deletes from
      select-then-update into indexed single-statement updates/lookups using
      `(bucket, key, write_sequence DESC)`; focused version-selection tests and
      property checks stayed clean, while isolated timing samples remained
      within noise (`authz_model_phase2_get_object_acl_existing_matrix`
      15.225s, oversized version-listing 15.426s when sampled concurrently)
    - current statement profiling for the authz representative emitted about
      747k SQLite profile events; remaining high-frequency production-shaped
      buckets are still command-log/digest bookkeeping (`metadata_command_log`,
      `metadata_table_digests`, replica-state loads/updates and digest
      revision reads), followed by bucket info loads, version allocation, and
      object insert/currentness statements
    - a final small command-record optimisation now returns the digest revision
      from the same transaction-local cached table-digest scan used to compute
      the persisted metadata state digest, then marks the clean revision only
      after `COMMIT`; this removes the extra post-commit
      `SELECT revision FROM metadata_digest_revision` from successful command
      apply while preserving rollback invalidation
    - after that change, the same authz representative measured 15.107s
      isolated without statement profiling; with profiling enabled it emitted
      about 703k SQLite profile events, with 6 standalone revision reads, about
      4.2k standalone `metadata_table_digests` scans, and about 44.5k combined
      table-digest-plus-revision scans
    - listing representatives remain improved in isolated runs:
      `list_object_versions_clamps_oversized_max_keys` measured 5.164s and
      `test_versioning_list_object_versions_oversized_max_keys_returns_at_most_1000_entries`
      measured 13.985s
  - Phase 7.6 is complete for the multihost transition plan:
    - the full-suite runtime recovered from the Phase 7.5 closeout regression
      and is back below the original `b553724a13da5979971435bfac4e5f4d706cc609`
      comparison point in the latest clean run
    - current-path work recovered the main performance loss without changing
      request semantics or storage command granularity
    - Phase 7.6.1 is closed. Remaining optimisation candidates are either
      deeper command-log/validation design work, mostly test-harness bootstrap
      cost, or semantic batching; do not continue current-path optimisation
      here unless new production-shaped measurements show a concrete
      regression.
    - `DeleteObjects` storage-level batching has been moved to
      [delete-objects-batch-command-plan.md](delete-objects-batch-command-plan.md)
      because it is a semantic API change that needs separate cost/benefit and
      failure-mode analysis before implementation

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

The policy decision and future handoff model are recorded in
[temporary-write-availability.md](../guides/temporary-write-availability.md).
The summary is:

- keep strict writes as the implemented policy for now
- allow degraded reads when EC reconstruction is safe
- do not make degraded writes the default policy, because they lower effective
  parity until repair completes
- design future handoff writes as deterministic placement under a temporary
  availability overlay, not arbitrary per-object shard-location exceptions

Default policy until this phase is completed:

- strict writes only
- degraded reads are allowed when the PG state permits reads and at least `k`
  valid shards are reachable
- do not acknowledge a write that leaves the committed generation below the full
  intended shard count
- do not implement handoff writes until metadata can record the placement view
  used by a payload generation and repair/reclaim can operate on that view

Exit criteria:

1. the chosen policy is documented with explicit success and failure conditions
2. node availability states distinguish temporary down/planned unavailable from
   durable `out`
3. metadata requirements for future placement-view handoff writes are documented
   without requiring implementation in this phase
4. repair/backfill behavior is defined for every committed write state allowed
   by the current strict policy
5. tests cover transient target-node outage during direct put, streaming put, and
   multipart complete
6. tests cover a second failure before repair for any policy that acknowledges
   writes below full redundancy

## Phase 9: Replace Process-Local Coordination

Remove process-local mechanisms from logical correctness.

Work items:

1. replace metadata command index allocation, apply serialization, and pending
   command convergence with durable PG-primary command stream state
2. replace stream segment VID allocation with durable or command-owned stream
   append allocation
3. replace multipart completion locks with PG-primary serialization
4. replace bucket write drain waits with durable reservation or primary-owned
   state
5. replace coordinator-local object payload generation leases with volatile
   storage-node-owned read handles and storage-node physical delete fences
6. make reclaim work claiming durable and idempotent
7. add a physical shard scavenger for unreferenced shard files that can be left
   by crashes or persistent delete failures after metadata has already stopped
   referencing a payload, including omitted multipart part shards cleaned up
   after CompleteMultipartUpload
8. make lifecycle sweeping ownership and retry state cluster-visible before
   multiple processes can sweep the same cluster
9. make bucket cache freshness depend on PG or cluster notifications rather than
   local invalidation alone
10. audit tests for hidden single-process assumptions

Proposed subphases:

1. Phase 9.1 process-local coordination audit
   - inventory every correctness-relevant `Mutex`, `RwLock`, condition
     variable, background worker flag, local cache invalidation path,
     in-memory lease/fence, and test helper that assumes one process
   - classify each item as performance/cache only, request serialization,
     read/write lifetime protection, cleanup/reclaim ownership,
     per-connection safety, integrity fast path, or test-only
   - record the replacement owner for every correctness item before changing
     behavior
   - exit when the plan has an explicit checklist of process-local correctness
     mechanisms and their target durable or PG-primary replacement
2. Phase 9.2 metadata command stream runtime state. Complete for the local
   PG-primary command-stream runtime state.
   - implement the target model in
     [metadata-command-stream.md](../guides/metadata-command-stream.md):
     a PG command stream is strictly single-writer and single-pending, with
     concurrency coming from different PGs rather than concurrent mutation
     inside one PG
   - replace process-local command log index allocation with durable PG-primary
     allocation
     - status: production command creation now derives the next log index from
       the routed PG-primary's durable command log, including already-open
       handles after another handle has appended a command; unresolved durable
       bucket-PG and object-PG pending slots are loaded and converged rather
       than skipped; the old `LocalClusterRuntimeState` index helper has been
       removed and tests that need hand-built commands now allocate from the
       durable PG-primary state
   - replace process-local pending metadata command maps with durable pending
     command state or retry derivation from the durable command log
     - status: bucket and object command publishers now install durable pending
       slots on the routed PG primary and remove them only after an
       applied/abandoned terminal log record is durable; duplicate-index
       zero-apply reissue computes a safe replacement first, rejects divergent
       non-primary-only histories, and then atomically replaces the exact stale
       durable slot on the primary
     - status: duplicate-index reissue reloads the durable primary pending slot
       before treating a higher non-primary log index as durable divergence, so
       a second drainer can converge an already-reissued command during the
       normal primary-last fanout window rather than surfacing a false conflict
     - status: new command publishers treat a durable PG-slot install race as
       normal contention: the losing publisher drains/converges the winner and
       either retries its pending-slot install or, for request paths whose
       preconditions are not fully encoded in command apply, restarts from a
       fresh metadata snapshot before rebuilding the command. This restart
       shape is used for object metadata mutation, conditional object delete,
       delete-marker insertion, direct PUT finalization, stream PUT
       segment append, stream PUT session creation, stream PUT abort,
       stream PUT finalization, multipart upload creation, lifecycle
       current-object expiry, lifecycle noncurrent expiry, and expired
       delete-marker removal. The bucket-PG publishers and object
       generation/version reservation helpers also drain command-id contention
       after their pending-slot checks, so a same-PG winner does not surface as
       an internal metadata-log conflict. Tests cover both an
       unrelated-object retry
       (`object_metadata_pending_install_race_drains_winner_and_retries`) and a
       same-key race where the winner changes the losing request's object-tag
       precondition
       (`object_metadata_pending_install_race_reruns_precondition_action`).
       `direct_put_pending_install_race_reruns_precondition_action`,
       `direct_put_command_id_race_drains_winner_and_reruns_precondition_action`,
       `stream_put_append_command_id_race_drains_winner_before_ack_publish`,
       `stream_abort_pending_install_race_rebuilds_staged_segments`,
       `stream_put_finalize_pending_install_race_reruns_precondition_action`,
       `stream_put_create_pending_install_race_reruns_authorization_action`,
       `multipart_create_pending_install_race_reruns_authorization_action`,
       and `lifecycle_noncurrent_pending_install_race_reruns_selector` pin the
       equivalent payload creation/finalization, MPU creation, and lifecycle
       selector cases.
     - status: bucket-PG and object-PG pending slots now have typed command
       decoding and can be rehydrated from the durable primary slot after
       reopen or when a second handle sees the same PG; production request
       paths now read, install, reissue, and remove the durable slot directly
       rather than consulting an in-memory pending-command map
     - status: `LocalClusterRuntimeState::pending_metadata_commands` and its
       test-only helpers have been removed; local-cluster tests now inject and
       inspect pending commands through the durable PG-primary slot so the test
       surface exercises the production recovery primitive
     - draining a PG slot must preserve command-owned resources for the original
       request; non-matching generation reservations are not released by the
       drainer because they may belong to active concurrent work
   - replace the process-local metadata command apply lock with a durable
     compare-and-append serialization point
     - status: the old global metadata command apply lock has been removed.
       Local command fanout now uses a PG-scoped runtime apply mutex as a
       temporary local-cluster serialization point, so one PG does not expose
       the primary-last non-primary/primary window to a concurrent drainer
       while still allowing different PGs to progress independently. This is
       not the remote-process durability mechanism; the Phase 9 target remains
       a durable PG-primary compare-and-append point.
     - status: replica apply now rechecks command acceptance inside the same
       SQLite transaction that applies the metadata mutation and records the
       log entry. This closes the in-flight double-apply window where a second
       drainer could validate before the first drainer committed, then replay
       a non-idempotent mutation such as `ReserveObjectVersion`.
   - replace stream segment VID allocation with durable per-session allocation
     or command-owned append IDs
     - status: stream sessions now store a durable `next_segment_vid` allocator
       on the PG primary; `LocalClusterRuntimeState` no longer owns stream VID
       allocation, reopen preserves the next VID, and append command apply
       advances replica allocator floors from committed segment records
   - exit when two processes cannot allocate conflicting command indexes,
     hide pending command convergence from each other, or allocate duplicate
     stream segment VIDs; required tests must cover two different buckets on
     the same PG contending for one PG-scoped slot, zero-replica-apply
     abandonment, nonzero-apply convergence, and replica rollback of
     mutation-without-log or log-without-mutation failures
   - closeout proof matrix:
     - durable PG-primary log-index allocation:
       `metadata_command_log_index_allocator_reads_durable_log_from_already_open_handle`
       and
       `metadata_command_log_index_allocator_drains_unresolved_durable_pending_slot`
     - one unresolved durable PG slot, not one slot per bucket:
       `pending_metadata_command_slot_is_pg_scoped_and_persistent` and
       `create_bucket_drains_different_bucket_pending_command_on_same_pg`
     - pending-slot convergence after restart or from another handle:
       `create_bucket_rehydrates_durable_pending_slot_after_reopen`,
       `object_generation_reservation_rehydrates_durable_pending_slot_after_reopen`,
       and the durable-slot lookup/reissue tests around bucket and object
       command publishers
     - no process-local command apply lock:
       `LocalClusterRuntimeState::metadata_command_apply_lock` has been
       removed; the local cluster uses PG-scoped apply serialization to avoid
       transient primary-last fanout conflicts inside one process, while
       command ordering is enforced by PG-primary pending-slot ownership plus
       per-replica validate/apply/record transactions
     - stream segment VID allocation:
       `stream_segment_vid_allocation_survives_reopen_without_runtime_state`
       and
       `stream_segment_vid_allocation_is_visible_to_already_open_handle`
     - zero-apply abandon and nonzero-apply convergence:
       `zero_apply_command_failure_records_tombstone_for_later_hash_chain_convergence`,
       `zero_apply_generation_reservation_records_tombstone_and_later_reserves`,
       `required_reservation_release_keeps_durable_slot_until_partial_apply_retry`,
       and the partial-apply retry tests for create bucket, bucket properties,
       object generation reservation, direct PUT, stream append, MPU create,
       abort, and completion
     - mutation/log atomicity:
       `metadata_command_apply_and_record_rolls_back_metadata_on_record_conflict`
       and
       `metadata_command_apply_and_record_rolls_back_log_on_metadata_failure`;
       `apply_metadata_command_and_record_rechecks_already_applied_before_mutation`
       pins the duplicate in-flight apply case for non-idempotent allocator
       commands
   - status: Phase 9.2 is complete for the local PG-primary command-stream
     runtime model. Later phases may add remote RPC, repair, and compaction,
     but a Phase 9.2H hardening gate comes before the remaining multipart
     serialization work in Phase 9.3.
3. Phase 9.2H command stream hardening
   - goal: turn the recent command construction, pending-slot contention,
     reissue, and replica convergence review findings into reusable
     guardrails before adding more multipart command complexity
   - status: complete for the Phase 9.2H hardening scope. The
     publisher/path inventory, non-multipart snapshot-sensitive retry shape,
     stream upload command/runtime split, reissue model, crash-step coverage,
     stateful trace coverage, and mechanical boundary checks are in place.
     Multipart and UploadPart publishers were explicitly inventoried for Phase
     9.3 where they still used multipart-specific `set_pending...` or
     `try_set...` shapes. The follow-up finish/convergence pass is also in
     place: command-log conflicts surfaced while finishing a partially applied
     command are classified separately from pending-slot install contention,
     broad conflict matching is mechanically inventoried, and tests cover
     partial exact-command retry, divergent same-index fail-closed behavior,
     normal pending-command retry, and terminal pending-slot cleanup before a
     later command.
   - freeze Phase 9.2 semantics in
     [metadata-command-stream.md](../guides/metadata-command-stream.md) as
     invariants rather than implementation notes:
     - a PG command stream has one primary-owned durable pending slot
     - stale snapshot commands must rebuild after pending-slot contention
     - reissue requires matching payload, next safe index, matching
       `previous_log_hash`, and matching resulting log hash
     - terminal cleanup validates command-owned cleanup state, not runtime
       allocator state
   - classify every metadata command publisher/path, not only every command
     kind, as one of:
     - `SnapshotSensitive`: this publisher builds the command from a
       request-specific metadata snapshot, authorization result, or
       precondition decision outside command apply and must restart from a
       fresh snapshot after slot contention
     - `ApplyValidated`: command apply fully validates every precondition this
       publisher depends on against current materialized state, so the same
       command may be reused after draining a competing slot
     - `AllocatorCleanup`: the command is cleanup or allocator state with a
       known external owner. It must not steal another request's resources when
       a pending slot is drained.
     - `MatchingOutcomeRetry`: the publisher is snapshot-sensitive for
       unrelated contention, but an equivalent pending command is also the
       caller-visible result. These paths must finish the matching command and
       return the outcome derived from its command-owned row image instead of
       draining it through the generic snapshot-sensitive helper.
     - `TerminalSessionRetry`: the publisher is snapshot-sensitive and has a
       terminal command that deletes the stream/upload session it is
       finalizing or aborting. These paths must leave an equivalent pending
       contender visible to the top-of-loop matching branch instead of
       generically draining it and then retrying after the session row has
       disappeared.
     The command kind matrix is still useful as a summary, but it is not the
     authority. The same command kind can be safe in one publisher and
     snapshot-sensitive in another if the surrounding request path performs
     different precondition or authorization work before pending-slot install.
   - inventory every production call site that creates or installs a pending
     metadata command, with special attention to direct users of
     `try_set_pending_metadata_command_for_bucket`,
     `try_install_pending_metadata_command_for_bucket`, and
     `set_pending_metadata_command_for_bucket`; each call site must be covered
     by the publisher/path classification above
     - status: initial publisher/path classification is documented in
       [metadata-command-stream.md](../guides/metadata-command-stream.md), and
       `scripts/check-storage-cluster-boundaries` now fails if the production
       pending-command install call-site inventory changes without updating the
       documented classification and script allowlist
   - add or consolidate a generic snapshot-sensitive command-publish wrapper:
     - load fresh snapshot
     - run request preconditions/action
     - build command
     - try to install the durable pending slot
     - on slot contention, drain/converge the winner and restart from the
       snapshot step
     - apply/record the installed command
     This should make the safe shape the convenient API for new Phase 9.3
     work, rather than another one-off retry loop.
     - status: introduced `install_snapshot_sensitive_metadata_command_or_drain`
       and converted representative object metadata and specific-version
       delete publishers, then expanded it to the straightforward current
       delete, delete-marker insertion, lifecycle expiry, stream abort,
       stream finalization, and MPU create publishers so contention drains the
       winning slot and returns to their fresh-snapshot loop through a named
       result. Versioned stream-finalize contention now pins the intentional
       `ReserveObjectVersion` allocator-gap behavior when a fresh retry fails
       after a durable version reservation. Stream PUT creation now also covers
       the post-generation-reservation command-id race: if another same-PG
       command wins the durable slot before the create command id is allocated,
       the path drains the winner, releases the reservation, and restarts from
       a fresh request snapshot. Stream PUT finalization and the non-multipart
       object metadata/delete publishers now treat the same pre-publish
       command-id conflict as pending-slot contention, including noncurrent
       lifecycle expiry: drain the winner and restart from a fresh object
       snapshot instead of surfacing
       `MetadataCommandLogConflict` to the request. The lower-level stream PUT
       session-record
       publisher also treats pending-slot install contention as a retry signal
       instead of draining and reusing a prebuilt command. Multipart/UploadPart
       publishers remain classified and allowlisted, but their conversion is
       split by shape: `create_multipart_upload` now handles pre-publish
       command-id contention by draining and rerunning authorization from a
       fresh snapshot, while the upload-wide terminal paths remain Phase 9.3
       multipart serialization work because they need upload-wide and multi-PG
       ordering rules rather than the simple object-path wrapper alone
   - split command-owned records from runtime/local fields where equality has
     been risky:
     - start with stream upload records, separating command-owned session
       identity/state/encryption fields from allocator-floor/runtime state such
       as `next_segment_vid`
     - define terminal cleanup records that contain exactly the fields terminal
       cleanup validates
     - avoid raw `==` over structs that mix command-owned state with runtime
       allocator/progress fields
   - add a local command-stream invariant checker for tests and property
     traces:
     - at most one durable pending slot per PG primary
     - unresolved slots exist only on the current primary
     - no sparse accepted prefix is treated as clean
     - successful public operations leave acting replicas agreeing on applied
       index/hash
     - same-index accepted commands have identical bytes and hash-chain fields
     - materialized command-owned rows match the canonical state digest
     - status: initial `assert_clean_metadata_command_stream` helper validates
       post-operation unresolved pending slots and accepted-prefix/tail
       agreement before running replay validation, so terminal pending-slot
       cleanup cannot be hidden by the validation path; randomized
       local-cluster traces now run the clean command-stream invariant whenever
       the generated route/epoch state is representable on disk
   - extract the reissue safety decision into a pure model over compact
     summaries, then proptest gaps, divergent prefixes, same-payload
     replacements, missing log rows, abandoned rows, stale primary state, and
     primary-last fanout windows
     - status: reissue now uses a pure decision helper over primary state,
       acting-set max index, current pending command index, and per-replica
       prefix/hash-chain match summaries; property coverage exercises
       fail-closed behavior, with targeted cases for primary-last fanout,
       divergent prefixes, and below-replacement replicas whose accepted
       prefix differs from the primary
   - add reusable crash-step tests around the durable pending-command lifecycle:
     - slot installed, no replica applied
       - status: create-bucket and object-generation reservation reopen tests
         cover durable primary slots that have not yet applied to any replica;
         retry rehydrates the slot and converges it instead of allocating a new
         command
     - non-primary applied, primary not applied
       - status: local-cluster reopen now has an explicit fail-closed
         regression for a non-primary replica with a terminal applied command
         while the primary still has the unresolved pending slot; until repair
         exists, this shape is rejected as replica-state divergence
     - primary applied, pending slot still present
       - status: local-cluster reopen now covers the crash shape where the
         command is terminal on the acting set and the primary durable pending
         slot survived; replay validation must clean the slot and preserve the
         applied metadata
     - abandoned row written, replica state not advanced
       - status: replay validation now advances contiguous abandoned log tails
         even when a non-primary replica has no pending slot, but preserves the
         previous materialized-state digest so abandoned rows cannot bless
         unexpected metadata mutations; it fails closed for unadvanced applied
         log tails; PgStore and local-cluster reopen tests cover this crash
         shape
     - terminal row present, slot not removed
       - status: primary terminal pending-slot cleanup is covered by the same
         local-cluster reopen test and by PgStore validation tests for applied
         and abandoned terminal rows
     - reopen after each state
       - status: the crash-step cases above now all have PgStore-level,
         local-cluster reopen, or retry-after-reopen coverage; automatic
         repair is still deliberately narrower than fail-closed detection
   - add finish/convergence hardening for every
     `finish_pending_metadata_command_to_acting_set` caller:
     - classify each caller's finish errors as one of:
       - pre-publish failure: may return the request error or abandon only if
         zero acting-set replicas accepted/mutated/logged the command
       - partial exact-command apply: may retry/converge only with proof that
         already-applied replicas recorded the exact command bytes/checksum and
         matching `previous_log_hash`/`log_hash`; if the first failing replica
         already has the exact terminal row, it can establish the proof only by
         matching that row against the primary prefix hash-chain
       - divergent command-log state: must fail closed and must not be
         swallowed as ordinary contention
       - post-publish cleanup failure: must not turn an externally visible
         successful mutation into a 500 without a convergence path
     - inventory every production finish-helper call site, including bucket-PG
       commands (`CreateBucket`, bucket ACL/versioning/property/subresources,
       `MarkBucketDeleting`, completed-MPU order/deletion) and object/MPU
       paths that drain bucket-PG commands as part of multi-PG flows
     - add a reusable fault matrix for the finish helper:
       - zero replicas applied
       - one non-primary applied and primary rejects the same command
       - one non-primary applied and another replica has a divergent
         same-index row
       - all replicas applied while the primary pending slot remains
       - terminal row exists but pending-slot cleanup is interrupted
     - require request-level behavior tests where external state is visible:
       a request must not return an internal command conflict after making the
       mutation observable unless a retry/convergence path is guaranteed
     - require matching-outcome install-race tests for any publisher classified
       as `MatchingOutcomeRetry`: inject an equivalent pending command after
       the losing request has built its command but before pending-slot install,
       then assert the request returns the command-derived outcome and leaves a
       clean command stream. This is distinct from unrelated contention tests,
       because generic drain/retry can erase the state needed to reconstruct
       the caller-visible result.
     - require terminal-session install-race tests for any publisher classified
       as `TerminalSessionRetry`: inject an equivalent terminal command after
       snapshot/command construction but before pending-slot install, then
       assert the request preserves the equivalent pending command for its
       matching branch and leaves a clean command stream.
     - add a mechanical boundary check for broad
       `MetadataCommandLogConflict` matching in request paths. Matching the
       variant directly is allowed only in named helpers that prove command
       identity and hash-chain state, or in allocation paths that immediately
       restart before publishing state.
     - split object-PG pending-slot finishing into explicit exact-outcome and
       generic-drain APIs. The exact-outcome API requires a checked-request
       proof token and is only for branches returning the pending command as
       this caller's result. Generic drain may finish any PG-scoped object
       command only as contention progress before the caller restarts from a
       fresh snapshot.
     - status: finish/convergence paths are documented in
       [metadata-command-stream.md](../guides/metadata-command-stream.md), the
       boundary script inventories every production command-log conflict
       match, bans the legacy bucket-named object finisher helpers, and
       local-cluster tests cover partial exact `MarkBucketDeleting` retry,
       divergent same-index bucket-delete and bucket-update fail-closed
       behavior, ordinary partial bucket-command retry, and terminal
       pending-slot cleanup before a later bucket command.
   - expand the local-cluster stateful model to include two handles,
     pending-slot contention, reissue, zero-apply abandon, partial
     primary-last apply, restart/open validation, and stale-handle attempts
     - status: randomized local-cluster traces now assert that generated
       operation traces leave no unresolved pending slots. Traces that remain
       in the initial command epoch also run the clean command-stream
       invariant checker, so unapplied log tails and replay validation
       failures are caught where the trace has not used synthetic epoch
       mutation. The trace model now also injects an unresolved durable
       create-bucket slot and uses a second cluster handle to drain that PG-slot
       contender before creating another bucket on the same PG. It can also
       reopen the local cluster mid-trace and run the command-stream invariant
       immediately when the generated route/epoch state is representable on
       disk, and it injects a duplicate-index pending create-bucket command so
       normal request retry must reissue the command to the next safe log
       index
   - extend mechanical boundary checks for unsafe command-stream patterns:
     - direct pending-slot installation in snapshot-sensitive paths that does
       not restart from a fresh snapshot after contention
     - raw equality on records known to contain runtime allocator fields
       - status: boundary checks reject terminal stream cleanup payloads that
         reintroduce `Vec<StreamUploadRecord>` and production direct
         `stream_uploads` vector comparisons
     - production use of direct `PgStore` mutators for command-owned tables
     - command apply paths that bypass apply+record transaction handling
     - reissue paths that compare command bytes without hash-chain validation
   - minimum exit criteria before Phase 9.3:
     - Phase 9.2 invariants are documented
     - all current metadata command publisher paths are classified, with a
       secondary command-kind summary; historical multipart-specific
       pending-slot deferrals were closed in Phase 9.3
     - all production pending-command install call sites are inventoried and
       tied to that publisher/path classification
     - non-multipart snapshot-sensitive publishers use the common
       restart-on-contention shape or are explicitly documented as already
       covered; multipart/UploadPart publishers are covered by Phase 9.3
     - stream upload command-owned records are separated from runtime allocator
       fields
       - status: terminal MPU cleanup commands now carry
         `TerminalStreamCleanupRecord`, which excludes `next_segment_vid`;
         `CreateStreamUpload` carries a command-owned stream session projection
         plus an explicit initial allocator floor, so retry matching validates
         allocator state deliberately instead of comparing a broad runtime row
     - invariant checker and boundary checks run in the normal verification
       path
     - targeted reissue and crash-step model coverage exists for the recent
       bug classes
     - targeted finish/convergence coverage exists for partial exact-command
       conflicts and divergent same-index command-log conflicts, and broad
       `MetadataCommandLogConflict` matching is mechanically guarded
4. Phase 9.3 multipart serialization
   - goal: replace multipart completion, abort, UploadPart, and streamed
     UploadPart serialization that still depends on local locks or
     multipart-specific pending-slot loops with PG-primary command
     serialization and fresh-snapshot retry rules
   - scope:
     - convert the Phase 9.2H multipart publisher deferrals from
       [metadata-command-stream.md](../guides/metadata-command-stream.md):
       `begin_upload_part_stream_session`,
       `create_upload_part_stream_session`, `finalize_upload_part_stream`,
       `complete_multipart_upload_commit_serialized`,
       `abort_multipart_upload_locked`, and
       `abort_authorized_multipart_upload_locked`
     - include UploadPartCopy explicitly. It uses the UploadPart stream-session
       path, appends copied source segments, finalizes the destination part, and
       aborts the stream session on copy/source failure, so it must be covered
       by the same serialization and cleanup rules rather than treated as plain
       UploadPart
     - keep the Phase 9.2 rule that command streams are PG-local; this phase
       does not introduce a cross-PG transaction protocol
     - make every multi-PG multipart flow derive later commands only from
       terminal earlier commands. In particular, completed-MPU order allocation
       on the bucket PG must be terminal before the object-PG completion command
       uses that order
     - local locks may remain as short in-process contention reducers while this
       is still a single process, but correctness must not depend on them
   - non-goals:
     - durable bucket write-drain state remains Phase 9.4
     - storage-node-owned read handles and physical delete fences completed in
       Phase 9.5
     - durable reclaim worker claiming and physical shard scavenging remain
       Phases 9.6 and 9.7
     - broad test-harness de-single-process cleanup remains Phase 9.10, except
       for tests directly needed to prove the multipart command-stream
       invariants here

   Proposed subphases:

   1. Phase 9.3.1: multipart command-stream audit and invariants
      - status: complete. The multipart command-stream invariants are now
        documented in
        [metadata-command-stream.md](../guides/metadata-command-stream.md),
        and the boundary script inventory remains the source of truth for
        multipart pending-slot publisher shapes while the following subphases
        convert them.
      - inventory every multipart publisher, finisher, cleanup path, and helper
        that touches:
        - `multipart_uploads`, `multipart_parts`,
          `multipart_part_segments`, `stream_uploads`,
          `stream_upload_segments`, `completed_multipart_uploads`, and
          `buckets.completed_multipart_upload_sequence`
        - uploaded-part payload cleanup refs and active UploadPart stream
          cleanup refs
        - local locks such as `lock_multipart_completion_bucket` and
          `lock_bucket`
      - classify each publisher path as `SnapshotSensitive`,
        `ApplyValidated`, or `AllocatorCleanup`, matching the Phase 9.2H
        publisher table
      - document the multipart-specific invariants:
        - one upload ID has one terminal lifecycle outcome: in-progress,
          completed, or aborted
        - a terminal command owns cleanup of all active UploadPart stream
          sessions and staged segments for that upload
        - part replacement is command-owned and idempotent; displaced staged
          segment cleanup is carried by the command, not inferred from current
          rows after publication
        - complete and abort must rebuild their snapshots after PG-slot
          contention
        - duplicate UploadPart/finalize retries are accepted only when the
          command-owned row image and cleanup refs match exactly
      - update the boundary script so multipart pending-slot publishers fail
        loudly unless documented, and so temporary multipart deferrals are
        visible only while the relevant subphase is in progress
      - exit when the audit table, guide text, and boundary allowlist agree

   2. Phase 9.3.2: UploadPart stream session creation
      - status: complete. `begin_upload_part_stream_session` and
        `create_upload_part_stream_session` now use the snapshot-sensitive
        install-or-drain wrapper and restart from fresh MPU state after
        command-id or slot contention. Coverage includes a competing UploadPart
        stream session that forces the caller action to rerun, abort winning the
        slot before session creation, raced aborted/completing upload state,
        and crash-shape pending-slot rehydrate/convergence after reopen.
      - convert `begin_upload_part_stream_session` and
        `create_upload_part_stream_session` to the snapshot-sensitive
        install-or-drain shape:
        - load a fresh in-progress MPU row and run caller authorization/action
        - build the `CreateStreamUpload` command from that row
        - try to install the object-PG pending slot
        - on contention, drain the winning slot and restart from a fresh MPU
          snapshot and authorization/action result
      - ensure command apply revalidates the `UploadPart` target against the
        current in-progress MPU row on every acting-set replica
      - preserve exact retry matching for session id, upload id, part number,
        encryption, created-at row fields, and the initial allocator floor
      - add deterministic two-handle tests for:
        - create session vs abort of the same upload
        - create session vs complete of the same upload
        - create session losing a PG-slot race to an unrelated same-PG command
          and rerunning authorization/action
        - retry after partial create apply and reopen
      - exit when active UploadPart sessions cannot be created for an upload
        that has been aborted or completed by a command that wins the slot

   3. Phase 9.3.3: streamed UploadPart finalization
      - status: complete. `finalize_upload_part_stream` now uses the
        terminal-session retry shape: unrelated contention is drained through
        the fresh-snapshot loop, while an equivalent terminal
        `CommitStreamPart` contender is left visible for the matching branch
        instead of being drained generically. It reloads stream session, MPU,
        existing part, staged segment, and displaced-part state after
        contention before rebuilding `CommitStreamPart`. Coverage now includes
        duplicate finalize retry through a matching pending command, a
        same-command install race, same-part replacement with displaced payload
        cleanup, and finalize losing the object-PG slot to both abort and
        complete terminal commands. The complete-wins-slot coverage stages
        multiple copied-source-style segments, matching the storage
        representation used by UploadPartCopy, and proves terminal cleanup
        removes every copied staged payload. It also covers the crash shapes
        where a matching terminal `CommitStreamPart` command applied but the
        durable pending slot survived, and where a partial `CommitStreamPart`
        apply reopens with the primary pending slot, rehydrates the command,
        and converges.
      - convert `finalize_upload_part_stream` to the snapshot-sensitive
        install-or-drain shape:
        - reload the stream session, MPU row, existing part row, staged segment
          list, and displaced segment refs after every contention event
        - rerun caller action on the fresh `StreamUploadPartSnapshot`
        - build `CommitStreamPart` only from the fresh command-owned snapshot
      - tighten retry matching so pending `CommitStreamPart` acceptance compares
        command-owned rows and cleanup refs exactly, with only intentional
        timestamp/idempotence fields normalized
      - ensure terminal session cleanup clears command-owned rows and local
        runtime allocator state through the same post-apply hook whether the
        command is applied directly or by a later pending-slot drain
      - add deterministic tests for:
        - duplicate finalize for the same session/part
        - two sessions finalizing the same part number, where one replaces the
          other and displaced payload cleanup is carried by the terminal command
        - UploadPartCopy finalization using copied source segments, including
          cleanup of staged copied segments when the destination MPU becomes
          terminal before finalize
        - finalize losing the slot to abort and returning the AWS-compatible
          missing/invalid upload result
        - finalize losing the slot to complete and not staging a part after the
          upload is terminal
        - partial `CommitStreamPart` apply, reopen, rehydrate, and converge
      - exit when streamed UploadPart finalization has no correctness
        dependency on same-process upload locks

   4. Phase 9.3.4: multipart abort serialization
      - status: complete. `abort_multipart_upload_locked` and
        `abort_authorized_multipart_upload_locked` now use the
        terminal-session retry shape: unrelated contention is drained through
        the fresh-snapshot loop, while an equivalent abort contender is left
        visible for the matching branch so the request returns the successful
        abort outcome. Coverage includes active UploadPart stream sessions,
        UploadPartCopy-style staged copied segments, streamed-part finalize
        winning the slot before abort, completion winning the slot before
        abort, stale authorized upload rows, partial abort apply followed by
        local-cluster reopen and convergence, and terminal pending-slot cleanup
        before later multipart work.
      - convert `abort_multipart_upload_locked` and
        `abort_authorized_multipart_upload_locked` to the same
        snapshot-sensitive shape, or narrow existing loops until they are
        equivalent:
        - drain unrelated pending slots
        - rebuild upload, part, active stream session, staged segment, and
          payload cleanup snapshots after contention
        - install `AbortMultipartUpload`
        - apply through the acting set
      - make authorization-bound abort retry compare the current upload row to
        the authorized row after every contention event; if the row changed,
        fail with the normal S3-visible outcome instead of applying stale auth
      - keep best-effort payload cleanup cluster-owned, but make command-owned
        cleanup refs sufficient for retry after restart
      - add tests for:
        - abort vs UploadPart session create
        - abort vs stream append/finalize
        - abort vs UploadPartCopy after stream session creation and after copied
          segment append
        - abort vs complete
        - abort after one or more replicas accepted the command, then reopen
          and converge
        - terminal pending-slot cleanup before later multipart work
      - exit when abort is a single terminal upload-lifecycle command and active
        UploadPart streams cannot survive it as valid sessions

   5. Phase 9.3.5: multipart completion and bucket-PG order flow
      - status: complete. The completion path now handles object-PG
        pre-publish command-id contention after the bucket-PG order reservation
        by draining the winning command and restarting from a fresh object-PG
        snapshot. Coverage pins that a winner published in that window is
        included in the retried completion's stale-payload reclaim snapshot.
        Completion-vs-abort coverage now exists in both directions: abort
        drains a pending completion, and completion drains a partial pending
        abort before returning the normal missing-upload result and applying
        abort cleanup. Partial bucket-PG order apply is also covered: retrying
        completion finishes the pending order command before publishing the
        object-PG completion command. Same-upload completion retry now drains
        the matching pending completion and returns that exact terminal outcome
        without allocating another completed-MPU order. Different uploads for
        the same destination key are covered by draining the first pending
        completion, then resnapshotting it as stale payload for the second
        completion. Partial object-PG completion apply is covered across reopen:
        open-time recovery converges the pending `CommitMultipartObject` and
        clears the durable slot. UploadPartCopy source-read failure after the
        destination UploadPart stream session exists is also pinned; the failed
        copy aborts the destination session and leaves no committed part.
      - split `complete_multipart_upload_commit_serialized` into explicit
        phases:
        - validate upload and requested parts from the object PG
        - reserve completed-MPU order through a terminal bucket-PG
          `AdvanceCompletedMultipartUploadSequence` command
        - reload the object-PG completion snapshot after the bucket-PG command
          is terminal
        - build and install the object-PG `CommitMultipartObject`
        - prune completed-upload idempotence rows only after the object-PG
          command is terminal
      - on object-PG contention after order reservation, restart from a fresh
        object-PG snapshot while either reusing the terminal reserved order when
        it belongs to the same request or issuing a safe follow-up order command
        if the request must be rebuilt
      - verify `CommitMultipartObject` carries all command-owned rows needed for
        deterministic replica apply:
        - live object row, object parts, selected streamed part segment rows,
          omitted part cleanup refs, active UploadPart stream cleanup refs,
          write sequence, completion order, completed-upload idempotence row,
          and stale-payload reclaim refs
      - add tests for:
        - complete vs abort of the same upload
        - complete vs streamed UploadPart finalize
        - complete vs UploadPartCopy session create/finalize, including source
          read or copy failure after the destination stream session exists
        - complete vs complete of the same upload
        - two completions for different uploads to the same destination key
        - same bucket completions on different object PGs allocating unique
          completed-MPU order through the bucket PG
        - partial bucket-PG order apply, partial object-PG commit apply, reopen,
          and convergence
      - exit: met. MPU completion is deterministic across acting-set replicas,
        completion order is reserved through the bucket-PG command stream, and
        race/retry coverage no longer depends on
        `lock_multipart_completion_bucket` for correctness.

   6. Phase 9.3.6: race matrix and model coverage
      - status: complete. This phase added a reusable terminal MPU lifecycle
        invariant checker for storage local-cluster tests and a focused
        multipart trace model. The checker pins that terminal abort/complete
        paths leave no live upload row, no active UploadPart stream sessions or
        staged stream segments for that upload, no post-abort multipart part
        segment rows, and only completed streamed part segment rows that are
        backed by a live multipart object manifest. It also verifies the
        expected completed-upload idempotence record shape across acting nodes.
      - the trace model drives real `StorageCluster`
        create/upload-part/complete/abort/reopen APIs. It covers multiple
        concurrent uploads in one bucket, keys on two object PGs with different
        primaries, repeated streamed parts, and UploadPartCopy-shaped copied
        parts represented as multi-segment UploadPart stream state before
        finalization. It includes delete/recreate of empty bucket incarnations,
        with a deterministic trace proving the generated delete/recreate branch
        runs. It also includes stale-handle attempts for MPU create, UploadPart
        stream-session create, and abort; those operations must fail before
        mutating upload state. Terminal abort/complete outcomes check lifecycle
        invariants after generated operations and across reopen.
      - the multipart command-stream invariant checker extends the Phase 9.2
        clean-stream helper with upload lifecycle checks:
        - no active `stream_uploads` or `stream_upload_segments` remain for a
          terminal upload
        - an upload has at most one terminal command outcome
        - selected parts and streamed part segments are referenced by the
          completed object or cleaned by abort/complete cleanup refs
        - UploadPartCopy staged copied segments are either committed as selected
          destination part segments or covered by terminal stream/abort cleanup
          refs
        - omitted parts and displaced part payloads are either cleaned or have
          explicit retry/scavenger records
      - the focused stateful trace model covers one bucket with multiple keys
        and uploads:
        - create MPU, create UploadPart stream session, append stream segment,
          UploadPartCopy-shaped session create/source-copy append, finalize
          part, abort, complete, delete/recreate bucket, reopen
        - stale-handle attempts are generated inside the trace; PG-slot
          contention, zero-apply abandon, primary-last partial apply, terminal
          pending-slot leftovers, and two-handle races are pinned by
          deterministic tests so failures are reproducible
      - public multipart mutating request coverage at closeout:
        - `CreateMultipartUpload`: contention/retry is covered by
          `multipart_create_pending_install_race_reruns_authorization_action`
          and
          `multipart_create_command_id_race_drains_winner_and_reruns_authorization_action`;
          partial apply/retry and partial apply/reopen are covered by
          `multipart_create_partial_apply_retry_reuses_pending_command` and
          `multipart_create_partial_apply_reopens_and_converges`.
        - `UploadPart`: session-create contention is covered by
          `begin_upload_part_stream_pending_install_race_reruns_action`,
          `begin_upload_part_stream_drains_pending_completion_before_create`,
          and `upload_part_stream_create_pending_install_race_reloads_after_abort`;
          create/finalize partial apply and terminal-slot recovery are covered
          by `upload_part_stream_create_zero_apply_reopens_and_converges`,
          `upload_part_stream_finalize_partial_apply_reopens_and_converges`,
          and `upload_part_stream_finalize_finishes_terminal_pending_slot`.
        - `UploadPartCopy`: destination command-stream behavior shares the
          UploadPart stream-session/finalize commands and is exercised in the
          trace as multi-segment copied-part state. Public request cleanup and
          source-object races are covered by
          `upload_part_copy_staged_segments_are_cleaned_when_complete_wins_finalize_slot`,
          `upload_part_copy_source_read_failure_aborts_destination_stream_session`,
          `upload_part_copy_is_consistent_during_concurrent_overwrite`, and
          `upload_part_copy_survives_source_metadata_delete_mid_read`.
        - `CompleteMultipartUpload`: command-id contention, matching pending
          completion, pending abort, zero-apply, partial apply/reopen, and
          bucket-PG order reservation retry are covered by
          `multipart_completion_command_id_race_drains_winner_and_resnapshots_stale_payload`,
          `multipart_completion_drains_matching_pending_completion`,
          `multipart_completion_drains_pending_abort_before_completing`,
          `multipart_completion_zero_apply_failure_retains_pending_command_for_retry`,
          `multipart_completion_partial_apply_reopens_and_converges`, and
          `multipart_completion_retries_partial_bucket_order_command`.
        - `AbortMultipartUpload`: install contention, pending completion,
          cleanup races, zero-apply, partial apply/reopen, and stale handles are
          covered by `multipart_abort_retries_after_pending_install_conflict`,
          `multipart_abort_drains_pending_completion_before_aborting`,
          `multipart_abort_pending_install_conflict_cleans_upload_part_stream_session_and_segments`,
          `multipart_abort_pending_install_conflict_cleans_committed_stream_part`,
          `multipart_abort_zero_apply_leaves_upload_in_progress_before_retry`,
          `multipart_abort_partial_apply_retry_cleans_uploaded_part_payload`,
          `multipart_abort_partial_apply_reopens_and_converges`, and the trace
          stale-abort branch.
      - the final publisher cleanup pass was completed in Phase 9.3.7, which
        removed the temporary `metadata-command-stream.md` deferral wording and
        boundary-script exemptions.

   7. Phase 9.3.7: remove local-lock authority and clean up deferrals
      - status: complete. `begin_upload_part_stream_session` now uses
        `install_snapshot_sensitive_metadata_command_or_drain`, so it drains a
        winning PG slot and restarts from fresh state.
        `complete_multipart_upload_commit_serialized` is intentionally
        classified as `MatchingOutcomeRetry`: unrelated contention restarts
        from a fresh completion snapshot, while an equivalent completion
        contender is finished and returned from the matching-pending branch
        rather than drained generically.
      - remove Phase 9.3 deferral wording from
        [metadata-command-stream.md](../guides/metadata-command-stream.md)
      - update `scripts/check-storage-cluster-boundaries` so multipart
        publishers are no longer exempt from snapshot-sensitive install rules
      - make any remaining local multipart locks clearly performance-only, or
        remove them if they no longer reduce useful contention
        - status: `lock_multipart_completion_bucket` and its stripe storage
          have been removed. Completed-upload order and object publication
          correctness are covered by durable bucket-PG/object-PG command
          stream tests for completed-upload order allocation, command ID races,
          partial command retry, and completion publication.
      - remove or gate any remaining direct/test-only multipart mutators that
        can bypass the command stream
        - status: remaining direct multipart seeders/mutators are `#[cfg(test)]`
          or `#[cfg(any(test, feature = "test-hooks"))]`; production multipart
          mutation paths go through the metadata command stream
      - run:
        - `cargo fmt`
        - `./scripts/check-storage-cluster-boundaries`
        - targeted storage multipart command-stream tests
        - targeted server-core and s3-tests multipart suites
        - `cargo clippy --all-targets --all-features -- -D warnings`
        - full `cargo nextest run`
        - status: all targeted checks and clippy passed; full nextest passed
          for this closeout
      - exit when the Phase 9.2H publisher table has no Phase 9.3 deferrals,
        the boundary script enforces that state, and the race matrix passes
5. Phase 9.4 bucket write drain
   - goal: make the DeleteBucket write fence and in-flight write reservations
     cluster-visible. Bucket deletion must block new writes, wait for existing
     bucket write reservations, survive process restart, and resume or roll
     back without depending on a local condition variable, same-process waiter,
     or process-local counter.
   - original problem addressed by Phase 9.4:
     - `begin_bucket_delete` started with
       `SharedStorageNode::begin_bucket_write_drain`, which wrote
       `buckets.write_reservations_blocked` and waited for
       `active_write_reservations` through node-local locking/wakeup
     - write reservations were bucket-row counters, not command-stream state and
       not durable ownership records; they could not distinguish "active in another
       process" from "process crashed while holding the reservation"
     - a drain fence can be temporary: DeleteBucket may discover visible data
       and roll back the fence. That means a crash after blocking writes but
       before either MarkBucketDeleting or rollback needs a deterministic
       recovery rule
     - finalization is correctly after `MarkBucketDeleting`, visible-data
       checks, and reclaim checks, but the pre-delete write drain is still the
       last major bucket-delete coordination path that assumes one process
   - target model:
     - bucket write reservations become explicit bucket-PG primary records, not
       anonymous counters on the bucket row
     - a DeleteBucket drain is an explicit durable bucket-PG fence with an owner
       token and recovery policy
     - the write fence is not S3 metadata and does not become part of object
       visibility semantics, but it is correctness metadata and must be
       protected by durable validation, boundary checks, and recovery tests
     - writers encountering an active drain wait/retry from fresh bucket state
       while the drain is temporary; once `MarkBucketDeleting` is durable they
       fail as bucket-not-found/deleting through the existing request semantics
     - release of an acquired write reservation is not new work. It must remain
       possible after the acquiring handle becomes stale, using the exact
       bucket-PG/owner/reservation identity captured at acquire time
     - a write reservation is an apply-time fence, not just an admission token
       for loading a bucket snapshot. Every bucket-write metadata publisher that
       builds a command under a reservation must carry the bucket incarnation
       and reservation identity into the command or an equivalent validated
       apply context. Command apply must reject the write if the reservation was
       reaped, the bucket incarnation changed, or the bucket drain reached a
       terminal deleting state before the command is accepted
     - object-PG commands that depend on a bucket write reservation must not
       trust the publisher's stale bucket snapshot. The command payload or
       command envelope must carry a bucket-PG reservation reference:
       bucket name, bucket execution generation or bucket row digest,
       reservation id, reservation owner token, and bucket PG/cluster epoch.
       Normal apply, pending-command retry, and open-time convergence must
       re-read the bucket-PG reservation state and fail closed if the referenced
       reservation is missing, expired/reaped, owned by a different owner,
       bound to a different bucket incarnation, or covered by a terminal bucket
       drain. This is the cross-PG fence that prevents a later drainer from
       converging an old object-PG command after DeleteBucket has decided the
       reservation can no longer publish
     - reservation reaping must not strand partially accepted object-PG
       commands. Phase 9.4 must choose and implement one explicit rule:
       - either a reservation is not reapable while any pending object-PG
         command or accepted-but-not-converged object-PG log entry references
         it, across the full acting set and open-time recovery shapes
       - or the first object-PG acceptance durably records a reservation proof
         in the command/log state, and later exact-command convergence validates
         that proof rather than requiring the live reservation row to still
         exist
       The chosen rule must be fail-closed for divergent command bytes or hash
       chains, but must allow an already accepted exact command to converge
       instead of leaving partial metadata because DeleteBucket reaped the live
       reservation after one replica applied it

   1. Phase 9.4.1: audit and freeze write-drain semantics
      - inventory every production caller that acquires bucket write protection:
        direct PUT, CopyObject, stream PUT create/finalize, CreateMultipartUpload,
        CompleteMultipartUpload, UploadPart/UploadPartCopy paths that create or
        finalize destination stream state, object metadata writes that can
        affect bucket emptiness, user-visible object delete commands, lifecycle
        current/noncurrent expiry and expired delete-marker cleanup, any other
        lifecycle/background object metadata writers, and all bucket-PG
        control-plane mutators such as PutBucketPolicy, PutBucket CORS,
        versioning, ACL, ownership controls, public access block, object lock,
        encryption, lifecycle, tagging-style subresources, and bucket delete
        begin/finalize helpers
      - classify each caller as one of:
        - needs a short bucket write reservation before taking a bucket snapshot
          and building/publishing object or MPU metadata
        - is already blocked by an existing MPU/session/object command and only
          needs to observe the current bucket state
        - is a bucket-PG control-plane command publisher and must either acquire
          the durable reservation, be explicitly blocked by the active drain, or
          prove it is safe because the command itself is the drain/delete
          transition
        - is delete/reclaim/background cleanup and must not acquire a new
          write reservation
      - pin expected AWS-facing outcomes for DeleteBucket races:
        - DeleteBucket racing an already-reserved write waits for the write to
          publish or fail, then returns BucketNotEmpty or proceeds
        - a new write that arrives while DeleteBucket is only probing/draining
          waits/retries rather than observing a transient internal state
        - a new write that arrives after `MarkBucketDeleting` is durable fails
          as the bucket no longer accepts writes
      - add the audit result to
        [bucket-write-drain.md](../guides/bucket-write-drain.md), and link it
        from this plan
      - status: complete. The Phase 9.4.1 audit is captured in
        [bucket-write-drain.md](../guides/bucket-write-drain.md), including the
        retired counter authority, publisher classification, cross-PG
        apply-time reservation fence, reservation reap vs object-PG convergence
        rule, DeleteBucket drain loop, and required test matrix.

   2. Phase 9.4.2: introduce durable bucket-PG write-drain records
      - status: complete. The first storage slice added durable
        `bucket_write_reservations` and `bucket_write_drains` tables plus
        PgStore primitives/tests for exact identity, owner token, cluster epoch,
        bucket execution generation, drain blocking, exact release/clear
        matching, and proof that these primary-owned coordination rows do not
        dirty the replica-wide metadata command digest. Phase 9.4.3 moved
        production writers onto these records, and Phase 9.4.6 removed the
        legacy bucket-row counters.
      - replace anonymous bucket-row counters with explicit coordination rows:
        - `bucket_write_reservations`: bucket, reservation id, owner/process
          token, cluster epoch, bucket execution generation or bucket row
          digest, operation kind, creation time, last heartbeat/lease deadline,
          and optional request target context for tracing
        - `bucket_write_drains`: bucket, drain id, owner/process token, cluster
          epoch, state (`Draining`, `MarkingDeleting`, `Abandoned`/expired),
          creation time, last heartbeat/lease deadline, and the bucket
          execution generation or bucket row image the drain was created against
      - decide and document whether these rows are:
        - bucket-PG-primary durable coordination state outside the metadata
          command log for Phase 9, or
        - command-owned bucket-PG metadata with dedicated
          `BeginBucketWriteDrain` / `EndBucketWriteDrain` commands
      - initial preference for this phase:
        - make the `MarkBucketDeleting` state change remain the durable
          command-stream transition
        - make reservations and temporary drain fences bucket-PG-primary
          durable coordination rows outside the replica-wide command-state
          digest, with their own integrity/recovery checks
        - remove the current bucket-row counter authority once the replacement
          is in place
      - add local PG validation for the coordination rows:
        - no active reservation may reference a missing bucket incarnation
        - at most one active drain may exist for a bucket
        - a drain's bucket-generation/preimage must match the current bucket row
          while the bucket is still Active
        - release and reap operations must match bucket, reservation id, owner
          token, and bucket incarnation. A stale release after bucket
          delete/recreate must not release a reservation for the new bucket
          incarnation
        - expired owner tokens make reservations/drains reclaimable, not
          silently successful

   3. Phase 9.4.3: move writer acquire/release to `StorageCluster`
      - status: complete. `StorageCluster::with_bucket_write_snapshot` now
        acquires and releases exact durable `bucket_write_reservations` rows on
        the bucket-PG primary. The old `SharedStorageNode` anonymous-counter
        snapshot wrapper is test-only. The stream PutObject and CreateMultipartUpload custom
        snapshot publishers use the cluster wrapper rather than the node-local
        counter path. Both request-level and low-level PutObject stream-create
        publishers now hold a durable reservation around command publication.
        Durable reservation
        IDs use 128 bits of random entropy instead of a per-handle counter, so
        independent `StorageCluster` handles and reopen do not collide on
        `(bucket, reservation_id)`. PutObject stream-create commands now carry
        an encoded durable reservation proof; normal apply, matching
        pending-command retry, and local-cluster open-time convergence validate
        that proof against the bucket-PG primary before applying, and the
        reservation remains live until the pending command converges. Direct
        buffered PutObject commit commands now also carry an encoded durable
        reservation proof; if the direct PUT metadata command becomes pending or
        partial, the command-owned reservation remains live and is released only
        after terminal convergence. CreateMultipartUpload commands now carry
        an encoded durable reservation proof; partial/reopen convergence
        validates and releases the proof before clearing the pending slot.
        Stream PutObject finalization commands now carry the reservation proof
        supplied by the coordinator's bucket-write handle; partial/reopen
        convergence validates and releases the proof before clearing the
        pending slot. UploadPart stream-session creation now also carries a
        durable reservation proof in the CreateStreamUpload command, including
        pending-slot retry and proof release coverage for both request-level
        UploadPart and the low-level path used by UploadPartCopy. The
        `CreateStreamUpload` command encoding now requires that proof, so
        proofless replay/open-time command bytes fail closed instead of
        applying outside the bucket-write fence. UploadPart stream finalization
        now carries the proof in `CommitStreamPart`; matching pending retry,
        partial/reopen convergence, and open-time recovery validate and release
        it before clearing the terminal pending slot. CompleteMultipartUpload
        now carries the proof in `CommitMultipartObject`; matching pending
        retry, partial/reopen convergence, and open-time recovery validate and
        release it before clearing the terminal pending slot.
        Object metadata updates now carry the proof in `PutObjectMetadata`;
        matching pending retry, partial/reopen convergence, and open-time
        recovery validate and release it before clearing the terminal pending
        slot. Object delete/lifecycle command families now carry the proof in
        `DeleteObjectVersion` and `InsertDeleteMarker`; matching pending retry,
        partial/reopen convergence, and open-time recovery validate and release
        it before clearing the terminal pending slot. The transitional
        proof-optional storage request surfaces for direct PUT commit and stream
        PUT finalization have been removed, so production writer boundaries now
        require an explicit `BucketWriteReservationProof` rather than
        synthesizing one after write admission. Internal retry helpers may still
        return an optional proof only to represent an active drain wait/retry,
        not to publish a proofless command. Phase 9.4.3 closeout audited the
        proof-bearing writer command surface and added a boundary guard for
        proof-optional publishing fields. The remaining old anonymous-counter
        and drain primitives are not writer-publish authority anymore, and Phase
        9.4.6 removes the old counter fields and low-level APIs.
      - introduce a cluster-level bucket write reservation guard that captures:
        bucket PG id, bucket name, reservation id, owner token, acquire epoch,
        and the node/store that accepted the reservation
      - `with_bucket_write_snapshot` should become a cluster-owned wrapper:
        - acquire durable reservation on the bucket PG primary
        - load the bucket snapshot under that reservation
        - run the caller action
        - release the exact reservation on every return path
      - contention and recovery rules:
        - if a temporary drain is active, wait/backoff and reload from the
          bucket PG; do not wait on a same-process condition variable
        - if `MarkBucketDeleting` is already durable, fail through the normal
          missing/deleting bucket path
        - every command built while holding the reservation must encode or
          otherwise carry enough bucket reservation context for apply to verify
          that the reservation is still live for the same bucket incarnation
        - for object-PG command families, apply-time validation must consult the
          bucket PG using that encoded reservation context. This check must run
          on:
          - the initial object-PG command apply
          - matching pending-command retry/finish paths before the command has
            already been durably accepted by that replica
          - open-time in-flight command convergence
        - exact `AlreadyApplied` retry paths may run only idempotent terminal
          cleanup and proof release without revalidating current reservation
          liveness, because the matching command log entry is the safety proof
          for already-materialized metadata on that replica
        - if any object-PG replica has already accepted the exact command, retry
          and open-time convergence must follow the Phase 9.4 reservation-reap
          rule above: either the reservation is still protected from reaping, or
          the command's durable reservation proof is sufficient for exact-command
          convergence
        - if the reservation is reaped before command apply, apply fails closed
          and the caller must restart from fresh bucket state rather than
          publishing a write that DeleteBucket has already decided cannot
          publish
        - if the handle becomes stale after acquire, release through the
          captured reservation identity rather than treating release as new work
        - if release fails after the caller action has returned, preserve the
          caller error ordering but leave a typed trace and retryable cleanup
          signal for the reservation
      - old anonymous-counter cleanup: complete. Phase 9.4.4 moved
        DeleteBucket off `SharedStorageNode::begin_bucket_write_drain`, and
        Phase 9.4.6 removed the remaining counter-based write-drain path:
        `SharedStorageNode::with_bucket_write_snapshot`,
        `PgMetadataStore::acquire_bucket_write_reservation`,
        `release_bucket_write_reservation`, `begin_bucket_write_drain`, and
        `end_bucket_write_drain`.

   <a id="phase-944-make-deletebucket-begin-durable-and-recoverable"></a>
   4. Phase 9.4.4: make DeleteBucket begin durable and recoverable
      - status: complete. DeleteBucket begin now uses the bucket-PG durable
        drain as its correctness authority, drains object-PG work while waiting
        for durable reservations to empty, handles expired no-waiter drains
        conservatively, and has regression coverage for every required test
        item below. The first implementation slice added a
        cluster-owned durable delete-drain helper using `bucket_write_drains`,
        installed that durable drain at the start of `begin_bucket_delete`, and
        rolls it back on pre-terminal failure while leaving it terminal after
        successful `MarkBucketDeleting`. Durable reservation waiting drains
        bucket-relevant object-PG pending work each pass so command-owned
        reservation proofs can converge and release. `begin_bucket_delete`
        also recognizes an already-Deleting bucket before trying to install a
        new Active-only durable drain, and the durable-drain conflict path
        reloads raw bucket state so a raced terminal `Deleting` transition is
        handled as idempotent success instead of going through active-only
        `HeadBucket` semantics. Targeted regressions cover non-empty rollback,
        terminal drain persistence/idempotent retry, durable-drain conflict
        after terminal delete begin, and an admitted writer that publishes
        visible data after the delete drain starts. The second slice added an
        atomic bucket-control pending-slot install path for bucket versioning,
        ACL, property, and subresource publishers; those commands are now
        explicitly blocked by an active durable delete drain instead of being
        allowed to publish behind the drain. The third slice added focused
        DeleteBucket/object-delete drain regressions: a pending versioned
        delete-marker insertion is drained before the emptiness check and
        correctly returns `BucketNotEmpty`, while a pending specific-version
        delete that removes the last visible version is drained before
        `MarkBucketDeleting` is published. The fourth slice pinned the
        lifecycle publishers specifically: pending lifecycle current expiry,
        noncurrent expiry, and expired delete-marker cleanup commands are
        drained before DeleteBucket trusts the post-drain emptiness check. The
        fifth slice pinned the bucket-incarnation boundary: an old terminal
        delete drain cannot clear a fresh drain installed after delete/recreate,
        because durable drain cleanup is matched by exact drain identity and
        bucket execution generation. The sixth slice added conservative
        no-waiter recovery for durable drains with an explicit expired lease:
        a restarted DeleteBucket can atomically roll back the expired durable
        drain from fresh bucket state before installing its own drain.
        The seventh slice pinned primary-last `MarkBucketDeleting` reopen
        convergence: a durable drain plus primary pending slot survives the
        crash boundary, open-time recovery converges the command, and the
        terminal drain remains durable.
      - rewrite `begin_bucket_delete` as a bucket-PG-primary state machine:
        - install or resume a durable drain fence
        - drain/finish any pending bucket-PG command for the bucket under that
          fence
        - drain object-PG pending commands that can publish visible data or MPU
          state for the bucket under that fence
        - wait/poll active durable write reservations until the set is empty,
          ignoring/reaping only reservations whose owner is provably dead or
          expired according to the Phase 9.4 owner-token rule
        - after the reservation set reaches empty, drain/finish all object-PG
          pending commands for the bucket again. A writer that already held a
          reservation may have published, partially applied, or left a pending
          object command after the pre-drain. This post-reservation drain is
          required before any emptiness decision is trusted
        - re-check visible data and in-progress multipart state after the
          post-reservation object-PG drain. If draining produces new relevant
          state or another reservation appears, repeat the wait/drain/check
          loop from fresh bucket state
        - if the bucket is not empty, roll back the drain fence durably and let
          waiting writers proceed
        - if the bucket is empty, publish `MarkBucketDeleting` through the
          bucket-PG command stream and make the drain terminal
      - explicitly define and audit the synchronous DeleteBucket return
        boundary. The request may return after the durable drain, fresh
        emptiness proof, and terminal `MarkBucketDeleting` are complete; it
        must not wait for async finalization, payload reclaim, storage-node read
        handles, or final row deletion. During the rewrite, classify every wait
        in `begin_bucket_delete` as response-correctness required,
        transitional-implementation required, or deferrable background work,
        and remove or move waits that are not needed to make the S3 response
        correct.
        A future optimization for classifying publish-ready durable write
        reservations without waiting is tracked separately in
        [DeleteBucket Reservation Classification Optimization](delete-bucket-reservation-classification-plan.md).
      - crash/restart rules:
        - drain fence present, no `MarkBucketDeleting`, owner alive: writers
          continue to wait/retry
        - drain fence present, no `MarkBucketDeleting`, owner dead/expired:
          another request or recovery helper rolls the fence back unless it can
          safely resume the DeleteBucket decision from fresh state
        - `MarkBucketDeleting` durable, drain fence still present: cleanup is
          idempotent and writers fail as deleting/missing
        - partial `MarkBucketDeleting` command apply follows the Phase 9.2H
          exact-command retry rules and must not be hidden as a transient
          drain conflict
      - concrete implementation slices:
        1. Add a cluster-owned durable delete-drain helper around the existing
           `bucket_write_drains` table. The helper generates a random
           `drain_id`, uses the cluster bucket-write owner token and operation
           epoch, and calls `PgMetadataStore::begin_durable_bucket_write_drain`
           on the bucket-PG primary. A competing active drain is normal
           DeleteBucket contention: wait/back off, reload bucket state, and
           retry rather than surfacing an internal conflict. Rollback/persist
           must be explicit; do not rely on `Drop` for correctness.
        2. Rework `begin_bucket_delete` around the durable drain authority:
           drain the bucket-PG pending command for this bucket, drain all
           object-PG pending commands for this bucket, install or resume the
           durable drain, wait until durable bucket write reservations are
           empty, drain all object-PG pending commands for the bucket again,
           then re-check visible objects, stream uploads, and MPU state from a
           fresh snapshot.
        3. Add a return-boundary audit while reworking `begin_bucket_delete`.
           Keep only waits needed before returning a correct DeleteBucket
           result: admitted writers that can still publish visible data,
           bucket-relevant pending metadata commands, fresh visible-data/MPU
           checks, and terminal `MarkBucketDeleting` convergence. Confirm that
           finalization, physical reclaim, storage-node read-handle waits, completed
           async cleanup, and final metadata row deletion remain behind the
           async finalizer.
        4. Wire bucket control-plane publishers into the durable drain
           boundary. Bucket-PG mutators such as versioning, ACL, lifecycle,
           policy/CORS/tagging-style subresources, object lock, encryption,
           ownership controls, and public access block must either acquire the
           durable write reservation, be explicitly rejected/waited behind an
           active delete drain, or prove they are the delete transition itself.
           A DeleteBucket drain must not be able to make an emptiness decision
           while a bucket-PG control-plane command that changes write/list/delete
           behavior is pending or can be newly published.
        5. If the fresh post-drain check finds blocking state, clear the durable
           drain by exact identity and return the normal S3 non-empty outcome.
           If it is empty, publish `MarkBucketDeleting` through the bucket-PG
           command stream; once that command is terminal, the drain is terminal
           too and new writers fail through normal missing/deleting bucket
           semantics.
        6. Remove the legacy `active_write_reservations` bridge. DeleteBucket
           must not depend on the node-local condition variable, and writers
           must acquire only durable bucket write reservation rows. Status:
           complete; the legacy bucket-row counter fields and helper APIs were
           removed.
        7. Do not add broad stale-reservation reaping in the first slice. A
           reservation is not safely reapable while any pending object-PG
           command or accepted-but-not-converged object-PG log entry can
           reference it. Start conservative: wait for live durable reservations
           to release, and only add exact-owner stale cleanup when it can prove
           the reservation is not command-owned. More aggressive owner
           heartbeat/lease reaping can be layered after the durable drain loop
           is correct.
      - helper/API work expected in this phase:
        - cluster helper to install/resume a durable bucket delete drain
        - cluster helper to clear a durable drain by exact identity
        - bucket-PG helper to list active durable write reservations for the
          bucket
        - object-PG helper to drain every visible-data/MPU publishing pending
          command for the bucket across all metadata PGs
        - one fresh-state predicate for DeleteBucket blocking state, covering
          visible object versions, active stream uploads, and in-progress MPU
          rows
      - required test order for this phase:
        1. active durable reservation blocks DeleteBucket; after release, the
           delete sees the writer's published data and returns BucketNotEmpty
        2. empty bucket installs a durable drain, publishes
           `MarkBucketDeleting`, and finalizes
        3. non-empty bucket installs a durable drain, detects data, clears the
           drain, and later writers can proceed
        4. a writer already holding a reservation publishes a pending or partial
           object command after the first object-PG drain; DeleteBucket's
           post-reservation drain sees that command before deciding emptiness
        5. DeleteBucket races versioned current DeleteObject that inserts a
           delete marker during the drain
        6. DeleteBucket races specific-version delete that removes the last
           visible version
        7. DeleteBucket races lifecycle current expiry, noncurrent expiry, and
           expired delete-marker cleanup
        8. delete/recreate does not let stale drain clear/release affect the new
           bucket incarnation
        9. restart/open with a durable drain but no terminal
           `MarkBucketDeleting` resumes or rolls back from fresh state without a
           same-process waiter
        10. bucket control-plane writes racing an active delete drain are either
            blocked/retried behind the drain or covered by an explicit durable
            write reservation. Cover at least versioning, lifecycle, and one
            subresource/policy-style command because those change the semantics
            of subsequent object writes, deletes, or listing decisions
        11. partial primary-last `MarkBucketDeleting` apply with a durable drain
            survives retry/reopen convergence, leaves the terminal drain state
            coherent, and fails closed for divergent same-index command-log
            state rather than treating it as a transient drain conflict

   5. Phase 9.4.5: finalization and worker wakeup without local waiters
      - status: complete. The cluster finalizer is process-independent: a
        reopened cluster handle can finalize a bucket that another process moved
        to terminal `Deleting`, and finalization remains pending while reclaim
        metadata or storage-node read handles block physical cleanup. A focused
        regression now proves worker progress can clear the reclaim root and a
        later finalizer retry removes the bucket row without relying on the
        original DeleteBucket process.
      - `try_finalize_bucket_delete` must not rely on the process that began
        the delete:
        - any process/worker can observe a Deleting bucket and attempt finalize
        - finalization still checks visible data and reclaim roots before
          calling the finalized-delete acting-set fanout. It does not inspect
          reader lifetime directly; active storage-node read handles block
          finalization by making reclaim roots remain uncleared
        - missing local queue wakeups are performance issues only; progress can
          be made by polling/listing Deleting buckets or by a durable work item
      - keep storage-node read-handle work and durable reclaim/scavenger work
        in Phase 9.5-9.7 scope, but make Phase 9.4 finalization robust when
        the only remaining blocker is the write-drain state
      - add trace events for every terminal and retryable outcome:
        drain installed, drain wait, stale reservation ignored/reaped, rollback,
        mark-deleting command install/apply, finalize pending, and finalized

   6. Phase 9.4.6: remove the old counter authority
      - status: complete. Removed `write_reservations_blocked` and
        `active_write_reservations` from bucket schema, bucket row types, and
        command-owned bucket projections.
      - status: complete. Updated
        [metadata-model.md](../guides/metadata-model.md) and
        [storage-cluster-invariants.md](../guides/storage-cluster-invariants.md):
        - bucket write-drain state is no longer an unresolved Phase 9
          exception
        - the new reservation/drain rows are the only production authority
        - finalized bucket row deletion remains the explicit command-owned
          metadata exception
      - status: complete. `scripts/check-storage-cluster-boundaries` still
        fails loudly if old bucket write-drain counter helper names reappear in
        production code.
      - status: complete. Tests that only exercised the legacy counter API were
        removed; durable coordination row tests remain.

   7. Phase 9.4.7: tests and closeout
      - focused storage regressions:
        - active reservation blocks DeleteBucket until release, then DeleteBucket
          observes the published data and returns BucketNotEmpty
          - status: covered by
            `begin_bucket_delete_waits_for_durable_reservation_and_post_drains_visible_write`.
        - a command whose reservation was reaped or whose bucket incarnation no
          longer matches is rejected at apply time, even if it was built from a
          previously valid bucket snapshot
          - status: covered by
            `stream_create_command_rejects_missing_bucket_write_reservation_proof`
            and
            `stream_create_command_rejects_stale_bucket_incarnation_proof`.
            Live apply and open-time convergence now both require the proof to
            match the durable reservation row and the current active bucket
            incarnation generation. Ordinary bucket metadata generation changes
            are intentionally not incarnation changes.
        - an object-PG command partially applies while the reservation is live;
          a DeleteBucket drain then attempts to reap that reservation. The test
          must prove the selected rule: reaping is blocked until the exact
          command converges, or the command carries a durable reservation proof
          and retry/reopen convergence completes safely after reap
          - status: covered by the proof-bearing partial/reopen convergence
            regressions for direct PUT, stream-create, MPU create/complete,
            object metadata, object delete, and delete-marker insertion. The
            implemented rule is that accepted exact commands carry durable
            proof in the command/log state and release the reservation before
            terminal pending-slot cleanup.
        - temporary DeleteBucket drain against a non-empty bucket rolls back and
          a waiting write proceeds from fresh bucket state
          - status: covered by the durable-drain rollback tests for non-empty
            buckets and temporary drain wait/retry.
        - a writer with an already-acquired reservation publishes a partial
          object-PG command after the first DeleteBucket object-PG drain; begin
          delete waits for reservations empty, drains that object-PG command in
          the post-reservation drain, then bases BucketNotEmpty/finalize
          decisions on the converged state
          - status: covered by
            `begin_bucket_delete_waits_for_durable_reservation_and_post_drains_visible_write`.
        - empty-bucket DeleteBucket installs the drain, waits for reservations,
          publishes `MarkBucketDeleting`, and finalizes after restart without a
          same-process waiter
          - status: covered by the durable DeleteBucket restart/open
            regressions added in Phase 9.4.4.
        - crash/reopen with drain fence before mark-deleting rolls back or
          resumes according to the owner-token rule
          - status: covered by Phase 9.4.4 drain recovery tests.
        - crash/reopen with partial `MarkBucketDeleting` apply converges through
          the metadata command stream and does not leak the drain fence
          - status: covered by the partial primary-last `MarkBucketDeleting`
            durable-drain regression.
        - stale cluster handle cannot acquire a new bucket write reservation,
          but can release a reservation it already acquired
          - status: covered by durable reservation epoch/owner-token tests.
        - two `LocalClusterMap` handles/process simulations contend on the same
          bucket drain and reservation records without local condition variables
          - status: covered by two-handle durable drain/reservation contention
            tests; the production authority is the bucket-PG primary rows, not
            process-local counters or condition variables.
      - request-level race coverage:
        - DeleteBucket vs direct PUT
        - DeleteBucket vs CopyObject
        - DeleteBucket vs stream PUT create/finalize
        - DeleteBucket vs CreateMultipartUpload
        - DeleteBucket vs CompleteMultipartUpload
        - DeleteBucket vs UploadPart/UploadPartCopy stream session creation and
          finalization where the MPU itself is the visible blocker
        - DeleteBucket vs current DeleteObject inserting a delete marker
        - DeleteBucket vs specific-version delete removing the last visible
          version
        - DeleteBucket vs lifecycle current expiry, noncurrent expiry, and
          expired delete-marker cleanup
        - status: covered by the Phase 9.4.4 request/storage race matrix and
          the Phase 9.4.3 proof-bearing writer command regressions. Phase 9.4.7
          did not add another request surface; it closed the proof validation
          boundary with the bucket incarnation fence.
      - property/model coverage:
        - extend the local-cluster command-stream trace model with bucket drain
          records, durable reservations, owner expiry, restart/open validation,
          and two handles
        - invariant: no successful public operation returns with a non-terminal
          drain fence unless the operation is the active DeleteBucket attempt
        - invariant: a bucket cannot be finalized while any live write
          reservation for that bucket is active
        - invariant: a stale/expired reservation may unblock delete only after
          the owner-token rule says the writer cannot publish
        - status: covered by focused local-cluster regressions rather than a
          new model extension in this slice; the remaining model expansion is a
          later hardening opportunity, not a Phase 9.4 release blocker.
      - verification:
        - `./scripts/check-storage-cluster-boundaries`
        - targeted bucket delete/write-drain tests
        - targeted object write, multipart, UploadPartCopy, and lifecycle
          suites that use bucket write snapshots
        - `cargo clippy --all-targets --all-features -- -D warnings`
        - full `cargo nextest run`
      - exit when:
        - no production path mutates or reads bucket write-drain authority
          through process-local counters
        - DeleteBucket progress does not require a local condition variable or
          the process that started the delete
        - restart/open validation catches impossible or unsafe drain/reservation
          states
        - the guide, plan, boundary script, and tests agree on the single
          bucket write-drain authority
        - status: complete pending final verification. The implemented authority
          is the bucket-PG durable reservation/drain rows, and proof-bearing
          object-PG commands validate the durable reservation plus active bucket
          incarnation before non-accepted apply/retry/open-time convergence.
6. Phase 9.5 storage-node-owned read handles
   - status: complete. Phase 9.5 moved object payload generation
     lease/reclaim-fence authority out of the local-cluster runtime state and
     onto `SharedStorageNode`; tokens release the captured storage-node handles
     without depending on the current map epoch, and object reclaim uses the
     storage-node-owned fence before physical shard deletion. Production
     `ReadHandle` construction now computes the shard owners for each selected
     segment, including parity/recovery candidates, and acquires volatile
     handles from those storage nodes with all-or-release semantics instead of
     taking a coarse all-node handle. The older coarse generation acquisition
     is now test-hooks-only, with a boundary check that rejects production
     callers that do not acquire selected shard-location handles. A matching
     read-path inventory keeps low-level payload-shard reads inside the storage
     segment reader and keeps server-core payload-byte reads behind
     `ReadRuntime::read_segment_payload`, which is only reached by
     `ReadHandle` after handle acquisition. Physical payload shard deletion is
     crate-local and boundary-checked so production deletes stay behind the
     placed-delete reclaim/read-handle fence helper. Request-level degraded EC
     range coverage now asserts that recovery reads acquire and release the
     selected shard-owner handle set. The storage-node read-handle/reclaim
     methods are crate-local and boundary-checked so external production code
     cannot bypass the cluster read-handle and placed-delete APIs.
   - decision: do not add a metadata/database write on each object read. Reads
     are ephemeral request state; if the host handling the read fails, the
     client can retry from a fresh metadata snapshot. The durable state should
     describe reclaim work, not every active reader.
   - replace coordinator/local-cluster object payload generation leases with
     volatile read handles owned by the shard-owning storage node
   - every read path must obtain shard read handles from the nodes that own the
     selected shard files before streaming payload bytes
   - multi-shard reads must use all-or-release acquisition semantics. If handle
     acquisition succeeds for only part of the selected EC/recovery shard set,
     the read path must release every partial handle before retrying from a
     fresh metadata snapshot or selecting/acquiring a replacement shard set. A
     read may not stream until it owns handles for the complete shard set it will
     read from
   - every physical shard delete path must go through the shard-owning storage
     node delete/reclaim API, which refuses or defers deletion while local read
     handles are active
   - reclaim must fence new read-handle acquisition before physical deletion,
     and must leave durable reclaim metadata retryable when deletion is deferred
     by active read handles
   - bucket finalization waits on durable reclaim roots, not on reader lifetime
     directly. Active reads block finalization only by keeping reclaim roots
     from being physically cleared
   - crash semantics: storage-node read handles are volatile and disappear with
     the serving process/node; the read fails and the client retries. There is
     no durable reader cleanup path in this phase
   - guardrail: no production path may directly remove shard files without the
     storage-node read-handle/delete fence
   - required regressions:
     - two cluster handles/process simulations where one handle streams from a
       shard-owning node read handle and another handle's reclaim defers
     - dropping the read handle lets the same durable reclaim row complete
     - partial multi-shard acquisition failure releases already-acquired
       handles and leaves no leaked read handle state
     - EC recovery/read-repair replacement selection acquires handles for the
       replacement shard set before reading, and releases the abandoned partial
       set
     - reclaim that has started physical deletion rejects a new read handle and
       the read path retries/fails from a fresh metadata snapshot
     - bucket finalization remains pending while reclaim roots are blocked by
       active storage-node read handles and completes after reclaim progress
     - injected shard-delete failure keeps the delete fence/reclaim metadata
       retryable without allowing new reads of the reclaimed generation
   - exit when every production read and every physical shard delete uses the
     storage-node read-handle/delete API, and the old coarse payload lease API
     is removed from production surfaces or test-only. Exit criteria are
     satisfied: production reads acquire selected shard-owner handles,
     production physical deletes go through the placed-delete reclaim fence, the
     coarse lease API is test-hooks-only, and boundary checks cover direct
     payload reads, direct payload deletes, public low-level read APIs, and
     public storage-node handle/fence APIs
7. Phase 9.6 durable reclaim claiming
   - status: implemented. Phase 9.6 replaces process-local reclaim queue
     ownership with durable PG-primary claims. Durable reclaim rows remain the
     source of truth; local queues and condition variables become wakeup hints
     only; they are FIFO latency hints and no longer encode production
     correctness ordering between object reclaim and bucket finalization.
     Object payload reclaim now claims object-PG work before physical deletion
     and transfers the claim proof into the terminal
     `DeleteObjectPayloadReclaim` command. Bucket delete finalization claims
     bucket-PG work before attempting terminal cleanup.
   - concurrency decision: start with one active durable reclaim owner per
     PG/work-class, not multiple workers on the same PG. Object payload reclaim
     is claimed on the object metadata PG that owns the reclaim root. Bucket
     delete finalization is claimed on the bucket PG that owns the deleting
     bucket row. This deliberately trades reclaim parallelism within one PG for
     a smaller correctness surface: no two workers race the same reclaim
     metadata command stream, no duplicate bucket finalizer runs, and physical
     cleanup remains idempotent. Crash recovery, claim expiry, and retryable
     failed cleanup are still required because the single owner can stop at any
     point.
   - durable authority:
     - object payload reclaim roots are already durable in
       `object_segments_reclaim` and `multipart_reclaim`; Phase 9.6 adds
       durable claim state either directly to those rows or through an explicit
       object-PG reclaim-claim table keyed by
       `(bucket, bucket_incarnation, key, generation_id, reclaim_kind)`.
       Bucket incarnation is part of claim identity even though generation IDs
       are normally allocated from bucket-scoped metadata, because stale
       release/steal must fail closed across delete/recreate boundaries and may
       not rely on allocator assumptions
     - bucket delete finalization roots are deleting bucket rows; Phase 9.6 adds
       durable finalizer claim state keyed by bucket name plus bucket
       incarnation on the bucket PG
     - claim identity must include owner node/process identity, a unique claim
       token, cluster epoch, PG id, work kind, claimed_at, expires_at, attempt
       count, and last error/status fields useful for debugging stuck cleanup
   - object reclaim claim loop:
     1. a worker wakes from a local hint, a periodic poll, or startup scan
     2. it routes to the object PG primary and atomically claims one eligible
        reclaim root only if no non-expired object-reclaim claim is active for
        that PG/work-class. If the bucket row is already absent, the claim uses
        reserved bucket incarnation `0` for orphan root cleanup; live and
        deleting buckets use the bucket row's incarnation, so stale orphan
        workers cannot clear a claim for a recreated bucket.
     3. if the local hint refers to work already gone, claimed elsewhere, or
        blocked by an active non-expired PG claim, the worker returns without
        treating that as success
     4. once claimed, the worker calls the existing physical reclaim path, which
        must still fence storage-node read handles before deleting shard files
     5. successful physical cleanup installs/applies the existing
        `DeleteObjectPayloadReclaim` metadata command with the durable claim
        proof. Command apply clears the durable reclaim row plus the matching
        token-fenced claim in the same object-PG metadata transaction. If the
        command is already terminal/applied and the claim or pending slot
        survives, retrying the exact pending command re-runs the terminal
        cleanup using the proof carried by the command.
     6. if cleanup defers because read handles are active, the durable reclaim
        row remains, the claim is released or allowed to expire, and a later
        worker can retry
     7. if physical deletion starts and then fails, the storage-node reclaim
        fence remains closed as today, the durable row remains observable, and
        the claim records the failure before becoming retryable
     8. if the `DeleteObjectPayloadReclaim` command becomes terminal/applied but
        the durable claim or pending slot survives a crash or cleanup failure,
        the terminal cleanup remains retryable and must run before the PG's
        object-reclaim work-class can be considered unblocked. Reopen/startup
        scans must recognize terminal reclaim cleanup work, clear only the
        matching token-fenced claim, clean the pending slot, and then allow
        unrelated reclaim on the PG to proceed
   - bucket finalizer claim loop:
     1. a worker wakes from a bucket-finalize hint, periodic poll, or startup
        scan
     2. it routes to the bucket PG primary and atomically claims one deleting
        bucket finalization only if no non-expired bucket-finalizer claim is
        active for that bucket PG/work-class
     3. the claimed worker runs `try_finalize_bucket_delete`; if reclaim roots
        still exist, finalization remains pending and the claim is released or
        expires for later retry
     4. if metadata row deletion or completed-MPU cleanup partially applies, the
        existing command-stream convergence rules must make retry/open-time
        recovery idempotent before the claim is cleared
     5. if bucket finalization reaches terminal metadata state but the durable
        finalizer claim or pending terminal cleanup survives a crash/failure,
        startup/reopen must retry the terminal cleanup before treating the
        bucket-PG finalizer work-class as unblocked. Cleanup must be fenced by
        finalizer claim token and bucket incarnation
   - claim expiry and stealing:
     - claims are volatile ownership records backed by durable metadata, not
       proof that work completed
     - a worker may steal only an expired claim, and only after re-reading the
       current durable reclaim root/bucket incarnation on the PG primary
     - stale release must be fenced by claim token and bucket/object
       incarnation so an old worker cannot clear a newer claim
     - expiry should be long enough to avoid stealing live workers during normal
       large payload deletion, and workers may heartbeat/extend claims before
       expiry while making progress
   - startup and polling:
     - worker startup must scan durable reclaim roots and deleting buckets, so
       process restart does not depend on in-memory queue contents
     - local enqueue remains a latency optimization that wakes workers after
       request-path metadata changes or read-handle release; correctness must
       not depend on enqueue delivery
     - lost local hints, stopped workers, and process restart must eventually
       recover through durable scans
     - durable scans are best-effort per PG: an unavailable or corrupt PG must
       emit typed scan context but must not block discovery of reclaim roots on
       later healthy PGs
   - failure semantics:
     - multiple workers may race to claim, but only one durable claim wins per
       PG/work-class
     - duplicate physical delete attempts must remain idempotent because crash
       may occur after deleting some shard files but before clearing reclaim
       metadata
     - cleanup errors must not be swallowed as success; they remain observable
       on the claim/root and leave work retryable
     - terminal command cleanup is part of the durable reclaim protocol, not
       best-effort background tidying. If command convergence succeeds but claim
       release, pending-slot cleanup, or finalizer cleanup fails, enough durable
       identity must remain to retry the cleanup after restart; the surviving
       claim must not indefinitely block unrelated work on the same PG
     - bucket finalization must not complete while any durable payload reclaim
       root for that bucket remains
   - required regressions:
     - two workers race object reclaim on the same object PG; exactly one
       durable claim wins and the other observes claimed/no work
     - two workers race reclaim roots on different object PGs; both may proceed
       concurrently
     - two workers race reclaim roots on the same object PG; the second waits,
       skips, or retries after the first claim clears/expires
     - crash/reopen after claiming an object reclaim root but before physical
       cleanup; startup scan reclaims the expired claim and completes cleanup
     - crash/reopen after partial physical shard deletion but before reclaim
       metadata cleanup; retry completes idempotently and clears the durable row
     - `DeleteObjectPayloadReclaim` reaches terminal/applied, then claim release
       or terminal pending-slot cleanup fails; retry/reopen clears the matching
       claim and slot before allowing another same-PG object reclaim claim
     - injected physical delete failure records observable retry state and keeps
       the storage-node read-handle fence closed until retry converges
     - lost local queue after restart still discovers and completes object
       reclaim roots from durable metadata alone
     - stale object-reclaim worker release/steal cannot clear a newer claim for
       a recreated bucket with the same key/generation shape
     - two workers race bucket finalization on the same bucket PG; exactly one
       finalizer claim wins and no double-finalize/corruption occurs
     - crash/reopen after claiming bucket finalization; expired claim is
       reclaimed and `try_finalize_bucket_delete` resumes from durable state
     - bucket finalization reaches terminal metadata state, then finalizer claim
       cleanup fails; retry/reopen clears the matching claim before allowing
       another same-PG bucket-finalizer claim
     - stale worker release cannot clear a newer claim or a recreated bucket's
       finalizer claim
   - implementation slices:
     1. add claim schema/types, digest/replay coverage if the rows are
        replica-visible, and low-level claim acquire/heartbeat/release/expire
        helpers on the relevant PG primary (done for object reclaim and bucket
        finalizer claim tables)
     2. wire object payload reclaim workers to claim durable object-PG work and
        treat the local queue as a hint (done)
     3. add startup/periodic scans for durable object reclaim roots so restart
        without local queue state makes progress (done)
     4. wire bucket delete finalization workers to durable bucket-PG claims and
        add startup/periodic scans for deleting buckets (done)
     5. remove or test-gate production reliance on `LocalReclaimQueueState`
        ordering, leaving it only as wakeup/backpressure plumbing until Phase
        9.7/9.8 replace broader scavenger/lifecycle scheduling (done)
   - exit when reclaim and bucket finalization can resume after process restart
     without local worker state, worker ownership is durable and token-fenced,
     cleanup is idempotent across claim expiry/steal, and local queues are only
     wakeup hints
8. Phase 9.7 physical shard scavenger
   - status: done. The persisted per-physical-location observation
     table, record/resolve/list helpers, low-level non-authoritative
     location-keyed regression, local file/row mismatch audit scan, and
     writer-side publish validation of acknowledged shard files plus data-PG
     ack rows are implemented. The cluster audit now scans all local storage
     nodes for physical shard files, compares them with data-PG primary shard
     rows, builds a topology-aware reference set from object segments,
     committed MPU manifests, in-progress MPU parts, staged stream segments,
     MPU segment rows, durable reclaim roots, and durable pending metadata
     command payload references, and resolves apparent
     unreferenced observations once metadata is published. Pending-command
     reference decoding is implemented for payload-carrying command variants;
     explicit `scan_incomplete` observation rows are recorded when reference or
     shard-file scans fail, including focused coverage for malformed durable
     pending command references; observation metrics and trace events are
     emitted from the durable observation write path. Pending-command reference
     suppression is covered for both direct PUT and multipart completion.
     Coordinator worker wiring is implemented as a shared audit-only background
     sweeper. Embedded local S3 test servers expose an explicit final
     shard-scavenger clean assertion for focused tests, including an async
     settling helper that wakes reclaim workers and polls the final audit until
     cleanup converges. Automatic process-exit enforcement is deferred because
     one-test `s3-tests` processes can exit before asynchronous reclaim has
     settled.
   - start with audit-only orphan detection, not deletion. Negative reference
     scans are too dangerous to use as delete authority while slow writers can
     have acknowledged shard files that are not yet published by metadata. A
     candidate row means only "this scan could not prove a durable reference";
     it must never mean "safe to delete".
   - add a per-storage-node/per-data-PG persisted scavenger observation table
     for apparent orphan candidates, keyed by physical location identity:
     storage node id (or equivalent `ShardLocation` node context), data PG id,
     shard index/location, and shard key. The same `ShardKey` can exist on
     multiple storage nodes or on a stale wrong-node path, so observation
     identity must not collapse distinct physical files. Carry `first_seen_at`,
     `last_seen_at`, scan/observation count, data size/checksum when known,
     whether the shard file exists, whether the `shards` row exists, reason,
     and last scan error/context. Candidate rows are operational visibility and
     regression evidence only.
   - enumerate both local shard files and the data-PG `shards` table. Build the
     referenced set from every durable metadata source that can still make a
     payload reachable: live object segments, completed-object part manifests
     in `object_parts`, in-progress MPU part descriptors in `multipart_parts`,
     MPU shard-set segment rows in `multipart_part_segments`, staged stream
     upload segments, object/multipart reclaim roots, durable pending metadata
     commands, and terminal cleanup state that still owns payload cleanup.
     Current code covers the table-backed sources, reclaim roots, and
     payload-carrying durable pending metadata command variants.
   - classify observations at least as:
     - `file_without_shard_row`: a shard file exists but the data-PG `shards`
       row is missing
     - `shard_row_without_file`: a data-PG `shards` row exists but the shard
       file is missing; this is corruption/audit signal, not a cleanup target
     - `unreferenced_shard_row_and_file`: both local row and file exist, but the
       completed scan found no durable metadata reference
     - `scan_incomplete`: at least one metadata PG/reference source failed; no
       new candidate from that scan may be treated as stable
   - clear or mark resolved observations when a later scan finds a durable
     reference or the local file/row is gone. If any reference scan fails, emit
     typed context and leave existing candidate rows as observations rather than
     promoting them.
   - deletion remains limited to positive-proof paths from earlier phases:
     durable reclaim roots, terminal abandoned/cleanup commands that carry the
     exact payload identity, or other explicit durable evidence that the payload
     can no longer be published. Do not delete solely because a shard is absent
     from the reference set.
   - add writer-side safety-net validation before publishing metadata for
     payloads already written to storage nodes: every acknowledged shard must
     still have its physical file plus the data-PG `shards` ack row, and the row
     must match the expected `WriteAck` size/CRC. If validation fails, fail
     closed rather than publishing metadata pointing at missing or mismatched
     bytes. This does not authorize scavenger deletion; it only prevents
     corruption if another bug removes or rewrites a shard.
   - required regressions:
     - a shard file plus `shards` row with no metadata reference creates an
       `unreferenced_shard_row_and_file` observation and leaves the file intact
     - a file without a `shards` row creates a `file_without_shard_row`
       observation and leaves the file intact
     - a `shards` row without a file creates a `shard_row_without_file`
       observation and surfaces an audit/corruption signal
     - if any metadata PG/reference scan fails, the scan records
       `scan_incomplete`, keeps existing observations conservative, and does
       not report anything as stable/deletable (covered for cluster shard-file
       scan failure and focused reference-scan failure)
     - recording any shard scavenger observation emits observability counters
       and a trace event carrying the location identity and reason
     - a later durable metadata reference clears or resolves a prior apparent
       orphan observation (covered for the cluster slow-writer publish path)
     - slow-writer simulation: shard files are written and acknowledged before
       metadata publish; the audit scan may observe them but must not delete or
       block the later successful publish (covered for direct PUT)
     - pending-command reference suppression for a payload-carrying cleanup
       shape beyond direct PUT, preferably an MPU cleanup or completion command
       whose referenced shard set exists only in the durable pending slot
       (covered for multipart completion)
     - metadata publish fails closed if a previously acknowledged shard file is
       missing at final publish validation
     - focused local S3 harness tests can wait for async reclaim to settle,
       then run a final shard-scavenger audit and fail on unresolved
       observations
   - exit when apparent unreferenced shard files are detected, persisted,
     surfaced through logs/metrics/tests, and never deleted without positive
     durable abandonment/reclaim proof. Negative-reference deletion is deferred
     until a later phase adds durable write intents or an equivalent publish
     fence. Full suite passed after the final harness settling adjustment.
9. Phase 9.8 lifecycle/background mutation ownership (done)
   - make lifecycle sweeper ownership, progress, and retry state durable and
     cluster-visible. `LIFECYCLE_SWEEPER_REGISTRY` may remain as
     process-local thread deduplication/backpressure only; it must not be a
     correctness owner.
   - target model: durable, lease-backed lifecycle sweep ownership per bucket
     incarnation on the bucket metadata PG. Do not start by persisting every
     candidate object/version as scheduled work. Candidate work is derivable
     from durable bucket lifecycle config, object metadata, and multipart
     upload state, and the existing mutation paths already re-read and
     re-check lifecycle/config/current state at apply time. The durable unit
     of ownership should therefore be a bucket sweep pass, with object-PG
     metadata commands providing the final idempotency/recheck layer.
   - add a `lifecycle_sweep_claims` table keyed by bucket and
     `bucket_incarnation_generation`, with `claim_id`, `owner_token`,
     `cluster_epoch`, `claimed_at`, `heartbeat_at`, `lease_deadline`,
     `attempt_count`, and `last_error` or equivalent retry context. Claim
     acquire/release/heartbeat/steal operations must be token-fenced and
     incarnation-fenced. A stale claim for a deleted/recreated bucket must not
     block the new incarnation.
   - a lifecycle worker may sweep a bucket only after acquiring that durable
     claim. If another process owns a live claim, the bucket is skipped for
     this pass. If a process crashes, the lease expires and another process can
     resume by re-scanning the bucket and re-deriving candidates. Long sweeps
     must heartbeat the claim while enumerating and mutating candidates, so a
     healthy worker does not lose ownership solely because a large bucket takes
     longer than one lease interval to scan. Claim scan/acquire/heartbeat uses
     the wall-clock lease clock; lifecycle due evaluation may use an injected
     timestamp and must not be used to decide whether another worker's lease has
     expired.
   - lifecycle claims do not bypass bucket deletion fences. Claim acquisition
     and every lifecycle mutation must recheck that the bucket incarnation is
     still Active and not protected by an active durable DeleteBucket drain.
     If DeleteBucket starts after a lifecycle claim is acquired, current
     object expiration, noncurrent expiration, expired delete-marker cleanup,
     MPU abort, and finishing `Aborting` uploads must either observe the
     drain/Deleting state and stop without publishing, or be converged by the
     DeleteBucket drain before DeleteBucket trusts bucket emptiness.
   - durable root scanning must use the same stale-claim discipline as bucket
     finalizer claims. Expired lifecycle claims are surfaced first and
     deterministically, so their exact bucket/incarnation cannot be hidden by
     an earlier ordinary bucket root. Busy claimed buckets are skipped by the
     ordinary lifecycle-config/aborting-upload root scan. `lease_deadline =
     NULL` means busy/non-expiring; only `lease_deadline <= now` is expired
     recovery work. If the claimed bucket is gone, recreated with a different
     incarnation, or no longer a lifecycle/aborting root, stale claim cleanup
     must be token/incarnation-safe and must not clear a newer claim.
   - the bucket sweep claim covers all background lifecycle mutations for that
     bucket:
     - current object expiration
     - noncurrent version expiration
     - expired delete-marker cleanup
     - abort-incomplete-multipart-upload
     - finishing uploads already in `Aborting` state
   - preserve the current fail-closed mutation semantics: after claim
     acquisition, every object/MPU mutation still rechecks the current bucket
     lifecycle configuration, current object/upload state, object lock, and
     command-stream pending state before publishing metadata changes. A claim
     only serializes/retries the bucket sweep; it is not proof that a candidate
     is still due.
   - one bad candidate must not silently starve later due work in the same
     bucket. Candidate enumeration should isolate per-key/per-upload errors
     where it can do so safely: record typed error context on the claim,
     continue to independent later candidates, and keep enough retry state for
     the failed candidate to be revisited on a later pass. If a failure makes
     the bucket-wide snapshot incomplete or candidate ordering unsafe, the
     pass may fail closed, but it must emit an explicit starvation/error signal
     and a focused regression should document that tradeoff.
   - implementation slices:
     1. add the plan/docs, claim schema/types, and low-level PgStore helpers
        for acquire, release, heartbeat, expired-claim steal, stale terminal
        cleanup, and deterministic durable root listing (done)
     2. route `run_lifecycle_sweep_at` through durable bucket claims: discover
        roots through the deterministic root scanner, attempt a claim per
        bucket incarnation, skip busy buckets, run the existing sweep logic
        while holding and heartbeating the claim, release on success, and record
        error/retry context on failure (done)
     3. add startup/periodic durable root scanning so restart without local
        lifecycle worker state discovers buckets with lifecycle config,
        buckets with aborting multipart uploads, and expired lifecycle claims
        using the deterministic expired-claim-first and busy-claim-skipping
        rules above (done for the runtime sweep entry point and routed
        metadata-PG aborting-upload discovery, including stale expired-claim
        cleanup for gone, recreated, drained, or no-longer-eligible roots)
     4. downgrade or update the process-local lifecycle sweeper registry tests
        so they prove only local thread sharing, not correctness ownership
        (done)
     5. add observability for claim acquire/busy/steal/release/error paths and
        bounded per-pass scan stats (done)
   - required regressions:
     - two coordinators/process handles racing the same lifecycle bucket:
       exactly one acquires the durable sweep claim and the other skips/busy
     - expired lifecycle claim is stealable and the bucket sweep resumes after
       simulated worker loss
     - stale claim for a deleted/recreated bucket incarnation cannot block the
       recreated bucket
     - `lease_deadline = NULL` lifecycle claims are treated as busy in both
       root scanning and claim acquisition
     - an expired later-bucket lifecycle claim is rediscovered before an
       earlier ordinary lifecycle bucket root, so stale claimed work cannot be
       hidden by scan ordering
     - a busy claimed lifecycle bucket is skipped by ordinary root scanning
       without hiding unrelated unclaimed buckets
     - old-incarnation expired claims do not suppress a current-incarnation root
       for the same bucket during one sweep pass
     - long bucket sweeps heartbeat while enumerating and before mutating
       candidates, so the active owner cannot be stolen during normal progress
     - lifecycle claim acquired, then DeleteBucket starts: current-object,
       noncurrent-version, delete-marker, MPU abort, and `Aborting` upload
       finishing paths observe the drain/Deleting state or are drained before
       DeleteBucket emptiness is trusted
     - lifecycle config changed after claim acquisition is re-read before
       mutation, so stale candidates are not applied
     - object current-version/noncurrent/delete-marker lifecycle mutations
       remain idempotent when a matching pending metadata command already
       exists
     - abort-incomplete-multipart-upload and finishing `Aborting` uploads
       resume after local sweeper loss
     - a corrupt/transiently failing object or MPU candidate records error
       context and does not indefinitely starve independent later due work, or
       an explicit fail-closed starvation regression documents the intended
       bucket-wide abort behavior
     - local wakeup loss or process-local registry loss does not strand due
       lifecycle work
   - lifecycle transition rules are out of scope for this phase until
     transition support exists; the current lifecycle parser rejects transition
     elements as not implemented.
   - exit criteria met: lifecycle expiration, delete-marker cleanup, and multipart
     abort work does not depend on a process-local sweeper registry or local
     wakeup; multiple processes cannot concurrently own the same bucket
     lifecycle sweep; stopped workers resume through durable claim expiry and
     deterministic root scanning; and all lifecycle mutations still recheck
     current state before applying.
10. Phase 9.9 cache freshness across processes (done)
   - status: done. Bucket fast-path entries and parsed-policy cache entries
     are now process-local performance hints only: request paths validate the
     cached `BucketFastPathIdentity` against durable bucket metadata before
     using cached state, and fall back to full snapshot loading when the proof
     is stale or unavailable. The identity proof includes both execution and
     incarnation generation, so delete/recreate is fenced independently of
     ordinary bucket metadata changes. Independent-cache regressions cover
     stale allow and stale deny policy decisions, ABAC tags, ownership controls,
     public access block, delete/recreate, storage proof failure for both
     bucket summary and parsed policy paths, the storage-level field/mutator
     freshness matrix, and watcher hint behavior.
   - problem:
     - `CoordinatorSharedCaches`, `BucketFastPathCache`, the local watcher
       thread, and the process-local cache registry are process-local
       performance state.
     - Bucket mutations already advance durable `bucket_execution_generation`,
       but a second process can mutate policy, versioning, ownership controls,
       public access block, tags, lifecycle-visible state, or delete/recreate
       state while this process still has a warm fast-path entry.
     - The local watcher may reduce the stale window, but it cannot be the
       correctness mechanism: it polls, can skip unavailable PGs, and only
       observes entries known to the current process.
   - target invariant:
     - bucket fast-path state is never authoritative by itself.
     - every request that makes an authorization, ownership, versioning,
       public-access, tag/ABAC, encryption, lifecycle-visible, object-lock, or
       bucket-state decision from a cached fast-path entry must first prove that
       the cached bucket identity is still current in the bucket metadata PG.
       The proof must include both `bucket_execution_generation` and
       `bucket_incarnation_generation`, or an equivalent full bucket-row
       identity/digest. Generation-only freshness is not enough because
       incarnation is the delete/recreate fence introduced in Phase 9.4.7.
     - if the freshness proof cannot be obtained, the request must fail closed
       by reloading the full bucket snapshot or by returning the same safe
       error it would return if the bucket snapshot load failed. It must not
       continue with the cached entry.
     - the watcher remains an optimization that can proactively mark entries
       stale or remove missing buckets, but request-time generation validation
       is the correctness boundary.
   - implementation slices:
     1. audit all bucket fast-path reads and document which fields they can
        influence:
        - BOE object reads and parsed bucket policy fast path
        - bucket tags used for ABAC evaluation
        - ownership controls and ACL-free decisions
        - public access block and bucket policy public classification
        - versioning and object-lock-sensitive paths
        - default encryption and SSE-C-blocking decisions
        - lifecycle-visible state and lifecycle generation if exposed through
          fast-path data
        - owner/canonical-id fields used for expected-owner or authorization
          decisions
        - bucket state and delete/recreate handling
     2. build a field/mutator freshness matrix for `BucketFastPathInfo`:
        - every cached field must be mapped to the production mutations that can
          change it
        - every such mutation must have an existing or new regression proving
          that it advances the durable freshness token used by Phase 9.9
        - fields currently included in the fast-path record include owner,
          state, versioning, object lock/default retention, public access block,
          ownership controls, policy metadata, lifecycle metadata, ABAC/tags,
          encryption/SSE-C policy, execution generation, and incarnation
          generation
        - if a cached field does not affect any fast-path decision, either
          remove it from the fast-path record or document why stale values cannot
          affect behavior
     3. add a narrow storage/cluster freshness API:
        - input: bucket name, cached `bucket_execution_generation`, and cached
          `bucket_incarnation_generation`
        - output: fresh, stale/missing, or load error
        - group by bucket PG for batched checks where useful, but provide a
          single-bucket path for request-time use
        - stale/missing removes or marks the cached entry stale before falling
          back to a snapshot load
        - load errors are visible to callers; they must not be silently
          interpreted as fresh
     4. change `BucketFastPathCache` accessors so "fresh" means durable-fresh
        or so callers cannot accidentally use local freshness as the
        correctness proof:
        - local `known_generation` may remain as a watcher hint
        - request paths should call a coordinator helper that performs durable
          validation before returning a usable fast-path handle
        - parsed policy cache lookup must also be guarded by the same durable
          generation proof
     5. keep mutation-side cache updates as hints:
        - same-process bucket mutations should still observe the new generation
          or remove entries to avoid unnecessary reloads
        - correctness must not rely on these updates firing in the mutating
          process or being shared with other processes
     6. harden delete/recreate handling:
        - cache keys remain bucket-name based, so durable validation must treat
          missing buckets, lower/impossible generations, incarnation mismatch,
          and recreated buckets as stale and force reload/removal
        - a cached entry for an old incarnation must not authorize requests
          against a recreated bucket with the same name
     7. update docs and names to make the authority clear:
        - rename or comment local freshness helpers so they are visibly
          process-local hints. The cache entry local freshness and parsed-policy
          helper comments now state that request paths must still validate
          cached identity against durable bucket metadata before trusting
          fast-path state.
        - document that the metadata digest clean-revision cache remains
          acceptable only because every skip is guarded by durable revision
          state, not process-local invalidation
   - required regressions:
     - two independent process-shaped coordinator handles sharing storage, with
       independent `CoordinatorSharedCaches` rather than the normal
       same-process shared-cache registry path. The reader warms the BOE fast
       path, the writer deletes or tightens the bucket policy, writer-side
       mutation hints must not touch the reader cache, the watcher is
       disabled/delayed, and the reader must not authorize through stale cached
       policy before the watcher runs. Covered by the Phase 9.9 independent
       policy-tightening regression.
     - stale-deny/loosening coverage: reader warms a cached deny, writer loosens
       or removes the blocking policy/public-access/ownership/tag condition,
       watcher is delayed, and the reader must validate/reload before denying.
       Covered for policy loosening by the Phase 9.9 independent stale-deny
       regression.
     - the same cross-process stale-cache shape for bucket ownership controls
       changing away from BOE, public access block/policy-public state, and
       bucket tags used by ABAC. Bucket tags/ABAC are covered by an independent
       reader/writer cache regression. Ownership controls changing away from
       BOE, and public access block/restrict-public-buckets changing after a
       public policy is cached, are also covered by independent reader/writer
       cache regressions.
     - delete/recreate with the same bucket name: a cached entry from the old
       bucket must be removed or reloaded before any authorization decision for
       the recreated bucket. The regression must force old-incarnation cache
       rejection even if a test hook makes execution-generation behavior collide
       or appear unchanged. Covered by an independent-cache delete/recreate
       regression that keeps the reader cache locally fresh until request-time
       incarnation validation rejects it.
     - field/mutator matrix coverage for every `BucketFastPathInfo` field that
       can affect behavior, including versioning, object lock/default retention,
       default encryption/SSE-C blocking, owner/state, lifecycle-visible
       metadata, public access block, ownership controls, policy metadata, and
       tags/ABAC. The storage-level matrix now covers versioning, ACL/owner
       flags, tagging, policy, lifecycle, ABAC, public-access block,
       ownership controls, encryption, Deleting state, create owner identity,
       incarnation allocation, and object-lock default-retention mutation.
     - freshness-check storage failure: the fast path must be bypassed and the
       request must reload or fail safely; no stale cached allow/deny decision
       may be returned. Covered by a forced fast-path identity proof failure
       regression that verifies the request falls back to full storage load
       instead of making a cached decision, plus a parsed-policy cache
       regression that forces the same proof failure after a bucket summary is
       already loaded.
     - watcher-disabled or watcher-delayed test hook: correctness must still
       hold with only request-time validation.
     - batched watcher/load path continues to mark entries stale opportunistically
       and does not make unavailable PGs look fresh. Existing watcher
       regressions cover direct storage policy mutation, delete/recreate, and
       missing-bucket removal before recreate.
   - exit when:
     - no authorization or bucket-configuration correctness path depends on
       same-process invalidation, local watcher timing, or shared in-process
       cache registry state.
     - bucket fast-path cache is demonstrably a performance cache: durable
       generation validation or fail-closed reload is required before cached
       state can affect request behavior.
11. Phase 9.10 test harness de-single-process pass
   - status: implementation complete; full-suite clean on 2026-05-23. The audit
     table is in `plans/completed/phase-9.10-test-harness-audit.md`, covering
     shared coordinator setup helpers, independent-cache Phase 9.9 tests, BOE
     fast-path model tests, representative lifecycle claim tests, multipart API
     and model tests, multipart stream-race tests, reclaim trace tests,
     object-state concurrency tests, storage-cluster
     reopen/pending-slot/scavenger test shapes, and external/local S3 harness
     crates. The current test tree contains many valid single-process unit
     tests, and the audit now records which local helpers are acceptable local
     units or crash-state seeders. Tests that claim cross-process, restart,
     convergence, race, drain, reclaim, lifecycle, cache-freshness, or scavenger
     correctness must continue to avoid accidentally passing because they share a
     `StorageCluster`, shared `CoordinatorSharedCaches`, a local mutex/condition
     variable, process-global test hooks, or direct `PgStore` state.
   - audit tests and helpers that still use local constructors, raw hooks, or
     direct store access in ways that bypass the production cluster path. Classify
     each use rather than mechanically removing it:
     - production-shaped: uses `StorageCluster`/`LocalClusterMap` APIs in the
       same shape as runtime code, and for cross-process assertions uses
       independently opened handles or independent coordinator caches/watcher
       state as appropriate
     - acceptable local unit: pure parser/model/codec logic, storage-node
       internals, direct `PgStore` invariants, or tests whose purpose is to
       validate the raw helper itself
     - crash-state seeding: raw writes or pending-slot insertion used only to
       construct an otherwise unreachable partial/reopen state, followed by
       production-path recovery/convergence
     - suspect: a test whose name or assertion claims multi-process,
       restart/reopen, contention, idempotence, background-worker, or cache
       correctness while using one process-local handle, shared cache registry,
       local wakeup, local lock, or direct store mutation as the authority
   - start the audit from the Phase 9 primitive matrix:
     1. Phase 9.2 command stream: log index allocation, durable pending slot
        convergence, duplicate-index reissue, abandoned tail cleanup, reopen
        validation, and partial apply must be covered through PG-primary durable
        state. Raw pending/log rows are allowed only as crash-state seeders.
     2. Phase 9.3 multipart serialization: create/upload-part/finalize/copy/
        abort/complete races must have tests using separate request handles
        where local multipart locks would otherwise mask a race. Direct MPU
        table mutation is acceptable only for model setup or impossible crash
        states.
     3. Phase 9.4 bucket write drain: reservations, drains, DeleteBucket begin,
        rollback, idempotent retry, and finalization must be exercised with at
        least two independently routed storage/coordinator handles where
        relevant, and must not depend on node-local counters, bucket locks, or
        same-process waiters.
     4. Phase 9.5 read handles: read-handle acquisition and physical delete
        fences must be tested at storage-node/shard-owner boundaries, including
        multi-shard all-or-release and failed acquisition cleanup. Coordinator
        tests should not prove safety through coordinator-local lease state.
     5. Phase 9.6 reclaim claiming: object reclaim and bucket finalization
        tests must prove durable claims, token-fenced release/steal, stale
        terminal cleanup, lost wakeup recovery, and restart discovery without
        relying on local reclaim queues.
     6. Phase 9.7 shard scavenger: tests must distinguish local audit helpers
        from cluster-level location-aware scans, and must cover scan-incomplete,
        reference-source failure, pending-command references, and final harness
        clean checks without treating negative-reference observations as delete
        authority.
     7. Phase 9.8 lifecycle: lifecycle sweep tests must use durable bucket
        incarnation claims for ownership/retry, isolate local registry tests to
        local deduplication only, and prove drain/state rechecks for lifecycle
        mutations after a claim is acquired.
     8. Phase 9.9 fast-path cache freshness: stale-cache tests must use
        independent process-shaped `CoordinatorSharedCaches` and delayed or
        disabled watcher behavior; same-process cache invalidation is only an
        optimization test.
   - produce an audit table before broad rewrites. For every audited
     raw/local-helper use that is not already clearly production-shaped, record
     file, test/helper name, claimed invariant, current harness shape,
     classification, and required action. This includes acceptable local units
     and crash-state seeders, because the closeout proof depends on documenting
     why each non-production-shaped use is safe:
     - keep as local unit
     - rename/comment so it does not claim cross-process coverage
     - replace shared-cache/same-handle setup with independent process-shaped
       handles
     - add a new focused multi-handle regression and leave the local test as
       helper/unit coverage
     - remove obsolete raw-helper coverage once production-shaped coverage
       exists
   - likely first files/helpers to audit:
     - `crates/server-core/src/coordinator/test_support.rs`: the default
       `setup_same_process_coordinators_*` helpers share one `StorageCluster`
       and the process-local shared-cache registry; use explicit helpers for
       independent process-shaped caches when tests need that shape.
     - `crates/server-core/src/coordinator/core_tests.rs`,
       `multipart_tests.rs`, `multipart_stateful_tests.rs`,
       `multipart_trace_tests.rs`, `multipart_reclaim_trace_tests.rs`,
       `object_state_tests.rs`, `bucket_tests.rs`, `authz_model_tests.rs`, and
       `runtime.rs`: identify tests whose names mention concurrent, restart,
       reopen, retry, drain, reclaim, lifecycle, watcher, stale cache behavior,
       BOE fast-path invariants, process-global fast-path hooks, or custom
       coordinator setup.
     - `crates/storage/src/cluster/local.rs`: separate genuine
       `StorageCluster` authority tests from raw `get_pg`/pending-slot
       seeders, and add reopen/two-handle variants where a local map or PG
       mutex could otherwise hide the issue.
     - `crates/s3-local-tests`, `crates/s3-tests`, and `crates/s3-http-tests`:
       keep public API conformance tests production-shaped, and reserve direct
       final-audit hooks for explicit local harness assertions.
   - add targeted multi-handle or multi-process-simulated tests for gaps found
     by the audit. Prefer small regressions tied to one Phase 9 invariant over
     broad integration tests that obscure which process-local assumption was
     removed.
   - exit when tests prove the Phase 9 coordination invariants without relying
     on shared local locks, shared in-process caches, process-global hooks,
     local wakeups, or direct store mutation except where the audit table marks
     the test as local-only or crash-state seeding.

Phase 9.1 audit checklist:

1. metadata command log index allocation
   - former process-local mechanism:
     `LocalClusterRuntimeState::metadata_command_indexes` (removed in Phase
     9.2)
   - classification: request serialization and command stream ordering
   - risk: two processes can allocate conflicting or reordered log indexes for
     the same PG if allocation remains outside durable PG-primary state
   - replacement owner: Phase 9.2 PG-primary durable command stream allocator
2. metadata command apply serialization
   - former process-local mechanism:
     `LocalClusterRuntimeState::metadata_command_apply_lock` (removed in
     Phase 9.2)
   - classification: request serialization
   - risk: only commands inside one process are serialized; another process can
     apply a command concurrently unless the durable command append path becomes
     the serialization point
   - replacement owner: PG-primary durable pending slot plus per-replica
     validate/apply/record transactions and fail-closed replica history checks
3. pending metadata command convergence
   - former process-local mechanism:
     `LocalClusterRuntimeState::pending_metadata_commands` (removed in Phase
     9.2)
   - classification: request serialization and retry convergence
   - risk: partial apply state can be forgotten on process exit or invisible to
     another process, so retries can allocate later commands before the earlier
     command has converged
   - replacement owner: Phase 9.2 durable pending command rows or retry
     derivation from the durable command log and replica state
4. stream segment VID allocation
   - former process-local mechanism:
     `LocalClusterRuntimeState::stream_segment_vids` (removed in Phase 9.2)
   - classification: request serialization for staged payload identity
   - risk: two processes appending to the same stream session can allocate the
     same segment VID or clear each other's allocator assumptions
   - replacement owner: durable `stream_uploads.next_segment_vid` allocator on
     the PG primary; append command apply advances replica allocator floors
5. per-PG store mutexes
   - current process-local mechanism:
     `SharedStorageNode::stores`, a `HashMap<u32, Mutex<PgStore>>`
   - classification: per-connection safety and local request serialization
   - risk: the mutex protects one process's PgStore handle and serializes local
     access to a PG, but it is not a cross-process serialization primitive
   - replacement owner: keep SQLite transactions for per-connection database
     safety; move logical metadata ordering to Phase 9.2 durable command append
     and PG-primary state before Phase 10
6. multipart completion and upload lifecycle locks
   - current process-local mechanism:
     bucket locks formerly used around multipart complete, abort, and UploadPart
     stream validation. `SharedStorageNode::multipart_completion_locks` has
     been removed.
   - classification: request serialization
   - risk: complete, abort, UploadPart, and streamed UploadPart races are only
     serialized inside one process
   - replacement owner: Phase 9.3 PG-primary metadata command serialization
7. bucket operation locks
   - retired process-local mechanism:
     `SharedStorageNode::bucket_locks`, now cfg-gated to tests/test hooks in
     Phase 10.8
   - classification: request serialization and precondition stability
   - outcome: bucket property, delete, lifecycle, and object-operation request
     paths now rely on PG-primary command preconditions, durable bucket state
     checks, and bucket write-drain/reservation rows rather than same-process
     lock stripes
   - replacement owner: PG-primary command preconditions and durable bucket
     state checks in Phase 9.3 and Phase 9.4
8. bucket write drain waiters
   - retired process-local mechanism:
     `SharedStorageNode::bucket_coordination` condition variables and local
     wakeups in bucket write reservation/drain paths, removed in Phase 10.8
   - classification: request lifecycle and write-drain coordination
   - outcome: delete/finalize progress now depends on durable bucket
     write-drain/reservation rows and restartable polling, not same-process
     waiters or wakeups
   - replacement owner: Phase 9.4 durable or PG-primary bucket write-drain
     state
9. bucket write reservation counters
   - retired mechanism: durable bucket rows with reservation fields, treated as
     runtime coordination state rather than command-owned metadata before Phase
     9.4
   - classification: write lifetime protection
   - outcome: Phase 9.4 made reservation accounting restart-safe and
     multi-process visible through `bucket_write_reservations` and
     `bucket_write_drains`; Phase 9.4.6 removed the old bucket-row counter
     fields and helper APIs
   - replacement owner: Phase 9.4 durable reservation/drain tables
10. cluster object payload leases and reclaim fences
   - current process-local mechanism:
     `LocalObjectPayloadLeaseState` lease counts, reclaim fences, and active
     reclaim set
   - classification: read/write lifetime protection
   - risk: another process can reclaim payload shards without seeing active
     readers or an in-progress reclaim fence
   - replacement owner: Phase 9.5 shard-owning storage-node read handles and
     storage-node physical delete fences. Reads remain volatile; durable
     metadata tracks reclaim, not active readers
11. node-local payload leases, reclaim queue, and bucket-finalize queue
    - current process-local mechanism:
      `SharedStorageNode::object_payload_leases`,
      `SharedStorageNode::reclaim_queue`, and the node-level acquire/release,
      reclaim enqueue, bucket-finalize enqueue, and worker wait paths
    - classification: legacy read/write lifetime protection and
      cleanup/reclaim ownership
    - risk: any production path that still uses these node-local queues or
      leases bypasses the cluster-level replacement work and cannot coordinate
      with another process
    - replacement owner: keep storage-node-local read handles as the authority
      for local shard file lifetime, remove/test-gate coordinator-local lease
      state, and route reclaim ownership through Phase 9.6 durable reclaim
      claiming
12. cluster object and bucket reclaim queues
    - current process-local mechanism:
      `LocalReclaimQueueState` plus the `argmin-reclaim` worker wait queue
    - classification: cleanup/reclaim ownership
    - risk: reclaim work can be lost on process exit, duplicated between
      workers, or missed by another process
    - replacement owner: Phase 9.6 durable reclaim claiming and retry state
13. reclaim worker lifecycle
    - current process-local mechanism: coordinator-owned reclaim worker thread
      and local wakeups
    - classification: cleanup/reclaim ownership
    - risk: correctness must not depend on one coordinator process owning the
      only live worker or receiving a same-process wakeup
    - replacement owner: Phase 9.6 durable worker claim loop
14. physical shard cleanup after metadata stops referencing payloads
    - current process-local mechanism: request-path best-effort cleanup plus
      local reclaim worker follow-up
    - classification: cleanup/reclaim ownership
    - risk: crash or persistent delete failure can leave unreferenced shard
      files that no later worker knows about
    - replacement owner: Phase 9.7 physical shard scavenger
15. bucket fast-path caches
    - current process-local mechanism:
      `CoordinatorSharedCaches`, `BucketFastPathCache`, local watcher thread,
      and process-local cache registry
    - classification: cache/freshness, with authorization correctness impact
    - risk: one process can keep serving stale policy, public-access,
      ownership, or versioning state after another process changes bucket
      metadata
    - replacement owner: Phase 9.9 PG or cluster notifications, generation
      checks, or fail-closed reloads
16. metadata digest clean-revision cache
    - current process-local mechanism:
      `PgStore::clean_metadata_digest_revision`
    - classification: durability-guarded performance and integrity fast path
    - risk: the cache may skip a digest comparison only when the durable digest
      revision still matches; it must never become a logical freshness or
      ordering source
    - replacement owner: may remain process-local if every skip is guarded by a
      durable revision check and restart validation remains fail-closed
17. lifecycle sweeper registry and worker
    - current process-local mechanism:
      `LIFECYCLE_SWEEPER_REGISTRY`, one local lifecycle worker per cache key,
      and local wakeups
    - classification: background mutation owner
    - risk: multiple processes can sweep the same lifecycle work or no process
      may resume abandoned work after restart
    - replacement owner: Phase 9.8 durable scheduled lifecycle ownership
18. EC write-state cache
    - current process-local mechanism: `SharedStorageNode::ec_write_states`
    - classification: performance/cache only
    - risk: no logical correctness dependency identified; it may remain
      process-local if cached data is immutable or recomputable
    - replacement owner: none, except normal cache invalidation if EC layout
      parameters become dynamic
19. process-global and per-instance test hooks plus raw helper paths
    - current process-local mechanism: storage and coordinator `OnceLock` test
      hook registries, `StorageCluster::test_hooks`,
      `CoordinatorSharedCaches::stream_append_test_hooks`, direct PgStore test
      seeders, and raw metadata/shard helper APIs
    - classification: test-only
    - risk: tests can keep proving a single-process shape or bypass production
      command paths
    - replacement owner: Phase 9.10 test harness de-single-process pass

Exit criteria:

1. metadata command index allocation, pending command convergence, and command
   apply serialization are owned by durable PG-primary state
2. stream append segment identity allocation cannot collide across processes
3. a second process would not be required to see local mutexes or condition
   variables to preserve correctness
4. reclaim can resume after process restart from durable rows
5. multipart complete remains serialized for one upload and one destination
6. in-flight reads are protected from physical cleanup across process boundaries
7. lifecycle work can be claimed, retried, and resumed without a process-local
   sweeper registry
8. correctness-relevant caches fail closed or refresh across process boundaries
9. unreferenced shard files left by best-effort cleanup failures are eventually
   detected and removed without consulting process-local state

## Phase 10: Local Multi-Process RPC

Move from in-process multi-node to local multi-process nodes.

This phase should not change the logical API. It should only replace local
`ShardNodeClient` and PG-primary calls with internal connections.

The first implementation should use Unix domain sockets. Remote/TLS transport,
dynamic membership, failure detection, peering, degraded availability, and
control-plane replication remain Phase 11/12 work.

Design decisions:

1. local multi-process mode uses one storage-node process per configured
   storage node
2. every storage-node process opens exactly one node data directory and only
   the PG directories assigned to that node
3. the cluster map remains static at startup and carries:
   - cluster epoch
   - node id
   - node data directory
   - node Unix socket path
   - PG ids
   - PG routes, including primary and acting set
   - EC shape
4. internal authentication is local-only for this phase:
   - sockets must be created under private runtime directories with restrictive
     permissions
   - the client must connect only to configured absolute socket paths
   - the server should check Unix peer credentials where the platform exposes
     them, or otherwise document the mode as local-development-only until the
     Phase 12 control plane introduces real internal identity
   - startup should reject obviously unsafe socket directory permissions in the
     multi-process harness
5. request routing must still be epoch-, PG-, acting-set-, and
   shard-location-fenced exactly as in the in-process multihost path
6. RPC is a transport boundary, not a new logical API; `StorageCluster` keeps
   the same public API and swaps local node calls for typed node clients
7. RPC messages must be operation-shaped, not raw `PgStore` access over the
   wire

### Phase 10.1: Codec And Frame Contract

Add a storage-internal RPC frame format using the same style as the existing
metadata command encoding:

1. fixed magic bytes and encoding version
2. request id
3. message kind
4. payload length
5. payload CRC64
6. payload bytes

The receiver must validate the frame magic, version, size limit, and transport
CRC before decoding the payload. Unknown message kinds, trailing bytes,
oversized frames, invalid enum tags, and checksum mismatch must fail closed.

The request id is for matching responses to requests, tracing, and duplicate
diagnostics; it is not by itself an idempotency key. Every side-effecting RPC
must either be naturally idempotent by operation key or carry a recoverable
operation/session token that lets the server return the already-completed
outcome after a lost reply. A successful server-side mutation followed by a
lost response must not make client retry perform the mutation a second time or
return an ambiguous success.

Required idempotence rules:

1. metadata command RPCs are keyed by canonical `MetadataCommandEnvelope`
   identity and existing command-log/pending-slot convergence rules
2. shard writes are keyed by `ShardLocation`, `ShardKey`, expected size, CRC,
   and payload bytes; retry with the same bytes returns the same ack, while
   retry with different bytes for the same key fails closed
3. shard deletes are idempotent for the same location/key and must treat
   already-missing as terminal success only when the requested delete is still
   route/epoch valid
4. read-handle acquire/read/release must run inside a long-lived per-client
   storage-node session, not as unrelated one-shot RPCs. The session owns all
   volatile read handles acquired on that connection. If the acquire response
   is lost or the client crashes before release, disconnect releases the
   session's handles. Within a live session, acquire should also be idempotent
   by a client-supplied read operation id and a canonical sorted/unique
   shard-location set, so retrying a lost acquire response returns the same
   handle set instead of acquiring a second set.
5. durable claim heartbeat/release RPCs are fenced by claim-class-specific
   durable tokens, so stale retries cannot refresh or release a newer claim.
   Bucket finalizer and lifecycle claims carry bucket/incarnation/claim/owner/
   epoch/PG identity; object payload reclaim claims additionally carry key,
   generation id, and reclaim kind. Heartbeats preserve nullable lease-deadline
   semantics rather than forcing every claim to carry a non-null deadline.
6. bucket write reservation/drain proof release RPCs are fenced by reservation
   id/proof identity and are idempotent after the proof has already reached
   terminal state

Checksum layering is required:

1. every RPC frame has a transport checksum over the exact payload bytes
2. metadata commands keep their existing canonical command checksum and command
   bytes unchanged
3. shard writes keep validating `WriteAck` size and CRC64 semantics
4. user-provided checksum metadata must remain attached to object/part/session
   records and must not be stripped or replaced by the RPC transport checksum
5. persisted command logs and metadata rows store semantic checksums, not the
   transient RPC frame checksum
6. a receiver must validate embedded semantic checksums before accepting or
   applying work when the operation carries such an item

The existing `metadata_command` binary encoding should remain the durable
metadata mutation identity. Phase 10 may factor shared primitive helpers
(`put_u*`, `read_u*`, length-prefixed bytes/strings, optional/repeated helpers)
into a private storage codec module, but RPC message kind ids and metadata
command kind ids must remain separate.

Required tests:

1. [done] frame round-trip and stable encoding tests
2. [done] rejects trailing bytes and invalid tags
3. [done] rejects bad frame checksum before payload decode, including valid
   message-kind flips
4. [done] rejects oversized frames
5. [done] metadata-command RPC rejects command bytes whose embedded checksum no
   longer matches and rejects non-canonical command bytes even when their raw
   CRC matches
6. [done] shard-write RPC rejects corrupted payload bytes even if the transport
   frame decodes cleanly but the semantic `WriteAck` expectation is wrong
7. [done] user checksum metadata survives RPC encode/decode and later
   persistence
8. [done] codec-level operation identities are explicit for shard writes,
   shard deletes, read-handle acquire, durable claim heartbeat/release, and
   bucket-write proof release. Shard write requests carry `ShardLocation`,
   `ShardKey`, expected size, expected CRC, and payload bytes; read-handle
   acquire requests carry a client read operation id and a canonical
   sorted/unique shard-location set; claim requests carry claim-class-specific
   durable tokens including object reclaim key/generation/kind where required;
   proof requests carry full durable reservation proofs.
9. [pending Phase 10.2-10.5 call-site work] response loss after a successful
   side-effecting RPC is retried safely for metadata commands, shard writes,
   shard deletes, claim heartbeat/release, and proof release. The Phase 10.1
   codec now makes the required operation keys representable, but the
   behavioral lost-reply tests belong with the first local client/server
   implementations.
10. [done] lost read-handle acquire response returns the same handle set when
    retried on the same session, lost release responses are idempotent on the
    same session, and client disconnect before release frees the handles. Phase
    10.1 carries the client operation id and shard-location set; Phase 10.3 now
    binds that key to a long-lived storage-node session and tests that later
    cleanup-style handle acquisition can proceed after disconnect.

Phase 10.1 is complete. The frame codec, semantic checksum layering, and
operation-key payload shapes are in place. The read-handle lost-reply and
disconnect guarantees are now covered by the Phase 10.3 storage-node
client/server session tests; the remaining side-effecting RPC lost-reply
guarantees stay with the later call-site phases because the codec alone cannot
observe a server-side mutation followed by response loss.

### Phase 10.2: Node Client Boundary

Introduce a storage-node client abstraction before adding sockets.

The initial implementation should have a local adapter backed by
`SharedStorageNode`. `StorageCluster` should call the client abstraction, not
reach directly into node internals, for migrated operations.

The client boundary must cover two classes of operations:

1. shard-owner operations:
   - placed shard write
   - placed shard read and ranged read
   - placed shard delete
   - shard read-handle acquire/release
   - shard scavenger file listing/audit helpers
   - local physical reclaim helpers
2. metadata PG-primary and replica operations:
   - load bucket/object metadata snapshots needed by request paths
   - allocate/install pending metadata command slots
   - apply/record metadata command log entries
   - load/compare replica command state and materialized digests
   - durable bucket write reservation/drain operations
   - durable reclaim, finalizer, lifecycle, and scavenger claim operations

The boundary must not expose `MutexGuard<PgStore>`, raw `PgStore`, or generic
SQL-shaped methods to production coordinator code.

Required tests:

1. in-process local adapter preserves current behavior
2. migrated operations still reject stale epochs, wrong PG routes, wrong
   acting-set membership, and stale shard locations
3. boundary guardrail rejects production code that directly calls raw PG access
   for migrated paths

Progress:

- Started Phase 10.2 by introducing a `StorageNodeClient` boundary with a local
  adapter backed by `SharedStorageNode`.
- Migrated shard-owner placed shard file IO, shard scavenger file listing, and
  volatile read-handle/reclaim-fence operations through the local client
  boundary.
- Tightened the storage-cluster boundary script so production direct calls to
  the migrated `SharedStorageNode` shard-owner methods fail outside the node
  client adapter.
- Migrated pending metadata command slot install/load/remove/reissue helpers and
  bucket-control pending-slot install through the local node client, keeping
  those PG locks behind operation-shaped methods.
- Extended the boundary guardrail so production pending metadata command slot
  paths cannot call the migrated raw `PgStore` APIs directly.
- Migrated generic metadata command-id allocation through the local node client,
  including the pending-slot conflict check used before command construction.
  Locked-PG allocation remains only inside storage-client builders and
  test-only helpers where the PG is already owned by the storage side.
- Migrated metadata command replica state/log hash checks, apply-and-record, and
  abandoned-log record/read through the local node client, and extended the
  guardrail to keep those command-log paths behind the boundary.
- Included `cluster/local.rs` command acceptance and open-time command-log
  convergence in that migration, so the guardrail now scans local cluster
  production code for the migrated command-log operations too.
- Migrated local-cluster open-time metadata command replay-state validation
  through the local node client. Startup still owns replica agreement and
  in-flight recovery policy, but individual PG replay validation no longer
  exposes raw `PgStore` guards to local-cluster code; the command-log guardrail
  now covers those validation calls.
- Migrated durable bucket write reservation/drain acquire, validation, release,
  drain begin/clear, and reservation-list operations through the local node
  client, including open-time reservation validation/release. Added a guardrail
  for production raw `PgMetadataStore` bucket write coordination calls.
- Migrated durable object reclaim root scanning, object reclaim load/claim
  acquire/release, bucket-delete finalizer root scan/acquire/release, and
  lifecycle sweep root/claim acquire/heartbeat/error/release through the local
  node client. Added a guardrail for production raw finalizer/reclaim/lifecycle
  worker root and claim calls in cluster code.
- Migrated single-bucket snapshot loads, reserved bucket-write snapshot loads,
  bucket execution-generation batch reads, and bucket fast-path identity reads
  through the local node client. Added a guardrail so those production
  single-bucket snapshot and fast-path reads cannot return to direct
  `SharedStorageNode` helpers from cluster code.
- Removed the earlier `StorageCluster::load_bucket_snapshot_pair` raw
  `load_bucket_snapshot_from_pg` inventory exception after adding the
  pair-shaped node-client operation for ordered same-node pair snapshots.
- Migrated lifecycle sweep bucket discovery for lifecycle-config buckets and
  aborting multipart upload buckets through the local node client, and extended
  the background-worker guardrail so production cluster code cannot call the raw
  lifecycle bucket discovery PG methods directly.
- Added node-client operations for raw bucket info/record reads and migrated
  create-bucket existence checks, bucket-delete drain/begin/finalize checks, and
  object-reclaim bucket-incarnation fencing through those methods.
- Migrated active bucket info reads, bucket subresource reads, lifecycle context
  bucket reads, and owner bucket listing through the local node client. Added a
  guardrail so production cluster code cannot call the migrated active
  bucket/subresource/list PG reads directly.
- Migrated simple object metadata helper reads for existing-live-object lookup
  and payload-reclaim existence checks through the local node client.
- Migrated object read snapshot loading to a two-step node-client protocol:
  storage returns an exact stored-object auth subject, the coordinator authorizes
  locally, then storage reloads the object and returns the snapshot only if the
  subject identity still matches. A stale subject is retried by the cluster
  wrapper so a read cannot authorize one object row and snapshot another.
- Migrated object tag reads and object-lock legal-hold/retention reads through
  operation-shaped node-client methods. Tag reads use the same auth-subject plus
  identity-validated reload model as object read snapshots so the returned tag
  row cannot drift from the authorized object row. Remaining callback-shaped
  object reads are command-construction helpers that still need
  operation-specific build/snapshot APIs.
- Migrated the generic object auth/read callback wrapper used by delete
  authorization and object ACL reads through the local node client's stored-row
  subject load, and made the old `SharedStorageNode::load_object_if` test-only
  so production cannot bypass the boundary for that read shape.
- Made the remaining old `SharedStorageNode` object-read convenience wrappers
  for live-object lookup, payload-reclaim existence, subject/snapshot loading,
  and callback-shaped read snapshots test-only. Production object-read helpers
  now have to use the `StorageNodeClient` operation-shaped methods instead of
  node-local bypasses.
- Extended the object-read boundary guardrail so production cluster code cannot
  call the internal `SharedStorageNode::*_from_object_pg` object-read helpers
  directly; those helpers are reserved for node/node-client implementation code.
- Migrated bucket pair snapshot loading through a pair-shaped node-client
  operation. Same-node pairs keep the local adapter's ordered dual-PG lock, and
  cross-node pairs load through the routed storage clients without exposing raw
  `PgStore` guards to `StorageCluster`. The boundary guardrail no longer
  permits direct `SharedStorageNode::load_bucket_snapshot_from_pg` calls from
  production cluster code.
- Migrated operation-shaped multipart upload read helpers through the local
  node client: upload lookup, in-progress upload lookup/listing, completion
  preflight/snapshot, part listing, and management lookup. Added a guardrail so
  production cluster code cannot call those `SharedStorageNode` multipart read
  helpers directly.
- Made the old `SharedStorageNode` bucket-pair and multipart-read convenience
  wrappers test-only, and added a guardrail so those bypass surfaces cannot
  become public production APIs again.
- Migrated bucket-control command construction for delete-bucket,
  put-bucket-versioning, put-bucket-acl, bucket property updates, and bucket
  subresource updates through operation-shaped node-client builders/validators.
  `StorageCluster` no longer opens bucket PGs to read raw `BucketRecord` or
  allocate bucket execution generations for those migrated command-build paths,
  and the raw bucket-row guardrail no longer carries the former command-build
  exception.
- Migrated object version-id and generation-id candidate allocation for
  reserve-version/create-generation command construction through the local node
  client, and added a guardrail so production cluster code cannot call the raw
  allocation helpers directly.
- Migrated the existing object-generation reservation lookup in
  `reserve_put_object_generation` through the local node client, so that
  reserve-generation command construction no longer opens the object PG to check
  or allocate the reservation. Added a focused guardrail for that migrated
  reserve-generation lookup.
- Migrated the post-publish live-object lookups used to build direct PUT and
  stream PUT outcomes through the local node client, removing another raw object
  PG open from the successful PUT return path.
- Migrated stream-upload session loading and stream-segment append preparation
  through the local node client. The old `SharedStorageNode` stream-session
  convenience helpers are now test-only, and the boundary guardrail covers those
  migrated stream-session read/prepare paths.
- Migrated stream-create retry matching, stream-segment listing for append/abort
  retry paths, stream-upload visibility checks during bucket delete, and
  best-effort stream-session listing through the local node client. The remaining
  raw stream-session reads were stream finalize command-construction snapshots;
  those were migrated in the next slice through operation-shaped snapshot/build
  APIs to preserve TOCTOU fences.
- Migrated stream PUT and stream upload-part finalize command construction
  through operation-shaped node-client snapshot/build APIs. The coordinator now
  authorizes a typed stream-finalize snapshot and storage revalidates that exact
  snapshot before allocating ids and building the pending metadata command, so
  stream finalization no longer opens object PGs directly for session/segment
  reads.
- Migrated object metadata update command construction for tags, object ACL,
  legal hold, and retention through a two-phase node-client API. The coordinator
  authorizes against a stored-object snapshot, and the storage node reloads the
  same stored row before allocating the command id and building the
  `PutObjectMetadata` command. The guardrail now blocks raw object-row loads and
  locked-PG command-id allocation inside that cluster helper.
- Migrated direct delete command construction for specific-version deletes,
  current-object deletes, and explicit delete-marker insertion through
  node-client snapshot/build APIs. The cluster now runs delete precondition
  callbacks against a typed current/specific object snapshot, while the storage
  node reloads the same row before computing reclaim targets, write sequence, and
  metadata command id. The guardrail covers those migrated delete helpers.
- Migrated lifecycle noncurrent-version expiry and expired-delete-marker cleanup
  off raw object-PG version scans. Lifecycle selection now runs against a
  node-client version snapshot, then storage reloads the exact selected version
  and validates the full version-list identity before building the delete
  command, because eligibility can depend on neighboring versions. If the list
  changed, lifecycle defers the candidate to a later sweep rather than retrying
  immediately and competing with foreground writes. The guardrail now blocks raw
  `list_object_versions_for_key` and command-id allocation in those migrated
  lifecycle cleanup helpers.
- Migrated lifecycle current-object expiry command construction through the
  local node client. The coordinator now selects expiry from a current-object
  snapshot, then storage reloads the same current row before building either the
  delete command or versioning/suspended delete-marker command; suspended
  delete-marker stale-payload reclaim snapshots are computed under that
  revalidated storage-side PG lock. The guardrail now covers raw current-object
  lifecycle reads and command-id allocation.
- Migrated stream-session and multipart-upload create command construction
  through the local node client. PutObject stream and MPU create paths authorize
  against current-object snapshots, then storage reloads the same snapshot
  before allocating ids and building the command; upload-part stream create
  reloads the authorized multipart upload row before building
  `CreateStreamUpload`. The guardrail now blocks raw object/MPU reads, raw
  command-id/generation allocation, and raw create-command construction in those
  migrated create helpers.
- Migrated create-bucket command construction through the local node client.
  The storage-side builder rechecks bucket absence under the bucket-PG lock
  before allocating the execution generation and building `CreateBucket`, so the
  cluster no longer opens the bucket PG for the create-command path. The
  guardrail now blocks raw bucket-PG opens, execution-generation allocation, and
  raw `CreateBucketCommand::from_config` construction in
  `create_bucket_with_config_and_load_info`.
- Migrated completed-multipart order reservation command construction through
  the local node client. The storage-side builder reads and bounds-checks the
  bucket completion sequence under the bucket-PG lock and returns the
  `AdvanceCompletedMultipartUploadSequence` command with its reserved order; the
  cluster keeps the pending/apply retry loop but no longer opens the bucket PG
  for sequence allocation. The guardrail covers that migrated reservation
  helper.
- Migrated multipart abort command construction through the local node client.
  The storage-side builders snapshot abort cleanup and allocate the metadata
  command id under the object-PG lock for both direct and authorized aborts; the
  cluster retains proof acquisition, pending install, drain, and apply behavior
  without opening the object PG for abort-command construction. The guardrail
  covers the migrated abort-command helper paths.
- Migrated finalized bucket deletion fanout through the local node client. Each
  acting node now deletes the finalized bucket and refreshes its metadata digest
  behind the node-client boundary, while `StorageCluster` retains the primary
  missing-bucket outcome and replica-missing tolerance. The guardrail blocks raw
  finalized-bucket delete PG access in `delete_bucket_from_acting_set`.
- Migrated multipart completion command construction through the local node
  client. The cluster still owns the durable bucket-write proof, command-stream
  version reservation, completed-MPU order reservation, pending install, and
  apply loop; the storage-side builder now revalidates the upload row under the
  object-PG lock, reloads each selected part row to fence UploadPart replacement
  races, snapshots omitted parts/stream cleanup/stale null-version payload,
  allocates the object write sequence and command id, and returns the
  `CommitMultipartObject` envelope. A stale selected part row makes the
  coordinator retry the normal completion validation once, so a real replacement
  returns the normal `InvalidPart` response rather than publishing stale
  payload. The guardrail blocks raw multipart completion object-PG reads and
  direct command construction in `complete_multipart_upload_commit_serialized`.
- Migrated direct PUT commit command construction through the local node
  client. The cluster still owns written-shard ack registration/validation,
  command-stream version reservation, pending install, and apply behavior; the
  storage-side builder now reloads the authorized direct-PUT commit snapshot
  under the object-PG lock before computing stale payload reclaim, write
  sequence, and metadata command id. If the current object changes between
  precondition evaluation and command build, the coordinator reruns the normal
  precondition callback boundedly using the shared client-facing stale-snapshot
  retry budget, so conditional PUT does not publish a command based on stale
  current-object state. The guardrail blocks raw direct-PUT
  object-PG reads and direct `CommitDirectPutObject` construction in
  `commit_direct_put_object_from_payload_shards`.
- Migrated payload shard ack metadata operations through the local node client.
  The cluster still computes placement and validates shard-file contents, but
  registering, validating, loading, and deleting per-shard `WriteAck` rows now
  happen behind storage-client methods on the routed metadata-PG primary. The
  guardrail blocks raw shard ack row operations from production cluster code.
- Migrated completed multipart upload tombstone scans through the local node
  client. The cluster still merges and conflict-checks acting-node results for
  bucket delete and tombstone pruning, but individual per-PG tombstone reads now
  run behind storage-client methods. The guardrail blocks raw completed-MPU
  tombstone scans from production cluster code.
- Migrated shard scavenger audit metadata through the local node client. The
  cluster still computes cross-node file/row/reference comparisons, but shard
  row scans, durable payload-reference scans, and observation record/list/resolve
  calls now run behind storage-client methods on the routed PG primary. The
  guardrail blocks raw shard scavenger metadata calls from production cluster
  code.
- Migrated lifecycle MPU abort upload reload through the local node client. The
  lifecycle path still owns bucket/object lock ordering, pending-command drain,
  lifecycle predicate evaluation, and abort command publishing, but the upload
  row used for `should_abort` now comes from an operation-shaped storage-client
  read instead of direct object-PG access. The guardrail blocks raw upload row
  reloads in `abort_multipart_upload_if_due`.
- Migrated bucket listing page reads through the local node client. The cluster
  still owns cross-PG merge, delimiter/common-prefix handling, record caps, and
  delete-bucket emptiness policy, but object, object-version, and multipart
  upload pages now load through storage-client methods on each routed PG
  primary. The guardrail blocks raw bucket listing PG reads from production
  cluster code.
- Migrated the remaining cluster-side metadata command-id allocation through the
  local node client. Bucket/object command paths still own retry/drain policy,
  but max-log and pending-slot checks for fresh command ids now happen behind
  storage-client methods; the boundary guardrail no longer carries the
  cluster-side allocator exception.
- Migrated remaining production cluster topology, local lock, and EC
  shard-encoding use cases behind `LocalClusterMap` helpers. PG id
  derivation, default EC shape, EC scratch-pool-backed encoding,
  bucket/object-scoped local locks, and multipart completion contention locks
  are no longer reached through ad hoc `SharedStorageNode` calls from
  `StorageCluster`/`request_ops`; the local map is the only in-process
  implementation boundary for those local-only operations. Bucket coordination
  notifications were later removed entirely in Phase 10.8 after durable
  drain/reservation polling became the request lifecycle mechanism.
  The boundary guardrail now rejects production cluster code that calls direct
  node `get_pg`, `lock_bucket`, `lock_multipart_completion_bucket`, or
  `notify_bucket_coordination_change` helpers outside cfg-gated test hooks.
- Reviewed remaining direct test/helper use. The retained raw test hooks are
  state assertions, lock probes, crash-state seeders, lifecycle-claim seeders,
  or deliberately divergent replica/local-state setup; production-shaped request
  tests should continue to enter through coordinator or `StorageCluster` methods,
  and new raw helper usage needs the same exception shape rather than becoming a
  second production path.
- Status: complete. Phase 10.2 now routes the production storage-cluster
  operations covered by the phase through `StorageNodeClient` or `LocalClusterMap`
  helper boundaries, with guardrails for the migrated raw PG/node access shapes.
  Closeout keeps the local `s3-tests` embedded server on a single PG with
  automatic lifecycle and reclaim sweepers enabled, so the end-to-end harness
  continues to exercise same-PG pressure instead of avoiding it through topology
  fanout. Bucket-delete convergence was fixed directly: bucket finalization
  drains unleased durable payload-reclaim roots while holding the durable
  finalizer claim, so same-name bucket reuse does not depend solely on
  background reclaim worker scheduling, and the trace models cover the inline
  reclaim plus follow-on finalizer hint behavior. The direct PUT
  suspended-versioning reclaim
  edge case was fixed so replacing an existing null version under a numbered
  current version reclaims the old null payload without reclaiming the numbered
  current payload. Terminal bucket finalization now also clears the durable
  finalizer claim, accepting the bucket-row delete's FK-cascaded claim removal
  as a successful clear so later same-PG bucket finalizers are not held until
  lease expiry. Lifecycle root discovery now surfaces non-expired lifecycle
  claims as busy roots, and the local deterministic lifecycle sweep hook waits
  boundedly for those production background-sweeper claims to clear instead of
  disabling the sweeper or treating a busy claim as successful progress.
  Verification on 2026-05-27 included
  `cargo test --all-targets --all-features`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `./scripts/check-storage-cluster-boundaries`, and `git diff --check`; all
  passed.

### Phase 10.3: Unix Socket Storage-Node Server

Add an explicit process-role model before moving traffic over sockets. A
deployment may run frontend-only, storage-only, or combined frontend+storage
processes, but storage ownership remains one storage-node identity per storage
data directory:

1. `frontend`: runs the HTTP/coordinator service and talks to storage nodes
   through `StorageNodeClient`
2. `storage-node`: opens exactly one storage-node identity, one data directory,
   its configured PG set, and one Unix socket
3. `combined`: runs both roles in one OS process for small deployments or local
   development, but still exposes the storage-node RPC listener and preserves
   the same node-id/data-dir invariants

Phase 10.3 only makes the `storage-node` role executable. The `frontend` and
`combined` roles are parsed as part of the process-role model, but must fail
with an explicit unsupported-role error before opening PG directories until
Phase 10.4/10.5 add remote `StorageNodeClient` routing. Starting `combined`
against the current local frontend would reopen the same node data directory
through in-process `SharedStorageNode` handles, so it would not prove the socket
boundary or the one-owner storage invariant.

Most production hosts with multiple disks should run multiple storage-node
processes on the same host, one per disk/data directory. This keeps disk
failure, SQLite/shard-directory failures, process crashes, process locks,
placement identity, metrics, and logs aligned with a single `NodeId`. A failed
disk process can crash or restart without taking down other storage-node
processes on the same host. Phase 10.3 should therefore implement one
storage-node identity per storage process; multi-node-per-process hosting can
be added later only if it preserves the same per-node data-dir, socket, PG, and
failure-domain boundaries.

Add a storage-node process mode that:

1. opens the configured node data directory
2. opens only configured PG directories
3. listens on the configured Unix socket
4. serves a length-delimited stream of request/response pairs on a long-lived
   connection; one-shot exchanges may be used for stateless operations, but
   read-handle acquire/read/release must use the long-lived session model
5. runs blocking storage work off the async accept loop if using async sockets
6. rejects requests for unknown node id, unknown PG, wrong cluster epoch,
   inactive PG route, stale shard location, or non-acting-set access
7. returns typed storage errors that preserve enough context for caller-side
   fail-closed behavior and tests

The server must not share a PG directory or node data directory with another
process. Startup must reject duplicate node ids and duplicate/canonical-equal
data directories in the static config. It must also reject duplicate or
canonical-equivalent Unix socket paths before serving, so two node ids cannot
advertise the same endpoint or fail later during bind/connect. Each
storage-node process must take an exclusive ownership lock for its node data
directory before opening PGs. Frontend-only processes must not open PG
directories.

The multi-process harness must create private socket directories. Tests should
cover rejection of an incorrectly permissioned socket directory, or explicitly
mark the configuration as non-production-only if a platform cannot enforce
peer credentials or directory permissions.

Implementation slices:

1. add RPC response framing, typed error responses, blocking Unix stream
   read/write helpers, and a health/version request
2. add process role and storage-node config parsing for node id, cluster epoch,
   data directory, configured PG ids, socket path, and route/topology validation
   data. The first static config surface uses
   `ARGMIN_STORAGE_CLUSTER_EPOCH` for the advertised topology epoch and
   `ARGMIN_STORAGE_PG_IDS` for the comma-separated PG subset a storage-node
   process opens; absent `ARGMIN_STORAGE_PG_IDS` defaults to all PGs in
   `0..ARGMIN_PG_COUNT`.
3. add socket directory validation and storage data-directory ownership locks
4. add the storage-node listener/accept loop and per-connection session state
5. add route validation for wrong node id, unknown PG, wrong cluster epoch,
   inactive PG route, stale shard location, and non-acting-set access
6. add session-owned read-handle acquire/release idempotency keyed by client
   read-operation id plus canonical shard-location set; release retries after a
   lost response must be accepted on the same session and must not leave handle
   counts wrong
7. wire the storage-node process role into the binary without changing
   production coordinator request routing yet; `frontend` and `combined`
   remain explicit unsupported roles until Phase 10.4/10.5 move actual shard
   and metadata traffic through remote clients

Required tests:

1. node process starts and answers a health/version request
2. node process rejects unknown PG and wrong node id
3. node process rejects stale epoch and stale shard location
4. duplicate data directories are rejected before serving
5. restart reopens existing metadata and shard files cleanly
6. incorrectly permissioned socket directories are rejected or reported as
   non-production-only
7. frontend-only and combined roles fail explicitly before opening PG
   directories while remote frontend routing is not wired
8. combined role startup is deferred until remote frontend routing is available
   in Phase 10.4/10.5; 10.3 requires combined to fail explicitly before opening
   PG directories
9. second storage-node process for the same data directory is rejected by the
   ownership lock
10. duplicate/canonical-equivalent Unix socket paths are rejected before serving
11. lost read-handle acquire response retried on the same session returns the
    same handle set without increasing lease counts
12. lost read-handle release response retried on the same session succeeds
    idempotently and does not leave handle counts wrong
13. client disconnect after acquiring read handles releases all session-owned
    handles and later physical cleanup can proceed

- Status: complete. Phase 10.3 now has a runnable `storage-node` process role
  with explicit static node id, data directory, Unix socket path, cluster epoch,
  and configured PG subset parsing. Startup validates socket privacy,
  absolute/canonical socket uniqueness, duplicate node ids, duplicate/canonical
  data directories, complete route coverage, and consistent per-PG route views;
  bind-time ownership locks reject a second process on the same node data
  directory and stale Unix socket path cleanup is covered. The blocking Unix
  socket server accepts long-lived sessions without serializing the accept loop,
  answers health/version, returns typed errors for route/epoch/node failures,
  enforces read-handle resource limits, and covers lost acquire/release response
  idempotency plus disconnect cleanup. `frontend` and `combined` roles remain
  parsed but explicitly unsupported until Phase 10.4/10.5 route actual
  coordinator storage traffic through remote `StorageNodeClient`s.

### Phase 10.4: Remote Shard IO

Migrate placed shard IO to the node-client boundary first because it is already
location-routed and owned by the shard storage node. This slice must not create
a transition where shard files are remote but the data-PG ack rows are still
written through a shared local PG directory.

Phase 10.4 must include the minimal remote data-PG ack-row API needed for
publishing safety. A file-only remote shard transport is not sufficient because
it would prove that shard bytes moved while metadata publication still depends
on shared local PG access. The required fence is:

1. remote shard writes return a `WriteAck`
2. the authoritative data-PG owner records the exact `(ShardKey, WriteAck)` row
   through RPC
3. publish validation reads/validates those exact ack rows through the
   node-client boundary before metadata is published
4. a remote shard file without its matching durable ack row is not publishable

RPC messages should be operation-specific, for example:

1. write shard at `ShardLocation`
2. read shard at `ShardLocation`
3. read shard range at `ShardLocation`
4. delete shard at `ShardLocation`
5. acquire/release volatile read handles for one or more shard locations
6. record a batch of data-PG shard ack rows
7. validate a batch of data-PG shard ack rows
8. list/audit shard files for the scavenger

Large shard payloads must still be checksummed in transport. The shard write
path must also preserve the existing semantic size/CRC validation used to build
or verify `WriteAck`. Data-PG ack recording must be exact-idempotent for lost
responses: missing row inserts; existing identical row succeeds; existing
mismatched row fails closed and must not overwrite. The existing local
`INSERT OR REPLACE` behavior is acceptable for internal helper use only if the
remote-facing API wraps it with this stricter check or replaces it with an
exact-idempotent store primitive.

Publishing metadata after shard IO requires a process-boundary fence:

1. every acknowledged shard must have a durable data-PG ack row on the
   authoritative data-PG owner
2. publish validation must compare the expected shard set against those ack
   rows and their size/CRC before installing metadata
3. a remote shard file without its matching ack row is not publishable
4. a stale or wrong-node ack row must not satisfy publish validation

Implementation slices:

1. add storage RPC payload codecs for shard read/write/delete and batch
   ack-record/ack-validate requests, including payload-size caps before
   allocation and semantic checksum validation for shard writes
2. add storage-node server dispatch for shard file IO plus data-PG ack
   record/validate, reusing the Phase 10.3 route checks for node id, PG
   configured locally, cluster epoch, active PG state, and acting-set membership
3. add exact-idempotent data-PG ack-row recording to the storage store/client
   boundary; retrying the same ack batch after a lost response must observe
   success, while a different ack for an existing shard row must fail closed
4. add a Unix-socket `StorageNodeClient` implementation for one-shot shard file
   and ack-row RPCs, plus long-lived read-handle sessions for acquire/release
5. extend cluster construction/config so a frontend can build storage clients
   from node-id-to-socket routing without opening storage-node data
   directories; `storage-node` processes remain the only owners of their PG
   directories
6. migrate direct PUT as the first publishing path: write remote shard files,
   record the data-PG ack batch on the authoritative data-PG owner, validate the
   exact expected shard set, then publish metadata
7. broaden reads and cleanup to the remote client path: reads acquire/release
   storage-node read handles over RPC, and physical delete/reclaim fails closed
   while a remote read handle is active

Required tests:

1. direct PUT writes shards through remote node processes and publishes metadata
   only after all shard acks validate
2. reads acquire storage-node read handles through RPC and release them on
   success and error
3. partial multi-shard handle acquisition failure releases already-acquired
   remote handles
4. physical delete waits/fails closed while a remote storage-node read handle
   is active
5. corrupted shard RPC payloads are rejected and do not publish metadata
6. killing a shard-owner process during read/write returns a clear fail-closed
   error
7. remote shard write succeeds but the `ShardWrite` response is lost; retrying
   the same `(ShardLocation, ShardKey, payload checksum)` returns the same
   `WriteAck`, while retrying the same key with different bytes fails closed
   and does not overwrite the existing shard file or ack
8. remote shard delete succeeds but the `ShardDelete` response is lost; retrying
   the same delete under the same valid route/epoch treats an already-missing
   shard as terminal success, while stale epoch, wrong node, or wrong route
   still fails closed before treating absence as success
9. remote shard file write succeeds but ack-row RPC response is lost; retry
   records or observes the same ack row and publish validation remains exact
10. retrying ack-row record with a mismatched `WriteAck` for an existing shard
   row fails closed and does not overwrite the original row
11. remote shard file exists without a durable ack row and metadata publication
   fails closed
12. wrong-node or stale data-PG ack rows do not satisfy publish validation
13. frontend-only remote mode does not open storage-node PG directories
14. lost read-handle acquire response is retried on the same session and returns
    the original handle set without increasing lease counts
15. client process or connection dies after acquiring read handles and before
    release; the storage node releases the session-owned handles and physical
    cleanup can later proceed

- Status: shard-remote scope complete. Phase 10.4 now has storage RPC codecs,
  request payload caps, and storage-node dispatch for shard write/read/range
  read/delete, read-handle acquire/release, data-PG ack record/validate/load/delete,
  and shard-scavenger file listing. Direct PUT writes shards through Unix
  storage-node clients, records exact-idempotent ack rows on the authoritative
  data-PG owner, validates those rows before metadata publication, and refuses
  remote shard files without matching durable ack rows. Reads acquire
  storage-node read handles over RPC, direct segment reads acquire the full
  data-shard handle set before reading, partial multi-node handle acquisition
  releases already-acquired handles, disconnect cleanup releases session-owned
  handles, and physical delete fails closed while read handles are active.
  Shard write/delete lost-response idempotency, corrupted shard payload
  rejection, wrong-node/stale-route failures, stale/wrong ack rows, remote owner
  unavailability, and shard-scavenger remote file scans are covered. The
  10.4-specific frontend construction proof is shard-scoped: a frontend map can
  install Unix shard clients and write to storage-node-owned data directories
  while those storage-node processes already own their data-dir locks. Full
  `frontend` and `combined` process roles remain explicitly unsupported until
  Phase 10.5 routes metadata PG-primary and replica operations over RPC.

### Phase 10.5: Remote Metadata PG Operations

Migrate metadata PG-primary and acting-set replica operations after shard IO.

Status: metadata-command RPCs and the dedicated `MetadataCommandNodeClient`
boundary are in place. The first non-command metadata RPC surface is also in
place for create-bucket: bucket raw/info reads and create-bucket command
construction now route through a dedicated bucket metadata client, and a
frontend map can create a bucket through a storage-node-owned Unix bucket
metadata client plus the remote metadata-command client. Object generation
reservation lookup and next-generation allocation now route through a dedicated
object-generation metadata client, so direct PUT generation reservation can be
driven from a frontend through the storage-node-owned object PG. Generation
reservation now rereads the storage-node-owned allocator before final command-id
allocation and treats exact stale-generation apply conflicts as retryable, so
two frontend handles racing on the same key converge to distinct generations
instead of surfacing an internal uniqueness failure. Object version allocation
now routes through a dedicated object-version metadata client, so
`ReserveObjectVersion` command construction no longer reads the broad storage
client surface and frontend maps can reserve version ids on the storage-node
object PG. Durable bucket-write reservation acquire, proof validation, exact
record release, and command-owned proof release now route through a dedicated
bucket-write reservation client/RPC surface; frontend maps can acquire and
release reservation rows on the storage-node-owned bucket PG without writing the
frontend-local PG. Remote reservation acquire preserves the typed
`BucketWriteDraining` outcome, so callers keep the existing wait/retry behavior
when DeleteBucket has installed a drain. Bucket snapshot loads now route
through the same dedicated bucket metadata client/RPC surface, including
reserved bucket-write snapshot loads and same-node pair snapshot loads for
distinct buckets; frontend maps can load requested bucket subresources from the
storage-node-owned bucket PG without reading the frontend-local PG. Direct PUT
commit snapshot loads now route through a dedicated direct PUT metadata
client/RPC surface, so the precondition/auth snapshot is read from the
storage-node-owned object PG. Direct PUT commit command construction also now
routes through that dedicated RPC surface, preserving typed stale-snapshot
retries and validating the returned command identity before the frontend
publishes it. Completed-multipart order allocation now builds
`AdvanceCompletedMultipartUploadSequence` commands through the bucket metadata
RPC boundary as well, validating returned command id, bucket, and order
identity before publishing. Object read auth-subject loads and subject-checked
snapshot reloads now route through a dedicated object-read metadata client/RPC
surface, preserving typed not-found and stale-subject outcomes for the existing
bounded retry loop. Object tag reads now use the same dedicated object-read
metadata RPC boundary for the subject load and subject-checked tag reload, and
legal-hold/retention reads use the shared object-read auth-subject RPC instead
of the broad storage-client surface. PUT-object-metadata, object delete,
delete-marker insertion, and lifecycle noncurrent/delete-marker cleanup now
route object mutation snapshot loads, version-list reads, and command builders
through a dedicated object-mutation metadata RPC surface. Delete snapshots carry
the expected delete target, and Unix clients validate returned delete/reclaim
payload identity before publishing the command. Stream upload session creation
and multipart upload initiation now use the same object-mutation metadata RPC
surface for retry matching and command construction; Unix clients validate the
returned session/upload identity and bucket-write proof before publishing.
Stream PUT finalization and upload-part stream finalization now also route
their storage snapshot loads and command builders through the object-mutation
metadata RPC boundary; Unix clients preserve the typed stale-finalize-snapshot
retry outcome and validate the returned command route, object/session/upload
identity, staged segment rows, multipart part rows, and bucket-write proof
before publishing. Multipart completion and abort command builders now use that
same object-mutation metadata RPC boundary; Unix clients preserve typed stale
completion snapshots and validate returned completion/abort command identity
before publication. Broader multipart upload metadata read/list/management
helpers now also use the object-mutation metadata RPC boundary, so
authorization, completion preflight/snapshot, part listing, and management
lookups run against the storage-node-owned object PG. Stream upload session
loads, staged-segment listing, and stream segment append preparation now also
route through the object-mutation metadata RPC boundary; Unix clients preserve
typed stream-session-not-found outcomes and validate returned session/segment
identity before the frontend writes staged payload shards. Direct PUT and
stream PUT finalization outcomes now derive live-object tags, size, and
last-modified data from the installed/applied command instead of reloading the
post-commit object through the broad storage client, and the residual
live-object helper routes through the object-read metadata RPC boundary.
Bucket-delete payload reclaim root checks now route through the
object-mutation metadata RPC boundary, so delete finalization waits on
storage-node-owned object PG reclaim roots instead of frontend-local object PG
state. Durable object payload reclaim root discovery, per-object reclaim
loading, and reclaim claim acquire/release now use the same object-mutation
metadata RPC boundary, so reclaim cleanup metadata is coordinated on the
storage-node-owned object PG before the worker publishes the reclaim-delete
command.
Bucket-head reads used by bucket property/subresource writers, delete begin,
finalization checks, drain waits, and reclaim-incarnation lookup now use the
dedicated bucket metadata RPC boundary. Durable bucket write-drain existence,
acquire, clear, clear-expired, reservation listing, bucket-delete finalizer claim
acquire/release, finalize-root scans, and finalized-bucket deletion now route
through the bucket write coordination RPC boundary. Lifecycle sweep bucket/root
discovery and durable lifecycle claim acquire/heartbeat/error/release now use
that same bucket write coordination RPC boundary, so lifecycle sweeper
coordination runs against the storage-node-owned bucket PG. Mark-bucket-deleting
pending-command matching and command construction now route through the bucket
metadata RPC boundary, preserving the distinct AlreadyDeleting outcome. Bucket
versioning, ACL, property, and subresource command matching/building now route
through a bucket metadata control RPC, and subresource reads use the bucket
metadata RPC boundary. Public object listing page reads (`ListObjects`, object versions,
and multipart upload listing) now route through a dedicated object-listing
metadata RPC boundary, and bucket-delete emptiness checks use the same listing
client instead of the broad storage-client surface. Owner bucket listing,
bucket execution-generation batch reads, and bucket fast-path identity batch
reads now route through the dedicated bucket metadata RPC boundary with
response identity validation, so frontend maps no longer need frontend-local
bucket PG reads for these public/cache-freshness paths.
The early `frontend`/`combined` unsupported-role gate has been removed for the
Phase 10.5 request path. Frontend roles now require
`ARGMIN_STORAGE_NODE_SOCKETS` as a complete `node_id=/absolute/socket` map,
build a topology-only `LocalClusterMap` at the configured cluster epoch, and
install the full Unix storage-node client set for shard IO, shard acks, read
handles, metadata commands, bucket metadata/coordination, object
read/listing/mutation metadata, direct PUT metadata, and generation/version
allocation. Combined mode binds its configured storage-node listener first,
requires its own socket-map entry to match `ARGMIN_STORAGE_NODE_SOCKET_PATH`,
then starts the HTTP/coordinator frontend over the same socket client boundary.
The topology-only frontend map intentionally opens no local PG stores and skips
local metadata-command replay validation because the storage-node-owned PG is
the command-stream authority.
Remote frontend and combined coordinators initially keep object reclaim, bucket
finalization, and lifecycle workers disabled until Phase 10.6 routes those
worker metadata surfaces through the same RPC boundary and adds restart/resume
coverage. Shard-scavenger startup is owned by Phase 10.6 once its dedicated RPC
boundary is routed and tested.

Phase 10.5 is complete for the frontend/combined request-path RPC routing that
the current cluster-map shape can express. Background worker metadata access is
intentionally deferred to Phase 10.6, and full multihost end-to-end process
harness coverage is deferred to Phase 10.7 rather than adding a narrow smoke
test here.

Metadata command bytes should be reused directly inside RPC messages for
command install/apply/convergence operations. The RPC envelope routes the
request; the embedded `MetadataCommandEnvelope` remains the durable mutation
identity.

Every receiver must validate:

1. route epoch matches the RPC route and embedded command id
2. PG id matches the RPC route and embedded command id
3. log index is valid for the PG-primary or replica state transition
4. command checksum matches canonical command bytes
5. replica command-log and digest invariants still hold before returning
   success

Metadata reads and coordination operations get separate RPC messages rather
than being encoded as metadata commands, because they are not durable metadata
mutations.

Implementation slices:

1. add storage RPC payload codecs for metadata-command envelopes and metadata
   command state queries. The command RPC payload should carry canonical
   `MetadataCommandEnvelope` bytes plus route fields; decoding must reject
   corrupted transport bytes, non-canonical command bytes, stale embedded
   command checksums, mismatched route epoch, mismatched PG id, and invalid log
   indexes before calling any store mutation.
2. add storage-node server dispatch for metadata PG-primary and acting-set
   replica operations, reusing Phase 10.3 route checks for node id, PG
   configured locally, cluster epoch, active PG state, and acting-set
   membership. The receiver must additionally validate that the embedded
   command id matches the RPC route. `StorageNodePgRoute` carries an explicit
   primary node id separate from acting-set order; PG-primary-only operations
   such as bucket write proof release must validate against that field, not
   `acting_set[0]`.
3. split the current broad local metadata-command surface into a dedicated
   node-client trait, for example `MetadataCommandNodeClient`, covering:
   pending command slot insert/replace/read, metadata command acceptance,
   abandon acceptance, apply-and-record, record-abandoned, replica state,
   max-log-index, applied log-entry hash lookup, and matching-applied lookup.
   `LocalStorageNodeClient` remains the local implementation; the Unix client
   implements the same trait over RPC.
4. replace process-local PG command serialization with a storage-node-owned
   remote serialization boundary. The PG-primary storage node, not an
   individual frontend process, must own the command install/reissue/fanout
   critical section that Phase 9 previously protected with process-local
   locks. Multiple frontend processes must be able to race on the same PG and
   converge through the storage-node-owned pending slot, log index allocator,
   primary-last fanout ordering, and reissue logic without observing each
   other's half-complete in-process state.
5. add separate RPC surfaces for non-command metadata reads and coordination
   operations required before command construction. Bucket raw/info reads,
   bucket snapshot reads, object generation/version allocation, bucket-write
   reservations, proof release, direct PUT commit snapshot loads, and direct
   PUT commit command construction now have dedicated node-client/RPC surfaces.
   Completed-multipart order allocation also now routes through the bucket
   metadata RPC boundary. Object-read auth-subject/snapshot helpers now route
   through a dedicated object-read metadata RPC surface, including
   subject-checked object tag reads and legal-hold/retention auth-subject
   loads. Object mutation metadata helpers now route PUT-object-metadata,
   object delete, delete-marker insertion, lifecycle version-list reads, and
   lifecycle delete command construction through a dedicated RPC surface with
   delete-target response validation. Stream upload session creation and
   multipart upload initiation builders now route through the object-mutation
   metadata RPC boundary with request/proof response validation. Stream
   PUT/part finalization snapshot loads and command builders now use that same
   boundary, preserving typed stale-snapshot retries and validating returned
   stream commit/part command identity before publication. Multipart completion
   and abort command builders now also use the object-mutation metadata RPC
   boundary, preserving stale-completion retry outcomes and validating returned
   completion/abort command identity before publication. Stream upload
   session loads, staged-segment listing, PG-wide stream-upload listing for
   bucket delete/best-effort cleanup, completed-MPU tombstone listing for
   bucket-delete cleanup, bucket-delete payload reclaim root checks, durable
   object payload reclaim root/load/claim operations, and stream-segment append
   preparation now use the same object-mutation metadata RPC boundary. Data-PG
   shard ack load/delete now use the dedicated shard-ack RPC client instead of
   the broad storage-client surface for read recovery and cleanup.
   Multipart upload
   read/list/management helpers now use the same object-mutation metadata RPC
   boundary and validate returned upload, completion snapshot, part list, and
   management lookup identities. Bucket property/subresource command
   matching/builders and reads use dedicated bucket metadata/control RPC
   surfaces. Durable bucket write-drain, finalizer, and lifecycle sweep
   coordination checks now use the bucket write coordination RPC boundary.
6. migrate command construction and convergence helpers in `StorageCluster` to
   the new metadata-command client boundary. Start with bucket-PG command
   install/apply for `CreateBucket`, then direct PUT object-generation
   reserve/commit on object PGs, then broaden to stream PUT, multipart,
   delete-marker/object-delete, bucket versioning/ACL/properties/subresources,
   and completed-multipart bucket commands.
7. preserve command-log and digest invariants across RPC by keeping the
   existing `PgStore` apply/acceptance methods as the storage-node authority.
   The RPC layer may route and validate envelopes, but must not bypass
   `metadata_command_replica_state`, command-log hash-chain checks,
   digest-revision checks, duplicate retry handling, or conflict detection.
8. make side-effecting metadata RPCs exact-idempotent for lost replies. If the
   server commits a pending-slot install/replace, apply-and-record,
   abandoned-record insert, exact pending-slot removal, bucket reservation/proof
   release, or other cleanup mutation but the response is lost, retrying the
   same operation identity must observe success; retrying a different identity
   for the same durable row must fail closed.
9. after create-bucket, direct PUT metadata traffic, required non-command
   metadata reads/coordination, and storage-node-owned PG serialization are
   genuinely remote, replace the early `frontend`/`combined` unsupported-role
   failure with frontend cluster construction from static node-id-to-socket
   metadata and shard routing. Combined mode can then start both the
   HTTP/coordinator server and the local storage-node listener in one process
   while still using the same client boundary.

Required tests:

1. create bucket through remote PG-primary state
2. direct PUT metadata reserve/commit through remote PG-primary and replicas
3. pending command survives storage reopen/restart-shaped recovery and is
   converged by a later request; the full OS storage-node process restart
   version belongs to the Phase 10.7 harness
4. command-log conflict and digest mismatch fail closed across RPC
5. corrupted command bytes in transport are rejected before apply
6. command bytes with matching transport checksum but stale embedded command
   checksum are rejected
7. two independent frontend handles/maps converge/reissue the same PG while one
   command is mid-fanout in focused storage/RPC tests; the storage-node-owned
   serialization boundary must preserve primary-last ordering and prevent
   duplicate or divergent command application, and the separate-OS-process
   version belongs to the Phase 10.7 harness
8. non-command metadata RPCs used by create-bucket/direct PUT run remotely:
   bucket raw/info and bucket snapshot reads, object read snapshot and
   tag/legal-hold/retention subject reads, generation/version/order allocation,
   bucket-write reservation/proof release, stream-upload and multipart-upload
   initiation command builders, public object/version/multipart-upload listing
   pages, owner bucket listing, bucket execution-generation and fast-path
   identity batch reads, bucket-delete payload reclaim root scans, object
   payload reclaim root/load/claim operations, durable pending/drain checks,
   and remaining operation-shaped command builders. Proof
   release must reject the correct PG on a non-primary node even when that node
   appears first in the acting-set route list.
9. two independent frontend maps over one storage-node must handle stale
   object-generation selection: the loser may read generation `N`, the winner
   publishes `ReserveObjectGeneration(N)`, and the loser must retry to `N+1`
   without returning an internal apply/uniqueness error.
10. lost replies are retry-safe for pending-slot install/replace,
   `apply_metadata_command_and_record`, abandoned-record insert, exact
   pending-slot removal, bucket reservation/proof release, and cleanup
   mutations; same identity succeeds and mismatched identity fails closed
11. `frontend` and `combined` roles parse and build from a complete static
   `ARGMIN_STORAGE_NODE_SOCKETS` map; missing, relative, byte-duplicate,
   canonical-equivalent duplicate, incomplete, or combined-self-mismatched
   socket entries fail at config/build time, and the topology-only frontend map
   uses the configured cluster epoch without validating local PG replay state.
  Remote frontend workers may start once each worker's RPC
  boundary is routed and covered; full separate-process frontend/combined
  startup and HTTP S3 coverage belongs to the Phase 10.7 harness.

### Phase 10.6: Background Workers Across RPC (complete)

Route background workers through the same node-client boundary:

1. object payload reclaim
2. bucket delete finalization
3. lifecycle sweep claiming and mutation
4. shard scavenger audit

Workers must not rely on same-process wakeups. Local wakeups can remain as an
optimization, but progress must be restartable from durable rows and periodic
polling.

Required tests:

1. reclaim resumes after coordinator restart
2. reclaim resumes after storage-node process restart
3. bucket delete finalization completes after process restart
4. lifecycle claim heartbeat/release works across RPC and stale claims are
   cleaned safely
5. shard scavenger audit can scan remote storage nodes and reports
   location-keyed observations

Status: complete.

- Started Phase 10.6 with the shard-scavenger audit boundary. Shard file scans
  were already remote-capable through `ShardScavengerNodeClient`; the same
  narrow client/RPC surface now also carries data-PG shard rows, object-PG
  payload references, and shard-scavenger observation record/list/resolve
  operations. Metadata-side scavenger RPCs require PG-primary ownership, while
  physical file scans remain routed to the storage node that owns the shard
  location.
- Focused coverage now includes codec bounds/identity checks, non-primary
  rejection for all shard-scavenger metadata RPC messages, and a Unix routed
  shard test proving shard files, ack rows, scavenger shard rows, and
  observations are stored on the storage-node-owned PG rather than the
  frontend placeholder PG.
- Remote frontend/combined startup now uses an explicit background-worker mode.
  Object reclaim, bucket finalization, lifecycle, and shard scavenger are
  enabled for remote frontend mode once their focused RPC restart/resume
  coverage is in place. Focused coverage asserts this Phase 10.6 mode enables
  all routed workers.
- Object reclaim and bucket finalization have focused Unix-storage-node
  coverage proving a restarted frontend placeholder map rediscovers durable
  storage-node-owned reclaim/finalize rows, reclaims the remote shard payload,
  and finalizes the bucket through the remote metadata boundary rather than the
  frontend placeholder PG.
- Lifecycle now has focused Unix-storage-node coverage proving a restarted
  frontend placeholder map discovers storage-node-owned lifecycle roots,
  acquires and heartbeats a remote claim, releases it, and later recovers an
  expired stale claim through the same RPC boundary.
- Stale stream-upload session scavenging now uses a bounded PG-wide
  object-mutation metadata RPC scan instead of the broad local storage client.
  Focused coverage proves a frontend placeholder map lists and aborts
  storage-node-owned stale stream sessions without reading or writing
  placeholder PG state.
- Phase 10.6 is closed. Remote frontend/combined mode now starts the routed
  reclaim/finalizer, lifecycle, shard-scavenger, and stale stream-session
  scavenger workers, with focused Unix-storage-node coverage proving those
  workers discover and mutate storage-node-owned PG state rather than frontend
  placeholder PGs. The full separate-process frontend/combined harness remains
  Phase 10.7 work.

### Phase 10.7: Multi-Process Harness

Add an integration harness that starts:

1. one storage-node process per configured node
2. one coordinator/HTTP process using the static multi-process cluster config
3. a client test runner pointed at the HTTP endpoint

The normal local in-process mode should remain available for unit and fast
integration tests. The multi-process mode is the Phase 10 proof that the same
storage invariants hold without shared memory.

Required harness behavior:

1. creates distinct temporary data directories and socket paths per node
2. waits for node health before starting HTTP tests
3. captures node logs on failure
4. shuts down processes cleanly
5. can intentionally kill/restart a node process for targeted tests
6. builds frontend-only topology without opening placeholder PG directories,
   closing the Phase 10.5 transitional exception to the Phase 10.3
   frontend-only invariant

Required test coverage:

1. local S3 suite passes in multi-process mode
2. node restart preserves bucket, object, multipart, reclaim, lifecycle, and
   shard-scavenger state
3. killing a non-critical node fails closed with clear errors
4. no test depends on process-local cache invalidation, mutexes, or condition
   variables for correctness
5. full-harness parallel S3 test runs are rechecked under the multi-process
   framework, including the high-pressure multipart/SSE-C cases that have
   previously shown intermittent operation-attempt timeouts; any recurrence is
   treated as a server-side admission/backpressure bug to investigate, not as a
   reason to reduce test request concurrency or disable production workers

Status:

- Started Phase 10.7 by extending the UAT S3 test runner with explicit
  `local` and `multihost` topologies. `multihost` is the default for the UAT
  script so external local S3 runs exercise separate storage-node and frontend
  processes by default; `local` remains available for the older single-process
  standalone binary mode. The multihost runner creates per-node data
  directories, private Unix socket paths, per-process logs, starts all
  storage-node processes before the frontend, waits for storage sockets and the
  HTTPS frontend endpoint, and tears down/dumps all process logs together.
- Frontend-only cluster construction now uses a topology-only `LocalClusterMap`
  shape. It creates node IDs, PG routes, placement metadata, runtime state, and
  Unix storage-node clients without opening local placeholder node/PG
  directories; any accidental broad local storage access fails closed with
  `PgNotFound` instead of reading placeholder metadata.

### Phase 10.8: Closeout Audit

Before closing Phase 10, audit production code for remaining shared-memory
assumptions.

Required checks:

1. no production coordinator request path calls `SharedStorageNode::get_pg`
2. no production coordinator request path receives or returns
   `MutexGuard<PgStore>`
3. no production path depends on `SharedStorageNode` bucket locks,
   multipart locks, local reclaim queues, or local condition variables for
   correctness
4. raw shard read/write/delete helpers are either RPC-backed, storage-internal,
   or test-only
5. process-local caches are either correctness-neutral or protected by the
   Phase 9.9 freshness checks
6. every RPC message has explicit size and checksum validation
7. every semantic checksum already present before RPC remains present after
   RPC, persistence, replay, and readback

Exit criteria:

1. no PG directory is shared between processes
2. the same S3 suite passes against local multi-process mode
3. killing a non-critical process fails closed with clear errors
4. restarting a process preserves its local shard and metadata state
5. metadata command bytes remain the durable mutation identity across RPC
6. transport corruption and semantic checksum corruption are both detected
7. production request paths no longer require same-process storage-node mutexes
   or condition variables for correctness

Status:

- Started Phase 10.8 with the shared-memory audit. Bucket-PG request-path
  mutex helpers for create/delete/bucket-control/lifecycle and multipart
  completion have been removed; those paths now rely on durable bucket
  reservations/drains and the storage-node-owned metadata command
  serialization boundary. The storage boundary script now rejects reintroducing
  those bucket-PG lock helpers.
- Removed the remaining object-PG bucket lock helper from production request
  paths. Object metadata mutations, generation reservations, direct/stream PUT,
  multipart completion/abort, lifecycle actions, and reclaim deletion now rely
  on durable pending-command ownership, bucket-write reservations/drains, and
  storage-node snapshot validation instead of same-process object-PG bucket
  mutexes. The boundary script now rejects reintroducing bucket- or object-PG
  request-path lock helpers.
- Removed the ordered two-PG bucket snapshot pair lock helper and its exported
  guard type. Same-node source/destination bucket snapshot pairs now use the
  same operation-shaped routed snapshot reads as remote paths: same-bucket
  requests are merged into one snapshot load, and distinct-bucket requests load
  each bucket independently without holding multiple PG mutexes together.
- Removed the obsolete bucket coordination condition variables and notification
  helpers. Bucket delete/drain progress now uses durable bucket
  write-drain/reservation rows plus restartable polling; there are no remaining
  request-path same-process bucket coordination wakeups.
- Gated the legacy `SharedStorageNode::lock_bucket` stripe lock surface to
  tests/test hooks. Production `SharedStorageNode` no longer carries bucket
  lock stripes, and the remaining lock probes are explicitly test-shaped.
- Removed the remaining frontend-only placeholder PG directory dependency.
  Remote frontend startup now builds a topology-only local map and installs
  Unix storage-node clients over that topology, so frontend-only processes no
  longer open PG stores solely to satisfy cluster-map construction.
- Audited production coordinator request paths for direct PG guards after the
  topology-only frontend change. The remaining `SharedStorageNode::get_pg` and
  `MutexGuard<PgStore>` uses in `request_ops.rs` are under
  `test`/`test-hooks`; request paths route through node-client/RPC surfaces, and
  `scripts/check-storage-cluster-boundaries` passes as the guardrail against
  reintroducing production direct-PG access.
- Audited local reclaim queues and background wakeups. The in-process reclaim
  queue and condition variable are wake hints only: `wait_for_reclaim_work`
  refreshes durable object-payload reclaim roots and bucket-delete finalizer
  roots on each poll before blocking, and Phase 10.6 restart-shaped Unix
  coverage proves reopened frontend maps rediscover storage-node-owned work
  without relying on queued in-memory hints.
- Audited process-local cache authority. Bucket fast-path and parsed-policy
  caches remain process-local performance state guarded by the Phase 9.9
  request-time durable identity proof; both execution and incarnation
  generation are loaded through the bucket metadata client, so remote frontends
  validate against the storage-node-owned bucket PG. The batched generation
  loader is watcher-only and skipped/unavailable PGs do not make cached entries
  fresh.
- Audited metadata command serialization after the topology-only frontend
  change. The local PG mutex remains as local-mode serialization/backpressure,
  but remote frontend command install/reissue/fanout opens the
  storage-node-owned metadata-command critical-section session before reading or
  mutating command-stream state, so cross-process serialization is owned by the
  storage-node primary.
- Audited RPC request size/checksum validation. The request-frame cap table is
  now exhaustive for every `StorageRpcMessageKind`; health, durable claim,
  proof-release, shard-write, and metadata-command mutation messages no longer
  fall through to the generic 64 MiB cap. Metadata command request and response
  decoders share bounded item/envelope helpers that cap embedded canonical
  command bytes before allocation, while bucket/object names and durable
  claim-token fields use bounded decoders. Focused storage RPC tests cover
  kind-specific preallocation rejection plus nested metadata-command byte
  rejection for both checksum-item and plain command-envelope response paths.
- Ran the full local multi-process S3 UAT harness after the closeout audit
  changes. `./scripts/uat-s3-tests` built the server, started one frontend and
  six separate storage-node processes over Unix sockets, and completed
  `cargo nextest run -p s3-tests` with `1533 tests run: 1533 passed` in
  188.947s.
- Added a targeted `./scripts/uat-s3-tests --smoke storage-node-restart` mode
  for process-shaped restart evidence. The smoke starts a one-storage-node,
  one-PG multihost topology so every post-restart S3 request must route through
  restarted storage-node 0, runs a focused S3 probe, kills and restarts the
  storage node against the same data directory and Unix socket path, waits for
  the restarted process to bind, and runs a second focused S3 probe through the
  unchanged frontend. The initial smoke run passed both probes.
- Added a targeted `./scripts/uat-s3-tests --smoke
  storage-node-kill-fails-closed` mode for process-kill evidence. The smoke
  uses the same one-storage-node, one-PG topology, runs a focused S3 probe,
  kills storage-node 0 without restarting it, then requires the next S3 probe
  through the still-running frontend to fail with an explicit S3/client error
  instead of passing against stale local state or hanging. The initial smoke run
  returned a normal S3 `InternalError` 500 in under a second.
- Audited current guides and observability notes for stale bucket-lock
  authority language. The metadata model, threat model, and production
  observability plan now describe durable metadata command serialization,
  bucket write reservations/drains, and storage-side snapshot validation as the
  production correctness boundaries; process-local bucket locks are documented
  as test probes or retired surfaces.

### Phase 10.9: Multihost Stabilization Gate

Before continuing into failure, peering, repair, and migration work, stabilize
the Phase 10 multihost request path. Recent UAT runs have exposed three related
problems:

1. some expected storage races and command-stream contention paths can still
   escape request handlers as HTTP 500s instead of S3-shaped retryable
   responses
2. nondeterministic race failures are hard to diagnose without adding temporary
   trace code, which would not help production incidents
3. the multi-process topology adds remote RPC, metadata-command traffic, shard
   writes, and background-worker pressure, but the current backpressure model
   does not make overload visible or pace the S3 test harness consistently

The goal of this phase is not to relax S3 behavior or make the harness retry
through bugs. The goal is to make expected contention and overload explicit,
observable, and S3-shaped, while preserving fail-closed behavior for real
invariants.

Work items:

1. error semantics audit
   - enumerate every storage/coordinator error family that can cross into HTTP:
     `ObjectPgActionError`, `BucketSnapshotLoadError`, `BucketWriteDrainError`,
     `StoreError`, `MetadataError`, and structured storage-RPC errors
   - classify each crossing as expected contention, overload/backpressure,
     client error, or invariant/internal failure
   - map expected metadata-command contention, stale-generation reservation
     conflicts, retryable drain races, and similar normal same-key races to
     S3-shaped retryable responses such as `OperationAborted`
   - map overload and admission failures to an S3-shaped retryable overload
     response, not EOF, timeout, or generic HTTP 500
   - keep true invariant failures as HTTP 500, but require a stable cause label
     and structured context
   - replace request-path ad hoc `ServerError::Store` /
     `ServerError::Metadata` mappings with shared operation-specific mappers
     where the same error families are expected
2. request-path mapping tests
   - add tests for public request paths, not only central mapper functions
   - cover direct PUT, stream PUT finalization, multipart completion, bucket
     subresources, lifecycle mutations, delete-bucket begin/finalize, and
     metadata-command reissue/fanout contention
   - add guardrail coverage that rejects new coordinator request paths which
     directly map expected storage contention to generic `Store`, `Metadata`,
     or internal errors
3. permanent race diagnostics
   - add structured request/RPC/metadata-command events for contention and
     retry paths: request id, operation, stable bucket/key hashes where
     possible, PG id, node id, RPC kind, command id/log index, retry attempt,
     final status, and mapped cause label
   - require explicit redaction rules for every diagnostic field: no secret
     material, no SSE-C keys or derived plaintext key material, no request
     authorization headers, no object payload bytes, no policy/tag bodies unless
     separately redacted and bounded, and no unbounded bucket/key/header strings
   - bound diagnostic field sizes and prefer stable hashes over raw names for
     bucket/key/object identity; raw names may be emitted only in existing
     ordinary request logs where they are already part of the configured access
     log policy
   - add one compact cause-chain event for every HTTP 500
   - add counters for HTTP 500s, `OperationAborted`, overload/`SlowDown`,
     storage-RPC failures by kind/code, metadata-command conflicts by PG/kind,
     pending-slot drain/reissue attempts, and storage-node command-session wait
   - add a bounded in-memory flight-recorder ring buffer per process that can
     be dumped on abort-on-500, panic, or explicit debug endpoint/trigger
   - make any explicit debug dump endpoint or trigger local/admin-only and
     disabled by default unless an operator enables it deliberately
4. backpressure and admission control
   - define explicit foreground budgets for frontend request admission,
     request-body bytes in flight, per-storage-node RPC concurrency,
     per-PG metadata-command concurrency, shard IO concurrency, and shard bytes
     in flight
   - make overload decisions before expensive body reads or long shard/RPC work
     where possible
   - propagate storage-node saturation to the frontend as a typed retryable
     overload response
   - enforce a side-effect boundary for overload responses: return overload
     before accepting a side-effecting operation, or only after the operation has
     a durable idempotent command/session/reservation identity that makes client
     retry safe
   - fail closed instead of returning retryable overload if the server cannot
     prove whether a non-idempotent shard write, shard ack, metadata command,
     reservation, read handle, or cleanup side effect was accepted
   - reserve lower-priority budgets for lifecycle, reclaim, and scavenger work
     so background workers cannot starve foreground S3 requests
   - ensure the UAT harness does not hide failures by retrying transport EOFs
     or HTTP 500s; pacing must come from server-side admission/backpressure
5. multihost UAT observability
   - make the UAT harness always preserve a concise metrics/log summary on
     failure: slowest operations, 409/503/500 counts, transport failures, RPC
     latency, queue wait histograms, and storage-node saturation events
   - add a repeated-run mode for nondeterministic failures that runs selected
     UAT tests N times with `ARGMIN_ABORT_ON_500=1`, preserves the failing
     iteration's data/logs, and prints the flight-recorder dump location
   - add a stress/pacing UAT mode that intentionally exceeds configured budgets
     and asserts S3-shaped retryable overload responses instead of timeouts,
     EOFs, or process aborts

Required tests:

1. direct buffered PUT and streamed PUT expected object-PG contention return
   `OperationAborted`, not HTTP 500
2. multipart completion and abort expected object-PG contention return
   operation-appropriate S3 errors, not HTTP 500
3. bucket subresource concurrent mutations and stale execution-generation races
   return retryable/conditional S3-shaped errors, not HTTP 500
4. lifecycle and delete-bucket/finalizer races preserve eventually-convergent
   behavior without returning generic 500s for expected contention
5. frontend request admission overload returns the chosen S3 overload response
   before request bodies time out
6. storage-node RPC saturation returns a typed overload response and does not
   close the connection mid-request
7. overload injection before write/RPC admission leaves no command installed, no
   shard ack written, no leaked bucket-write reservation, no leaked object
   generation reservation, and no leaked read handle
8. overload after idempotent command/session identity is established can be
   retried safely and converges without duplicate mutation or leaked cleanup
   state
9. background lifecycle/reclaim/scavenger load cannot starve a bounded foreground
   S3 PUT/GET/MPU workload
10. every HTTP 500 in a focused failure-injection test emits a structured cause
   label and enough request/RPC/PG context to debug without temporary tracing
11. diagnostics and flight-recorder dumps redact secrets, payload context,
    request headers, SSE-C material, and unbounded names; explicit dump access
    is local/admin-only
12. repeated multihost UAT subsets run with abort-on-500 enabled and preserve
   deterministic diagnostics for the first failing iteration
13. guardrails fail if a new coordinator request path maps expected
    metadata-command contention directly to generic `Store`, `Metadata`, or
    internal errors

Exit criteria:

1. full local multi-process UAT passes repeatedly with `ARGMIN_ABORT_ON_500=1`
   and without transport EOFs, operation-attempt timeouts, or generic HTTP 500s
   for expected contention
2. overload under configured stress returns bounded S3-shaped retryable
   responses and recovers when load drops
3. all remaining HTTP 500s are reserved for invariant/internal failures and
   include stable structured diagnostics
4. Phase 10 multihost request paths have central, reviewed error semantics
   rather than ad hoc per-request mappings
5. production diagnostics are sufficient to debug the known race classes without
   adding temporary trace code
6. Phase 11 starts only after this stabilization gate is closed

Status:

- Started Phase 10.9 with the error semantics audit in
  [`phase-10-9-error-semantics-audit.md`](phase-10-9-error-semantics-audit.md).
  The first slice classifies expected metadata-command contention, stale object
  generation reservations, stale bucket metadata command generations, current
  RPC/overload gaps, and the request mappers that still need follow-up tests or
  operation-specific review.

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
That model should remain, but shard-owning storage nodes must own volatile read
handles/delete fences, and reclaim claims must become cluster-visible before
cleanup can run on multiple nodes.

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
