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
           fresh snapshot. For direct streamed `PutObject`, the durable
           stream-upload row alone is staging state, not proof of a writer that
           can still complete: a stream row with a still-valid stream-create
           bucket-write reservation proof blocks DeleteBucket and must return
           the normal non-empty outcome, while a row whose proof no longer
           validates has been abandoned and DeleteBucket may abort that unowned
           stream session before making the final emptiness decision. Direct
           `PutObject` stream-create proofs are renewable durable leases: a live
           frontend refreshes the lease while the request can still append or
           finalize, and disconnect/crash stops renewal so the proof eventually
           fails validation. DeleteBucket performs this abandoned-session cleanup
           synchronously before failing a bucket as non-empty, and a shared
           background stream-session sweeper periodically applies the same
           proof-validation rule so cleanup is eventually reliable even without
           a DeleteBucket request. This distinction must be explicit; do not use
           age-based heuristics as the liveness authority.
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
          rows. "Active stream uploads" means a direct `PutObject` stream
          session whose stream-create bucket-write reservation proof still
          validates and can still commit visible object state; a durable stream
          row whose proof no longer validates is abandoned cleanup state and
          must not by itself make the bucket user-visibly non-empty.
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
        - a direct `PutObject` stream session created by one frontend blocks
          DeleteBucket on another frontend while its stream-create
          bucket-write reservation proof still validates; after stream abort or
          finalize releases that proof, DeleteBucket can treat any remaining
          unowned stream row as abandoned cleanup state
          - status: covered by the independent-frontend active stream
            DeleteBucket regression. The authority is the storage-node-owned
            durable reservation row, not process-local stream ownership or an
            age threshold.
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
- Strengthened `./scripts/uat-s3-tests --smoke
  storage-node-kill-fails-closed` from a generic post-kill setup-probe failure
  into a 1+1 two-node loss smoke. The smoke writes an object while both shards
  are present, kills the non-primary storage node, verifies the old object
  remains readable/listable from the surviving shard, and then verifies a new
  `PutObject` fails closed while the cluster is below the write placement
  requirement.
- Added the companion primary-loss smoke:
  `./scripts/uat-s3-tests --smoke storage-node-primary-kill-fails-closed`.
  It uses the same 1+1 topology, writes and verifies an object before failure,
  kills the primary storage node, and requires old-object `GET`, object
  listing, and a new `PutObject` to fail closed with coded S3 service errors
  rather than transport failures or stale success. `HEAD` is also checked for a
  service-level failure response, but the SDK does not expose a parsed S3 error
  code for the empty-body HEAD 500 response.
- Audited current guides and observability notes for stale bucket-lock
  authority language. The metadata model, threat model, and production
  observability plan now describe durable metadata command serialization,
  bucket write reservations/drains, and storage-side snapshot validation as the
  production correctness boundaries; process-local bucket locks are documented
  as test probes or retired surfaces.

### Phase 10.9: Multihost Stabilization Gate

Status: complete.

Closeout summary:

- The old broad failure patterns from multihost UAT and soak runs no longer
  reproduce: widespread slowdown cascades, expected-contention HTTP 500s,
  storage RPC/transport EOFs, and SDK operation-attempt timeouts have been
  replaced by bounded request work, typed retryable contention, typed overload,
  and permanent diagnostics.
- The request-path unbounded-work audit is closed. Public request paths found
  during Phase 10.9 now either consume `RequestWorkBudget`, use typed capacity
  admission/backoff, or hand longer work to durable background ownership; the
  storage-cluster boundary checker rejects production regressions back to the
  known unbudgeted helper shapes.
- The strict overload validation gate is now `./scripts/uat-forced-overload`.
  It constrains multihost storage-RPC admission, keeps the SDK
  operation-attempt timeout fixed at 30s by default, requires observable
  overload/contention pressure, and fails if pressure escapes as HTTP 500,
  storage RPC errors, transport EOF, connection refusal, panic, or SDK timeout.
- Detailed per-PG/adaptive tuning, production SLO policy, and workload-specific
  capacity weights remain deferred until Phase 11 or the production
  backpressure plan has a more representative workload benchmark than the
  bucket-delete-heavy S3 test suite.

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
   (for example, full UAT runs have seen large multipart copy, object
   attributes, and SSE-C multipart cases fail with AWS SDK
   `OperationAttempt` timeouts around 30 seconds while focused single-test runs
   pass quickly and the server logs show no HTTP 500)

The goal of this phase is not to relax S3 behavior or make the harness retry
through bugs. The goal is to make expected contention and overload explicit,
observable, and S3-shaped, while preserving fail-closed behavior for real
invariants.

Scope boundary:

Phase 10.9 is a stabilization gate, not the full production backpressure design.
It should fix correctness-adjacent request behavior found by UAT: unbounded
request work, retry storms, expected contention escaping as HTTP 500, storage
RPC/session saturation surfacing as EOF or SDK operation-attempt timeout, and
known places where the local six-process UAT harness can hide or amplify server
progress bugs. Broader throughput tuning, adaptive admission, workload-specific
capacity weights, and production SLO policy belong in
[`production-backpressure-plan.md`](production-backpressure-plan.md) after a more
representative production-style harness exists.

Work items:

Closeout classification: items 1-3, the fixed/static admission and bounded-work
pieces of item 4, the required implementation-order pieces for Phase 10.9 in
item 5, and the UAT observability/forced-overload gate in item 6 are complete
for this stabilization gate. The remaining broader capacity-policy bullets in
items 4-5 are deliberately deferred to Phase 11 or
[`production-backpressure-plan.md`](production-backpressure-plan.md), where they
can be tuned against a production-realistic workload rather than the local S3
test harness.

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
3. permanent race diagnostics [done]
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
   - add aggregate counters for HTTP 500s, `OperationAborted`,
     overload/`SlowDown`, storage-RPC failures, metadata-command conflicts,
     pending-slot drain/reissue attempts, and storage-node command-session
     wait; keep the kind/code/PG dimensions in bounded flight-recorder records
     instead of unbounded metric labels
   - add a bounded in-memory flight-recorder ring buffer per process that can
     be dumped on abort-on-500, panic, or explicit debug endpoint/trigger
   - make any explicit debug dump endpoint or trigger local/admin-only and
     disabled by default unless an operator enables it deliberately
4. backpressure and admission control
   - treat backpressure as a capacity protocol, not as extra
     `CommandAborted` paths. The server must either hold capacity for the next
     side effect, stop reading client body bytes so TCP applies natural
     backpressure, or return S3 `SlowDown` before doing more work. A timeout or
     EOF is a backpressure bug unless the client disappears first.
   - introduce a shared capacity-admission abstraction used by frontend,
     storage-cluster, and storage-node code. Each lease records:
     resource kind, operation class, estimated work units, wait start/end,
     timeout budget, queue depth at acquire, and release outcome. Initial
     resources are frontend requests, streaming body segment buffers, internal
     copy bytes, per-storage-node RPC sessions, per-storage-node shard reads,
     per-storage-node shard writes, per-storage-node bytes in flight, per-PG
     metadata command apply, and pending-command recovery work.
   - define an operation cost model before reading or generating large bodies.
     Metadata-only operations charge metadata/RPC units. Reads charge read RPCs
     and response bytes. Direct PUT, streamed PUT, `UploadPart`, multipart
     completion, `CopyObject`, and `UploadPartCopy` charge mutating request
     units plus per-segment shard write/read bytes. Unknown-size streaming
     writes acquire one segment of body/shard capacity at a time before reading
     that segment from the client.
   - make mutating requests side-effect aware:
     - before durable command/session/reservation identity exists, capacity
       failure returns `SlowDown` and must leave no metadata command, shard ack,
       bucket-write reservation, generation reservation, read handle, or reclaim
       fence behind
     - after staging has started but before publish, capacity failure aborts the
       staging session through the normal cleanup path and returns `SlowDown`
       only after the server knows no visible object mutation committed
     - after publish/commit is known durable, the server must return the
       committed result or fail closed if the commit result is unknown; it must
       not return retryable overload for an ambiguous non-idempotent write
   - add typed storage-node admission instead of connection drops. A saturated
     storage-node must produce a framed `ResourceExhausted` response for the
     RPC, and the storage client/coordinator mapper must convert that to S3
     `SlowDown`. The current `accept_and_spawn` path that silently returns when
     `STORAGE_NODE_MAX_ACTIVE_SESSIONS` is exhausted is not acceptable for UAT
     or production backpressure because it appears upstream as EOF/timeout.
     Because active-session exhaustion happens before the server has read an RPC
     frame, the protocol must explicitly support one of these shapes:
     a connection-level overload frame before request upload that carries no
     request id/kind; a client/protocol handshake where the client sends a small
     header first, waits for either continue or `ResourceExhausted`, and only
     then streams a large payload; or an explicit bounded close-and-retry signal
     that the storage client maps to typed resource exhaustion only before any
     request payload side effect could have been accepted. A server-only
     header-reader is not sufficient with the current synchronous client,
     because the client writes the full encoded frame before reading a response.
     If a header-first responder is used, it may read only the fixed frame
     header, request id, kind, declared payload length, and checksum outside the
     active work budget, and must reply before the client sends a large payload;
     it must not allocate, drain, or ask the client to upload the full declared
     payload while saturated. If payload draining is chosen for a small subset
     of requests, it must be behind a tiny overload-responder byte cap and must
     reject large payload-bearing requests such as `ShardWrite` before
     allocation. The overload path must also have its own small hard cap so
     overload reporting cannot become an unbounded worker pool or
     body-bandwidth sink.
   - add client-side node admission before opening storage-node RPC work. The
     frontend process has the cluster map and placement result, so it can avoid
     launching more RPCs to a node than the configured per-node budget can
     sustain. The storage-node server remains the final authority and still
     returns typed overload if the local process is saturated or the frontend
     estimate is stale.
   - isolate foreground and background work with separate weights/reservations.
     Lifecycle, reclaim, delete finalization, scavenger, repair, and scrub may
     use spare capacity, but they must not consume the reserved foreground
     budget for PUT/GET/HEAD/list/control-plane requests. Background workers
     that cannot acquire their low-priority lease should back off and keep the
     durable row as the source of truth rather than spinning.
   - replace internal retry storms with single-flight recovery and bounded retry
     budgets. For a given `(node, pg, cluster_epoch, pending slot/log index)`,
     at most one worker performs drain/reissue/recovery. Other contenders wait
     on that single-flight result for a short bounded budget, then return the
     operation-appropriate retryable S3 response (`OperationAborted` for
     same-key metadata contention, `SlowDown` for capacity exhaustion) instead
     of installing more durable commands. Every waiter must revalidate the
     recovered pending command against its own expected command identity,
     checksum, operation scope, bucket/key or bucket-only scope, generation or
     reservation identity, and side-effect boundary before proceeding. A
     different contender's successful recovery is only a wakeup signal until
     that revalidation succeeds.
   - add per-PG metadata mutation admission/backoff above the storage-RPC
     queue. Slow-host UAT showed a hot metadata PG producing hundreds of
     retryable `OperationAborted` responses without request-admission timeout,
     storage-RPC admission wait, or metadata-recovery timeout. That means the
     system admitted too many logical metadata contenders rather than filling a
     low-level RPC queue. Bound the number of active new metadata mutation
     attempts per PG, keep a reserved path for completion/recovery/drain work,
     and make excess starts wait briefly within a request-local budget or
     return `SlowDown` before doing expensive snapshot/command-build work.
   - add jittered backoff after real metadata-command contention outcomes,
     not blind sleeps. Triggers include metadata command log conflicts,
     pending-slot conflicts, partial exact-command conflicts, command-id
     allocation conflicts, and pending-slot install conflicts. Start with small
     exponential full-jitter delays such as 2-5 ms initial, factor 2, capped at
     50-100 ms, all charged to the request-local work budget. The goal is to
     dephase same-PG retry herds while keeping the PG making useful command
     progress.
   - cap object-version reservation retries as part of the same retry budget.
     A single client PUT attempt may not burn unbounded
     `ReserveObjectVersion` commands for one bucket/key. Reuse the request's
     durable identity where possible; otherwise bound allocator retries, record
     `reservation_attempts`, `reservation_conflicts`, and
     `commands_per_visible_commit`, and return `OperationAborted` or
     `SlowDown` according to whether the bottleneck is metadata contention or
     capacity.
   - add request-local work budgets for bounded server progress, independent of
     client SDK timeouts. Any public request path that waits for durable bucket
     write drain, drains or reissues pending metadata commands, finalizes bucket
     delete work, enumerates completed multipart-upload records, or performs
     whole-bucket/page cleanup must consume a fixed operation budget and return
     the operation-appropriate retryable S3 response when that budget is
     exhausted. In particular, `wait_for_durable_bucket_write_drain` is only a
     short sleep plus bucket-existence check today, while many callers wrap it in
     unbounded outer loops for bucket writes, object metadata writes, stream
     creation, multipart creation/completion, upload-part stream creation, and
     stream-session records. Delete-bucket begin now has a local budget, but the
     same pattern remains outside that path. Budget exhaustion must not become
     an SDK operation-attempt timeout or an internal storage error.
   - defer adaptive overload control to the production backpressure plan. Phase
     10.9 may add fixed limits, typed overload, bounded waits, and diagnostics
     needed to keep request behavior correct under stress, but it should not
     tune adaptive capacity policy from the current single-host UAT harness as
     if that harness represented production workload shape.
   - bias admission toward completing already-admitted work when unfinished work
     is accumulating, without starving new starts. Split mutating work into at
     least `Completion`, `Progress`, and `StartWrite` classes. Completion work
     includes pending-command apply/drain, stream segment append commit,
     multipart complete/abort cleanup, reservation/session release, and other
     paths that have already created durable side effects or hold durable
     identity. `StartWrite` includes `CreateStreamUpload`, create-multipart,
     and other first-side-effect admission for new writes. The controller must
     keep a small nonzero floor for new starts, but as completion pressure rises
     it should reduce the new-start share and reserve more capacity for closing
     existing operations. `SlowDown` should be emitted primarily at `StartWrite`
     and bulk/list admission boundaries; completion paths should either finish,
     clean up and then return the operation-appropriate retryable response, or
     fail closed if the outcome is ambiguous.
   - split cheap read work from expensive list/scan work. `ShardRead`,
     `ShardReadRange`, read-handle acquisition, and ordinary `GET`/`HEAD`
     support should have a separate bounded `Read` class with a low-latency
     reservation. Object listing, version listing, bucket listing, multipart
     listing, stream-upload listing, and other page/scan style operations should
     use a separate `List` class with a smaller cap and earlier overload
     behavior. Internal cleanup enumeration must not accidentally share the
     user-list class when it is needed to finish already-admitted work.
   - emit `SlowDown` early enough for SDK retries to pace clients. The response
     should include the existing `Retry-After` header, with a later follow-up
     allowed to derive the value from overload debt instead of the current
     constant. The first correctness target is that clients see bounded 503
     `SlowDown`, not that the retry delay is perfectly tuned.
   - keep measurement-first diagnostics, but use them to validate the control
     loop rather than to justify larger timeouts. Preserve UAT time series for
     every capacity resource: in-use, queue depth, wait histogram, acquire
     timeout count, `SlowDown` count, EOF/timeout count, bytes in flight,
     completed work units, and background-vs-foreground split.
   - ensure the UAT harness does not hide failures by retrying transport EOFs
     or HTTP 500s; pacing must come from server-side admission/backpressure.
     Higher `S3_TEST_TIMEOUT_SECS` values may be used only as a diagnostic
     control after the server-side stage metrics identify the bottleneck.
   - use deterministic local pressure injection for RPC acquire, shard IO,
     metadata apply, pending-command recovery, EC work, and response streaming
     so faster developer hosts can reproduce slow-disk/slow-CPU queueing
     without relying on host-specific timing.
5. backpressure implementation order
   - first fix the lossiest overload surface: storage-node session/RPC
     saturation must return typed `ResourceExhausted` instead of closing or
     dropping accepted connections. Choose and implement either a
     connection-level overload frame, a header-first continue/overload
     handshake for large payload RPCs, or a tightly scoped close-and-retry
     signal that the client maps to resource exhaustion before payload side
     effects are possible. Add a low-limit test that proves the public S3
     response is `SlowDown`, not a client write block, EOF, or operation-attempt
     timeout.
   - add the shared capacity lease type and instrumentation with fixed static
     limits only. Wire it into storage-node RPC sessions, shard read/write
     operations, and frontend request admission before adding adaptive behavior.
   - add frontend client-side node admission from placement results so expensive
     operations do not launch RPC fanout that the target nodes already cannot
     accept. Keep storage-node typed overload as the authoritative fallback.
   - add per-segment streaming write admission: acquire body buffer,
     destination shard-write bytes, and target-node RPC capacity before reading
     the next segment. If the wait budget is exhausted, stop reading and return
     `SlowDown` through the normal abort/cleanup path.
   - add explicit mutating-operation outcome gates for overload after staging:
     no `SlowDown` may be returned until staging cleanup has completed and the
     server has proven no visible publish occurred. If publish/commit outcome is
     unknown, return the existing fail-closed internal/invariant path with full
     diagnostics rather than a retryable overload response.
   - add internal-copy pacing on the same per-segment capacity path, retiring
     the separate medium-priority plan unless a copy-specific policy is still
     useful after the shared byte budget exists.
   - add per-PG metadata apply and pending-command recovery leases, then replace
     drain/reissue loops with single-flight recovery, bounded waiter budgets,
     and waiter-side command identity/checksum/scope revalidation. Waiters that
     cannot prove the exact command was applied must return typed retryable
     contention without performing owner-side payload/generation cleanup, because
     a recovery leader may have reissued and applied a matching command.
   - add a shared request work-budget helper and thread it through durable
     bucket-drain wait paths before widening it to bucket delete finalization,
     completed-MPU cleanup/pruning, and other page/scan helpers. The first
     target is to make slow durable-drain or pending-command convergence return
     bounded `OperationAborted`/`SlowDown` responses rather than letting many
     small server-side waits accumulate past the SDK operation-attempt timeout.
   - close the remaining unbounded request-work audit before Phase 11. Every
     public request path and background worker entry point that can run because
     of a public request must either finish within an operation-local work
     budget, transfer ownership to durable background state, or return a typed
     retryable S3 response. This includes retry loops around bucket write
     drain waits, pending-command drain/reissue, command-id allocation,
     object-version/generation reservation, stream-session create/finalize,
     multipart create/complete/abort cleanup, bucket delete begin/finalize,
     completed-MPU cleanup/pruning, lifecycle-triggered cleanup, and any
     whole-bucket/page scan helper. A loop may be intentionally unbounded only
     if it is outside the request path, owns no request worker, is paced by a
     durable work queue or explicit capacity lease, and has observability that
     proves it is not hiding request progress.
   - add foreground/background capacity classes and make lifecycle, reclaim,
     delete finalization, scavenger, repair, and scrub use low-priority leases
     with backoff.
   - split the coarse storage-RPC bulk class into separate `Read` and `List`
     classes before tuning limits. Large listing/page work must feel overload
     before ordinary cached reads, and metrics must report read/list admission
     totals, waits, timeouts, and active counts independently.
   - add fixed or stepped completion-pressure shaping after the static class
     split is stable. This Phase 10.9 version may use configured thresholds
     rather than a continuous controller, but it must reserve a floor for new
     starts while shifting additional capacity to completion/progress work as
     unfinished stream sessions, pending appends, pending metadata commands,
     staged payloads, and cleanup backlog grow.
   - leave the bounded adaptive controller to
     [`production-backpressure-plan.md`](production-backpressure-plan.md). Before
     Phase 11, require only the fixed/static admission and bounded-work pieces
     needed to avoid EOFs, HTTP 500s, unbounded request loops, and SDK
     operation-attempt timeouts for expected contention.
   - after every slice, run the focused pressure tests with low limits, then a
     repeated UAT subset on the slow host before widening the tested surface.
6. multihost UAT observability
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
7. storage-node active-session exhaustion returns a framed
   `ResourceExhausted` RPC response or an explicit connection-level overload
   frame, maps to S3 `SlowDown`, and produces no coordinator-visible or
   public S3-client-visible EOF, raw `StorageRpc` error, or operation-attempt
   timeout. A deliberate pre-side-effect close-and-retry signal may be observed
   only inside the storage client and must be converted there to typed
   `ResourceExhausted` before it crosses into storage-cluster/coordinator code.
   Large-payload `ShardWrite` coverage must prove the client can observe
   overload before uploading the full payload, or that the internal
   close-and-retry signal maps to `SlowDown`; it must not block in
   `write_storage_rpc_frame_to`, allocate or drain the full declared payload on
   the server, or depend on the normal generic payload limit while the node is
   saturated.
8. client-side node admission prevents RPC fanout from exceeding configured
   target-node capacity, while stale estimates still converge through
   storage-node typed overload
9. streamed PUT and `UploadPart` acquire per-segment capacity before body reads;
   under forced low shard-write capacity, the client sees either TCP pacing or
   S3 `SlowDown`, not a body idle timeout or dropped connection
10. `CopyObject` and `UploadPartCopy` consume the same byte/RPC capacity budget
    as client-body writes and cannot starve unrelated bounded PUT/GET traffic
11. pending-command drain/reissue single-flight allows one recovery worker per
    `(node, pg, epoch, pending slot/log index)` and prevents duplicate durable
    command storms under concurrent contenders
12. single-flight waiters revalidate the recovered command identity, checksum,
    operation scope, object/bucket scope, generation or reservation identity,
    and side-effect boundary before proceeding; divergent contenders must wake
    and return the correct retryable contention error or fail closed rather than
    treating another command's recovery as their own success. Direct PUT waiter
    ambiguity must not delete payload shards or release generation reservations
    that could now be referenced by a reissued applied command.
13. object-version reservation retries are bounded per request attempt, expose
    `reservation_attempts` and `commands_per_visible_commit`, and return
    `OperationAborted` or `SlowDown` instead of looping until the SDK
    operation-attempt timeout fires
14. foreground capacity reservations keep bounded PUT/GET traffic making
    progress while lifecycle, reclaim, delete-finalizer, scavenger, repair, or
    scrub work is saturated
15. overload injection before write/RPC admission leaves no command installed, no
   shard ack written, no leaked bucket-write reservation, no leaked object
   generation reservation, and no leaked read handle
16. capacity failure after destination staging has started but before publish
    aborts the staging session, completes cleanup, proves no visible object
    mutation committed, and only then returns S3 `SlowDown`
17. capacity failure or transport loss with unknown publish/commit outcome fails
    closed with invariant/internal diagnostics and must not return retryable
    `SlowDown` or `OperationAborted`
18. overload after idempotent command/session identity is established can be
   retried safely and converges without duplicate mutation or leaked cleanup
   state
19. background lifecycle/reclaim/scavenger load cannot starve a bounded foreground
   S3 PUT/GET/MPU workload
20. full-suite multihost large-object pressure, including multipart copy,
    checksum/object-attributes MPU completion, and SSE-C multipart PUT/GET,
    either completes within the configured operation-attempt budget or returns
    bounded S3-shaped overload responses; it must not fail only as SDK
    operation-attempt timeouts
21. read/list class separation under forced list pressure keeps bounded
    `GET`/`HEAD` and shard-read work making progress while large listing/page
    operations either wait within budget or return S3 `SlowDown`
22. fixed or stepped completion-pressure shaping under many unfinished stream
    sessions/pending appends shifts capacity toward append/finalize/cleanup
    work, reduces new `CreateStreamUpload`/create-multipart admission, and
    still admits at least the configured new-start floor
23. every HTTP 500 in a focused failure-injection test emits a structured cause
   label and enough request/RPC/PG context to debug without temporary tracing
24. diagnostics and flight-recorder dumps redact secrets, payload context,
    request headers, SSE-C material, and unbounded names; explicit dump access
    is disabled by default and local/admin-only
25. repeated multihost UAT subsets run with abort-on-500 enabled and preserve
   deterministic diagnostics for the first failing iteration
26. guardrails fail if a new coordinator request path maps expected
    metadata-command contention directly to generic `Store`, `Metadata`, or
    internal errors
27. guardrails or focused tests fail if a request-path retry loop waits on
    durable drain convergence, pending-command recovery, command-id allocation,
    reservation allocation, stream-session progress, multipart cleanup,
    bucket-delete cleanup, lifecycle cleanup, or whole-bucket/page scanning
    without consuming a request-local work budget or handing off to durable
    background work

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
6. all request-path retry loops discovered in the unbounded-work audit are
   either bounded, converted to typed capacity admission/backoff, or explicitly
   moved behind durable background ownership with tests
7. Phase 11 starts only after this stabilization gate is closed

Status: complete.

- Started Phase 10.9 with the error semantics audit in
  [`completed/phase-10-9-error-semantics-audit.md`](completed/phase-10-9-error-semantics-audit.md).
  The first slice classifies expected metadata-command contention, stale object
  generation reservations, stale bucket metadata command generations, current
  RPC/overload gaps, and the request mappers that still need follow-up tests or
  operation-specific review.
- Added the first drift guardrail for the audit: production coordinator code
  may not add new hand-written `ObjectPgActionError` match arms that map raw
  storage `Store`/`Metadata` variants directly to `ServerError::Store` or
  `ServerError::Metadata` outside the central mapper.
- Continued the error semantics audit by routing active-bucket-summary loads
  through the central bucket snapshot mapper and adding a drift guardrail that
  rejects production bucket snapshot metadata fallbacks unless they explicitly
  handle stale bucket metadata command generations.
- Continued the delete-bucket side of the audit by routing bucket-finalize
  helpers through the central bucket-write drain mapper, mapping escaped
  command-stream contention in bucket-write drain paths to `OperationAborted`,
  and adding a drift guardrail against ad hoc bucket-write drain Store/Metadata
  mappings.
- Mapped escaped object version reservation conflicts through the central
  object-PG mapper to `OperationAborted`, matching generation reservation
  conflict handling after the storage allocator retry loop has been exhausted.
- Added direct PUT request-path regression coverage for metadata command log
  conflict during `CommitDirectPutObject` apply, verifying the public request
  maps the contention to `OperationAborted`.
- Added delete-object request-path regression coverage for metadata command log
  conflict during both `DeleteObjectVersion` and `InsertDeleteMarker` apply,
  verifying the public request maps the contention to `OperationAborted`.
- Added request-path regression coverage for create bucket, streamed PUT
  begin/append/abort/finalize, object metadata writes, bucket
  subresource/control-plane mutations, create/complete/abort multipart, streamed
  upload-part append/finalize, and lifecycle-owned object mutations so escaped
  metadata command contention maps to `OperationAborted` instead of HTTP 500.
- Extended that coverage through public copy wrappers: `CopyObject` now covers
  destination stream-create and stream-finalize command contention after source
  authorization/read setup, and `UploadPartCopy` covers destination
  `CommitStreamPart` contention after source snapshot/read succeeds.
- Started the permanent diagnostics slice by adding redacted
  `ServerError::diagnostic_cause_label()` classification, carrying those labels
  through `S3Response::error*`, and emitting compact HTTP `request_error`
  diagnostics with `error_code` plus `cause_label` for immediate error
  responses and streaming body errors. Added counters for exact HTTP 500
  responses, `OperationAborted`, and `SlowDown`. This covers the first compact
  HTTP failure breadcrumb; the later permanent-diagnostics status bullets close
  out RPC, metadata-command, command-session wait, and bounded
  flight-recorder/debug-dump coverage.
- Added UAT harness support for `--repeat N` / `ARGMIN_UAT_REPEAT=N` so
  nondeterministic full-suite or focused failures can be rerun under one
  process group, plus failure diagnostics that grep all frontend/storage-node
  logs for HTTP 500, cause-label, panic, and abort markers before printing the
  usual log tails. This does not replace the planned flight recorder, but it
  makes the next intermittent EOF cascade report the first server-side abort
  line more directly.
- Added delete-bucket request-path regression coverage for both
  `MarkBucketDeleting` begin and explicit bucket-finalizer cleanup of completed
  multipart tombstones. Both inject metadata command log conflict and verify
  the public/worker-facing path returns `OperationAborted`.
- Made public `CreateBucket` persist the requested ownership-control mode in
  the initial create metadata command instead of issuing a follow-up
  `PutBucketOwnershipControls` mutation. This removes a partial-create window
  where a second-command failure could leave an active bucket with incorrect
  ownership controls, and the create-bucket RPC/config payload now carries the
  required ownership mode.
- Closed the mapper-audit slice in
  [`completed/phase-10-9-error-semantics-audit.md`](completed/phase-10-9-error-semantics-audit.md)
  after adding typed storage-RPC resource exhaustion mapping to `SlowDown`,
  preserving shard-delete-in-progress as internal/recoverable only, and adding
  drift guardrails for object-PG mappers, bucket snapshot mappers,
  bucket-write drain mappers, and payload-read storage-error mapping.
- Continued the permanent diagnostics slice by adding a process-local bounded
  flight recorder in `observability`. Request finish/error/slow-request
  emitters now record redacted summaries with stable path hashes, query shape,
  status, body/byte counts, lifetime, outcome/error code, and cause label.
  `ARGMIN_ABORT_ON_500` dumps the recent ring before aborting so UAT failures
  preserve context without enabling deep tracing. Remaining diagnostics work:
  per-RPC/per-metadata-command wait/conflict events and counters, plus any
  explicit local/admin-only debug dump trigger.
- Added the first storage-side race diagnostics for that remaining slice:
  metadata-command log and pending-slot conflicts now increment a counter and,
  when a trace context is attached, record redacted PG/node/epoch/log-index and
  command-kind context in the flight recorder. Storage-node-owned
  metadata-command critical-section waits now likewise increment a wait counter
  and record node/PG/wait duration without bucket/key names. Storage-node RPC
  handlers attach a local per-frame trace context so these records are present
  in multihost storage-node processes even when the frontend trace context is
  not propagated over the storage RPC frame.
- Continued the permanent diagnostics slice by adding storage-node RPC error
  diagnostics at the central response boundary. Every typed storage-RPC error
  response now increments a counter and records node id, RPC kind, error code,
  bounded message length, and a stable message hash in the flight recorder
  under the per-frame storage-node trace context. Raw RPC payloads and error
  messages are not written to the flight record.
- Continued the permanent diagnostics slice by adding a separate
  metadata-command pending-slot action counter and flight-recorder event for
  drain and reissue attempts. The central storage-cluster drain/reissue helpers
  record only node id, PG id, cluster epoch, log index, action, and command
  kind, with tests covering real pending-slot drain and reissue paths and
  asserting bucket names are not included.
- Continued the permanent diagnostics slice by adding an explicit local debug
  endpoint gate. `ARGMIN_LOCAL_DEBUG_ENDPOINT` is disabled by default, is
  accepted only for frontend roles listening on loopback socket addresses, and
  exposes only bounded counter snapshots over HTTP. The flight-recorder trigger
  is `POST /__argmin/debug/flight-recorder/dump`; it dumps the already-redacted
  bounded ring to stderr rather than returning request/RPC details, headers,
  bucket/key names, payload context, or SSE-C material in the HTTP response.
- Continued the permanent diagnostics slice by having `argmin-s3` install a
  process-wide panic hook after configuration succeeds. Any frontend,
  storage-node, or legacy-local panic now dumps the same bounded, redacted
  flight-recorder ring to stderr before delegating to the normal Rust panic
  hook. The UAT wrapper also enables the local-only debug endpoint on loopback
  frontends and requests a flight-recorder dump before teardown on failed runs,
  while gating extra metadata-command conflict stderr diagnostics behind
  `ARGMIN_METADATA_COMMAND_CONFLICT_DIAGNOSTICS`.
- Continued the permanent diagnostics slice by emitting a separate bounded,
  redacted `request_500_cause_chain` flight-recorder event for every HTTP 500.
  The chain uses stable diagnostic labels only, preserving nested storage
  causes without exposing bucket names, keys, request headers, policy text, or
  backend error strings.
- Closed Phase 10.9 part 3, permanent race diagnostics. The implemented shape
  uses aggregate always-on counters for the abnormal classes and stores
  dimensional context such as RPC kind/code, PG id, node id, log index, command
  kind, action, status, and cause label in bounded redacted flight-recorder
  records. That keeps production metrics cardinality controlled while still
  making intermittent race failures diagnosable from abort-on-500, panic, and
  explicit local debug dumps.
- Started Phase 10.9 part 4 with measurement-first admission diagnostics.
  Frontend request-semaphore waits over a small threshold now emit bounded,
  redacted `request_admission_wait` records and aggregate wait counters; permit
  acquisition timeouts emit `request_admission_timeout` records before returning
  S3 `SlowDown`. The local debug metrics endpoint exposes the new counters, and
  the UAT wrapper samples that endpoint once per second into the retained log
  directory so slow-host timeout runs preserve the pressure ramp.
- Reworked the remaining Phase 10.9 backpressure direction after slow-host UAT
  showed that adding more command-aborted/contention mappings does not make
  clients slow down to sustainable work capacity. The plan now requires a
  capacity-admission protocol with side-effect-aware leases, typed storage-node
  overload instead of EOFs, client-side node admission, per-segment body/shard
  pacing, single-flight pending-command recovery, bounded object-version
  reservation retries, foreground/background budget separation, and only then a
  bounded adaptive controller.
- Started the first client-side node-admission implementation slice. Unix
  storage-node clients now share a static per `(node_id, socket_path)` RPC
  admission gate before opening request sockets or writing frames. Long-lived
  read-handle and metadata-command session opens hold a permit for the session
  lifetime, and `ShardWrite` acquires the gate before checksum/RPC payload
  encoding so a saturated client does not build or upload a large shard-write
  frame. This is intentionally a fixed-limit first step; runtime configuration,
  per-resource byte budgets, and adaptive control remain follow-up work in this
  phase.
- Added the first storage-node defensive fallback for active-session pressure:
  the server now reserves an active-session slot before accepting a socket and
  waits for release when the limit is reached. That keeps excess connections in
  the Unix socket backlog instead of accepting and silently dropping them. A
  framed/handshaked overload protocol is still a later phase item, but this
  removes the immediate EOF/drop behavior from the main `serve_forever` path.
- Added UAT SlowDown visibility and a first tuning knob for forced pressure
  runs. The UAT wrapper now prints a concise SlowDown summary from the final
  frontend metrics sample and matching logs, and frontend topology setup accepts
  `ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT` so slow-host or stress runs can
  deliberately set a low per-node Unix RPC admission limit and verify bounded
  S3 `SlowDown` responses instead of opaque client timeouts.
- Changed the local Unix storage-RPC admission gate from immediate fail-open
  shedding to bounded waiting on actual in-flight RPC capacity. A full
  per-node client gate now waits for release before mapping sustained overload
  to storage `ResourceExhausted`/S3 `SlowDown`, reducing retry storms where a
  short burst could previously exhaust the SDK retry budget even though the
  node was draining work. The wait path now exports
  `storage_rpc_admission_total`,
  `storage_rpc_admission_wait_total`,
  `storage_rpc_admission_wait_us_total`, and
  `storage_rpc_admission_timeout_total`; the timeout counter is the signal that
  the bounded wait was actually hit before shedding, while wait/total gives the
  proportion of storage RPCs that encountered local backpressure. The bounded
  wait default for bulk/list RPCs is intentionally short (`250ms`) and can be
  tuned with `ARGMIN_STORAGE_NODE_RPC_ADMISSION_WAIT_MS`; sustained bulk
  overload should return `SlowDown` quickly enough that client retry/backoff,
  not blocked server workers, provides the pacing. Control and mutation RPCs
  use a separate bounded wait (`ARGMIN_STORAGE_NODE_RPC_CONTROL_ADMISSION_WAIT_MS`,
  default `1000ms`) so bucket cleanup, metadata publication, and ordinary writes
  are not prematurely surfaced as public `SlowDown` while bulk traffic is being
  shed.
- Split the local storage-RPC admission gate into bulk and control classes.
  High-volume read/list RPCs (`ShardRead`, read-handle acquire, object and
  bucket listing pages) are capped below the total per-node admission limit,
  reserving a small amount of capacity for metadata publication, delete-bucket
  drains, cleanup, and other control work. Forced-pressure UAT with
  `ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT=10` showed that a single undifferentiated
  gate could make cleanup/control requests time out behind list pagination even
  though the node was otherwise responding with typed `SlowDown`. One-row
  listing probes used to prove bucket emptiness are classified as control work;
  completed-multipart cleanup enumeration also uses control admission. Normal
  client listing pages remain bulk and should feel backpressure first. The
  mixed-class condition variable wakes all waiters on release because a freed
  slot can be usable by control work while bulk waiters are still over their
  class cap.
- In remote-frontend mode, cap the effective HTTP in-flight request semaphore by
  the per-node storage RPC admission limit. A frontend with a deliberately low
  storage-RPC limit must not admit substantially more S3 handlers than the
  storage layer can serve; otherwise pressure appears only after each handler
  has already fanned out into storage RPCs, producing public `SlowDown` and long
  cleanup tails instead of request-level pacing.
- Added the next Phase 10.9 admission-shaping direction to the plan. The coarse
  control/bulk split is not enough for soak stability: already-admitted work
  such as stream append commit/finalize/cleanup should receive increasing
  reserved capacity as unfinished work accumulates, while new write starts keep
  a small floor and otherwise feel `SlowDown` first. The plan now also requires
  splitting cheap `Read` work from expensive `List`/scan work so large listing
  pressure does not suppress ordinary cached reads.
- Started implementing that direction in the local Unix storage-RPC admission
  gate. The old control/bulk split is now explicit `Control`, `Completion`,
  `Progress`, `StartWrite`, `Read`, and `List` classes. `List` has a smaller
  cap than `Read`; single-row cleanup/listing probes and completed-multipart
  cleanup enumeration use completion admission. `StartWrite` keeps a nonzero
  floor but its cap shrinks as active completion/progress RPCs rise, so stream
  append/finalize/cleanup work can use reserved capacity while new
  `CreateStreamUpload`/create-multipart command builds feel pressure first.
- Tightened object-PG pending-slot fairness for cheap command publishers seen
  in slow-host UAT traces. Object generation reservation/release, object
  version reservation, and stream segment append now allocate a fresh metadata
  command id and install the durable pending command while holding the local
  per-PG metadata-command lock. Expensive snapshot reads and shard validation
  remain outside the lock, but once a local frontend observes an empty pending
  slot, unrelated version/delete/reclaim work cannot refill the slot before the
  current progress/completion command is published.
  Storage-RPC admission wait/timeout diagnostics now include the admission
  class. This is still a deterministic stepped policy, not the later full
  adaptive controller.
- Tightened the direct PUT completion retry loop after another slow-host UAT
  run showed many versioning tests stalling without storage-RPC admission
  pressure. Direct PUT now registers the acknowledged shard set before
  building a new metadata command and validates it before publishing,
  preserving the fail-closed pre-publish validation hook while shortening the
  command-build-to-pending-install window. On command-id or pending-slot
  contention it drains at most one competing pending command per retry instead
  of turning a single install conflict into a full object-PG backlog drain.
- Tightened the first admission-class implementation after review. Non-reserved
  work (`Progress`, `StartWrite`, `Read`, and `List`) now shares a cap below the
  total per-node RPC limit so `Progress` shard writes cannot consume the
  completion/control reserve. Long-lived metadata-command critical sections are
  classified as `Completion` for their held session permit. GET/HEAD metadata
  support RPCs are classified as `Read`, and MPU listing support loads are
  classified as `List`, so they no longer bypass the read/list caps by falling
  through to `Control`.
- Removed the storage-RPC admission classifier fallback. Every
  `StorageRpcMessageKind` must now be explicitly classified, so new or
  overlooked high-volume user mutations cannot silently consume the protected
  control/completion reserve. Direct PUT commit RPCs are `Completion`; ordinary
  object put/delete command-build RPCs are `StartWrite`.
- Added explicit call-site overrides for context-sensitive helper RPCs. Shared
  helpers such as object-version allocation, bucket-write reservation acquire,
  and metadata-command id allocation default to `StartWrite`, but direct PUT
  commit, stream PUT finalize, completed-MPU sequence-order reservation, and
  multipart completion use completion-specific helper methods once durable work
  already exists. This prevents completion paths from being throttled by the
  shrinking new-start cap without giving all callers privileged admission.
- Tightened the public storage-RPC admission configuration floor to reject
  pathological test-only values. The admission math needs at least two
  non-reserved read slots for a remote object read to hold a read-handle
  session while issuing the shard read, plus reserved control/completion
  capacity and room for list/progress/start-write work. The environment knob
  now requires a minimum of 8; lower values remain available only to direct
  low-level unit tests that are deliberately exercising exhaustion behavior.
- Started the pending-command recovery single-flight slice after slow-host UAT
  showed versioned direct PUTs stuck behind repeated `ReserveObjectVersion`
  drain/reissue work on one object PG. Generic object/bucket PG pending-slot
  drains now coalesce by exact PG, command log index, and command checksum: one
  worker performs the drain while contenders wait, emit `drain_wait`, and then
  re-read the pending slot before deciding what to do next. Waiters have a
  bounded recovery budget and return typed metadata-command contention on
  timeout. Exact owner apply paths remain unchanged.
- Tightened the single-flight waiter result handling for direct PUT and stale
  reservations. Generic drain recovery now abandons zero-apply stale
  generation/version reservations instead of surfacing reservation conflicts
  from unrelated pending slots. Direct PUT waiters that observe an ambiguous
  partial exact conflict now fail retryably without deleting segment shards or
  releasing the generation reservation, because the recovery leader may have
  reissued and applied a matching command that owns those side effects.
- Added metadata-command recovery counters to the debug metrics and UAT summary:
  leader/wait/timeout/outcome totals, total and max waiter time, top recovery
  admission and outcome PG/command pairs, and top pending-slot actions. This
  makes the next slow-host timeout distinguish between one legitimate recovery
  leader with bounded waiters, repeated reissue storms, and waiters timing out
  behind a stuck recovery leader.
- Added the remaining unbounded request-work audit to Phase 10.9. The main
  high-risk shape is request paths that repeatedly wait for durable bucket-write
  drain, drain/reissue pending metadata commands, or perform page/cleanup work
  without a request-local budget, so many individually small server-side waits
  can extend past the SDK operation-attempt timeout. DeleteBucket begin now has
  a local budget, but the plan now tracks extending that fixed-budget pattern to
  durable bucket-drain callers, bucket-delete finalization, completed-MPU
  cleanup/pruning, and other whole-bucket/page helpers.
- Added the first request-work budget slice after versioning soak failures in
  `test_versioned_concurrent_object_create_concurrent_remove` and
  `test_versioning_multi_object_delete`. The retained UAT logs and metadata DBs
  showed hot object PGs with a primary-only `ReserveObjectVersion` pending slot
  at the next log index, many waiters timing out behind single-flight recovery,
  and leftover durable bucket-write drain rows from cancelled cleanup. Object
  version reservation retries, stream PUT session creation behind an active
  durable drain, and the common bucket write-reservation snapshot loops now use
  request-local retry budgets that return retryable metadata contention instead
  of consuming an SDK operation-attempt timeout.
- Added the next backpressure direction after a local UAT setup cleanup failure
  deleting an owner-root probe bucket. The failed bucket's PG had fully
  converged by shutdown and storage-RPC admission never waited, but the run
  emitted hundreds of `OperationAborted` responses while a hot metadata PG
  accepted many competing metadata mutations. The plan now calls out per-PG
  metadata mutation admission and jittered contention backoff as the next
  throughput-oriented control, rather than increasing delete priority or tuning
  the oversized storage-RPC queue depth.
- Clarified the Phase 10.9 unbounded-work target after soak tests exposed
  bucket-delete and versioned-write contention paths. The stabilization gate now
  requires closing the remaining request-path unbounded-loop audit before
  Phase 11, while keeping longer-running cleanup behind durable background
  ownership instead of request workers.
- Follow-up repeated UAT and soak runs no longer reproduce the old widespread
  slowdown, SDK operation-attempt timeout, transport EOF, or expected-contention
  HTTP 500 patterns after the bounded-work, admission-class, recovery, and typed
  RPC/error-mapping fixes. The only new observed failure was a concrete
  `UploadPart`/`AbortMultipartUpload` race where the Unix stream command-build
  RPC collapsed `NoSuchUpload` into a generic storage RPC error; that path now
  preserves typed `NoSuchUpload` and has focused Unix and S3 race coverage. The
  remaining Phase 10.9 work is therefore the closeout audit for request-path
  unbounded work plus any strict forced-overload validation needed to document
  bounded `SlowDown`/recovery behavior, not more broad soak-pattern debugging.
- The closeout audit converted the remaining bucket metadata request loops to
  carry `RequestWorkBudget` through command-id allocation, pending-slot drain,
  pending-slot installation, and acting-set apply/reissue. This covered bucket
  create, bucket control-plane updates, completed multipart order reservation,
  and the completed-multipart commit path, and removed the unused unbudgeted
  helper wrappers from production. The boundary checker now rejects production
  calls back to the unbudgeted bucket metadata drain/retry helpers while leaving
  test-only local helpers available for focused state-machine coverage.
- Added `./scripts/uat-forced-overload` as the strict Phase 10.9 overload gate.
  It runs a constrained multihost UAT with the minimum accepted storage-RPC
  admission limit, requires observable overload/contention pressure, and fails
  if pressure escapes as HTTP 500, storage RPC errors, transport EOF, connection
  refusal, panic, or SDK operation-attempt timeout. This gate proves bounded
  S3-shaped behavior; detailed per-PG/adaptive admission tuning remains deferred
  until Phase 11 or until a more production-realistic workload benchmark exists.
- Closed Phase 10.9. The phase now has central error-semantics guardrails,
  permanent race diagnostics, fixed/static storage-RPC admission classes,
  bounded request-work budgets for the audited request paths, repeated
  multihost UAT soak coverage for the previous failure classes, and the strict
  forced-overload validation wrapper. Remaining capacity-policy tuning is not a
  Phase 10.9 blocker and should proceed only with a production-realistic
  benchmark or in the production backpressure plan.

## Phase 11: Failure, Peering, Repair, And Migration

Add real distributed behavior after the normal path is already shaped correctly.

Core membership and heartbeat decision:

Phase 11 should introduce one authoritative cluster-map manager for membership,
heartbeat leases, cluster epoch bumps, and PG state transitions. The first
implementation may be single-authority and non-HA, but it must sit behind a
control-plane interface that Phase 12 can replace with a replicated consensus
implementation. Gossip or peer observations may be useful diagnostics and
failure hints, but they are not authoritative membership agreement. Even the
single-authority Phase 11 implementation must persist cluster-map state,
authority incarnation, and the latest issued cluster epoch before issuing any
lease or map that depends on them. After restart, it must either resume from the
durable latest epoch or allocate a strictly higher epoch; epochs and authority
incarnations must never be reused. This keeps stale frontends and storage nodes
from a previous authority instance fenced even before Phase 12 adds replicated
agreement.

Separate durable membership from temporary availability:

- durable membership states: `Joining`, `Active`, `Draining`, `Out`, and
  `Removed`
- temporary availability states: `Healthy`, `Suspect`, and `Unavailable`
- a missed heartbeat should not immediately remove a node from durable
  placement; it should mark the node temporarily unavailable, bump the cluster
  epoch, move affected PGs into peering, and let the new epoch choose primaries
  from the still-available acting-set members
- permanent `Out` membership changes are the later repair, backfill, and
  migration trigger

Heartbeat is a lease and fencing mechanism. A storage node heartbeats with its
stable `node_id`, current boot/incarnation id, advertised endpoint, observed
cluster epoch, and local PG health/state summary. The control plane replies with
the current cluster map, cluster epoch, and heartbeat lease deadline. Nodes and
frontends must stop accepting new mutating work when their control-plane lease or
map is stale, and every storage RPC / metadata command that can mutate state
must carry enough epoch and sender-incarnation context for receivers to reject
stale senders. Lease issuance is valid only after the authority has durably
recorded the epoch/map/incarnation tuple being leased.

Threat-model boundary: Phase 11 treats storage-node and single-authority
control-plane processes as trusted runtime components on trusted hosts, as
documented in [`guides/threat_model.md`](../guides/threat_model.md). The
authority validates heartbeat freshness, epoch/incarnation fencing, PG state,
pending-command absence, and peering/active metadata proof floors, but it does
not attempt to prove that a hostile storage node's future metadata proof is a
valid successor without trusting that node's local durable replay validation.
Remote authenticated control-plane transport is a future phase, and stronger
node attestation is a deployment/runtime trust problem rather than an S3 object
store authorization mechanism.

PG primary assignment should be deterministic once the authoritative map is
known. Given `(cluster_epoch, pg_id, acting_set, temporary availability view)`,
all nodes must compute the same primary, for example by choosing the first
available node in the deterministic acting-set order. The hard agreement problem
is deciding the authoritative map and epoch; the primary selection function
should remain simple and testable.

Epoch changes fence unsafe service. On membership, availability, or acting-set
change, the control plane bumps the cluster epoch and affected PGs enter
`Peering`. The new primary reconstructs authoritative PG state from durable
command logs and pending-command records across the acting set before marking
the PG `Active`. Requests that need complete PG truth, especially writes and
list operations, must fail closed or return typed retryable responses while the
PG is peering or the sender has only stale epoch information.

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
10. define the distributed correctness invariants that Phase 11 must preserve
    and wire them into trace/model checks where possible:
    one PG primary per epoch, stale senders cannot mutate state, metadata
    command ids/log indexes cannot fork, pending-command apply/reissue is
    idempotent, reservations and drains cannot be silently lost or stolen,
    bucket delete cannot finalize while visible object or MPU state remains,
    committed object metadata references readable shards or an explicit durable
    reclaim/repair state, and list operations fail closed across peering or
    epoch ambiguity
11. add deterministic fault-injection tests for operations that start under one
    epoch and finish, retry, or clean up under another: primary changes while
    commands are pending, stale frontend routes, storage-node RPC failure before
    and after durable metadata writes, partial acting-set success, retry of the
    same logical operation after leadership movement, and failure during bucket
    delete drain/finalize or multipart/stream publish
12. keep the single-host six-process UAT soak as an overload/backpressure signal,
    but add a separate smaller real-multihost correctness soak with independent
    storage processes, real interprocess transport, restarts, route/epoch
    changes, and controlled failure injection. Treat the former as evidence
    about request bounding and retry quality; treat the latter as evidence
    about distributed correctness.
13. before closing Phase 11, add targeted property/model tests for the main
    invariants that the soak tests sample but cannot exhaustively cover:
    control-plane epoch/heartbeat state transitions, metadata
    transfer/checkpoint/log import and retry, retained-route cleanup/history
    behavior in the local cluster trace model, and pure backfill planner
    classification/priority.

Status:

- Started Phase 11 with the control-plane authority foundation in
  `crates/storage/src/control_plane.rs`. The first slice defines durable
  membership states, temporary availability states, authority incarnations,
  heartbeat lease requests/responses, a file-backed single-authority store, and
  deterministic PG primary selection from an agreed map. The file-backed
  authority persists the epoch/map/incarnation tuple before issuing leases and
  bumps both authority incarnation and cluster epoch on restart so previous
  authority instances cannot reuse fencing tokens. This is intentionally still
  single-authority; Phase 12 can replace the store/authority implementation
  behind the same control-plane boundary with replicated consensus.
- Added authority-side heartbeat failure detection. Expired heartbeat leases
  mark nodes temporarily `Unavailable`, clear the stale lease, bump and persist a
  new cluster epoch once per transition, and return the affected nodes so the
  later PG-peering layer can move impacted PGs out of service before selecting
  new primaries.
- Added authoritative PG acting-set and PG-state records to the control-plane
  map. Acting-set changes start PGs in `Peering`, active PG primary selection is
  derived from the durable PG map plus node availability/observed-epoch state,
  and node expiry or removal moves affected active PGs back to `Peering` in the
  same durable epoch transition.
- Added explicit control-plane authorization helpers for serving nodes and PG
  primaries. Callers can now fail closed unless the sender has the current epoch,
  matching node incarnation, unexpired lease, healthy/observed map state, and an
  active PG where the sender is the deterministic primary. PG primary
  authorization returns the validated node lease deadline so callers can bound
  primary work, and PG peering completion uses the same lease-aware validation
  before persisting `Active`.
- Added bounded cluster-map history retention to the single-authority control
  snapshot. Epoch-changing commits and authority restarts durably retain the
  previous map before exposing the new epoch, old `version=2` state still loads,
  and history is pruned to a fixed recent window for stale-route diagnostics and
  future cross-epoch fault-injection checks.
- Tightened heartbeat-driven epoch changes so node incarnation changes, endpoint
  changes, and availability recovery move affected active PGs back to `Peering`
  before the new epoch is exposed. This prevents a recovered earlier acting-set
  member from becoming deterministic primary in a later epoch without an
  explicit peering completion.
- Added a central PG operation authorization policy for metadata reads, metadata
  lists, metadata writes, payload reads, and payload writes. The Phase 11 policy
  is intentionally conservative: every operation class requires current
  lease/epoch/incarnation fencing plus an `Active` PG served by the deterministic
  primary. Degraded or backfilling read relaxations remain deferred until repair
  can provide explicit shard/readability proofs.
- Added current-epoch PG state summaries to storage-node heartbeats. The
  authority persists per-node PG observations only for current-epoch heartbeats
  from nodes in the authoritative PG acting set, rejects malformed persisted
  observations, does not mutate summaries on stale heartbeats, and clears current
  observations on epoch changes while retaining the old summaries in cluster-map
  history for future peering diagnostics.
- Tightened PG peering completion so the authority only persists `Active` from
  `Peering`, and only after current serving acting-set members have heartbeated
  the PG as `Peering` in the current epoch. This keeps the Phase 11 authority
  from treating peering as a naked administrative toggle before the selected
  primary and available peers have demonstrated that they installed the current
  map. Durable command-log reconstruction and replica proof checks remain the
  next peering layer.
- Tightened active PG service so the deterministic primary is not considered
  serving, and cannot receive PG-operation authorization, until it has also
  heartbeated that PG as `Active` in the post-peering cluster epoch. This keeps
  peering completion, epoch fencing, and local primary state installation as
  distinct observable steps.
- Added explicit validation for issued PG-operation authorization tokens. Tokens
  now retain the validated primary node incarnation, and callers can re-check
  authority incarnation, cluster epoch, node incarnation, both the token and
  current node lease deadlines, active PG state, deterministic primary
  assignment, current primary PG observation, and the expected operation class
  before applying work that started under an earlier view.
- Added the same revalidation surface for issued node-service authorization
  tokens, so non-PG node work can re-check authority incarnation, epoch, node
  incarnation, and both captured and current lease deadlines before completing.
- Added an explicit metadata proof to current-epoch PG heartbeat observations.
  Peering completion now requires every currently serving acting-set member that
  participates in the transition to report `Peering` with the same applied
  metadata log index/hash/state digest before the authority can persist
  `Active`. This is the first control-plane hook for durable command-log
  reconstruction proof; later repair/backfill work can replace the placeholder
  proof source with storage-node computed replica state. Because the
  control-plane store is still pre-release, the persisted state loader now
  accepts only the current version 7 format rather than carrying compatibility
  for earlier Phase 11 scratch formats.
- Fixed the restart epoch-bump path to clear current PG observations just like
  ordinary epoch changes and to move Active PGs back to `Peering` before the
  restarted authority exposes its new epoch. Restart still records the previous
  map in history, so stale-route diagnostics retain the pre-restart
  observations and active-primary bindings, but the newly persisted current map
  cannot contain node observations from the old epoch or continue serving an
  Active PG without current-epoch peering.
- Bound `Active` PG records to the primary that completed peering. The
  authority now persists the active primary in the current control-plane state
  format, clears that binding whenever a PG returns to `Peering`, and fails
  closed rather than dynamically selecting a different acting-set member while a
  PG remains `Active`. Because the control-plane store is still pre-release, the
  loader now accepts only the current version 7 format rather than carrying
  compatibility for earlier Phase 11 scratch formats.
- Added a pure runtime-routing snapshot view derived from the durable
  control-plane state. The exported route for an `Active` PG uses the persisted
  `active_primary`, requires that primary to still have a current heartbeat
  lease and current-epoch `Active` PG observation, and fails closed for `Peering`
  or stale-primary state instead of re-running dynamic primary selection.
- Added explicit conversion from the durable route snapshot into the existing
  local cluster route and storage-node process route shapes. This keeps the
  current startup paths static for now, but gives the later dynamic map install
  work a single tested bridge from authority state to runtime routing.
- Added the first storage-node-to-authority peering-proof bridge. A
  `SharedStorageNode` can now build a PG heartbeat observation from its durable
  metadata command replica state, carrying the applied log index, applied hash
  chain value, and metadata state digest that the control-plane peering checks
  already compare. Dynamic heartbeat wiring still remains to be added, but the
  proof source is now the PG's persisted command-log state rather than only a
  synthetic test placeholder.
- Extended that bridge to full storage-node heartbeat construction. Runtime
  heartbeat loops can now supply the installed PG states from their current
  route map and receive a `NodeHeartbeat` populated with node identity, observed
  epoch, lease request, endpoint, and per-PG durable metadata proofs from the
  local `SharedStorageNode`.
- Connected the storage-node process config to that heartbeat builder. A
  process config derived from a runtime map can now build its authority
  heartbeat from the installed PG routes and local durable PG state, rejecting
  route epoch mismatches and non-UTF8 endpoints instead of advertising stale PG
  state or a lossy route identity.
- Exposed the same heartbeat construction from a running `StorageNodeServer`, so
  the future heartbeat loop can use the server's installed config and opened
  local PG stores directly instead of reconstructing process state externally.
- Added an end-to-end authority/runtime/server heartbeat bridge test: a runtime
  map derived from the authority can configure a storage-node server, the server
  can build a current-epoch heartbeat from local durable PG state, and the
  authority persists that PG observation for peering.
- Added a small heartbeat sink interface for submitting node heartbeats. The
  single-authority control plane implements it today, and `StorageNodeServer`
  can submit through the interface so the later Phase 12 authority can replace
  the sink without changing storage-node heartbeat construction.
- Added a one-shot heartbeat/runtime-map refresh boundary. A storage-node server
  can now submit its local heartbeat through an authority interface, receive the
  post-heartbeat runtime map from the same authority view, and build the next
  validated process config that a later heartbeat loop can install or restart
  into.
- Added a topology-only local cluster constructor that accepts supplied PG
  routes and validates the route set against the configured epoch, PG set, and
  local node set before exposing it. This is the fail-closed install boundary
  the later dynamic control-plane route update path should reuse.
- Extended the control-plane route view to export every PG, not only active
  PGs. `Active` PGs still require the persisted active primary, current lease,
  and current active observation, while `Peering` and other non-active PGs are
  exported as non-active routes so runtime maps can reject unsafe work with
  `PgNotActive` instead of losing the PG as an unknown route.
- Added a control-plane runtime-map snapshot that bundles the all-PG route view
  with the advertised endpoints and node incarnations for every node referenced
  by those routes. The authority still stays transport-neutral, but the snapshot
  now fails closed if a routed node has not advertised an endpoint. Active route
  snapshots carry the primary lease deadline, and the runtime map exposes the
  minimum active-route deadline as its `valid_until_ms` so later install/serve
  paths know when the map must stop routing unsafe work.
- Carried that runtime-map validity bound into `LocalClusterMap`. Static maps
  remain unbounded, while control-plane-derived/supplied route maps can retain
  `valid_until_ms` and expose `require_route_map_valid_at(now_ms)` for later
  dynamic install and serving guards.
- Wired the runtime-map validity bound into metadata PG routing. Expired
  control-plane-derived maps now fail closed before selecting metadata
  primaries, acting sets, or accepting replica metadata commands.
- Wired the same validity bound into payload placement and shard IO routing, so
  expired maps cannot keep placing, writing, reading, or deleting payload shards.
- Added the first direct authority-to-local-runtime install bridge. A frontend
  topology-only `LocalClusterMap` can now be built from a
  `ClusterRuntimeMapSnapshot`, preserving the authority epoch, all exported PG
  routes, and the map validity deadline. The Unix local runtime also has a
  narrow helper that turns transport-neutral node route endpoints into
  storage-node client configs without teaching the authority about socket
  paths.
- Extended that bridge to `StorageCluster` construction. Runtime snapshots can
  now produce a cluster handle directly, optionally installing Unix
  storage-node clients from absolute runtime endpoints, while preserving the
  authority route-map validity deadline and exposing the same fail-closed
  validity check at the cluster boundary.
- Added a frontend replacement-handle refresh boundary. A `StorageCluster` can
  now ask an authority runtime-map source for the current map and rebuild a
  validated replacement cluster handle, preserving the existing immutable handle
  model while giving the later dynamic route-refresh loop a safe swap point.
  The shared frontend runtime-map handle installs refreshed clusters under a
  monotonic fence, so racing refreshes cannot roll a frontend back to an older
  epoch or a same-epoch map with reduced route-map validity. Unix storage-node
  client rebuilds take explicit RPC admission settings so forced-overload
  limits and wait-timeout tuning cannot be reset by refresh.
- Added the frontend control-plane runtime-map refresh loop wrapper around that
  install boundary. The loop rejects zero intervals, records last success/error
  status for diagnostics, preserves explicit Unix RPC admission settings when
  rebuilding storage-node clients, and stops/joins explicitly so future
  frontend process wiring can refresh maps without leaking workers.
- Added the matching storage-node process config bridge. A storage node can now
  derive its startup PG set, route table, cluster epoch, and socket endpoint
  from a `ClusterRuntimeMapSnapshot`, filtering the authority route view to only
  PGs where that node is in the acting set and failing closed if the node is not
  present in the runtime map.
- Carried the runtime-map validity deadline into storage-node process configs
  and storage-node serving validation. Heartbeats can still be built from an
  expired installed map so the node can refresh, but storage RPC serving now
  fails closed with a stale-route response once the installed map's active
  primary lease bound has expired.
- Tightened storage-node runtime-map refresh candidates so they are explicitly
  installable over the running process identity. Epochs, validity deadlines, PG
  sets, and route tables may change, but a refresh cannot silently change the
  node id, data directory, default EC shape, or bound Unix socket path.
- Extended the route-map expiry policy with an explicit cleanup exception for
  release-only storage RPCs. Serving, acquire, validate, heartbeat, and command
  paths still require a fresh installed map, but session metadata-lock release
  and durable reservation/drain/reclaim release paths validate only route
  identity so stale-map cleanup does not depend on transport teardown or lease
  expiry.
- Added an explicit storage-node runtime-map install boundary. A storage node
  can now submit a heartbeat, build a validated next process config from the
  authority runtime map, and install it for future sessions while preserving
  process identity and rejecting in-place PG-set changes until dynamic PG-store
  open/close is implemented.
- Made the installed storage-node route config replaceable through shared
  server access. Connection handlers still receive an immutable config snapshot
  for each session, while the heartbeat refresh path can install a validated
  next map for later sessions without requiring exclusive ownership of the
  running server. Installs are monotonic under the config write lock: older
  epochs and same-epoch route-map validity regressions are rejected so racing
  heartbeat refreshes cannot roll a node back to an obsolete map.
- Added a storage-node control-plane refresh loop wrapper around the one-shot
  heartbeat/runtime-map/install path. The loop rejects zero intervals, retries
  refresh failures on the next tick while the installed route map continues to
  fail closed on expiry, records last success/error status for diagnostics, and
  stops/joins explicitly so future process wiring does not leak heartbeat
  workers.
- Added a durable monotonic storage-node incarnation source. Each storage-node
  boot can now advance and persist a never-reused incarnation in its data
  directory before heartbeating, using an atomic write/sync/rename/sync
  sequence and rejecting corrupt or overflowed counters. The public boundary is
  on the bound `StorageNodeServer`, so process wiring advances the counter only
  after the server owns the data-directory lock, and the bound server serializes
  same-process callers around the read/modify/write. This provides the
  production fencing input for the heartbeat loop.
- Added the first process boundary for the single-authority manager. The
  `argmin-s3` binary now has a `control-plane` role that requires
  `ARGMIN_CONTROL_PLANE_STATE_PATH`, takes a sibling interprocess lock before
  opening the durable file-backed authority, and runs the heartbeat lease-expiry
  scan loop as the single owner.
- Added the first control-plane Unix transport endpoint. The `control-plane`
  role now also requires `ARGMIN_CONTROL_PLANE_SOCKET_PATH`, binds that Unix
  socket under the same single-authority state lock, and serves framed
  runtime-map snapshot and heartbeat+runtime-map refresh RPCs. The
  endpoint requires a private owner-owned socket directory, validates CRC64
  frame checksums over the header and payload, reads requests on bounded worker
  threads with socket IO deadlines before taking the authority mutex, and
  bounds each accept batch so lease expiry cannot be starved by a continuous RPC
  stream. The transport-neutral `UnixControlPlaneClient` implements the existing
  runtime-map and heartbeat refresh traits, so storage-node and frontend
  processes can talk to the manager process without opening the authority state
  file themselves.
- Wired `ARGMIN_CONTROL_PLANE_SOCKET_PATH` into storage-node and frontend
  process startup. Storage-node processes can now bootstrap their installed
  route map from the authority, advance their durable node incarnation after
  binding the data directory, and keep submitting heartbeat/runtime-map refresh
  RPCs on the existing refresh loop using configurable refresh and heartbeat
  lease intervals. Frontend processes can bootstrap their initial storage
  cluster from the authority runtime map without a static
  `ARGMIN_STORAGE_NODE_SOCKETS` map, preserving configured Unix storage-node RPC
  admission settings.
- Added the live frontend runtime-map install boundary. Frontend startup now
  creates one `StorageClusterRuntimeMapHandle`, starts the control-plane
  runtime-map refresh loop when `ARGMIN_CONTROL_PLANE_SOCKET_PATH` is set, and
  shares that handle with every coordinator. Coordinators snapshot the current
  `Arc<StorageCluster>` from the handle at request/runtime entry points, so
  foreground request routing can advance monotonically without rebuilding
  coordinators or duplicating background workers on every lease extension.
  Multi-step read, copy, direct PUT, streaming PUT/POST, UploadPart,
  UploadPartCopy, CompleteMultipartUpload, and AbortMultipartUpload paths now
  pin one request `Arc<StorageCluster>` across authorization, session creation,
  append/body IO, finalize/commit, and abort/cleanup work. The production
  streaming helper surface is biased toward explicit `_with_storage_node` calls
  so HTTP contexts must carry the pinned request map, and regression tests
  install replacement runtime maps at the previously vulnerable handoff points.
- Added fresh-state control-plane bootstrap from the existing
  `ARGMIN_STORAGE_NODE_SOCKETS` and PG configuration. When a manager opens an
  empty authority state and a storage-node socket map is present, it persists
  the initial active node membership and PG acting sets in one atomic authority
  commit before serving the Unix control-plane socket. The configured socket
  endpoints are persisted for pre-heartbeat runtime-map bootstrap, while
  serving eligibility still requires heartbeat/incarnation/observed-epoch/PG
  observation fencing.
- Added the first automatic Peering-to-Active handoff for control-plane-backed
  storage-node refresh. After a node heartbeat records matching current-epoch
  Peering metadata proofs, the authority can batch-complete ready PGs, bump the
  epoch once, and return a storage-node-only runtime map that contains the
  newly Active routes so nodes can install them and heartbeat Active in the new
  epoch. The normal frontend runtime-map export remains stricter and still
  withholds Active routes until the bound primary has heartbeated Active for the
  post-peering epoch.
- Completed the request runtime-map pinning audit for the currently implemented
  foreground multi-step paths. Object metadata subresources, DeleteObject,
  DeleteObjects, DeleteBucket, bucket create/recreate, bucket subresource
  writes, bucket ACL writes, and bucket control-plane mutators now capture one
  request `Arc<StorageCluster>` and use it across authorization, mutation, and
  reclaim/finalizer enqueue work. Focused regressions now swap in a replacement
  runtime map at object metadata policy-context load, delete authorization,
  bucket-delete authorization, and bucket subresource authorization boundaries.
- Hardened the control-plane Unix socket tests to use short `/tmp` socket paths
  rather than the repository-relative temporary directory, so they keep covering
  private directory creation, frontend bootstrap, runtime-map refresh, and
  storage-node bootstrap without depending on the checkout path being shorter
  than the platform `sun_path` limit.
- Added the first real multihost control-plane correctness smoke:
  `./scripts/uat-s3-tests --smoke control-plane-runtime-map`. The smoke starts a
  separate single-authority control-plane manager, two storage-node processes,
  and a frontend process that bootstraps and refreshes routing only through
  `ARGMIN_CONTROL_PLANE_SOCKET_PATH`; it then runs S3 probes, including a
  metadata-progress PUT/update/delete probe followed by a refresh wait and a
  second request before any restart, so post-write Active heartbeat lease
  renewal is covered. It then restarts one storage node, waits for
  heartbeat/peering refresh, and runs a final S3 probe. This is intentionally a
  narrow process/runtime-map wiring smoke, not the later full
  distributed-correctness soak.
- Started the deterministic epoch-transition fault regression set. The first
  tests cover exported runtime maps failing closed once their lease validity
  expires across an authority epoch transition, stale primary operation
  authorizations being rejected after lease expiry moves a PG back to
  `Peering`, and storage-node route-map expiry rejecting new unsafe work while
  still allowing cleanup/release route validation for already-held resources.
  The set now also covers the storage-node heartbeat refresh path after a lease
  expiry epoch transition: a node reporting its stale Active route receives the
  current Peering map and a non-serving lease, not an Active route backed by
  the expired lease. The direct PG-operation token regression now follows the
  same lease-expiry path through recovery: the expired token is fenced, the
  caught-up node cannot write while the PG is still `Peering`, and fresh write
  authorization is available only after current-epoch peering completion and an
  Active heartbeat. It also covers explicit temporary availability loss: a
  primary marked `Suspect` fences old authorizations, leaves the PG in
  `Peering`, and cannot resume unsafe work until availability recovery,
  current-epoch observation, and peering completion all happen. The same
  deterministic set now also covers non-PG node-service authorization tokens:
  an epoch bump fences a token issued under the old map, old observed-epoch
  requests fail closed, authority restart fences stale tokens by incarnation,
  and the node can receive a fresh authorization only after heartbeating the
  current epoch.
- Extended endpoint-change epoch-transition coverage from node serving to active
  PG service. A route endpoint change now has a deterministic regression proving
  old write tokens are fenced, the affected PG moves back to `Peering`, writes
  remain blocked after the node observes the new endpoint epoch, and fresh write
  authorization appears only after current-epoch peering completion plus an
  Active heartbeat.
- Extended the deterministic epoch-transition fault regressions to cover an
  acting-set change. A metadata-write token issued to the old active primary is
  fenced by the epoch bump, both old and new primaries fail closed while the PG
  is `Peering`, and only the replacement primary can receive and validate a new
  operation authorization after current-epoch peering proof completes. A
  separate Peering-state acting-set change regression proves observations
  collected under the previous acting set cannot complete peering after the
  epoch bump; the authority requires fresh current-epoch observations from the
  new serving acting set.
- Made the accepted peering metadata proof durable in the authoritative PG
  record. `Active` PGs now persist the proof that was validated during peering
  alongside the bound active primary, while any transition back to `Peering`
  clears both. The control-plane state format is now version 7 and rejects
  active PG records that lack a complete active metadata proof.
- Tightened current-epoch Active PG heartbeat observations to match the accepted
  peering metadata proof. After the authority completes peering, a storage node
  cannot replace its PG observation with `Active` unless the heartbeat carries
  the same log-index/hash/state-digest proof that was accepted for that PG.
- Extended that accepted-proof invariant to active-primary service/export,
  storage-node refresh-map export, and persisted-state load validation.
  Runtime-map active routes, storage-node refresh routes outside the explicit
  just-activated handoff, PG-operation authorization, and serving-primary
  selection now fail closed if the bound primary's current Active observation
  does not match the PG's accepted peering proof, and version 8 state files
  reject current Active observations with a mismatched proof.
- Tightened existing-node membership changes to fence active service before the
  new epoch is exposed. Any membership transition now moves affected Active PGs
  back to `Peering`, not only transitions to `Out` or `Removed`; regressions
  cover Active-to-`Joining` and Active-to-`Draining` so non-serving joins and
  draining handoff both require current-epoch peering before unsafe service can
  resume.
- Tightened the storage-node heartbeat proof source without putting unbounded
  command-log replay on the recurring heartbeat path. Heartbeat observations now
  read the incrementally maintained metadata replica proof and validate its
  state digest against the cached metadata table digest state while preserving
  the unresolved pending-command signal. Full command-log replay remains for
  startup/open validation, explicit diagnostics, and later repair/checkpoint
  work where O(retained history) validation is acceptable.
- Extended PG heartbeat observations with explicit unresolved pending-metadata
  command state. Peering completion, automatic ready-peering completion, active
  heartbeat ingestion, active route export, and persisted state loading now
  fail closed if an otherwise matching metadata proof still has a pending
  command slot. This closes the first pending-command part of Phase 11 peering:
  equal applied log/digest proof is not enough to mark a PG `Active` while any
  participating serving member still reports in-flight metadata command state.
- Current remaining Phase 11 work is now concentrated in the distributed
  correctness layers above the control-plane/runtime-map plumbing: real
  command-log reconstruction during PG peering, deterministic epoch-transition
  fault injection, shard repair, PG backfill/migration, command-log
  retention/compaction implementation, and a small real-multihost correctness
  soak separate from the single-host overload soak.

PG peering reconstruction design:

- Peering activation is now intentionally split into two layers. The
  control-plane layer only accepts a candidate active metadata proof after every
  currently serving acting-set member reports the current epoch, `Peering`
  state, no unresolved pending metadata command, and the same applied
  log-index/hash/state-digest proof. That remains the durable activation gate.
- The next storage-node layer must produce that converged proof from durable PG
  command logs instead of relying on already-equal heartbeats. The selected
  primary should gather each serving acting-set member's replica state, pending
  metadata-command state, retained command-log interval, and command-log entry
  hashes. If every serving member already shares the same proof, peering can
  hand that proof to the authority. If a member is behind and the primary has a
  contiguous retained suffix from the member's applied proof to the candidate
  proof, peering may apply/replay that suffix on the behind member and then
  retry the authority activation with fresh matching heartbeats.
- Reconstruction must fail closed, leaving the PG in `Peering`, when any serving
  member reports an unresolved pending metadata command, a same-index hash or
  state-digest fork, a missing retained log entry needed for catch-up, an
  unknown command payload, or an observation from the wrong epoch or acting set.
  Later repair/backfill can choose an explicit recovery path, but Phase 11
  peering must not mark the PG `Active` from ambiguous metadata history.
- The first reconstruction regressions now cover the control-plane activation
  contract directly: lagging serving replicas, same-index hash forks, and
  same-index state-digest forks all fail closed until the serving replicas
  converge on the exact reconstructed proof.
- Added the first pure retained-log reconstruction decision tests in the
  storage layer. The reconstruction helper is transport-neutral and takes
  per-replica metadata proof, pending-command state, and retained command-log
  hash-chain entries. It returns either already-converged, deterministic catch-up
  required for lagging replicas, or a fail-closed reason for pending commands,
  same-index metadata forks, missing retained suffix entries, retained
  hash-chain forks, stale epochs, or replicas ahead of the selected primary.
  Focused pure tests cover each of those fail-closed branches. The helper is now
  normal storage-layer code, not test-only scaffolding.
- Added the first production boundary for those range reads. `PgStore`, local
  metadata clients, Unix metadata clients, metadata-command sessions, and the
  storage-node Unix RPC protocol can now read a bounded sparse range of retained
  metadata command-log hash-chain entries for a PG. The request is capped at
  4096 log indexes, validates ordered non-zero indexes at the frame boundary,
  and verifies retained command-log entry checksums before returning stored
  previous/current hash pairs.
- Added the payload-bearing retained-log boundary needed before mutating
  catch-up. `PgStore` and the local/session/Unix metadata-command clients can
  now read a smaller bounded range of retained command-log entries: applied
  entries carry the verified canonical `MetadataCommandEnvelope`, while
  abandoned tombstones are exposed explicitly with the original command
  checksum instead of being treated as replayable payloads. The Unix RPC decoder
  caps entry ranges separately from hash-only ranges and rejects unknown entry
  kinds, keeping abandoned or unsupported replay cases fail-closed for the next
  catch-up slice.
- Added a pure replay-planning layer on top of the retained-entry boundary. The
  catch-up decision now carries both the starting and target log hashes, and the
  planner builds per-replica applied-command batches only when the retained
  payload suffix chains exactly from the lagging replica proof to the primary
  proof. Abandoned retained entries fail closed before any mutation, so the
  next production slice can apply only explicit all-applied replay plans and
  leave tombstone recovery to a later design.
- Added the first mutating catch-up path for all-applied retained suffixes. The
  storage cluster fetches primary retained payload entries in RPC-cap-sized
  batches, builds replay plans, applies each planned command to lagging acting
  set members through a distinct Peering-only metadata replay RPC, and then
  reruns the side-effect-free reconstruction gather to prove the acting set
  converged before any authority activation attempt. Normal metadata
  `apply-and-record` remains Active-only at the Unix storage-node boundary, and
  the Peering replay RPC rejects Active routes at the Unix storage-node
  boundary. The cluster catch-up entry point also requires a Peering route
  before mutation so local/direct clients cannot replay through an Active map.
  Primary retained
  abandoned tombstones still fail closed before mutation; replaying those
  entries needs a separate explicit tombstone recovery design. A focused
  end-to-end regression now covers the handoff sequence after catch-up:
  divergent heartbeat proofs reject authority activation, replay converges the
  lagging replicas, fresh Peering heartbeats report the reconstructed proof, and
  the authority persists `Active` with that proof. The cluster catch-up helper
  is also covered through Unix storage-node clients, proving the production RPC
  path can read retained payload entries, apply Peering-only replay commands on
  lagging replicas, and re-gather the converged proof. Local cluster coverage
  also forces replay across mixed replica lag distances, retry after a partial
  earlier catch-up, and multiple retained-entry RPC batches, so those
  production replay paths are part of the regression suite, not only the
  single-entry replay path.
- Added a side-effect-free cluster peering gather helper that reads the current
  acting set's replica state and pending slot state through
  `MetadataCommandNodeClient`, fetches the selected primary's retained suffix
  only when an acting-set member is behind, and feeds the reconstruction helper.
  The local route-map and storage-node Unix RPC boundaries now have explicit
  read-only peering-inspection validation, so this state collection can run
  while the PG route is `Peering` without weakening normal mutating RPC
  validation. Focused tests cover an already-converged acting set, a lagging
  replica that requires catch-up from the primary retained suffix, a Peering
  route-map gather, a stale replica epoch, a replica ahead of the selected
  primary, a Peering storage-node retained-log RPC, and a pending command that
  leaves the PG failed closed in peering.
- Extended the Peering availability fail-closed coverage for composite
  metadata listings. Object listings, object-version listings, and multipart
  upload listings now have a regression proving that one Peering metadata PG
  aborts the whole fanout operation with `PgNotActive` instead of returning a
  partial or empty result while the PG lacks complete truth.
- Extended the same Peering availability rule to point metadata reads. Object
  read snapshots and multipart-upload lookups now have regressions proving they
  fail closed with `PgNotActive` while their metadata PG is `Peering`, rather
  than authorizing against incomplete per-PG state.
- Extended the point-read coverage to bucket metadata snapshots. Single-bucket
  snapshots and two-bucket snapshot pairs now fail closed with `PgNotActive`
  when any participating bucket metadata PG is `Peering`.

Shard repair design:

- The current storage layer already has read-time EC reconstruction for placed
  segment payloads, but not yet a durable repair/backfill writer. Phase 11
  repair should build on that boundary explicitly: first identify missing or
  corrupt shards, reconstruct from at least `k` valid shards, then write the
  repaired shard through normal routed shard IO with current epoch/location
  validation rather than hiding repair as an implicit read side effect.
- Added read-time repair precondition regressions for missing and
  checksum-corrupt placed shards. A physically absent shard and a same-size
  corrupt shard now have explicit coverage proving that the read path treats the
  shard as unavailable and reconstructs the full segment from the remaining EC
  set before returning bytes.
- Added the first explicit shard-repair write primitive. The storage cluster
  can now reconstruct a segment through the existing read-time EC path, derive
  up to `m` target shards from that reconstructed segment in one encode pass,
  write them through normal routed shard IO, and refresh their shard acks in the
  authoritative data PG metadata. Focused regressions remove two physical
  shards and repair both in one call, and corrupt a same-size physical shard and
  repair it in place; both verify the repaired acks match the bytes on disk and
  then read the segment back normally.
- Added unrecoverable-shard coverage for the same boundary: when more than `m`
  shards are physically unavailable, both read-time reconstruction and explicit
  repair fail closed instead of returning partial or guessed payload bytes.
- Added repair-route fencing coverage. Explicit shard repair now has focused
  regressions proving it does not recreate a missing shard while the data PG is
  `Peering`, `Degraded`, `Backfilling`, or `Inconsistent`, and does not write
  through a stale cluster handle whose operation epoch no longer matches the
  current route map.
- Added segment-level shard repair target inspection. The storage cluster can
  now scan the full placed EC set for a segment, identify missing ack, missing
  file, wrong-size, or checksum-corrupt shards as repair targets, fail closed
  when more than `m` shards are unavailable, and return clean after the batch
  repair primitive rewrites the identified targets.
- Added a segment-level repair orchestration helper that performs target
  inspection and batch repair in one storage-cluster call. It no-ops for a clean
  segment, repairs all identified targets for mixed missing/corrupt shards, and
  keeps the same fail-closed behavior as the underlying inspection and repair
  boundaries.
- Added a read-recovery repair scheduling boundary. Normal placed-segment reads
  still use EC reconstruction on the foreground path, but when a successful
  reconstruction observes missing, wrong-size, or checksum-corrupt shards it
  enqueues deduplicated shard-repair work for later processing instead of
  repairing inline. The explicit repair primitive reconstructs without
  recursively scheduling itself, then verifies the full EC set after writing
  requested repairs so any other bad shards are queued too.
- Added coordinator-level coverage for the foreground/background repair split.
  A read-discovered checksum-corrupt shard is now tested with all background
  sweepers disabled, proving the foreground read reconstructs and returns the
  object bytes while leaving the corrupt shard file untouched, recording the
  durable repair row, and leaving only a background repair wake hint for the
  worker.
- Added focused shard-repair worker behavior coverage for retry and fail-closed
  paths. A one-shot storage-node shard-read overload now proves the worker
  records the durable claim error, waits for the retry/backoff path, then
  reclaims and drains the row once the transient failure clears. A separate
  coordinator test makes more shards unavailable than the EC set can recover,
  proving the worker records the unrecoverable repair error, leaves the durable
  row for operator/backoff visibility, and does not recreate any shard from an
  insufficient EC set.
- Made read-discovered repair scheduling durable. Successful read recovery now
  records an idempotent `PlacedSegmentShardRepairWorkItem` row in the affected
  data PG before enqueueing the in-memory wake hint and fails rather than
  dropping repair evidence if durable scheduling cannot be recorded; duplicate
  observations are coalesced, explicit repair resolves repaired rows, and the
  Unix storage-node RPC boundary exposes bounded-batch record/list/resolve
  operations for multihost workers.
- Added durable claim/retry/backoff semantics for placed shard repair rows.
  Workers acquire single-owner claims on data-PG repair rows with epoch-fenced
  owner tokens, lease deadlines, attempt counts, and `next_attempt_after`
  backoff. Failed claims preserve `last_error` while releasing ownership for a
  later retry; duplicate read/scrub observations update the observation count
  without erasing retry evidence. The local store, `StorageCluster`, Unix
  storage-node RPC boundary, and RPC payload round-trips all expose this claim,
  complete, and error flow.
- Wired background scrub findings into the durable placed-shard repair queue.
  The shard scavenger now carries stored-size/CRC proof for repairable placed
  segment references, records durable repair rows when a referenced shard row is
  missing its expected file, and emits the same in-memory wake hint used by
  read-discovered repair work. References without enough repair proof still
  suppress false orphan observations but remain observation-only.
- Clarified the checksum layering used by repair. Client/API checksums remain
  optional, but when supplied they are semantic object/part metadata and must be
  persisted and validated on reads according to S3 behavior. Internal storage
  CRC64 is separate and mandatory for data-plane stored items: direct object
  segments, stream-upload segments, multipart-part segments, whole placed
  multipart parts, and completed object parts now carry a non-optional CRC64
  used by read validation, EC reconstruction, scrub findings, and durable shard
  repair scheduling.
- Added shard-repair observability for the first worker/queue slice. The
  in-memory hint queue now reports queue depth plus queued, deduped, queue-full,
  and dequeued events; the repair worker reports durable-scan, claim, started,
  resolved-clean, repaired, failed, unrecoverable, and completion events.
  `repaired` means at least one shard was rewritten; `resolved_clean` means the
  repair/check operation found no shard write was needed. `complete_succeeded`,
  `complete_stale`, and `complete_failed` describe durable repair-row completion.
  The local debug metrics endpoint exposes aggregate `shard_repair_queue_depth`
  and `shard_repair_event_total` counters plus `shard_repair_event_by_pg_total`
  dimensions, and `scripts/uat-s3-tests` prints per-event shard-repair counts
  plus top families in the long-lived UAT metrics summary.
- Added the first background-work admission slice. Long-running process-local
  background work now shares one admission object per
  `StorageCluster::process_local_registry_key()` with RAII permits and fixed
  per-class concurrency caps. The initial classes are `KnownDamageRepair` for
  durable read/scrub-discovered shard repair, `ReclaimCleanup` for object
  payload reclaim and bucket-delete finalization, `LifecycleCleanup` for bucket
  lifecycle sweeps, `StreamSessionCleanup` for abandoned streaming sessions,
  and `OpportunisticScan` for shard scavenger integrity scans that can always
  defer because durable findings are recorded separately. Later
  backfill/migration/scanner output still needs to enter the same framework.
- Added the first adaptive admission policy hook. `KnownDamageRepair` and
  the concrete cleanup classes currently keep fixed nonzero lanes so known
  repair, reclaim/bucket finalization, lifecycle cleanup, and stream-session
  cleanup cannot starve each other behind a single shared cleanup permit.
  `OpportunisticScan` now denies with `denied_foreground_pressure` when recent
  request admission or storage-RPC admission pressure is observed, or while
  foreground request/read/write/list work is active. Process-wide
  metadata-command recovery counters are not treated as foreground pressure
  until they carry caller/class attribution. It denies with
  `denied_backlog_pressure` while durable cleanup or shard repair queues are
  nonempty. The foreground signal is held briefly based on counter deltas so
  lifetime cumulative metrics do not permanently suppress scans after a single
  old wait; deltas observed after a production-like scavenger sweep gap are
  treated as stale rather than recent pressure. The shard scavenger samples
  those pressure counters faster than the foreground-pressure hold cadence, with
  one bounded sample interval of scheduler slack, independently from the normal
  expensive scan interval. That keeps a recent wait just before a production
  scan visible without running the scan every second.
- Background-work policy rules: foreground S3 requests keep their own reserved
  capacity and must not wait behind background work. `KnownDamageRepair` should
  have a small nonzero reserved background lane because it fixes known durable
  damage. Reclaim/bucket finalization, lifecycle cleanup, and stream-session
  cleanup should each be paced but eventually make progress because they close
  already-admitted or already-visible state. `OpportunisticScan` should be the
  easiest class to deny under foreground pressure or when any durable
  cleanup/repair backlog is nonzero; denied scan work should sleep and retry
  later without recording failure. No background worker should spin if it cannot
  acquire a permit, and durable work rows remain the source of truth whenever
  admission is denied.
- Background-work instrumentation now exposes per-class admission totals,
  `denied_limit`, `denied_foreground_pressure`, `denied_backlog_pressure`,
  active counts, completed work counts, and elapsed work time through the local
  debug metrics endpoint and UAT summary. Existing shard-repair metrics still
  describe repair semantics (`started`, `resolved_clean`, `repaired`,
  `failed`, `unrecoverable`, completion events); the admission metrics describe
  scheduler behavior and must not be interpreted as successful shard rewrites.
- Remaining background-work admission order: tune the foreground-pressure
  thresholds after UAT data shows whether opportunistic scans are still making
  enough progress during quiet windows. Later backfill/migration/scanner output
  should enter through the same classes rather than adding separate
  uncoordinated loops.
- Added completed-repair outcome telemetry for shard repair. The worker now
  reports the number of shards actually rewritten on `repaired` and
  `resolved_clean` events, the local debug metrics endpoint exposes
  `shard_repair_shards_rewritten_total`, and the long-lived UAT summary prints
  that counter separately from generic repair event totals. Background scan
  cursor/progress does not need to be durable; a scanner can restart from a
  random or rotating point because any actual damaged shard it finds is recorded
  in the durable repair queue.
- Added a focused multihost UAT shard-repair smoke. The UAT runner can create an
  object through the long-lived S3 endpoint, corrupt one physical data shard on
  disk, verify that a foreground read reconstructs the object without inline
  mutation, wait for shard-repair metrics to show a real rewritten shard and
  durable completion, verify the corrupted file changed, and read the object
  again after repair. This complements the focused cargo tests with
  whole-process lifetime coverage using the same server group that UAT workloads
  exercise.
- Added a restart-shaped shard-repair UAT variant:
  `./scripts/uat-s3-tests --smoke shard-repair-restart` corrupts a shard, then
  restarts every storage-node process and the frontend before the recovery read.
  The smoke still requires the foreground read to reconstruct the object and the
  background worker to rewrite the corrupted shard, proving repair survives the
  process boundary and reopened storage-node state rather than relying only on
  the original in-memory process lifetime.
- Current repair writes replace the damaged file at the logical shard path via
  repair-specific storage-node write plumbing; normal shard writes keep their
  no-overwrite/idempotent retry behavior. A future storage-layout slice may
  instead make physical shard files generation-addressed, publish the repaired
  generation through metadata, and leave the old corrupt generation for reclaim.
  That would need schema, read-path, scavenger, and cleanup invariants and is
  deliberately left open rather than folded into this UAT coverage slice. See
  [versioned physical shard files option](versioned-physical-shard-files-option.md)
  for the standalone evaluation note.

PG backfill and migration design notes:

- Treat write placement migration and read backfill as separate concerns. New
  writes must use the current authoritative PG mapping and must place the full
  EC shard set; the system should fail closed rather than creating writes with
  missing shards. Existing reads should continue to use the segment's recorded
  data PG, EC shape, and stable shard identity plus cluster-map history to
  reconstruct the deterministic old shard locations; they should not require a
  persisted per-segment placement vector. Reads may proceed degraded when at
  least `k` valid shards are readable.
- Recommended storage capacity should be at least one node above the minimum EC
  shape so writes can continue across one unavailable node. For example, a
  `4+2` layout needs six nodes for the EC shape but should normally run with at
  least seven nodes if one-node-down writes are expected to remain available.
  If the current map cannot choose `k + m` eligible shard locations, writes
  should return a typed retryable/capacity failure while reads keep using their
  reconstructed historical placement where recoverable.
- Cluster-map and PG-history generations are still first-class for discovery
  and closure, but they are history hints rather than the main per-object
  routing mechanism. Writes should record or imply enough map-generation context
  to reconstruct placement through retained cluster-map history without
  rewriting object metadata for every acting-set change. The control plane can
  track which PG history ranges remain live because object metadata still
  references data in those ranges. Backfill can prioritize older ranges when
  safety is otherwise equal so old history can be closed off, but generation
  age alone is not a safety signal.
- A returning node has a new availability/incarnation context but may still
  hold useful shard files from older PG-history placements. Backfill against
  returned nodes should validate existing shard content and no-op where the
  old shard is already correct. New writes created while the node was down may
  still need migration/backfill if the desired current placement includes that
  node again.
- Actual safety is per placed segment, not global per generation or per node.
  Risk must be computed from the shard locations reconstructed for that
  segment from PG state and cluster-map history plus current validation
  results: `valid >= k + m` is healthy, `k <= valid < k + m` is degraded and
  should be repaired/backfilled, and `valid < k` is unrecoverable unless a node
  returns or lower-level metadata/storage repair restores shards. Node
  availability and PG-history changes can identify candidates, but they do not
  prove which objects are at risk without inspecting the reconstructed shard
  set.
- Use two related queues. A candidate queue is populated from placement
  history changes, node availability/incarnation changes, background scanner
  output, and old-history tracking. A verified repair/backfill queue is
  populated only after inspecting a segment's actual shard validity and
  computing its risk. The scheduler should prioritize verified work by
  remaining EC tolerance first, then by PG-history closure/age, then by routine
  convergence to the current placement.
- Backfill admission should reuse the background-work framework rather than
  adding an independent loop. Routine convergence should be cheap to deny under
  foreground pressure. Work that restores segments close to the EC `m` failure
  limit should receive increasing priority relative to other background work,
  while still preserving foreground S3 capacity and the rule that new writes
  never intentionally omit shards.
- Started the read-only backfill foundation by factoring placed-segment shard
  inspection into a health summary with per-shard validation state, valid shard
  counts, repair targets, and healthy/degraded/unrecoverable risk. Existing
  shard repair target selection is now a compatibility wrapper over that health
  model, so the first backfill risk semantics are exercised by current-route
  repair tests before historical placement resolution or a mutating backfill
  worker is added.
- Added the first read-only historical placement reconstruction primitive. The
  storage layer can now place a segment shard set from an explicit PG acting
  set and cluster epoch without consulting the current route state. Current
  placement still uses the existing active-route path, but this gives backfill
  planning a deterministic way to reconstruct old shard locations from retained
  PG/cluster-map history without adding per-segment placement vectors.
- Added the first control-plane lookup for reconstructed PG routes. A snapshot
  can now build a non-serving `PgRouteSnapshot` for the current or a retained
  cluster epoch, including Active PGs without treating their primary lease as
  live service authority, and storage has a route-snapshot placement helper
  that checks the route PG matches the segment data PG before placing shards.
- Added route-snapshot health inspection for placed segments. The storage
  layer can now compute the existing healthy/degraded/unrecoverable shard
  summary through a reconstructed PG route, which is the read-only verification
  step needed before a later backfill worker queues mutating repair/migration
  work from PG-history candidates. The local-cluster implementation can inspect
  historical route locations without treating the old route as current serving
  authority.
- Added the Unix storage-node boundary for non-serving historical shard
  inspection. The storage-node RPC protocol now has a dedicated historical
  shard read that carries the reconstructed `ShardLocation`, validates node/PG
  ownership without treating the historical epoch as current serving authority,
  and still verifies the expected size/CRC before returning bytes. Existing
  serving `ShardRead` and read-handle RPCs remain active-route/epoch gated.
- Added a read-only backfill planner for one placed segment. Given a verified
  source route and desired route, storage now classifies desired shard indexes
  as already present, direct-copy candidates from valid same-index historical
  shards, EC-reconstruction candidates, or unrecoverable. This is the verified
  work-item shape a later background worker can enqueue after candidate
  discovery; it does not mutate shard files yet.
- Added the first mutating backfill primitive for direct-copy targets. Storage
  can now copy valid same-index historical shards to desired-route locations
  through repair-specific write plumbing, register the copied shard acks, and
  verify the desired-route health after copying. EC reconstruction targets,
  durable queueing, and worker admission remain follow-up work.
- Extended the mutating backfill primitive to handle EC-reconstruction targets
  when the historical source route still has at least `k` valid shards. Storage
  reconstructs the segment bytes from the historical route, re-encodes only the
  missing desired-route shard indexes, registers their acks, and verifies the
  desired-route targets. Worker admission remains follow-up work.
- Added the first durable PG-store backfill candidate table. The durable row is
  segment-level and records the data PG, segment identity, EC shape, and
  source/desired cluster epochs; it deliberately does not persist an exact
  placement vector, so the later worker must reconstruct both routes from
  retained cluster-map history. The first slice supports record/list/resolve
  with coalescing and validation.
- Added durable PG-store claim/retry wiring for backfill candidates. Backfill
  rows now use a single-owner finite-lease claim, preserve rows on failed
  attempts with a retry deadline, allow expired claim stealing, and require full
  segment request identity for claim completion. Admission policy, worker
  execution, and end-to-end remote worker coverage remain follow-up work.
- Added storage-node boundary wiring for durable backfill candidates. Backfill
  record/list/claim/complete/error/resolve now flow through the
  `ShardAckNodeClient`, in-process storage nodes, Unix RPC codecs, storage-node
  server dispatch, and storage-cluster helper methods. This makes durable
  backfill lifecycle operations route through the PG primary across the
  same-process and Unix-node boundary; worker admission and execution remain
  follow-up work.
- Added the first durable backfill execution worker. Runtime maps now carry
  retained non-serving historical PG routes, storage clusters preserve those
  routes after runtime-map refresh, and the coordinator starts a shard backfill
  sweeper that claims durable rows, reconstructs source/desired routes from
  retained route history, executes the existing backfill primitive, and
  completes or retries the durable claim. This is intentionally still a simple
  single-lane worker using the existing known-damage background admission; the
  remaining follow-up is the risk/priority model that distinguishes urgent
  EC-safety backfill from routine convergence and integrates scanner output.
- Added end-to-end remote storage-node coverage for durable backfill worker
  execution. The regression test writes a segment under a historical route,
  serves the desired epoch through real `StorageNodeServer` Unix RPC endpoints,
  records a durable backfill row, runs the coordinator worker once, and verifies
  the durable row completes and the desired-route shard set becomes healthy.
  This also pinned the Unix RPC error boundary: missing historical shard reads
  now preserve `StoreError::NotFound` through a dedicated `NotFound` RPC
  error code, so desired-route gaps classify as recoverable shard health instead
  of fatal internal storage RPC failures.
- Added first-class shard-backfill observability. The worker now emits
  structured backfill events for durable scans with no claimable work, claims,
  starts, no-op resolutions, real shard writes, failures, and completion
  outcomes; the local debug metrics endpoint exposes aggregate queue depth,
  event totals, per-PG/event dimensions, and
  `shard_backfill_shards_written_total`, and UAT prints the same summary. The
  queue-depth signal uses an exact durable count rather than the bounded row
  listing API, and also counts as durable background backlog for opportunistic
  scan admission. This is instrumentation only; scheduling priority is handled
  by the later EC-risk priority slice.
- Added the first EC-risk priority signal to durable shard-backfill scheduling.
  Backfill rows now carry `remaining_tolerance`, the number of additional shard
  losses the verified source segment can survive before it becomes
  unrecoverable. Manual/routine records default to EC `m`; producers that have
  just computed a backfill plan can record the derived source tolerance. The
  durable claim selector now prefers lower remaining tolerance, then older
  source epochs for PG-history closure, then age and segment identity. Duplicate
  observations preserve the most urgent tolerance seen without changing the
  segment proof tuple. Scanner/candidate integration and more nuanced admission
  class tuning remain follow-up work.
- Added a placement-history hint to persisted payload metadata. Object
  segments, stream staging segments, committed multipart part segments,
  in-progress multipart parts, and committed object part manifest rows now
  record the `placement_cluster_epoch` used when their shard set was written.
  This is still not a per-segment placement vector: scanners and backfill
  workers must reconstruct locations from data PG, EC shape, stable shard
  identity, the recorded epoch hint, and retained cluster-map history. Streamed
  multipart parts still use their `multipart_part_segments` rows as placement
  truth; the zero-sentinel multipart part row only carries a valid epoch for
  command replay and uniform metadata shape. Scanner/candidate integration can
  now consume direct part rows and segmented rows through the same historical
  placement reconstruction model.
- Integrated shard-scavenger live-reference output with durable backfill
  candidate production. Scavenger payload references now carry the placement
  epoch for placed segment rows and routed multipart parts, and local
  reference accounting reconstructs historical placement from that epoch rather
  than treating all live payload as current-route payload. The opportunistic
  shard-scavenger sweep now deduplicates live references by segment identity
  and source epoch, verifies source and desired shard health through retained
  route history, and records durable backfill rows with the derived EC-risk
  tolerance. Reclaim-only references remain cleanup protection only and do not
  create backfill candidates. Because scanner candidate production depends on
  current route history, the coordinator shard-scavenger sweeper now resolves
  the current storage runtime-map handle each iteration instead of holding the
  initial storage cluster forever.
- Split shard-backfill worker admission by EC risk. Durable claim ordering
  still prefers rows with lower `remaining_tolerance`, but the runtime now also
  maps those claims onto different background-work classes: a source that has
  already lost tolerance (`remaining_tolerance < ec_m`) runs as
  known-damage repair, while a full-tolerance convergence row runs as routine
  backfill. Routine backfill is denied under recent foreground pressure and
  waits behind active known-damage repair, keeping low-risk PG convergence from
  competing with foreground work while still allowing urgent EC-safety backfill
  to progress.
- Added shard-backfill candidate-scan metrics. The shard scavenger's
  historical-placement candidate pass now increments aggregate counters for
  scan invocations, candidates scanned, current-epoch skips, already-complete
  candidates, durable enqueues, unrecoverable candidates, and failed
  verifications, plus separate hard scan errors. This keeps scanner discovery
  visible in the local debug metrics and UAT summary separately from durable
  worker claim/execution events.
- Bounded the shard-backfill candidate verification pass. Each opportunistic
  scanner sweep now spends a fixed budget on new historical-placement health
  checks, reports when that budget is exhausted, and skips candidates that
  already have durable backfill rows before consuming verification budget. The
  already-queued check is an exact lookup by the candidate backfill work item's
  data PG, not a bounded listing from the metadata PG being scanned. This keeps
  scanner discovery compatible with the background-work admission model without
  allowing an already queued prefix of references to starve later candidates.
  Debug metrics and UAT now expose the already-queued and limit-reached counters
  alongside the existing scan outcome totals.
- Added the first whole-lifetime UAT smoke for PG backfill migration. The
  multihost harness now has a `pg-backfill-migration` smoke that runs a
  file-backed control-plane topology, forces PG 0 from an old `1+1` acting set
  to a new one while keeping the metadata primary stable, verifies historical
  reads continue after the acting-set change, writes a new object on the desired
  placement, and waits for scanner/backfill metrics to show candidate discovery,
  durable enqueue, routine-backfill admission, shard writes, and durable
  completion. The smoke is part of `scripts/ci`. Moving a metadata primary away
  from replicas that hold the bucket/object metadata is intentionally left to
  the metadata PG migration work; this smoke isolates data-shard backfill.

Metadata PG migration and backfill design notes:

- Metadata PG migration is different from payload-shard backfill. Payload data
  can use independent source and desired routes as long as the historical source
  route remains readable and the desired route can receive the full shard set.
  Metadata PGs cannot activate an empty new acting set by reconstruction from
  object payloads; the new acting set must derive its command-log/state proof
  from an existing authoritative source or from an explicit transfer artifact.
- The safety invariant is: a metadata PG may become `Active` on a new acting set
  only from a proof derived from either an authoritative overlapping source in
  the old acting set or a validated metadata transfer artifact. Mere membership
  overlap is not enough: the overlapping source must be the active primary or a
  replica whose metadata proof satisfies the active proof floor and whose
  retained checkpoint/log suffix covers the cutover. If neither source exists,
  the PG must remain `Peering` or enter an explicit
  disaster-recovery/manual-restore path; it must not silently choose an empty or
  divergent state.
- The first implementation slice should be overlap-required. Automatic metadata
  acting-set migration should handle changes such as `[A, B] -> [B, C]`, where
  `B` is not just present in both sets but can prove current authoritative
  metadata state and carry retained command-log/checkpoint state forward. The
  normal peering catch-up path can then validate the destination acting set.
  Direct non-overlap changes such as `[A, B] -> [C, D]` should be rejected,
  deferred, or left in `Peering` until an explicit transfer primitive exists.
- Non-overlap metadata migration is still a real requirement. Large node-add or
  rebalancing events can legitimately choose desired acting sets with no common
  member even when the old replicas are healthy and readable. This is not data
  loss by itself, but it does require a transfer step before the new acting set
  can serve metadata.
- Prefer a checkpoint/log-transfer primitive for non-overlap metadata migration
  rather than bridge generations. Bridge migration, for example
  `[A, B] -> [B, C] -> [C, D]`, avoids a new transfer RPC but forces the control
  plane to invent synthetic intermediate generations that were not the intended
  durable placement. Those generations would still need durable recording,
  retry, recovery, observability, and operator explanation.
- A metadata transfer primitive should export a validated metadata checkpoint
  plus the retained command-log suffix needed to prove the state, import it
  into the desired acting set as bootstrap state, and let the destination
  acting set report peering metadata proofs from that imported state. The
  control plane should activate the PG only after the destination acting set
  agrees on the imported proof and normal peering validation passes.
- The same transfer primitive can later support manual disaster recovery. An
  operator-provided or offline-restored metadata snapshot should enter through
  the same import/proof path, with explicit operator intent and diagnostics,
  instead of a special code path that bypasses peering invariants.
- The next metadata-migration work should therefore start with focused tests
  for overlap-required migration and fail-closed non-overlap behavior, then
  define the checkpoint/log-transfer proof shape before wiring non-overlap
  migration into live acting-set changes or UAT.
- Started the overlap-required metadata migration slice. The control-plane PG
  record now carries a peering metadata proof floor whenever a previously
  active PG moves back to `Peering` due to acting-set migration, restart, or
  availability/membership change. Peering completion must satisfy that floor
  before the PG can become `Active` again, and later `Peering` acting-set
  changes preserve the floor and require an overlapping current `Peering`
  source whose proof satisfies it. Active acting-set migration now requires an
  authoritative overlapping source: an overlapping node must have a current
  `Active` observation whose metadata proof satisfies the accepted active proof
  floor. Direct non-overlap metadata migration fails closed with an explicit
  transfer-required error until the checkpoint/log-transfer primitive exists.
  Focused control-plane regressions cover overlap migration, stale peering
  proof rejection, floor-preserving Peering changes, and non-overlap rejection.
- Added the first explicit metadata-transfer proof shape at the control-plane
  layer. A transfer proof records the source cluster epoch and imported
  metadata proof separately from the normal peering proof floor, so later code
  can distinguish overlap catch-up from an explicit checkpoint/log import.
  The ordinary acting-set update remains fail-closed for non-overlap migration;
  only the explicit transfer-backed API can move a PG to a non-overlapping
  `Peering` acting set, and only when the source proof satisfies the current
  active/peering floor. The imported proof is then stored as the destination
  Peering activation floor. The transfer marker is persisted while peering,
  rejected if partial/stale/future, and cleared on activation. This still does
  not move metadata bytes or export/import checkpoint data; that remains the
  next storage-node transfer primitive.
- Wired transfer-backed acting-set changes through the live control-plane Unix
  RPC/admin boundary. The normal live `set-pg-acting-set` command remains the
  overlap/fail-closed path, while the explicit transfer-backed request carries
  the source epoch and imported metadata proof to the authority and persists
  the same peering transfer marker as the in-process API. This makes the proof
  path usable by later migration drivers and UAT without weakening ordinary
  non-overlap rejection.
- Added an explicit live control-plane fencing step for metadata transfer.
  `control-plane-fence-pg-for-metadata-transfer-live` moves an `Active` PG to
  `Peering` without changing its acting set, preserves the accepted metadata
  proof as the peering floor, and is idempotent once the PG is already
  `Peering`. This gives the storage export primitive a quiesced authoritative
  source route instead of requiring migration code to abuse a broad PG-state
  setter or race an active route.
- Split explicit metadata-transfer proof into source and imported proofs. A
  retained-log transfer may rebase command-log hashes onto the destination
  cluster epoch, so the proof that authorizes the transfer from the old acting
  set is not necessarily the same proof the destination Peering replicas will
  report. The control plane now validates the source proof against the old
  floor, stores the imported proof as the Peering activation floor, and persists
  both values in the transfer marker. Older single-proof records are still read
  as source/imported-same for focused tests.
- Added the first storage-level metadata transfer export primitive. It packages
  an authoritative source PG's metadata proof with a validated retained
  command-log prefix and fails closed if the source has a pending metadata
  command, a stale epoch, a missing retained prefix entry, a forked hash chain,
  an abandoned entry that cannot be replayed from retained command payloads, or
  a serving `Active` route. Until a metadata migration fence exists, export is
  limited to quiesced `Peering` routes so no new metadata command can be
  accepted between the proof and retained-log reads. This is intentionally
  export-only and currently covers the replayable-log bootstrap case, not full
  checkpoint import. The next slice is to define the import side: either replay
  this artifact into an empty destination PG when it contains a complete applied
  prefix, or import a real checkpoint plus retained suffix once checkpoint
  materialization exists.
- Added the first storage-level metadata transfer import primitive for
  complete retained-log artifacts. Import rebases each retained applied command
  onto the current destination `Peering` epoch, then either replays it to
  canonical empty destination PGs or adopts the rebased command-log proof over
  destination PGs whose existing materialized metadata digest exactly matches
  the transfer artifact. All destination replicas must converge on the same
  destination-epoch metadata proof. This handles the common reshuffle case
  where a destination node already has the same PG metadata from an older
  acting-set generation, while still failing closed for unrelated or forked
  materialized metadata. This path is idempotent for retry after partial import.
  It intentionally rejects empty artifacts for now because empty PG migration
  needs a state/checkpoint bootstrap RPC rather than command-log replay.
- The retained-log import primitive is not the final general reshuffle path.
  During a long-lived cluster reshuffle, destination nodes may already have a
  materialized PG store from an older acting-set generation. The current
  complete-prefix import now handles the exact-digest case, but it still does
  not perform general checkpoint/suffix reconciliation. A later migration path
  must validate whether an existing local state is a safe base for the same PG,
  with no pending commands, a proof at or below the transfer floor, and
  retained-log or checkpoint coverage to reach the target proof. Depending on
  that validation, migration can no-op, catch up from a retained suffix, or
  replace from an authoritative checkpoint plus suffix. Only unrelated or
  forked metadata state should fail closed as dirty. This keeps the current
  complete-prefix import safe while leaving the checkpoint/suffix
  reconciliation work explicit for high-volume PG reshuffles.
- Added a first live retained-log transfer admin path:
  `control-plane-transfer-pg-metadata-live` fences the source PG, exports the
  retained-log artifact from the fenced Peering primary, computes the expected
  imported proof for the next destination epoch, installs the transfer-backed
  acting set through the control plane, and imports the artifact into the
  destination Peering acting set using the exact destination runtime map
  returned by the transfer-install RPC. This is still an operator/UAT primitive
  rather than an autonomous migration scheduler. The authority rejects stale
  transfer source epochs before persisting the transfer marker, and the command
  no longer refetches an arbitrary later control-plane map between install and
  import.
- Added a focused whole-lifetime UAT smoke for metadata PG migration. The new
  `metadata-pg-migration` smoke starts a four-node control-plane topology,
  places PGs initially on nodes `0:1`, creates bucket and object metadata on a
  selected PG while keeping payload data on a different PG, transfers that
  metadata PG to the non-overlapping acting set `2:3` through the retained-log
  transfer admin path, writes new metadata there, transfers the PG back to
  `0:1`, and verifies both old and new reads after the round trip. This keeps
  metadata transfer coverage separate from the existing
  `pg-backfill-migration` smoke, which continues to isolate payload shard
  backfill.
- Tightened the fenced metadata-transfer source proof path for repeated
  migrations. A fenced `Peering` source may now authorize transfer with an
  epoch-local proof produced by writes after a prior transfer, while ordinary
  unfenced `Peering` still requires the strict stored floor. Active PG records
  also persist whether their active proof came from a metadata-transfer import,
  so lower-index destination-epoch proof acceptance remains scoped to
  transferred Active PGs; ordinary Active primary observations still use strict
  rollback/fork detection.
- Added the first retained-log prefix/base-proof path for repeated metadata
  reshuffles. Applied metadata command-log entries now persist the materialized
  metadata digest before and after that command, retained-log transfer export
  carries those pre/post state proofs, and import may replay only the retained
  suffix over an empty destination or a same-epoch destination whose log
  index/hash/digest exactly matches a rebased prefix proof. Digest-only matching
  of either the state before the first retained command or an older
  post-command prefix is not a proof and fails closed; supporting that
  round-trip shape needs an explicit base proof, checkpoint, or state bootstrap
  primitive. This keeps dirty or forked destination state fail-closed. Empty PG
  transfer still needs the same explicit state/checkpoint bootstrap primitive.
- Added the explicit empty metadata-transfer bootstrap primitive. A retained-log
  artifact with no commands can now initialize a canonical empty destination PG
  by verifying the destination has no pending metadata command, has initialize-
  eligible materialized state, and its materialized digest equals the exported
  empty proof. This avoids abusing command replay for empty PG migration while
  keeping the lower-level command adoption RPC non-empty.
- Extended retained-log import for repeated reshuffles where a destination
  already has a proven older prefix. Older-prefix adoption is allowed only when
  the destination's old cluster epoch, applied log index, applied log hash, and
  materialized digest exactly match the retained prefix proof reconstructed at
  that old epoch, and replay-state validation confirms the same proof before
  any suffix command is applied. Digest-only base matches, digest-only
  post-command prefix matches, and invented new-epoch suffixes over an imported
  materialized base still fail closed. The remaining general path is an
  explicit checkpoint/base-proof artifact that can prove a non-empty base state
  before replaying a retained suffix.
- Added a matching-state bootstrap primitive for state-only metadata transfer
  imports. When the retained-log artifact contains no commands and a
  destination PG already has the exact materialized digest named by the imported
  proof, the destination can install the imported proof tuple after validating
  there is no pending metadata command. This covers empty or already-equivalent
  state transfer without reopening the lower-level command-adoption path to
  empty command lists.
- The metadata migration UAT now reaches the harder return-transfer case:
  after `[0,1] -> [2,3]`, a write on the new acting set can require returning a
  retained suffix to `[0,1]` where those nodes still hold a non-empty
  historical base from the old epoch. Retained-log artifacts now carry the
  source base proof immediately before the transferred suffix. Import may
  bootstrap a matching destination materialized base to the destination epoch
  only when that base proof is the canonical zero-log proof `(0, 0, digest)`,
  the destination has no pending metadata command, and replay-state validation
  confirms the destination's existing old-epoch proof before any suffix replay.
  This covers the current return-transfer UAT shape while keeping general
  non-zero checkpoint/base adoption fail-closed until artifacts can carry a
  durable checkpoint proof for that base.
- Retained-log transfer artifacts can now represent a true suffix instead of
  requiring the full command-log prefix. The first retained command anchors a
  source base proof from its previous log hash and pre-state digest, and import
  rebases the suffix as a fresh destination-epoch log starting at index 1. A
  destination may replay that suffix over a non-zero historical base only when
  its existing replica proof exactly matches the artifact's source base proof
  and replay-state validation confirms that proof before any mutation. The
  destination is then checkpoint-initialized at `(0, 0, base_digest)` for the
  new epoch and the retained suffix is replayed from there. Digest-only base
  matches and forged non-zero destination proof tuples remain fail-closed.
- Added focused repeated-reshuffle coverage for retained-log metadata transfer.
  The local-cluster regression now moves one metadata PG from `[0,1]` to
  `[2,3]`, writes on the new acting set, moves it back to `[0,1]`, writes
  again, and transfers it to `[2,3]` a second time. The final leg proves that
  destination replicas with a historical prefix can adopt only the proven
  prefix and replay the retained suffix for later writes, matching the
  long-lived cluster case where a PG moves several times before checkpoint
  artifacts exist.
- Extended the `metadata-pg-migration` UAT smoke to cover the same repeated
  reshuffle shape in a live multihost process lifetime. Each iteration now
  writes on the original acting set, transfers to `[2,3]`, writes there,
  returns to `[0,1]`, writes again, transfers to `[2,3]` a second time, verifies
  all objects, and finally returns the PG to `[0,1]` before cleanup.
- Started the explicit checkpoint/base-proof artifact shape. Metadata transfer
  artifacts now classify their base as an empty base, a retained-log prefix
  base, or a checkpoint base instead of relying only on a bare base proof.
  Empty destination bootstrap is valid only for empty-base artifacts, retained
  suffix replay still requires the destination to prove the retained prefix
  before mutation, and checkpoint-base artifacts fail closed until the next
  slice adds materialized checkpoint contents plus an install/validation RPC.
  This keeps the current retained-log transfer path safe while giving the
  checkpoint import work a concrete artifact boundary.
- Added the first checked metadata checkpoint summary. A PG store can now
  export a read-only checkpoint envelope that covers the current replica proof,
  canonical metadata-state encoding version, per-table canonical digest
  statistics, and a checkpoint CRC. Export fails closed if the replica has a
  pending metadata command, if replay validation does not match the requested
  epoch, or if the recomputed table digest summary does not match the stored
  replica state digest. This still is not a restorable row payload: the next
  checkpoint slice must carry materialized row blocks, verify them against the
  summary, and add the storage-node install/validation RPC before checkpoint
  base artifacts can import instead of failing closed.
- Added materialized row blocks to the checked metadata checkpoint payload.
  Each table block now carries the canonical table identity, ordered row values,
  per-row digests, table digest statistics, and the table digest itself. The
  checkpoint verifier recomputes row digests, table summaries, the full PG
  state digest, and the checkpoint CRC from the payload, so corrupted or
  mismatched row contents are rejected before any future install path can trust
  them. This is still export/verification only: installing checkpoint contents
  into a destination PG remains deferred until the storage-node install RPC can
  validate an empty or explicitly replaceable destination and apply the row
  blocks atomically.
- Added the first local metadata checkpoint install primitive. `PgStore` can
  now install a verified checkpoint into an empty destination PG inside one
  immediate transaction: it rejects pending commands, non-empty destinations,
  PG mismatches, and tampered checkpoint payloads before mutation; then it
  clears command-owned metadata tables, inserts the materialized row blocks,
  refreshes table digests, verifies the restored full-PG digest, and moves the
  destination replica to the requested epoch as a `(0, 0, state_digest)`
  materialized base. This deliberately does not claim retained-log coverage or
  wire live metadata transfer yet. The next slice is the storage-node/RPC
  boundary for this install primitive and then checkpoint-base import wiring.
- Added the storage-node/RPC boundary for metadata checkpoint base install.
  The Unix storage-node protocol now carries a bounded checkpoint-base install
  request, validates that the destination route is a `Peering` inspection route,
  and calls the local PgStore installer under the per-PG metadata command guard.
  Local and Unix metadata-node clients expose the same staged method so later
  cluster import code can install checkpoint bases without caring whether the
  destination is same-process or a storage-node process. Focused codec and
  storage-node boundary tests cover checkpoint payload roundtrip and restoring
  parent/child metadata rows through the RPC. Checkpoint-base transfer artifacts
  still fail closed until the artifact/import path carries checkpoint contents
  and selects this install path before retained suffix replay.
- Wired checkpoint-base transfer artifacts into the storage import path. A
  checkpoint-base artifact now must carry the materialized checkpoint payload;
  import verifies the checkpoint proof, installs the checkpoint as a `(0, 0,
  state_digest)` destination-epoch base through the metadata node client, and
  keeps the existing cross-replica proof convergence check. Missing checkpoint
  payloads and mismatched proofs fail closed before mutation. This covers
  full-checkpoint metadata transfer into an empty Peering destination; exporting
  checkpoint-plus-retained-suffix artifacts from live storage-node processes is
  still a later slice.
- Added storage-node/RPC checkpoint export for metadata transfer. Metadata node
  clients can now request a verified `MetadataCommandCheckpoint` from a
  quiesced Peering route, and the cluster export helper can package that
  checkpoint as a full checkpoint-base transfer artifact for import. The
  storage-node endpoint rejects serving `Active` routes, so checkpoint export
  remains behind the existing metadata-transfer fence. Exports that would
  exceed the storage-RPC frame cap now fail as structured `ResourceExhausted`
  responses rather than surfacing as connection/write failures.
  Checkpoint-plus-retained suffix artifacts and live admin selection between
  retained-log and checkpoint export remain later work.
- Future metadata migration work must add a recovery path for
  `ResourceExhausted` checkpoint exports. The current behavior is fail-closed:
  no destination mutation occurs, but the migration cannot complete with the
  full-checkpoint primitive. Large metadata PGs need either chunked checkpoint
  export/import with a manifest and end-to-end digest verification, or a
  selection policy that can use retained-log transfer / checkpoint-plus-retained
  suffix transfer when that avoids a frame-sized checkpoint.
- The live metadata-transfer admin path now selects between retained-log and
  checkpoint export. It still prefers retained-log artifacts, but if the
  retained log is structurally present and cannot be used because the retained
  entries lack state-proof material or contain unreplayable abandoned entries,
  it falls back to a full checkpoint-base artifact. Missing or corrupt command
  log rows remain fail-closed because today's checkpoint exporter still proves
  the full command-log hash chain before trusting a materialized checkpoint.
  Recovering from true command-log compaction therefore still requires an
  explicit checkpoint/base-proof compaction model rather than this selector
  alone.
- Checkpoint-base artifacts can now carry a retained suffix after the
  checkpoint proof. Import validates the checkpoint payload, validates the
  retained suffix continuity from that checkpoint proof to the advertised final
  source proof, installs the checkpoint as the destination `(0, 0,
  checkpoint_digest)` base, and replays the rebased suffix from there. This
  gives the import side the needed checkpoint-plus-retained-suffix shape. Retry
  recognizes the checkpoint-only base, any already replayed destination-epoch
  suffix prefix, and the final imported proof, so an interrupted import can
  resume instead of trying to reinstall the checkpoint over a non-empty command
  log. Exporting such artifacts from a stored historical checkpoint, and
  selecting them to avoid full checkpoint transfer, remain later work.
- Added the export primitive for supplied-checkpoint plus retained-suffix
  metadata transfer artifacts. Given a verified checkpoint for the source PG,
  the storage cluster can now fetch retained entries from the checkpoint proof
  to the source's current proof, build a checkpoint-base artifact carrying that
  suffix, and validate the combined artifact before returning it. This still
  needs a durable historical-checkpoint store and selection policy before live
  migration can use checkpoint-plus-suffix to avoid full checkpoint transfer.
- Fixed the full-checkpoint live-transfer fallback to preserve the fenced
  source replica epoch. The fallback now reads the selected Peering source
  replica state first, requests the checkpoint at that replica epoch, and emits
  the artifact with the source/checkpoint epoch rather than the current fenced
  route epoch. This matches retained-log and checkpoint-plus-suffix export
  semantics and keeps interrupted live metadata transfer from constructing a
  destination proof from the wrong source epoch. Durable historical checkpoint
  storage and selection remain the next step before live transfer can prefer a
  smaller checkpoint-plus-retained-suffix artifact over a full checkpoint.
- Added the first checkpoint-candidate selection boundary for live metadata
  transfer. The live export selector can now consume verified historical
  checkpoint candidates for the fenced source replica, prefer the newest
  checkpoint-plus-retained-suffix artifact when the full retained prefix is not
  replayable, skip candidate-local corrupt or unusable checkpoint entries, and
  fall back to a full checkpoint only when no candidate applies. The default
  admin path still has no durable checkpoint catalogue to pass in, so
  persisting/selecting retained historical checkpoints remains the next storage
  piece.
- Added the first local durable metadata-checkpoint catalogue. Each PG store
  can persist verified checkpoint payloads keyed by source epoch, PG, log
  index/hash, and state digest, then list newest valid candidates bounded by a
  source log index. Corrupt or identity-mismatched catalogue rows are skipped
  as candidate-local failures so an older valid checkpoint can still be used.
- Exposed durable metadata-checkpoint candidates through the storage-node Unix
  RPC boundary and wired live metadata transfer export to fetch the source
  replica's newest bounded candidates after retained-log export falls into a
  checkpoint-compatible failure. The selector now uses durable checkpoint-plus-
  suffix artifacts when available and keeps the full-checkpoint fallback for
  sources with no usable catalogue entry.
- Added the first routine metadata-checkpoint scheduler. The shard scavenger's
  quiet-window pass now has a low-priority `routine_metadata_checkpoint`
  admission class that backs off under foreground pressure or durable
  repair/backfill backlog. Each admitted pass scans active metadata PG
  primaries, skips PGs whose current proof is already represented by the
  newest durable checkpoint candidate, and records a small bounded number of
  verified current checkpoint candidates through the storage-node RPC boundary.
  The record-current RPC returns only the checkpoint proof/state identity, not
  the full checkpoint payload, so a large checkpoint can be durably recorded
  without a post-mutation `ResourceExhausted` response failure. This gives
  long-lived PGs natural transfer bases before reshuffles.
- Added checkpoint-recording observability for the routine scheduler. The
  debug metrics endpoint and UAT summaries now expose checkpoint record scan
  attempts, PGs scanned, newly recorded candidates, already-current skips,
  cadence skips, inactive skips, record failures, scan errors, and limit hits,
  alongside the `routine_metadata_checkpoint` background-work admission class.
- Added the first checkpoint cadence/retention policy. Routine checkpointing
  records when no retained checkpoint exists, when the log has advanced far
  enough from the newest retained checkpoint, or when the newest retained
  checkpoint approaches the storage-RPC frame-risk threshold. Otherwise it
  skips the PG as cadence-limited. This connects directly to large-checkpoint
  recovery: long-running metadata PGs should acquire usable transfer bases
  before full-checkpoint export would become `ResourceExhausted`, while the
  durable checkpoint catalogue remains bounded to the newest candidates needed
  for checkpoint-plus-retained-suffix transfer.
- Wired checkpoint-backed metadata command-log compaction through the storage
  RPC/client/server boundary and the routine metadata-checkpoint scheduler. A PG
  store now treats the newest verified checkpoint as the replay validation base,
  reports an exclusive `compactable_before` bound from that checkpoint, and can
  delete retained command-log rows covered by the checkpoint when there is no
  pending command or retained tail. The quiet-window scheduler now attempts
  compaction after recording a checkpoint and when an existing checkpoint is
  current or cadence-limited, with the same mutation budget used for checkpoint
  records. Replay validation and checkpoint export continue from the checkpoint
  proof plus retained suffix after compaction.
- Added the first cross-epoch checkpoint retention bound. Each PG checkpoint
  catalogue now keeps only the newest checkpoint candidates within an epoch and
  the newest recent epochs for that PG. This keeps checkpoint scheduling from
  growing without bound across repeated metadata migrations while still leaving
  recent checkpoint-plus-suffix bases available for transfer retry and near-term
  reshuffles. Remaining retention work is to tune when to keep extra retained-
  log suffixes or longer-lived historical checkpoint candidates for operational
  rollback/diagnostics rather than immediate pruning.
- Added compaction-volume observability for metadata command-log retention.
  Routine checkpoint/compaction scans now report both the number of successful
  compaction operations and the total retained command-log entries deleted by
  those operations. The debug metrics endpoint and UAT summaries expose this as
  a separate counter so retention cadence can be judged by actual log shrinkage,
  not just by whether a compaction path ran.
- Hardened the metadata PG migration UAT smoke to exercise checkpoint-backed
  transfer after retention has actually run. The smoke now uses a local-debug
  hook to record and compact a checkpoint for the specific target metadata PG
  after metadata mutations, checks that checkpoint hard-failure counters did not
  advance, and only then performs a further non-overlap transfer. This avoids
  relying on opportunistic background checkpoint timing while still proving the
  whole-lifetime metadata migration path through durable checkpoint state rather
  than only the retained-log path.
- Added focused restart coverage for checkpoint-backed command-log retention.
  A local-cluster regression now records a current checkpoint on every replica,
  compacts away all covered metadata command-log rows, reopens the cluster from
  disk, and performs another metadata write. The reopened cluster must validate
  from the durable checkpoint base and allocate the next command index after
  the checkpoint instead of reusing a deleted log index.
- Strengthened the metadata PG migration UAT smoke with a process-restart
  boundary after checkpoint recording and compaction. The smoke now restarts
  the current source storage nodes, waits for the PG and runtime map to become
  serving again, verifies all objects remain readable, and only then performs
  the next non-overlap metadata transfer. This makes the checkpoint-backed
  transfer path prove durable checkpoint/catalogue reload rather than only
  continuous in-process state.
- Hardened live metadata-transfer retry after interrupted operator commands.
  Peering transfer markers now persist the quiesced source route epoch and
  source node id, and retrying the live transfer command against the same
  destination acting set re-exports from that retained source route instead of
  guessing from the current Peering route. Storage-node process configs carry
  retained historical PG routes so old source nodes can serve read-only
  metadata-transfer inspection/log export calls after they have left the
  current acting set, while transfer/import mutation RPCs remain current
  Peering-only. Retrying a live transfer whose destination acting set is
  already Active is treated as an idempotent success before fencing, with a
  bounded wait for the serving runtime map to catch up after import. The
  `metadata-pg-migration-failover` UAT smoke injects failures after fencing,
  after transfer-marker install, and after import, then proves the same live
  migration can resume and complete without re-fencing an already-migrated PG.
- Added the metadata PG migration failover smoke to the standard local CI
  script alongside the regular metadata PG migration smoke. The retry/failpoint
  path is now part of the same UAT gate as the steady-state transfer path,
  instead of relying on an explicit one-off smoke invocation.
- Started cluster-map history retention floors. The control plane now treats
  a Peering metadata-transfer marker's persisted source route epoch as a
  protected history reference: fixed-window pruning may discard older
  unprotected maps, but it must retain any map still needed to retry or inspect
  the transfer source route. Reload also fails closed if a persisted transfer
  marker names an old source route epoch whose map is missing. This covers the
  first control-plane-owned floor; storage metadata floors from object/segment
  placement epochs and durable backfill rows still need a storage-side
  reference scan/count path before history pruning can be fully data-aware.
- Added the first storage-side cluster-map history reference summary. Each PG
  store can report the oldest live payload placement epoch across object,
  multipart, and stream payload metadata, plus the oldest durable shard-backfill
  source/desired epoch. Shared storage nodes and local cluster maps merge those
  per-PG summaries by oldest epoch, giving the next slice a storage-owned floor
  to expose over the storage-node RPC boundary and feed into control-plane
  history pruning.
- Exposed that storage-side history reference summary over the storage-node
  Unix RPC/client boundary. Local cluster maps now merge summaries through the
  node-client surface rather than reaching directly into in-process storage
  nodes, so remote Unix storage nodes can contribute storage-owned history
  floors before retained cluster-map pruning is made data-aware.
- Fed storage-owned cluster-map history floors into control-plane pruning.
  Storage-node heartbeats now carry the merged history reference summary from
  the node's local PG stores; the single-authority control plane persists the
  oldest required epoch per node and protects those epochs during fixed-window
  cluster-map history pruning. This means live payload placement epochs and
  durable backfill source/desired epochs can keep the route history they need
  across repeated epoch changes and control-plane restarts, alongside the
  existing metadata-transfer source-route floor.
- Surfaced storage-owned cluster-map history floors in runtime-map diagnostics.
  Runtime-map node snapshots now carry each node's persisted storage history
  floor, and the `control-plane-runtime-map-diagnostics` admin command reports
  aggregate and per-node floor epochs without changing the existing
  `control-plane-runtime-map-ready` output consumed by UAT readiness checks.
- Wired the runtime-map diagnostics into UAT failure reporting before process
  teardown. Migration and backfill smoke failures now include the control-plane
  runtime epoch, route counts, active serving counts, retained-history floor,
  and per-node storage history floors so long-running transition failures can
  be debugged without rerunning with extra instrumentation.
- Added positive PG-backfill UAT coverage for storage-owned cluster-map history
  floors. The smoke now writes and reads an old-placement object, moves the data
  PG, and waits until control-plane runtime-map diagnostics report per-node
  storage history floors on the old acting-set nodes at or before the old
  placement epoch before relying on historical reads/backfill. In repeat runs
  the floor is a per-node minimum across all retained objects, so later
  iterations snapshot the pre-object floor and only require a new floor signal
  for old acting-set nodes that were not already retaining a sufficiently old
  epoch; current-iteration backfill is still pinned separately by per-data-PG
  completion metrics. The first iteration also restarts the control-plane
  manager after the floor is reported, waits for the target PG/runtime map to
  become serving again, and re-checks the per-node floors before historical
  reads so the smoke proves the persisted floor survives control-plane reload.
- Added the release-side retained-history regression for storage-owned floors.
  Once a storage node heartbeat no longer reports any live-placement or durable
  backfill epoch reference, the control plane clears that node's persisted
  history floor; later unrelated epoch churn can then prune the formerly
  protected old cluster map, and reload preserves the cleared floor. This keeps
  the new data-aware retention model from becoming an unbounded one-way pin.
- Next deterministic epoch-change fault-injection direction:
  - build scoped, named failpoints at existing command/RPC boundaries rather
    than sleeps or random chaos. Each hook must carry an operation/test token so
    parallel cargo tests, background workers, or unrelated requests cannot
    satisfy the wrong gate.
  - tests drive epoch changes explicitly: start an operation, wait until it
    reaches a precise failpoint, mutate the PG acting set or cluster epoch,
    release the operation, then assert it either commits exactly once, retries
    through the correct route/history, or fails closed with no visible partial
    state.
  - start with local/in-process tests for precision, then add only a small UAT
    subset for whole-process/RPC confidence. UAT hooks need an inert-by-default
    dev/test-hook surface; pure cargo tests can use `cfg(test)` hooks.
  - first operation matrix should cover normal S3 paths with the highest
    partial-state risk:
    - PUT object with an epoch change after payload shard writes but before
      metadata publish.
    - PUT overwrite with an epoch change after generation reservation/proof but
      before publish.
    - DELETE object with an epoch change after metadata command allocation or
      reservation.
    - GET/HEAD with an epoch change between metadata resolution and payload
      read, proving recorded placement/history is used or the operation fails
      closed rather than reading the wrong current placement.
    - LIST with an epoch change during metadata pagination or route refresh,
      proving results are either from a coherent command-log/materialized-state
      view or fail closed without externally visible partial listings.
    - later expand to multipart complete and tag/ACL/legal-hold/retention
      metadata mutations.
- Started the deterministic epoch-change fault-injection harness. Coordinator
  tests now have a token-scoped deterministic fault gate that lets a test wait
  for a specific operation to reach a named boundary, mutate runtime-map state,
  and then release only that operation. The first regression covers direct PUT
  at the boundary after payload shards are written/validated and before the
  direct-put metadata command is applied: it installs a newer runtime-map epoch
  while the operation is paused, resumes the request, and verifies the object is
  committed once and readable through the newer map using retained historical
  placement routes. The regression also checks the object PG metadata proof:
  the generation-reservation command is the only object-PG command applied
  before the gate, and exactly one direct-put commit command is applied after
  release. The same local pinned-route fault shape now covers overwrite and
  verifies the newer body is visible after the epoch transition, and covers
  unversioned DELETE by proving no object-PG command is applied before the gate
  and exactly one delete command is applied after release. GET now also has a
  local after-read-snapshot epoch-change regression proving the selected payload
  route remains pinned through body construction. LIST now captures one storage
  map for authorization and object listing, with a regression that swaps the
  current handle to an empty next-epoch map immediately before the storage-list
  call and still expects a complete sorted page. LIST continuation now also has
  a between-page epoch-change regression that fetches page one, advances the
  runtime-map epoch over the same materialized metadata stores, then resumes
  from the returned continuation token and verifies the remaining sorted page
  without duplicates or omissions. HEAD now uses the same
  after-read-snapshot gate as GET, proving metadata-only reads keep the
  authorized object snapshot across a current-map swap. This pins the local
  in-process/pinned-route behavior before adding stricter stale-primary and
  remote storage-node fault-injection cases.
- Started the stale-primary/fail-closed slice with direct PUT publish. A local
  storage regression now writes payload shards through the current epoch, then
  attempts to publish object metadata through a non-current operation-epoch
  handle backed by the same current map. The publish must fail at the
  metadata-primary epoch boundary, append no object-PG command, expose no object
  metadata on any acting node, release the caller-owned bucket write proof, and
  clean the unowned payload shards. This pins the cleanup and fail-closed
  behavior needed by stale-primary handling before the full older-epoch peering
  transition is modeled through control-plane/runtime-map machinery. The same
  direct PUT stale-epoch publish shape now has a remote Unix storage-node
  cleanup regression: payload writes, shard acknowledgements, bucket-write
  proof release, and payload cleanup all route through Unix clients. The stale
  operation is rejected by local route validation before the command-build RPC,
  and the regression verifies that no remote object metadata, proof, shard file,
  or ack row is left behind. Streamed PUT segment append now has the same
  remote Unix cleanup shape: after payload shards are written, a stale commit is
  rejected before command build, the in-progress stream session remains, no
  staged segment metadata or pending command is published, and the remote shard
  files plus ack rows are removed. Staged stream and multipart cleanup helpers
  now route shard-file and ack cleanup through the segment records' retained
  placement epochs rather than the caller's current operation epoch, so abort,
  completion, and abandoned pending-command cleanup can still remove shard sets
  after route refresh. The stale-primary/fail-closed slice now also covers
  unversioned DELETE through a non-current operation-epoch handle: the
  delete must fail at the metadata-primary epoch boundary, append no object-PG
  command, leave the live object and reclaim metadata unchanged on every acting
  node, and leave no pending command or bucket-write reservation behind. The
  same DELETE stale-epoch boundary is now covered through installed Unix
  storage-node clients, proving the remote object PG keeps the live object, does
  not publish reclaim metadata, and has no pending command or leaked bucket
  write reservation. The shared object metadata mutation path now has the same
  stale-epoch coverage using
  object tags as the representative tag/ACL/legal-hold/retention operation:
  non-current handles fail at route resolution, append no object-PG command,
  preserve the live object's metadata, and leave no pending command or
  bucket-write reservation behind. The tag-style metadata mutation stale-epoch
  boundary is now covered through installed Unix storage-node clients as well,
  proving the remote object PG keeps the original live metadata without tag
  rows, pending commands, or leaked bucket write reservations. Multipart
  completion now has the same local stale-epoch fail-closed coverage: a
  non-current handle cannot start the completion command, does not publish
  object metadata or completed-upload state, preserves the in-progress upload
  and selected part staging rows, and leaves no pending command or bucket-write
  reservation behind. The same multipart completion stale-epoch boundary is now
  covered through installed Unix storage-node clients, proving the remote
  object PG remains unmutated and the remote bucket PG has no leaked write
  reservation. UploadPart stream-session creation now has the matching Unix
  coverage: a direct storage-node RPC regression proves the command-build path
  rejects stale route epochs before building an UploadPart stream command,
  while the cluster-level installed-Unix regression proves the stale session
  create appends no object-PG command, preserves the in-progress multipart
  upload, and leaves no remote stream session, segment rows, pending command,
  or bucket write reservation behind. UploadPart stream finalization now has
  the same direct-RPC stale route rejection coverage and installed-Unix
  fail-closed coverage: a stale finalization leaves the active stream session,
  staged segment rows, placed shard files, and shard acknowledgements intact,
  publishes no multipart part metadata, appends no object-PG command, and leaks
  no bucket write reservation. The same installed-Unix finalization boundary is
  now covered for UploadPartCopy-shaped staging as well: copied source bytes are
  represented as multiple UploadPart stream segments, and a stale finalization
  preserves every copied staged segment row, shard file, and shard
  acknowledgement without publishing the part or leaking a bucket write
  reservation. Started the remote read-path slice with a storage-level
  installed-Unix segment read regression: payload shards are written under an
  old acting set, the frontend advances to a disjoint next-epoch acting set
  while retaining the old route, and the read succeeds only when supplied with
  the stored placement epoch. A current-route read is shown to fail closed, so
  the positive read proves retained-route historical shard inspection rather
  than accidental overlap with the new placement. The next remote read-path
  regression lifts this to the coordinator GET/HEAD/LIST path: object metadata
  stays on its serving PG, the payload data PG moves to a disjoint acting set
  in the next epoch, and storage-node access is installed through Unix clients.
  GET reads the old payload shards through the segment's recorded placement
  epoch; HEAD, object LIST, and version LIST remain available from metadata
  after the data-PG move without claiming payload-route coverage. LIST now also
  has delimiter continuation coverage across a runtime-map epoch change:
  objects are spread across object metadata PGs, the first page returns common
  prefixes with a continuation token, the map advances, and the next page must
  return the remaining prefix plus root object rather than a partial result.
  Started the
  stronger control-plane-driven stale-primary slice. Direct PUT now has a storage
  regression where a real `SingleAuthorityControlPlane` acting-set change
  removes the old object-PG primary and moves the PG into Peering. The
  in-flight old-primary publish keeps the source epoch while the local runtime
  map has advanced to the Peering epoch; it must fail closed with
  `StaleMetadataOperation`, append no object-PG command, publish no object
  metadata, leave no pending source/current command, release the old
  bucket-write proof, and delete the staged payload shard files. This shook out
  two retained-cleanup requirements: bucket-write proof release must resolve
  the proof's retained metadata route, and best-effort payload cleanup must use
  retained placement/ack routes instead of the caller's current route.
  The same direct PUT stale-primary shape is now covered through installed Unix
  storage-node clients: after the real control-plane acting-set change moves
  the object PG into Peering, the old-primary frontend fails closed, appends no
  remote object-PG command, publishes no object metadata, releases the retained
  bucket-write proof, and deletes the remote payload shard files plus
  data-PG ack rows using the source epoch routes.
  Unversioned DELETE now has the same control-plane-driven Peering shape: after
  a real acting-set change removes the old object-PG primary, the old-primary
  delete must fail closed at the stale metadata-operation boundary, append no
  object-PG command, preserve the live object, publish no reclaim metadata,
  leave no pending source/current command, and leave no bucket-write
  reservation behind. This boundary fails before the DELETE path can acquire a
  reservation, so there is also a focused deterministic hook regression that
  injects a stale-operation failure immediately after DELETE has acquired its
  bucket-write reservation. That test proves the proof was actually durable
  before the failure and is then released without appending an object-PG
  command or publishing reclaim metadata.
  The DELETE shape now also has Unix storage-node coverage for the
  control-plane-driven Peering boundary: source-epoch object metadata is written
  into remote storage-node directories, the storage-node servers refresh to the
  current Peering epoch with historical routes retained, and the old-primary
  frontend fails closed without appending a remote object-PG command, deleting
  the live object, publishing reclaim metadata, leaving pending source/current
  commands, or leaking bucket-write reservations.
  Object metadata mutation now has a
  matching control-plane-driven Peering stale-primary regression using tags as
  the representative shared metadata command shape: the old primary fails
  closed, appends no object-PG command, preserves the live object, publishes no
  tags, leaves no source/current pending command, and leaks no reservation.
  The tag-style metadata mutation shape now also has Unix storage-node coverage
  after a real control-plane Peering transition: remote source-epoch object
  metadata remains unchanged after the storage-node servers refresh to the
  current Peering epoch with historical routes retained, and the old-primary
  frontend cannot append a remote object-PG command, publish tags, leave
  pending source/current commands, or leak bucket-write reservations.
  Multipart completion now has the same control-plane-driven Peering shape: a
  staged multipart upload survives the old-primary completion attempt without
  publishing object metadata or completed-upload state, without mutating the
  object-PG command log, without losing the selected part/segment rows, and
  without leaking pending commands or bucket-write reservations.
  The multipart-completion Peering boundary now also has installed Unix
  storage-node coverage: the staged upload is written into remote source-epoch
  storage-node directories, storage-node servers refresh to the current Peering
  epoch with historical routes retained, and the old-primary completion cannot
  append a remote object-PG command, publish object or completed-upload state,
  lose selected multipart staging rows, leave source/current pending commands,
  or leak bucket-write reservations.
  UploadPart stream-session creation now has the same installed Unix
  storage-node Peering coverage: an in-progress multipart upload remains
  durable in the source-epoch remote stores, storage-node servers refresh to
  the current Peering epoch, and the old-primary UploadPart session create
  fails closed without appending an object-PG command, publishing a stream
  session or segment rows, leaving pending commands, or leaking bucket-write
  reservations. UploadPart stream finalization now has the matching installed
  Unix Peering coverage: a source-epoch active stream session and staged
  segment stay durable after the storage-node servers refresh to the current
  Peering epoch, and the old-primary finalize attempt cannot append an
  object-PG command, publish part metadata, remove staged segment rows, delete
  staged shard files or ack rows, leave pending commands, or leak a
  bucket-write reservation. The same Peering coverage now includes the
  UploadPartCopy-shaped finalization case, where copied source bytes are staged
  as multiple stream segments and must remain intact when the old-primary
  finalize attempt fails closed after the storage-node refresh.
- Started the later multipart expansion for deterministic epoch-transition
  faults. CompleteMultipartUpload now has the same local pre-metadata-apply
  gate as direct PUT and DELETE: the test pauses after the multipart completion
  has built the multipart commit but before any object-PG command is applied,
  installs a newer runtime-map epoch, releases the gate, and verifies exactly
  one multipart commit command is appended and the completed object is readable
  through the newer map. UploadPart stream finalization now has matching local
  pre-metadata-apply coverage at the `CommitStreamPart` boundary: the part
  commit pauses before object-PG apply, the runtime map advances, and the part
  finalizes exactly once before being completed into a readable object.
  UploadPartCopy now has the same deterministic `CommitStreamPart` coverage:
  copied source data is staged into the destination stream, the part commit is
  paused across a runtime-map epoch change, and the copied part commits exactly
  once before multipart completion reads back the copied payload.
  CopyObject now also has local pre-destination-commit
  coverage: the test copies from an existing source object, pauses before the
  destination `CommitDirectPutObject`, advances the runtime-map epoch, then
  verifies the copied destination is committed once and remains readable through
  the newer map. The stronger control-plane-driven Peering matrix now covers
  the CopyObject destination commit boundary as well: a live source object is
  copied into destination payload shards, a real acting-set change removes the
  old destination object-PG primary, and the old-primary destination commit
  must fail closed without appending destination metadata, mutating the source,
  leaking bucket-write reservations, or leaving copied shard files/ack rows,
  including through installed Unix storage-node clients. Serving read/list paths
  now also have explicit Peering fail-closed coverage: with the bucket PG still
  Active and only the object metadata PG Peering, GET, HEAD, object LIST, and
  version LIST must return the Peering `PgNotActive` error rather than stale
  object data or a partial listing. Version-list pagination, including
  delimiter/common-prefix continuation, now also has retained-route coverage
  across a runtime-map epoch change, matching the object-list pagination
  coverage. Object tag updates now
  have the same local pre-metadata-apply
  gate over the shared `PutObjectMetadata` command path: PutObjectTagging pauses
  before the object-PG metadata command is applied, crosses to a newer
  runtime-map epoch, then applies exactly one metadata command and returns the
  updated tags. Legal-hold updates now cover the same boundary with an
  object-lock-enabled bucket and version-specific object metadata mutation,
  proving the object-lock side path also commits exactly once across the epoch
  change. Retention updates now cover the governance retention side of the same
  object-lock metadata path. ACL uses the same storage metadata command shape,
  so it remains a lower-priority serialization variant rather than separate
  storage-level fault-injection coverage.
- Started the whole-process UAT route-change slice for ordinary S3 behavior.
  The new `./scripts/uat-s3-tests --smoke route-change` smoke runs the
  control-plane/frontend/storage-node topology, writes a versioned object whose
  payload lands on a selected data PG, changes that data PG's acting set through
  the live control-plane command, waits for serving runtime maps on the
  frontend and storage nodes, then verifies `GET`, `HEAD`, `ListObjectsV2`, and
  `ListObjectVersions` still expose the old object without partial external
  results. It also writes and reads a second object whose payload lands on the
  moved data PG after the route change, so the smoke covers retained historical
  reads plus new writes under the new placement. This is intentionally a small
  whole-process/RPC confidence check; the precise stale-primary and
  crossing-boundary assertions remain in the cargo failpoint tests.
- Added a restart-focused variant of the route-change UAT smoke:
  `./scripts/uat-s3-tests --smoke route-change-restart` performs the same
  data-PG acting-set move, then restarts every storage-node process after the
  new route is serving and before retained historical reads. The smoke then
  rechecks old-object `GET`, `HEAD`, object listing, and version listing before
  writing a new object under the moved placement. This pins the storage-node
  reload side of retained historical route installation without expanding the
  precise cargo failpoint matrix.
- Added a converging-route storage-node restart variant:
  `./scripts/uat-s3-tests --smoke route-change-node-restart` performs the same
  data-PG acting-set move, then restarts the newly added storage node before the
  target PG has finished installing the new serving route. After convergence it
  runs the same retained old-object `GET`/`HEAD`/listing checks and
  new-placement write/read checks. This covers startup ordering during route
  convergence, distinct from the existing all-node restart-after-convergence
  smoke.
- Added the frontend-restart companion smoke:
  `./scripts/uat-s3-tests --smoke route-change-frontend-restart` performs the
  same data-PG acting-set move, then restarts the frontend before retained
  reads/listing and the new-placement write. This pins fresh frontend startup
  against the control-plane runtime-map path after route history has already
  become necessary, complementing the storage-node restart smoke without
  duplicating the full precise failpoint matrix.
- Added the control-plane-restart companion smoke:
  `./scripts/uat-s3-tests --smoke route-change-control-plane-restart` performs
  the same data-PG acting-set move, then restarts the control-plane process
  before retained reads/listing and the new-placement write. This pins
  persisted control-plane route history and runtime-map reconstruction after a
  route change that already requires historical placement.
- Added the full-restart route-change companion smoke:
  `./scripts/uat-s3-tests --smoke route-change-full-restart` performs the same
  data-PG acting-set move, then restarts the control-plane, every storage-node,
  and the frontend before retained reads/listing and the new-placement write.
  This pins the combined persisted route-history path across a whole
  process-group restart without duplicating the precise cargo failpoint
  assertions.
- Added a separate whole-process correctness soak wrapper:
  `./scripts/uat-correctness-soak` composes the route-change, restart,
  PG-backfill, metadata-PG migration, storage-node loss, and shard-repair
  restart smokes with a repeat count. This is deliberately separate from
  `./scripts/uat-forced-overload`: the correctness soak is for long-lived
  multihost route-history and restart behavior without intentionally creating
  admission pressure or host-local disk contention.
- Planned final Phase 11 property-test close-out before moving on to Phase 12:
  add a control-plane epoch/heartbeat model that generates membership changes,
  acting-set changes, stale/current/future heartbeats, endpoint/incarnation
  changes, lease expiry, and peering completion, and asserts that future
  observed epochs are rejected without mutation, stale heartbeats can update
  non-serving liveness but cannot install active PG observations, serving routes
  require current observed epoch plus accepted proof and no pending command,
  active PGs always have an accepted metadata proof, and retained storage
  history floors are never persisted unless the referenced epoch is still
  retained.
- Planned metadata-transfer property coverage: generate command logs,
  checkpoints, compaction points, transfer export/import artifacts, and partial
  retry points; assert that invalid transfer/checkpoint artifacts never mutate
  the destination, final imported proof equals the expected proof,
  checkpoint-plus-suffix retry is idempotent after every prefix length,
  compaction never regresses the next log index, candidate selection skips
  corrupt or oversized candidates while preserving hard failures, and
  non-overlap metadata transfer either imports safely or fails closed.
- Planned retained-route/local-cluster trace property coverage: extend
  `prop_local_cluster_trace_preserves_epoch_route_and_cleanup_invariants` with
  route changes that retain historical routes, storage-node restart/refresh,
  retained-epoch cleanup, repair/backfill enqueue and claim, and metadata
  checkpoint ticks; assert that stale/current operations fail closed in the
  right places, cleanup uses the recorded placement epoch, failed publish and
  cleanup paths do not leak shard files or ack rows, historical reads resolve
  through retained placement, and foreground operations do not perform inline
  repair or backfill work.
- Planned pure backfill planner property coverage: generate source and desired
  shard-health vectors for `(k, m)` and assert that unrecoverable plans are
  emitted exactly when valid source shards are below `k`, direct-copy targets
  only use valid same-index source shards, reconstruction targets require enough
  valid source shards, priority/remaining-tolerance is monotonic with risk, and
  already healthy desired shards are not targeted.
- Added the first Phase 11 close-out property test:
  `prop_backfill_plan_classifies_targets_and_priority` generates EC shapes plus
  source/desired shard-health masks and pins the pure planner classification
  rules. It verifies exact target partitioning for already-present, direct-copy,
  reconstruction, and unrecoverable shards, and verifies that durable scheduling
  tolerance is derived from source risk rather than a default.
- Added the control-plane epoch/heartbeat close-out property test:
  `prop_control_plane_epoch_heartbeat_model_preserves_invariants` runs bounded
  randomized sequences of membership-ready heartbeats, current/stale/future
  observed epochs, acting-set changes, peering completion, lease expiry, and
  storage-history floor reports against the real single-authority control
  plane. It asserts future observed epochs and rejected acting-set changes do
  not mutate in-memory or durable state, stale heartbeats can update liveness
  but cannot install current PG observations, persisted node floors only point
  at current or retained epochs, current observations are epoch-scoped and in
  the acting set, and Active PGs always carry an accepted metadata proof.
- Extended the local cluster trace property with retained-route historical read
  coverage. The trace can now write staged segment payload shards plus durable
  ack rows under one data-PG route, advance the PG to a new epoch with an
  Active route that replaces one source acting-set node with a spare node while
  retaining the old route, assert the old and current route placements move at
  least one shard to a different node, and then assert
  `read_segment_payload_stored_bytes_at_placement_epoch_into` recovers the
  payload through the recorded historical placement epoch. This composes the
  retained-route read invariant with the existing randomized stale operation,
  restart, lease, repair, and command-reissue trace operations.
- Extended the local cluster trace property with routine metadata checkpoint
  ticks. The trace now runs the real
  `record_routine_metadata_command_checkpoints()` background path at randomized
  points and asserts that checkpoint recording/compaction reports no failures
  and leaves no unresolved pending metadata command slots at the current epoch.
  This composes checkpoint cadence/compaction with the same randomized stale
  operation, retained-route, restart, repair, lease, and command-reissue trace
  operations.
- Extended the local cluster trace property with durable repair queue coverage.
  The trace can now corrupt a staged segment shard with durable ack rows, read
  the segment twice through EC recovery, assert that the observed corrupt shard
  is recorded once durably with coalesced observations, and assert that durable
  repair scans enqueue the row once while subsequent scans dedupe against the
  in-memory queue. This composes repair observation/dedupe with the randomized
  epoch/state, retained-route, checkpoint, restart, lease, and command-reissue
  trace operations.
- Extended the local cluster trace property with durable backfill claim
  coverage. The trace can now write staged segment payload shards plus durable
  ack rows under one data-PG route, advance to a retained historical route plus
  a new Active route that moves at least one shard to a spare node, record the
  corresponding durable backfill row, acquire a finite-lease claim, assert a
  second worker cannot claim the row while the lease is active, and complete
  the claim. This composes backfill row ownership with randomized epoch/state,
  retained-route, checkpoint, restart, repair, lease, and command-reissue trace
  operations.
- Extended the local cluster trace property with stale direct-PUT cleanup under
  retained route history. The trace can now stage direct-PUT payload shards,
  register data-PG ack rows, advance to a new Active route while retaining the
  source route, attempt the commit through a stale source-epoch handle, and
  assert the failed commit deletes both the staged shard files and retained
  data-PG ack rows. This composes failed publish cleanup with randomized
  epoch/state, retained-route, checkpoint, repair, lease, and command-reissue
  trace operations.
- Added retained-log metadata transfer artifact property coverage:
  `prop_metadata_transfer_retained_log_artifact_preserves_valid_chain` generates
  synthetic applied command-log chains with pre/post state digests and asserts
  that the artifact base proof, final proof, base kind, retained entries, and
  rebased destination command ids/digests are internally consistent.
  `prop_metadata_transfer_retained_log_artifact_rejects_corrupt_chain` mutates
  those chains by removing state proofs, forking intermediate or final digests,
  forking retained log hashes, removing retained entries, or replacing applied
  commands with abandoned tombstones, and asserts that artifact construction
  fails closed before any import path can mutate destination metadata state.
- Added command-log compaction property coverage:
  `prop_metadata_command_log_compaction_preserves_next_index` generates bounded
  metadata command prefixes, records a checkpoint at a generated prefix, may
  continue with a retained suffix, compacts through the checkpoint, optionally
  reopens the local cluster map, and then issues one more metadata command. It
  asserts that compaction leaves the applied proof intact, reports no missing
  applied prefix, keeps `max_metadata_command_log_index()` at the durable proof
  floor even when no log rows remain, and allocates the next command after the
  compacted proof rather than reusing an old log index.
- Added checkpoint-plus-suffix metadata transfer retry property coverage:
  `prop_metadata_transfer_checkpoint_suffix_retry_resumes_every_prefix`
  generates a source command log, captures a checkpoint at a generated prefix,
  exports a checkpoint-base plus retained-suffix artifact, partially imports
  every generated prefix length on one destination replica, and then retries the
  full import twice. It asserts the retry resumes from checkpoint-only,
  partially replayed, and fully replayed destination states, converges both
  destination replicas to the expected imported proof, and preserves all
  checkpoint and suffix metadata rows.
- Added checkpoint candidate-selection property coverage:
  `prop_metadata_transfer_checkpoint_candidate_selection_picks_newest_usable`
  generates multiple source checkpoints, corrupts every candidate newer than a
  generated valid checkpoint, adds wrong-PG and ahead-of-source candidates, and
  forces retained-log export to fall back to checkpoint selection. It asserts the
  selector skips invalid newer candidates, chooses the newest usable checkpoint,
  exports the retained suffix from that checkpoint, and preserves the final
  source metadata proof.
- Closed the Phase 11 property-test close-out. The targeted property/model set
  now covers the control-plane epoch/heartbeat invariants, metadata transfer
  artifact/checkpoint retry/candidate-selection and command-log compaction
  invariants, retained-route historical read/cleanup/repair/backfill invariants
  in the local-cluster trace, and backfill planner classification/priority
  invariants. A `PROPTEST_CASES=512` run of the local-cluster trace passed
  after adding stale direct-PUT cleanup coverage. Restart/refresh after retained
  route installation is intentionally covered by the persisted route-change UAT
  smokes above rather than folded into the local-cluster trace property: that
  trace mutates route state through test-only in-memory installs, so restart
  there would mostly validate the harness rather than a production persisted
  refresh path. No further Phase 11 property-test gaps are currently tracked.

Phase 11 soak/runtime-state stabilization follow-up:

Recent Phase 11 soak runs have repeatedly found failures around `DeleteBucket`
cleanup rather than ordinary route-change read/write correctness. At least
three recent failures were in or adjacent to bucket teardown:

- route-map lease expiry during long `DeleteBucket` begin work surfaced as
  `InternalError` instead of a retryable `OperationAborted`. The immediate fix
  maps `RouteMapExpired` to retryable contention at the server-core error
  boundary, but the broader invariant is that foreground teardown work must
  either finish within its route-map validity window or return a typed retryable
  response.
- previous soak failures exercised `DeleteBucket` drain/finalizer/retry paths
  more than the steady-state object path. This is partly because the correctness
  and UAT harnesses create many short-lived buckets, but it also means
  `DeleteBucket` is now the main residual stress point for Phase 11.
- failures during test cleanup are still correctness-relevant: cleanup uses the
  same public S3 `DeleteBucket` path, so a 500 there indicates a real retry/error
  semantics bug even if user data operations already passed.

More recent failures have broadened the hardening scope. They still often show
up under cleanup-heavy UAT profiles, but the root cause is not always
`DeleteBucket` itself. Several failures have been in process-local runtime-map
state, background worker queues, checkpoint diagnostics, and retry
classification around refreshed routes. Treat these as Phase 11 correctness
bugs: route-map refresh must not lose process-local state, background workers
must stay coherent with durable metadata, and soak diagnostics must preserve
enough state to distinguish real unfinished cleanup from stale volatile queue
state.

Keep an explicit Phase 11 close-out before treating the phase as soak-clean:

1. status: partial. Audit all `DeleteBucket`-begin and finalizer error mappings so stale routes,
   stale metadata primaries, pending-command displacement, route-map expiry, and
   storage-node overload consistently become `OperationAborted` or `SlowDown`,
   not `InternalError`. Bucket write-reservation conflict/not-found outcomes are
   now classified with the other metadata-command contention cases so a
   competing drain or recovery worker cannot leak an internal reservation state
   to clients instead of the retryable conflict surface;
2. status: partial. Audit foreground work budgets against route-map validity deadlines. Any
   synchronous `DeleteBucket` loop that can run close to the validity window
   must either refresh/retry the whole operation from a fresh storage-cluster
   snapshot or return a typed retryable response before the map expires;
3. status: open. Prefer whole-operation retry boundaries over mid-operation route-map swaps:
   when authorization and begin-delete are tied to a pinned storage snapshot,
   retry by re-running authorization plus begin-delete with a fresh snapshot and
   bucket identity check, rather than continuing a partially completed decision
   on a different route map;
4. status: completed. Add a focused regression for `DeleteBucket` begin crossing route-map expiry
   or using an already-expired route map, ideally through the coordinator/S3
   boundary rather than only the low-level mapper;
5. status: partial. Add deterministic failpoint tests for the precise cleanup interleavings soak
   has been sampling: route-map refresh during begin-delete, storage-node
   restart between drain acquisition and mark-deleting apply, finalizer restart
   with an already-deleting bucket, and stale frontend retry after the bucket was
   recreated;
6. status: completed. Add finalizer-side regression coverage for stale route-map and stale
   metadata-route outcomes. DeleteBucket begin and asynchronous finalization have
   different correctness boundaries, so both need explicit retry/error
   semantics coverage;
7. status: completed. Add or extend a bounded UAT cleanup stress profile that creates versioned
   buckets across many metadata/data PGs, deletes all object versions, and then
   repeatedly calls `DeleteBucket` while route maps refresh and storage nodes
   restart;
8. status: completed. Add metrics/assertions to distinguish expected retryable cleanup pressure
   from real bugs: count `OperationAborted`/`SlowDown` cleanup retries, route-map
   expiry retries, finalizer queue depth, and any `InternalError` during
   cleanup;
9. status: partial. Make the soak harness preserve enough DeleteBucket context on failure:
   failing bucket name, bucket metadata PG, current route-map epoch/valid-until,
   finalizer queue depth, pending bucket command, durable delete drain row, and
   a short recent flight-event slice for the same request id;
10. status: completed. Check the public cleanup helpers used by `s3-tests` and UAT. They should
    retry AWS-compatible retryable responses, but they must not hide server
    `InternalError`; local 500s during cleanup are bugs and should continue to
    fail the run with enough diagnostics;
11. status: open. Update [`delete-bucket-reservation-classification-plan.md`](delete-bucket-reservation-classification-plan.md)
    if the audit shows synchronous begin work is still too conservative. The
    optimization should remain secondary to correctness: `DeleteBucket` must not
    return success while visible object/MPU state can still appear, but it should
    avoid long foreground waits when a safe `BucketNotEmpty` or retryable
    response is already knowable;
12. status: completed. Replace stringified remote `StorageRpc` retry classification with typed
    storage RPC error-code propagation through `StoreError`, so server-core can
    classify stale route/epoch/PG-state and command-contention responses without
    parsing display text;
13. status: completed. Audit runtime-map refresh construction for every process-local state holder,
    not just request-visible route data. Refreshes must preserve reclaim queues,
    shard repair/backfill queues, metadata-command recovery single-flight state,
    per-PG command locks, and background/admission registry identity when they
    replace only the serving route map for the same process;
14. status: completed. Add deterministic regressions for runtime-map refresh while background work
    is queued, dequeued but not finished, pending due to durable rows, and
    returning retryable errors. Cover both plain in-process clusters and the
    Unix storage-node-client path used by UAT;
15. status: partial. Reconcile volatile queue state with durable metadata in tests and
    diagnostics. A bucket-delete outstanding item should not survive indefinitely
    when there is no bucket row, no finalizer claim, no object rows, and no
    reclaim root. If this state appears, the worker should clear it or the
    diagnostic should identify it as a volatile queue/runtime-state bug rather
    than an unfinished durable cleanup. The reclaim queue metrics now split
    active-bucket `BucketDeleteBegin` retry queue depth from
    `BucketDelete` finalizer queue/outstanding depth, so soak output can
    distinguish a stuck begin retry from stuck final deletion instead of
    reporting both as one bucket-delete queue. The cleanup-versioned-stress
    health gate now also requires the begin retry queue to drain, not only
    finalizer outstanding depth;
16. status: completed. Extend failure preservation for cleanup/runtime-state soak failures to dump
    durable state, not only public S3 visibility: bucket row, finalizer claim,
    bucket write drain, pending command slot, object rows, object reclaim roots,
    payload reclaim claims, current route-map epoch/valid-until, and queue
    depths for the current runtime generation. A local-only debug endpoint now
    exposes a routed bucket-PG snapshot for a named bucket: route-map
    epoch/operation epoch/valid-until/primary node id, current bucket row
    state/generations when present, active durable bucket write drain when
    present, the current bucket-PG pending metadata command slot and whether it
    targets the named bucket, the current bucket-delete finalizer claim row for
    the bucket PG and whether it targets the named bucket, plus the durable
    `DeleteBucket` attempt outcome/progress row including outcome, phase,
    post-reservation frontier, and detail. It also reports one routed object
    version/delete-marker sample per object PG, plus one validated
    bucket-scoped object payload reclaim root per object PG, with object PG id,
    key, generation id, reclaim kind, creation time, and item count where the
    reclaim command row is still present. It also reports any active durable
    object-payload reclaim claim per object PG, including the claimed work
    identity, lease, attempt count, and last error without exposing the worker
    owner token. This lets a failing soak run inspect the bucket-PG attempt
    state and durable bucket-owned object/reclaim blockers directly without
    relying only on public S3 visibility. The route-change, PG backfill,
    metadata-PG migration, and cleanup-versioned-stress UAT cleanup wrappers
    now query that endpoint when their cleanup command fails, before teardown
    removes the live debug surface. No specific durable-state debug field gap is
    currently known for bucket-delete soak failures;
17. status: open. Define a repeatable soak gate for closing this hardening phase. At minimum,
    `cleanup-versioned-stress --repeat 30`, the correctness soak, and one
    restart/failover cleanup variant should pass without HTTP 500s, stuck
    bucket-delete finalizers, unexplained `OperationAborted` growth, or stale
    volatile queue state.
18. status: open. Rework `DeleteBucket` begin from repeated full-cluster rediscovery into a
    durable `DeleteBucket` attempt state machine. The current durable delete
    drain records only ownership/lease identity, and an error path rolls that
    drain back, so every retry must rescan all object PGs, stream sessions, and
    pending-command state from zero. That is correct but does not prove forward
    progress under route churn or high PG counts. A retryable foreground
    `OperationAborted` should mean the current HTTP request did not complete the
    delete attempt, not that the delete attempt was abandoned. Keep the durable
    fence/attempt alive across retryable failures, make later client requests
    and background workers adopt or advance the attempt, and make other
    operations that encounter the temporary fence either help converge required
    pending work, return a retryable conflict, or observe the terminal deleting
    state. The attempt should carry durable phase/progress state, such as the
    current object PG/frontier, stream-cleanup phase, reservation-drain phase,
    and final visibility-check requirement, so later retries resume from the
    unfinished work instead of restarting the whole proof. Record terminal
    attempt outcomes durably or in bounded diagnostic history even when no
    client request is still waiting: successful `MarkBucketDeleting`, failed
    not-empty proof with the blocking source, stale bucket generation/recreate,
    and retryable/internal failure context should all be visible to debug
    tooling and test hooks. The protocol must preserve the Phase 9.4 rule that
    no admitted writer can publish after `DeleteBucket` has decided the bucket
    is empty.
    - staged implementation:
      1. status: started. Add the durable attempt semantics before adding per-PG frontiers:
         retryable foreground failures leave the active drain/attempt in place,
         and later `DeleteBucket` calls can adopt it for the same bucket
         generation. Terminal not-empty/stale-generation outcomes still clear
         the temporary fence. The first slice preserves leased active delete
         drains across retryable begin failures, lets later requests adopt them,
         and clears stale-generation drains by exact identity.
      2. status: partial. Record bounded attempt outcome/debug state on the durable bucket-PG
         authority: incomplete retryable context, terminal not-empty blocker,
         stale generation/recreate, and successful `MarkBucketDeleting`. The
         current slice adds a single-row-per-bucket durable outcome record on
         the bucket PG, exposed through local and Unix storage-node clients, and
         records retryable preserved attempts, terminal not-empty blockers,
         stale-generation drain rollback, and successful `MarkBucketDeleting`
         best-effort from `DeleteBucket` begin. This is diagnostic state only;
         background adoption and resumable proof frontiers are still tracked by
         later items.
      3. status: completed. Add a background worker path that advances the same state machine used
         by foreground requests rather than inventing a second cleanup path. The
         current slice queues a distinct background `BucketDeleteBegin` reclaim
         item when foreground begin work preserves a retryable active drain, and
         the work item carries bucket execution/incarnation generation fences so
         the worker re-enters the normal storage `begin_bucket_delete_if_current`
         path for that exact bucket identity. Dequeued begin work is retained
         across early retryable route/map failures with a cooldown instead of
         being dropped or hot-looped, stale roots are dropped when the bucket is
         deleted/recreated, and a successful background begin enqueues the
         normal bucket-delete finalizer work. Explicitly queued foreground
         handoffs carry live preserved attempts; durable reclaim scans are
         limited to expired/orphaned delete-begin drains so they cannot steal a
         live foreground drain and race the original caller. Those scans work
         over the Unix storage-node RPC boundary used by UAT and enqueue the
         same generation-fenced `BucketDeleteBegin` root after process restart
         or volatile queue loss once the abandoned drain lease has expired.
         Finalizer work remains a separate `BucketDelete` queue item.
      4. status: partial. Add durable proof progress/frontiers for the expensive all-object-PG
         exact-bucket drain once the attempt can already survive and be adopted.
         The current slice stores a typed
         `post_reservation_next_object_pg_id` on the durable attempt row and
         uses it only for the post-reservation exact-bucket object-PG drain,
         after the bucket write drain has fenced new writers and the durable
         write reservations have emptied. The pre-reservation convergence pass,
         reservation-wait helper pass, and abandoned-stream cleanup pass remain
         conservative full scans because admitted writers or stream cleanup can
         create object-PG work behind any earlier frontier. Entering stream
         cleanup must durably reset the frontier to zero before cleanup
         proceeds, so retries cannot treat the pre-cleanup proof as covering
         newly created abort-stream work. The frontier is trusted only when the
         stored drain id, cluster epoch, and bucket execution generation match
         the adopted active delete drain. The durable outcome row now also
         carries a typed begin phase (`initial`, reservation wait,
         post-reservation object drain, stream cleanup, final visibility check,
         or mark deleting), so retryable/not-empty diagnostics no longer need
         to infer the failed phase from free-text detail. Deterministic storage
         coverage now verifies the frontier identity fence, reset persistence,
         resumed scanning from the persisted frontier, and phase recording,
         while metadata/RPC tests verify the fields round-trip through storage
         and Unix RPC. End-to-end storage coverage now proves a foreground
         `DeleteBucket` can persist a partial post-reservation frontier, fail
         retryably, preserve that frontier when recording the retryable outcome,
         and let the generation-fenced `BucketDeleteBegin` worker entry point
         adopt the active drain and resume from the later object PGs. A
         coordinator-level reclaim-worker regression now also drives the actual
         background thread through a foreground attempt that persisted a partial
         post-reservation frontier and then failed retryably.
         The durable attempt row is now also updated before entering the final
         visibility check, preserving the reset post-reservation frontier and
         exposing `final_visibility_check` as the current retryable phase while
         the check is still only started/in progress. Once that check completes
         without visible data, the attempt row advances to
         `final_visibility_proven`; a retryable failure after that point can be
         adopted with a fresh budget and resume directly at `MarkBucketDeleting`
         instead of paying for another all-PG visibility proof. A focused
         storage regression observes the proven phase before command-id
         allocation and then verifies the terminal `mark_deleting` outcome
         overwrites it. Adoption treats a matching retryable
         `final_visibility_check` attempt row as a conservative resume cursor:
         if the bucket PG has no pending command, it skips the already-proven
         exact object-PG drain, stream cleanup, reservation wait, and
         post-reservation drain phases and re-enters at the final visibility
         proof. It treats a matching retryable `final_visibility_proven` row as
         stronger evidence and skips the visibility proof too. Storage
         regressions install failing post-reservation and final-visibility hooks
         and prove the two adoption points reach `mark_deleting` without
         repeating the wrong scan. Stream cleanup now has a
         conservative adoption point as well: entering cleanup records a
         retryable `stream_cleanup` attempt row, and a later matching adoption
         can skip the initial pre-cleanup exact-bucket scan while still
         re-running stream cleanup, reservation wait, post-reservation object-PG
         validation, and final visibility. A storage regression verifies that
         stream-cleanup adoption still runs the post-reservation validation
         before reaching `mark_deleting`; a coordinator-level reclaim-worker
         regression now seeds the same durable phase and proves the actual
         background `BucketDeleteBegin` worker adopts it, revalidates the
         post-reservation object-PG drain, and reaches deleting/finalized
         progress. Remaining work: decide whether finer stream-cleanup
         page/frontier state is needed, or whether the current conservative
         cleanup resume point is enough for soak closure. Final-visibility
         adoption now also has coordinator-level reclaim-worker regressions that
         seed durable `final_visibility_check` and `final_visibility_proven`
         attempts and fail if the worker repeats an already-proven phase instead
         of resuming at the right point.
         A route-change-restart soak failure at `a3ce3c64` showed another
         final-visibility boundary issue: the foreground request completed the
         final visibility scan after its begin budget was already exhausted,
         then installed `MarkBucketDeleting` and immediately failed applying it
         because the shared proof/apply budget had no time left. The begin path
         now checks the begin budget again after final visibility and before
         allocating/installing the mark command, so an over-budget request
         preserves the durable final-visibility-proven cursor instead of
         dirtying the command slot. Later route-change-node-restart soak
         failures at `7e9d95d5` showed this cursor still needed a distinct
         proven phase: both failed buckets were publicly empty, with no MPU,
         object, reclaim-root, finalizer-claim, or pending-command blocker, but
         the active drain was stuck at `final_visibility_check` after budget
         exhaustion. The phase split now lets a completed visibility proof
         resume directly at mark-deleting. Once a matching mark command exists,
         applying it uses a fresh command-apply budget with separate diagnostics
         (`bucket_delete_mark_deleting_apply`), because at that point the system
         should advance the irreversible command rather than fail immediately on
         already-spent proof budget.
      5. status: open. Revisit reservation classification only after attempts are resumable;
         it should be an optimization on top of a convergent state machine, not
         the convergence mechanism itself.

Status update:

- The first error-mapping pass now treats stale route/epoch/PG-state failures,
  route-map expiry, remote storage-node route-state errors, and remote metadata
  command contention as retryable `OperationAborted` at the server-core storage
  boundary. Storage-node overload remains `SlowDown`.
- Added coordinator-level regressions for `DeleteBucket` using an already
  expired route map, for `DeleteBucket` begin using a stale metadata route, for
  `DeleteBucket` begin crossing route-map expiry after durable drain
  acquisition, for bucket-delete finalization using an expired route map, and
  for bucket-delete finalization using a stale metadata route. The finalizer
  command-contention regression remains in place.
- A route-change-full-restart soak failure showed storage-node heartbeat refresh
  can wedge when a future-epoch metadata pending slot survives without a local
  terminal log entry while the durable replica state remains at an older epoch.
  Heartbeat now detects and removes only those epoch-mismatched orphan pending
  slots before reporting the node's durable proof; if a local terminal log entry
  exists for the slot, the node still fails closed instead of silently ignoring
  the log.
- `DeleteBucket` begin/finalize foreground loops now explicitly check the pinned
  route map validity deadline at their loop gates, so a route map that expires
  while cleanup is in progress returns a typed retryable route-map expiry
  instead of relying only on the next metadata-primary lookup to notice it.
- The shared `s3-tests`/UAT `delete_bucket_retrying_operation_aborted` cleanup
  helper now keeps retry counts and, on final failure, dumps bucket-visible
  diagnostics from `HeadBucket`, `ListObjectsV2`, `ListObjectVersions`, and
  `ListMultipartUploads`. This does not hide local `InternalError`; it makes the
  failure report show whether cleanup still saw live objects, versions,
  delete markers, or MPUs when `DeleteBucket` failed.
- Added a storage-level regression for restart between delete-drain acquisition
  and `MarkBucketDeleting` apply. Reopening with an active pre-mark delete drain
  now proves `DeleteBucket` returns retryable metadata-command contention
  quickly, preserves the active drain, and leaves the bucket Active until the
  drain can be retried or expire-recovered.
- Added a bounded `uat_pg_backfill_smoke cleanup-versioned-stress` profile for
  soak harnesses to run during route-map refresh and storage-node restart
  cycles. It creates versioned buckets spread across the configured bucket
  metadata PGs, writes multiple versions for keys selected across object
  metadata/data PGs where possible, and then exercises the shared versioned
  cleanup plus `DeleteBucket` helper. The profile is wired into
  `scripts/uat-s3-tests --smoke cleanup-versioned-stress` and the correctness
  soak.
- Extended the standard UAT failure metrics summary with `http_500_response_total`
  and cleanup-specific log-match counts for route-map expiry, generic 500
  request-error evidence, and bucket-delete failure flight events. This keeps
  backend-neutral `s3-tests` unchanged while making UAT soak failures report the
  counters needed to separate expected retry pressure from local cleanup bugs.
- Tightened `scripts/uat-s3-tests --smoke cleanup-versioned-stress` so it now
  records pre-workload retry/error counters, fails if HTTP 500s increase, waits
  for `bucket_delete_finalize_outstanding_depth` to drain, and prints
  retry-pressure deltas for `OperationAborted`, `SlowDown`, request errors, and
  bucket-delete finalizer queue/outstanding depth.
- Added a typed `metadata_command_checkpoint_record_error_by_pg_total` debug
  metrics family and UAT summary output for checkpoint-record hard failures and
  stale-route skips. This keeps route-race evidence such as unknown PGs,
  inactive routes, route-map expiry, metadata-command contention, and
  transfer-route inspection races visible in ordinary soak logs instead of
  requiring a second run with deep tracing.
- Hardened bucket-delete finalizer claim cleanup across pinned route-map
  expiry. The finalizer now releases a claim through the same
  bucket-write-reservation client that acquired it, and the coordinator
  regression covers expiry after claim acquisition returning
  `OperationAborted` while leaving the claim releasable for a fresh retry.
- Hardened runtime-map refresh to preserve process-local runtime state across
  route-map replacement. Commit `36c6a4d1` carries `LocalClusterRuntimeState`
  and the process-local registry key forward when constructing refreshed storage
  clusters, preventing reclaim/finalizer queues and worker/admission state from
  being tied to throwaway route-map generations. The storage regression proves a
  bucket-delete finalizer queued before refresh is still visible, dequeueable,
  and finishable after refresh. Follow-up regressions now also cover the shard
  repair hint queue, the Unix storage-node-client refresh constructor, the
  per-PG metadata-command lock registry, and active metadata-command recovery
  single-flight state, so every current `LocalClusterRuntimeState` field has
  deterministic refresh-preservation coverage.
- Trimmed redundant synchronous `DeleteBucket` begin work on the empty/no-pending
  path. The live stream-upload blocker check and abandoned stream-upload cleanup
  now share one pre-reservation-wait scan, preserving the expired direct-PUT
  stream proof cleanup that releases durable reservations. The extra
  exact-bucket all-PG drain before the visibility check is skipped unless
  post-wait stream cleanup actually aborted sessions. This reduces the
  foreground work that caused cleanup-only failures to exhaust the begin budget,
  but it is not the durable proof-progress protocol called out in item 18.
- A route-change-restart soak failure showed the pre-mark `DeleteBucket` proof
  can legitimately outlive the durable delete-drain lease under route churn:
  two attempts reached final visibility, then failed at `MarkBucketDeleting`
  with `stale bucket delete drain before mark deleting`. Adopted delete drains
  now renew their lease immediately, and the begin loop heartbeats the drain
  before long stream-cleanup and visibility-check phases rather than waiting
  until command construction. The storage regression advances logical time
  during an adopted pre-mark proof and verifies the original drain identity is
  preserved while its lease is extended before the bucket reaches `Deleting`.
- Remaining close-out work is concentrated in deterministic interleaving tests,
  cleanup/runtime-state diagnostics, process-local state continuity across
  refresh/restart, and checking whether any foreground loops still need an
  earlier retry boundary before route-map validity expires.

Exit criteria:

1. stale primaries cannot accept writes after an epoch change
2. PGs enter peering before serving unsafe requests
3. repair restores missing shards from available EC data
4. migration can move a PG acting set without per-object metadata rewrites
5. command-log retention bounds long-running disk growth without breaking
   restart, peering, or repair correctness
6. failure-injection tests cover primary loss, replica loss, restart, and repair
7. deterministic epoch-transition tests prove that stale frontends, stale
   primaries, and operations crossing epoch changes either converge through the
   command log or fail closed without duplicate mutation, leaked reservations,
   orphaned visible payloads, or partial list results
8. a real-multihost correctness soak runs separately from the single-host
   overload soak and exercises restarts, route changes, and injected RPC/storage
   failures without relying on host-local disk contention to find bugs
9. targeted Phase 11 property/model tests cover control-plane epoch/heartbeat
   invariants, metadata transfer/checkpoint/log retry and compaction
   invariants, retained-route cleanup/history invariants, and backfill planner
   classification/priority invariants

## Phase 12: Replicated Control Plane

Replace the Phase 11 single-authority, file-backed control-plane manager with a
real replicated control plane. Phase 11 already established the state shape,
fencing rules, runtime-map publication path, retained history, metadata-transfer
markers, and storage-node heartbeat contract. Phase 12 should preserve those
semantics while making the authority itself replicated and linearizable.

Because bucket metadata is now PG-sharded, this control plane should remain
small. Its initial authoritative scope should be:

1. cluster map
2. node membership
3. cluster epoch
4. PG count
5. PG state
6. PG acting sets

Bucket rows should not move back into the global service unless there is a
separate design decision to reverse the bucket-metadata-sharding model.

Detailed work items:

Phase 12.1 starting slice:

- keep the initial command/state-machine split inside the `storage` crate,
  using a new module/file rather than a new crate. The boundary still depends
  directly on `ClusterControlSnapshot`, PG/node ids, metadata-transfer proofs,
  heartbeat records, and control-plane errors, so a crate split would create
  churn before the command model is stable. Revisit a crate split only after
  the command encoding, replay, and OpenRaft integration points have settled.
- introduce explicit `ControlPlaneCommand` values for the durable, linearized
  state changes. The initial set should cover bootstrap/admin membership
  changes, PG acting-set changes, peering completion, metadata-transfer
  fence/install, and durable heartbeat state such as node incarnation,
  endpoint, PG observations, and storage history floors. High-frequency
  heartbeat lease renewal remains separate until its leader-local/read-index
  semantics are designed.
- introduce a deterministic command-apply boundary whose inputs are a
  `ClusterControlSnapshot` and a `ControlPlaneCommand`, and whose output is a
  new snapshot plus a typed response. Any timestamp that can affect persisted
  state must be committed in the command itself. The apply layer must not
  perform file I/O, Raft I/O, RPC, or apply-time clock reads.
- adapt `SingleAuthorityControlPlane` to call the command-apply boundary while
  preserving the Phase 11 API and file-backed test harness. This keeps the
  single-authority implementation as the compatibility oracle for the later
  replicated implementation.
- add replay/snapshot tests at this boundary before wiring OpenRaft: command
  sequence replay equals the final snapshot, corrupt/incompatible command
  decode fails closed, retained-history floors and metadata-transfer fences
  survive replay, and cluster epochs remain monotonic and derived only from
  applied commands.
- after this boundary is in place, run the OpenRaft spike against the command
  enum and snapshot type. The spike should prove append/apply, snapshot
  install, restart with durable term/vote/index metadata, leader-only
  linearized runtime-map reads, and not-leader redirect/error behavior before
  replacing production control-plane paths.

Phase 12.1 progress:

- Added an explicit versioned byte codec for `ControlPlaneCommand` inside the
  storage crate. The codec covers the current durable command variants,
  appends and verifies a per-command CRC64, rejects unknown magic/version/tags,
  enforces bounded collection lengths before allocation, rejects trailing bytes,
  reports decode failures separately from RPC framing failures, and has
  round-trip, degenerate-value, malformed, semantic-decode-error, and corrupted
  payload tests. This gives the OpenRaft spike a concrete command-log payload
  without adding a new production serialization dependency or relying solely on
  consensus-log integrity checks.
- Added the matching versioned, CRC64-protected `ClusterControlSnapshot` byte
  wrapper for snapshot install/restart. It reuses the existing canonical
  control-plane state text and parser for invariant validation, rejects corrupt
  or incompatible snapshot frames before parsing, and tests that a decoded
  snapshot can continue applying commands identically to the original state.
- Added a dependency-free replicated state-machine adapter around the command
  boundary. It tracks committed log term/index metadata outside the canonical
  state payload, rejects non-contiguous indexes or term regressions before
  mutating state, builds snapshot artifacts with the last-applied log id, and
  installs CRC-checked snapshot artifacts without changing state on corrupt
  input. This gives the OpenRaft spike a small append/apply/snapshot contract
  before introducing the dependency.
- Tightened snapshot artifact install so a valid but stale artifact cannot roll
  back an already-applied state machine. The adapter rejects missing, lower, or
  term-incompatible last-applied log ids before decoding the payload, and has
  tests for both same-position reinstall and forward snapshot catch-up. The
  remaining trust boundary is intentionally left with the consensus layer:
  OpenRaft must supply a snapshot payload whose internal state matches its
  claimed last-included log id.
- Made committed deterministic command rejection an explicit replicated
  state-machine outcome. Consensus/storage log-position errors still fail
  before mutation and do not advance `last_applied`, but a semantically
  rejected committed command now records a rejected log outcome, leaves the
  snapshot unchanged, and advances `last_applied` so replay and the later
  OpenRaft adapter cannot silently stall.
- Added an explicit runtime-map freshness proof to `ClusterRuntimeMapSnapshot`
  and the Unix control-plane RPC format. Single-authority live reads now carry
  the authority incarnation and issue timestamp, reconstructed/diagnostic maps
  are marked separately, and malformed proof tags fail closed. This keeps the
  Phase 11 runtime-map path working while giving the OpenRaft read-index or
  lease-read integration a concrete field to populate with term/index/freshness
  semantics later.
- Added a low-level linearized authority interface split for the next
  integration layer. `ControlPlaneLinearizedCommandSink` submits already-built
  durable `ControlPlaneCommand` values and
  `ControlPlaneLinearizedRuntimeMapSource` names the serving runtime-map read
  path that must later be backed by OpenRaft read-index or lease-read
  freshness. The current single-authority implementation satisfies both traits
  and has focused tests for command persistence and freshness-proof reads,
  while existing Phase 11 operation-level traits remain unchanged.
- Extended the runtime-map freshness proof with a dependency-free
  read-index-shaped variant carrying authority incarnation, committed
  control-plane log id, and issue timestamp. The Unix RPC codec round-trips
  this variant and rejects zero read-index values, so the OpenRaft spike can
  populate a stable proof shape without changing the runtime-map frame again.
- Connected the dependency-free replicated state-machine adapter to that proof
  shape. It can now build a runtime map with a read-index freshness proof only
  when the requested control-plane log id exactly equals the state machine's
  current `last_applied` id, and rejects unapplied, future-term, or historical
  read ids with a typed error. The OpenRaft spike must therefore stamp runtime
  map proofs with the actual reflected `last_applied` id at serve time, not a
  captured read-index round id that the state machine may have already applied
  past. `ControlPlaneLogId` now rejects zero term as well as zero index so
  Raft-shaped proof points cannot encode reserved values.
- Inspected the current OpenRaft 0.10 API for the first integration spike
  without adding it as a production dependency yet. The concrete version
  inspected was `openraft 0.10.0-alpha.26`; the spike should pin that exact
  release, use `default-features = false` to avoid pulling in `clap`, and add
  only the runtime feature needed by the integration, most likely `tokio-rt`.
  OpenRaft's split maps cleanly onto the boundary already introduced here:
  `RaftLogStorage` owns durable vote, log, committed index, append/truncate/
  purge, and reader visibility; `RaftStateMachine` owns committed-entry apply,
  applied membership, snapshot build/install, and current snapshot exposure.
  The state-machine adapter should wrap deterministic semantic rejections in
  the application response rather than returning them as `apply()` errors, so
  OpenRaft can advance `last_applied` for rejected committed commands. Linear
  runtime-map reads should use OpenRaft `ReadPolicy::ReadIndex` first and stamp
  the returned map with the state machine's actual applied log id after waiting,
  preserving the exact-proof contract above. The spike must also implement
  OpenRaft's optional `save_committed`/`read_committed` path or an equivalent
  startup gate, because OpenRaft documents that a transient state machine can
  otherwise restart behind a previously observed committed read until recovery
  catches up.
- Added the first compile-time OpenRaft bridge in the storage crate. The bridge
  declares the control-plane Raft type config using `ControlPlaneCommand` as
  application data, a response type that can represent applied or deterministic
  rejected outcomes, OpenRaft's `BasicNode`, advanced `(term,node_id)` leader
  ids, and cursor-backed snapshot data. It also pins explicit conversions
  between OpenRaft log ids and `ControlPlaneLogId`, including rejection of
  reserved zero term/index values. This is intentionally type/log-id wiring
  only; no production control-plane path is served through OpenRaft yet.
- Added the first in-memory OpenRaft state-machine wrapper around the
  dependency-free replicated adapter. It applies OpenRaft blank and membership
  entries as committed no-ops that advance `last_applied`, applies normal
  `ControlPlaneCommand` entries through the deterministic command boundary,
  maps semantic rejections into the application response instead of an apply
  error, and builds/installs cursor-backed snapshots while preserving the full
  OpenRaft log id and membership metadata. Snapshot metadata generation fails
  closed if the CRC-protected artifact's `ControlPlaneLogId` cannot be matched
  to the wrapper's full OpenRaft `(term,node_id,index)` log id, avoiding a fake
  leader node id. Snapshot install validates rollback and same-position
  conflicts against the full OpenRaft log id before mutating state, and rejects
  snapshot membership metadata that is invalid, future, or not included in the
  installed snapshot's last log id. This wrapper is still integration
  scaffolding and does not serve any production control-plane path.
- Added an OpenRaft `RaftSnapshotBuilder` adapter for the in-memory wrapper.
  Snapshot builder creation captures a point-in-time snapshot view, so later
  committed entries do not change the snapshot returned by the builder.
- Implemented OpenRaft's async `RaftStateMachine` trait for the in-memory
  wrapper. `apply` drains OpenRaft's stream of
  `EntryResponder<ControlPlaneRaftTypeConfig>` values, applies each committed
  entry through the deterministic wrapper, sends each per-entry response
  through the optional responder, and returns stream/protocol failures as
  `std::io::Error`. Deterministic semantic command rejection remains an
  application response, so a committed rejected command still advances
  `last_applied` rather than stalling replay. This required adding
  `futures-util` as a direct storage dependency because OpenRaft's trait
  signature exposes `futures_util::Stream`.
- Added an in-memory OpenRaft `RaftLogStorage`/`RaftLogReader` adapter for the
  spike. It keeps vote, committed log id, purged boundary, and contiguous log
  entries behind a shared reader-visible handle; rejects append/read holes and
  invalid purge/truncate boundaries; calls OpenRaft's flush callback after
  appended entries are visible; and implements the optional
  `save_committed`/`read_committed` path so restart-gating semantics are
  represented in tests. The adapter also models OpenRaft's special
  initialization membership entry at `(term=0, index=0)` separately from
  `ControlPlaneLogId`, so Raft cluster formation can proceed without making
  zero a valid durable control-plane command index. This is intentionally
  scaffolding only: it does not choose the production durable Raft-log format
  or its fsync/batching policy.
- Tightened the in-memory OpenRaft log store's committed-index boundary. The
  `save_committed` path now rejects committed watermarks that point at a
  missing, future, mismatched, regressing, or cleared log id, and
  `truncate_after` and `purge` reject unknown targets or movement past the
  committed point. This keeps the restart gate from recording a committed read
  watermark that the local log cannot actually prove. This is intentionally
  stricter than OpenRaft's generic log-store conformance baseline, which permits
  operations such as `truncate_after(None)` in cases where Argmin has already
  recorded a committed restart gate; the current spike does not run
  `openraft::testing::log::suite` against this guarded in-memory store.
- Tightened the in-memory OpenRaft log store's durable-vote boundary.
  `save_vote` now rejects vote regression according to OpenRaft's `Vote`
  partial-order contract, including lower terms, lower same-term leader ids,
  and attempts to replace a committed vote with the same uncommitted vote. This
  keeps the spike store aligned with OpenRaft's rule that a node grants and
  persists only votes greater than or equal to the last vote it has seen.
- Added restart-shaped validation for the in-memory OpenRaft log store. The
  spike store can now export an opaque restart artifact carrying vote,
  committed watermark, purged boundary, and remaining contiguous entries, and
  rebuild a fresh store from that artifact only if the restored state satisfies
  the same append, purge-boundary, and committed-watermark invariants. Focused
  tests cover continuing appends after restoring a purged committed log, and
  reject malformed artifacts with entries at the purged boundary, log holes,
  future committed watermarks, mismatched committed log ids, or committed
  points before the purged boundary. The committed-watermark path also requires
  restored or live vote metadata whenever a committed watermark is present, and
  rejects votes whose full OpenRaft leader id `(term,node)` does not cover the
  committed log id, so durable restart state cannot lose or roll
  current-term/vote metadata behind an already committed entry. Purge now also
  requires a committed restart gate, and restart artifacts with a purged
  boundary but no committed watermark are rejected, so truncation metadata
  cannot describe discarded log entries that are not backed by a durable
  committed point. This is still not a production durable Raft-log format; it
  pins the restart invariants the durable implementation must preserve.
- Tightened in-memory OpenRaft state-machine restart construction. The wrapper
  constructor now rejects restart state where the dependency-free command state
  and the full OpenRaft applied log id disagree on term/index, where the
  OpenRaft wrapper claims a non-bootstrap log id but the command state has no
  applied command, where the command state has an applied command but the
  wrapper has no log id, or where stored membership metadata is invalid or not
  included in the applied point. The command state intentionally does not store
  OpenRaft's leader node id, so the full `(term,node,index)` identity remains
  supplied and persisted by the OpenRaft metadata layer; the wrapper validates
  the term/index portion it can prove from the inner state.
- Added restart-shaped validation for the in-memory OpenRaft state-machine
  wrapper. The spike wrapper can now export an opaque restart artifact carrying
  the dependency-free command state, full OpenRaft applied log id, and applied
  membership metadata, then rebuild only through the checked constructor above.
  Focused tests cover restoring after applied and deterministically rejected
  committed commands, continuing replay from the restored point, and rejecting
  malformed artifacts with missing wrapper log ids, phantom wrapper log ids, or
  membership metadata beyond the applied point. The cached current snapshot is
  intentionally not part of this artifact because it is rebuildable from the
  authoritative state-machine state and OpenRaft metadata.
- Added a combined in-memory OpenRaft restart artifact for the spike. It
  captures the log-store and state-machine restart artifacts together and
  restores them only after cross-validating their restart-gate invariants: an
  applied state-machine log id must still be provable from the retained or
  purged log metadata, must not be behind the purged boundary, and must not be
  ahead of the persisted committed watermark. This permits a restarted state
  machine to lag a committed watermark only when the retained log still has the
  catch-up entries, and rejects independently-valid artifact pairs that would
  lose committed work or serve from an uncommitted state.
- Tightened the in-memory OpenRaft state-machine wrapper's apply cursor. Before
  dispatching blank, membership, or normal entries, the wrapper now rejects
  first-entry gaps, duplicate/replayed bootstrap entries, lower-term next
  entries, and lower same-term leader ids at the full OpenRaft
  `(term,node,index)` boundary. The dependency-free command adapter still
  enforces the inner `ControlPlaneLogId` sequence for normal/no-op entries, but
  the wrapper now also protects OpenRaft metadata-only entries and the
  `(term=0,index=0)` bootstrap bypass.
- Added OpenRaft adapter overflow regressions. The state-machine wrapper now
  has explicit coverage for refusing any entry after `u64::MAX` without moving
  `last_applied`, and the in-memory log store has coverage for refusing an
  append after a restored `u64::MAX` purged boundary without changing the log
  watermark.
- Added an OpenRaft-shaped runtime-map read-index helper on the state-machine
  wrapper. The wrapper now requires the proof input to exactly equal the full
  applied OpenRaft log id `(term,node,index)` before delegating to the
  dependency-free command state-machine's `ReadIndex` proof builder, and
  rejects invalid bootstrap/zero read indexes, unapplied indexes, and
  same-term/index leader-node mismatches. This keeps the eventual OpenRaft read
  adapter from accidentally publishing a freshness proof for a log point that
  is only partially represented by the inner `ControlPlaneLogId`.
- Added an adapter-facing runtime-map helper that always stamps the proof with
  the wrapper's current applied OpenRaft log id. A regression covers the
  standard read-index race where a read arrives at an earlier committed round
  but the state machine has applied further by serve time: the stale captured
  id is rejected, while the current reflected tip is accepted and published as
  the proof point.
- Added a single-node OpenRaft initialization smoke over the in-memory log
  store and state-machine wrapper. The test uses a deliberately unreachable
  network factory, constructs an actual `Raft` instance, calls
  `Raft::initialize`, and verifies that OpenRaft's `(term=0, index=0)`
  bootstrap membership entry is persisted and reflected as the effective
  membership. This pins the previous index-zero compatibility issue at the
  public OpenRaft API boundary; production networking, durable Raft-log
  storage, and multi-node replication remain future slices.
- Added a restart smoke that takes the combined in-memory restart artifact
  through OpenRaft's public `Raft::new` path. The test restores a log store
  whose committed watermark is ahead of the state machine, verifies OpenRaft
  replays the retained committed entries during startup, and checks that the
  effective membership and local committed id advance to the committed
  watermark without relying on election or client-write timing.
- Added a companion restart replay smoke for deterministic command rejection.
  OpenRaft startup now replays a committed semantic rejection through the
  in-memory state-machine wrapper and verifies that both the full OpenRaft
  applied log id and the dependency-free control-plane `last_applied` advance
  through the rejected entry. This pins the no-stall contract on the recovery
  path, not only on direct state-machine apply.
- Added a public OpenRaft startup smoke for current-snapshot recovery. The test
  starts `Raft::new` with an empty transient state machine that exposes a
  cached current snapshot, then verifies OpenRaft installs that snapshot before
  serving state-machine reads and reflects the committed watermark and
  bootstrap membership without replaying the retained prefix.
- Tightened OpenRaft snapshot metadata identity. Generated and installed
  snapshot IDs now must match the full OpenRaft last log id
  `(term,node,index)`, with a distinct empty-state ID, so empty snapshots,
  bootstrap membership snapshots, and same-index snapshots from different
  leader ids cannot collide in adapter metadata.
- Added a public OpenRaft read-index smoke for the non-leader path. With
  elections disabled and no learned leader, an initialized two-node membership rejects
  `ensure_linearizable(ReadPolicy::ReadIndex)` with `ForwardToLeader` instead
  of permitting a local runtime-map read. This pins the fail-closed read
  boundary until the production adapter wires leader-side read-index serving.
- Added the matching public OpenRaft read-index leader smoke for the
  single-node deterministic authority. The test lets OpenRaft's initialization
  path elect the single voter, waits for this node to expose a committed local
  leader vote, applies a bootstrap command, and verifies the runtime-map
  freshness proof is stamped with the state machine's applied log id rather
  than an earlier captured round id.
- Added an adapter-facing OpenRaft read-index runtime-map helper that performs
  the same sequence as a single operation: wait for `ReadPolicy::ReadIndex`,
  fail closed if the state-machine read is behind the returned barrier, and
  publish the map using the current applied tip. The leader and non-leader
  public OpenRaft smokes now exercise this helper directly.
- Added a deterministic public OpenRaft `client_write` smoke for a single-node
  authority by disabling timer-driven elections, letting OpenRaft's
  initialization path elect the single voter, and waiting for a committed local
  leader vote rather than sleeping or relying on randomized election timeouts.
  The smoke proves both an applied bootstrap command and a deterministic
  semantic rejection return through OpenRaft's client response path, and
  verifies the rejected command advances the applied cursor without creating
  node state.
- Added an adapter-facing OpenRaft command-submit helper that maps
  `client_write` into an Argmin-shaped committed command outcome. Applied
  commands and deterministic semantic rejections both return as data with the
  OpenRaft log id, while OpenRaft API failures remain transport/consensus
  errors. The public OpenRaft smokes now exercise applied, rejected, and
  not-leader submit paths through this helper.
- Added a thin `ControlPlaneRaftAuthority` wrapper around the OpenRaft handle
  so the spike has a single authority-shaped object for linearized command
  submission and read-index runtime-map publication. The wrapper still uses
  the in-memory OpenRaft stores and test network, but callers no longer need
  to assemble the submit/read helper sequence directly.
- Added an authority status snapshot on the OpenRaft wrapper that reports the
  local node id, learned leader, last log id, committed id, and state-machine
  applied id. The single-node public smoke now verifies those fields after an
  applied command followed by a deterministic semantic rejection.
- Added authority lifecycle methods for OpenRaft membership initialization,
  initialized-state checks, and shutdown. The public OpenRaft smokes now use
  the wrapper for lifecycle operations, leaving raw `Raft` access only for
  white-box assertions and test-only leader waits.
- Added an explicit async linearized authority trait boundary for the OpenRaft
  wrapper. The Phase 11 `ControlPlaneLinearized*` traits remain synchronous for
  the local/file-backed authority, while the Phase 12 OpenRaft path exposes
  awaited command submission and read-index runtime-map reads without hiding a
  runtime block inside the interface. A public smoke now exercises the wrapper
  through those trait objects.
- Added a test-only in-memory OpenRaft network factory that forwards
  append/vote/pre-vote/snapshot RPCs between registered local `Raft` handles.
  A two-node public smoke now initializes membership through node 101, waits for
  the initialized leader state, submits a control-plane command through the
  leader, and verifies the follower applies the same command state.
- Added a two-node OpenRaft read-index runtime-map smoke over the same
  in-memory network. After a replicated control-plane command reaches the
  follower, the leader serves a linearized runtime map through
  `ControlPlaneRaftAuthority::linearized_runtime_map_snapshot`, and the test
  verifies the freshness proof is stamped with the leader state machine's
  actual applied tip.
- Added a two-node OpenRaft semantic-rejection smoke. A deterministic
  `UnknownNode` command now returns as a rejected client-write outcome, still
  advances the replicated applied cursor on the follower, and leaves the
  follower's control-plane state unmutated.
- Added a two-node OpenRaft membership-change smoke. After an initial
  control-plane command commits the bootstrap membership, the leader shrinks the
  voter set through the `ControlPlaneRaftAuthority` lifecycle wrapper, and the
  test verifies the leader's effective membership and state-machine
  `last_membership` agree on the final membership log id. The removed follower
  is not required to apply the final removal entry after OpenRaft has removed it
  from the voter set.
- Extended the OpenRaft authority status/wait boundary. The wrapper now exposes
  applied-index waiting and reports both OpenRaft's effective voter set and the
  state machine's applied voter set, so public smokes can verify replicated
  membership progress through the authority object instead of raw OpenRaft
  state reads.
- Added an OpenRaft learner lifecycle wrapper and smoke. A three-node in-memory
  cluster now adds node 603 as a learner through `ControlPlaneRaftAuthority`,
  waits for the learner to apply the learner-membership entry, promotes it into
  the voter set, and verifies both the leader and promoted node expose the final
  voter set through the authority status boundary.
- Added deterministic OpenRaft leader-transfer coverage. The in-memory network
  now forwards transfer-leader RPCs, the authority wrapper can request
  leadership transfer and wait for a learned leader, and a two-node smoke proves
  the old leader rejects a post-transfer command while the new leader commits
  and replicates the next command. The same smoke now also proves the old leader
  cannot publish a read-index runtime map after transfer, while the new leader
  publishes a serving map stamped with the post-transfer applied log id.
- Isolated the public OpenRaft smokes with distinct cluster names so the normal
  parallel `control_plane_raft` test group cannot share OpenRaft test identity
  while still exercising the real client-write and read-index paths.
- Added OpenRaft follower-restart catch-up coverage. A three-voter in-memory
  cluster now captures a follower restart artifact, removes that follower from
  the test network, commits a command through the remaining quorum, restores the
  follower, and verifies a later write drives the restarted follower through the
  missing committed prefix with the expected Raft membership and control-plane
  node-state effects.
- Added OpenRaft follower snapshot catch-up coverage. The spike now captures a
  stale follower artifact, commits a command and waits for the live follower to
  apply it, forces the leader to build and purge a snapshot covering that
  command, restores the stale artifact with test-only log reversion enabled,
  and verifies the next write brings the restarted follower current through the
  leader's snapshot transfer path rather than retained-prefix replay. This also
  pinned the log-store restart-gate rule for snapshot install: a purge through a
  snapshot watermark may advance the committed restart gate to keep the purged
  boundary restart-consistent.
- Extended the OpenRaft authority status boundary with the current state-machine
  snapshot watermark and pinned it in the snapshot catch-up smoke. This moves
  the Phase 12 diagnostics surface closer to the planned leader/term,
  committed/applied, membership, and snapshot-index report without exposing
  private state-machine internals to callers.
- Extended the same OpenRaft authority status boundary with the attached
  log-store purged watermark when the authority owns a log-store handle. The
  snapshot catch-up smoke now verifies both the leader and restarted follower
  report the snapshot-covered purge boundary through the public status surface.
- Extended that public status surface with the attached log-store's persisted
  OpenRaft vote and derived current term. Single-node command submission and
  snapshot catch-up smokes now pin the leader/term diagnostics without exposing
  raw store internals to callers.
- Added OpenRaft leader-restart resume coverage. A three-voter in-memory
  cluster now captures the leader's persisted OpenRaft/control-plane artifacts,
  removes the leader from the test network, verifies a follower cannot accept a
  linearized write while the leader is absent, restores the leader, and proves
  the restored leader resumes command submission, follower replication, and
  read-index runtime-map service from the committed prefix.
- Added deterministic OpenRaft post-transfer leader-loss coverage. A
  three-voter in-memory cluster now transfers leadership to a surviving voter,
  removes the old leader from the test network before the next write, commits a
  control-plane command through the transferred leader, and verifies the
  remaining follower applies the same node-state transition without losing the
  previously committed cluster map. Normal dead-leader election remains tied to
  OpenRaft's leader-lease/election-timeout semantics rather than a timer-free
  trigger.
- Added OpenRaft promoted-voter restart coverage. A learner promoted into the
  voter set now exports and restores its persisted OpenRaft/control-plane
  artifacts, verifies the restored authority still reports the final effective
  and applied voter set through the status boundary, and catches up a
  post-restart command under the promoted membership.
- Extended OpenRaft membership-removal coverage. The removed voter first becomes
  the serving leader and successfully serves both a read-index runtime map and a
  client write. After that leader is removed from the control-plane membership,
  the removed authority fails closed for both linearized runtime-map reads and
  client writes, while the surviving voter can continue committing under the
  reduced voter set and reports the retained membership boundary through the
  authority status surface.
- Extended the stale-leader lifecycle coverage in the OpenRaft authority
  wrapper. After deterministic leadership transfer, the old leader now fails
  closed not only for linearized runtime-map reads and client writes, but also
  for voter replacement and learner addition requests.
- Extended the OpenRaft authority status surface with effective and applied
  learner sets. The learner lifecycle smoke now proves the added learner is
  visible as a learner through both the Raft effective membership and the
  applied state-machine membership before promotion, and that promotion clears
  the learner set while exposing the final voter set.
- Extended the OpenRaft authority status surface with the canonical
  state-machine cluster epoch. The restarted-leader resume smoke now proves the
  recovered leader's diagnostic epoch matches the runtime map epoch it serves
  after committing a post-restart command.
- Extended the OpenRaft authority status surface with compact control-plane
  state diagnostics: authority incarnation, retained-history count and epoch
  bounds, and the oldest storage-reported history floor. The restarted-leader
  resume smoke now pins these fields against the served runtime-map proof and
  retained route history after post-restart epoch changes.
- Extended the same status surface with PG-state counters for the canonical
  state machine. The restarted-leader resume smoke now verifies the recovered
  leader reports the expected PG count and Peering/Active/degraded/backfill/
  inconsistent breakdown after replaying the post-restart command.
- Extended the OpenRaft authority status surface with replicated storage-node
  membership and availability counters. The restarted-leader resume smoke now
  verifies the recovered leader reports the expected active storage-node set
  and Suspect/Unavailable breakdown after replaying a post-restart node
  availability command.
- Extended the PG diagnostics on the same status surface with deterministic
  sub-state counters for assigned active primaries, peering metadata-transfer
  markers, and metadata-transfer fences. These deliberately avoid wall-clock
  lease checks, so the authority status remains a replay-safe view of
  replicated control-plane state.
- Extended the status surface with raw persisted lease-deadline diagnostics:
  counts and min/max deadline values for storage-node heartbeat leases and
  metadata-transfer fence source leases. These are reported as replicated state
  facts only; serving freshness and lease validity remain outside this
  replay-safe status path until the Phase 12 monotonic-clock lease-read design.
- Extended the OpenRaft authority status surface with the local OpenRaft server
  state, so diagnostics can distinguish a node that knows the current leader
  from a node that is itself Leader, Follower, Learner, Candidate, or shutting
  down. The single-node, leader-transfer, and restarted-leader smokes now pin
  the role transitions through the public authority boundary.
- Added derived OpenRaft authority status flags for local leadership, local
  effective/applied voter and learner membership, and whether the local node is
  currently expected to serve the linearized authority path. These are
  diagnostic facts only; the actual serving path remains OpenRaft
  `client_write` and `ReadPolicy::ReadIndex`.
- Added a compact linearized-authority readiness classification to the same
  status surface. It reports `Serving`, `NotLocalLeader`, or
  `NotEffectiveVoter` from the OpenRaft leadership and effective-membership
  state, so diagnostics do not need to reverse-engineer the serving reason from
  several booleans. This remains a status/readiness view only; request serving
  still goes through OpenRaft command submission and read-index barriers.
- Extended the OpenRaft authority status boundary with compact log-index
  accessors and signed committed/applied and log/committed gap diagnostics. The
  single-node command smoke now verifies a rejected committed command advances
  last-log, committed, and applied positions together, while the current-snapshot
  recovery smoke verifies restored purged, committed, applied, and snapshot
  indexes through the public authority status view.
- Added an object-safe authority status trait and folded it into the linearized
  OpenRaft authority boundary alongside command submission and runtime-map
  reads. The trait smoke now exercises command, read-index runtime map, and
  readiness/status access through a single boundary that later admin/readiness
  wiring can depend on without naming the concrete OpenRaft wrapper.
- Added a cloneable OpenRaft authority handle around the object-safe
  linearized authority boundary. The handle keeps command submission,
  read-index runtime-map reads, and status/readiness access behind an
  `Arc<dyn ... + Send + Sync>` dependency, so the next RPC/admin wiring can
  pass authority capabilities around without coupling callers to the concrete
  OpenRaft wrapper or test-only raft construction details.
- Hardened the two-node membership-change smoke so it no longer relies on a
  leader removing itself from the voter set. The removed node still proves it
  served a linearized read and write before removal, but leadership is handed
  back to the surviving voter before committing the removal. The same smoke now
  bounds each OpenRaft-facing operation so a stalled membership/read/write
  future fails with the operation label instead of hanging the whole suite.
- Added an object-safe OpenRaft authority admin boundary and cloneable admin
  handle for lifecycle and membership operations: initialization, initialized
  checks, voter replacement, learner addition, leadership transfer, applied/
  leader waits, and shutdown. A bounded smoke now drives leadership transfer
  and voter replacement through that handle, so future RPC/admin wiring can use
  an authority capability instead of naming the concrete OpenRaft wrapper.
- Added a composed OpenRaft authority service boundary and cloneable service
  handle that combines the linearized command/read/status surface with the
  lifecycle/admin surface. The mixed admin smoke now drives bootstrap command
  submission, leadership transfer, voter replacement, status inspection, and
  shutdown through that single object-safe capability, matching the shape an
  RPC-facing control-plane service can expose.
- Added an object-safe OpenRaft authority service directory boundary for
  node-id based service lookup. A test-only in-memory directory now routes
  bootstrap, leadership transfer, linearized runtime-map read, and shutdown
  through service handles resolved from the directory, giving future RPC/admin
  client routing a capability-level boundary without exposing the concrete
  OpenRaft wrapper.
- Added the cloneable OpenRaft authority service directory handle, matching the
  existing linearized/admin/service handle pattern. The directory smoke now
  routes through that handle, so future RPC/client code can receive a directory
  capability without depending on the concrete directory implementation.
- Removed the earlier status-derived leader route on the OpenRaft authority
  service directory after adding the directory-owned serving-authority selector.
  Direct and routed reads now use the same fail-closed directory decision, so a
  stale observer status cannot choose a serving capability.
- Renamed the routing handle's internal route helper from "current leader" to
  "current serving authority" so the code-level boundary matches the selector:
  operations route through the directory's serving-capability decision, not
  through an observer's current-leader status field.
- Added a cloneable OpenRaft authority routing handle that composes an observer
  service with the service directory. Linearized command submission and
  runtime-map reads now have a capability-level client surface that asks the
  directory for the current serving authority and executes the operation on
  that service. Its status view is routed to the same serving capability, with
  the observer-local status exposed only through an explicit observer method;
  the directory smoke proves the same client continues routing correctly after
  leadership transfer.
- Extended that routing handle with explicit leader-routed admin helpers for
  operations that require an initialized current leader: leadership transfer,
  voter replacement, and learner addition. Cluster initialization remains
  outside this routed-leader contract because there is no current leader to
  route through before bootstrap. The directory smoke now drives leadership
  transfer and voter replacement through the same routed client used for
  linearized writes, reads, and status.
- Split the initialized-cluster admin surface into a narrower object-safe
  leader-routed admin trait and cloneable handle. Full authority admin still
  owns bootstrap, wait, and shutdown operations, while post-bootstrap
  membership/leadership operations can be passed to RPC/client wiring as a
  capability that is safe for the leader-routing handle to implement. The
  directory smoke now proves that narrow handle can transfer leadership and
  replace voters through the routed client.
- Added a composed object-safe routed-authority trait and cloneable handle for
  post-bootstrap clients that need linearized command/read/status access plus
  leader-routed membership operations. The directory smoke now drives a routed
  command, read, status, and voter replacement through this single capability,
  while bootstrap remains on the full service/admin surface.
- Added an object-safe status-list capability to the OpenRaft authority service
  directory. This gives future RPC/admin wiring a narrow cluster-status surface
  that can enumerate known raft authorities without naming the in-memory test
  directory or the concrete OpenRaft wrapper; the directory smoke now verifies
  the status list after leader transfer.
- Split that status-list capability into its own cloneable handle, matching the
  existing command/admin/service handle pattern. Future diagnostics and admin
  clients can now depend only on the status-list surface when they do not need
  service lookup or routed command execution.
- Added a directory-owned current-serving-authority route helper. Leader-routed
  clients now ask the directory to derive exactly one serving authority from
  the status list instead of trusting a single observer's current-leader view,
  and the route fails closed if the directory reports no serving authority or
  multiple serving authorities.
- Pinned the current-serving-authority selector's deterministic failure modes:
  it now has direct coverage for selecting the sole serving authority,
  rejecting an empty serving set, and rejecting directory-key/status-node
  mismatches before any routed RPC is attempted.
- Added a directory helper that narrows the selected current-serving authority
  to the post-bootstrap routed-authority capability. This lets callers ask the
  directory for command/read/status plus leader-routed membership access without
  receiving bootstrap, wait, or shutdown methods. The directory smoke now uses
  that narrow capability for direct serving status and runtime-map reads.
- Split the in-memory directory smoke scaffold so node registration stores a
  full service capability and a separate routed-authority capability. The
  routed client is now built from the narrow routed-directory handle directly,
  while bootstrap and shutdown still go through the full service-directory
  surface.
- Tightened the routing handle to consume that narrow current-serving
  routed-authority capability internally for post-bootstrap command, read,
  status, and leader-routed membership operations. The full service lookup
  remains available from the directory for bootstrap/lifecycle callers, while
  routed clients now avoid depending on the broader service surface.
- Added a routed-authority directory capability and cloneable handle. Clients
  that only need post-bootstrap routed command/read/status and membership
  operations can now look up a node or the current serving authority without
  receiving the full bootstrap/lifecycle service surface; the existing service
  directory adapts to this narrow view for in-memory spike wiring.
- Tightened the routing client to depend on the routed-authority directory
  rather than the full service directory. Bootstrap and shutdown lookup still
  use the full service surface, but post-bootstrap routed clients no longer
  carry a capability that can fetch bootstrap/lifecycle services.
- Narrowed the routing client's observer dependency to a status-only authority
  handle. Routed command/read/status and membership operations already use the
  directory-selected serving routed authority, while observer-local diagnostics
  no longer require handing the client bootstrap, wait, or shutdown methods.
- Split bootstrap/lifecycle operations from leader-routed membership admin
  operations at the authority trait boundary. The full admin handle still
  composes both surfaces for callers that need it, while tests now exercise a
  lifecycle-only handle for initialization checks, applied-index/leader waits,
  and shutdown.
- Added an object-safe lifecycle-directory capability for explicit node
  lifecycle lookup. The full service directory still exists for bootstrap/full
  service callers, but tests now resolve lifecycle-only handles for
  applied-index waits, leader-observation waits, and shutdown instead of
  carrying command/read/status capability into those paths.
- Split raft bootstrap/init from node lifecycle wait/shutdown at the trait
  boundary. The existing broad lifecycle handle remains as a composition for
  callers that need both, while explicit directory lookup now returns a
  node-lifecycle-only handle so wait/shutdown paths do not receive bootstrap
  initialization capability.
- Added an object-safe bootstrap-directory capability. Initialization callers
  can now resolve bootstrap-only handles by node without receiving command,
  read, membership-admin, or wait/shutdown capability; the in-memory raft
  directory smoke verifies both missing-node rejection and initialized-state
  lookup through that narrow surface.
- Removed current-serving lookup helpers from the broad authority-service
  directory handle. Current-serving selection now stays on the routed-authority
  directory surface, so post-bootstrap routed reads, writes, status, and
  membership operations do not ask a full-service directory for bootstrap or
  lifecycle-capable services.
- Removed the remaining combined bootstrap/node-lifecycle handle. Test and
  initialization wiring now use bootstrap-only handles for membership
  initialization state and node-lifecycle-only handles for applied-index waits,
  leader observation, and shutdown.
- Removed the unused combined admin authority layer. The full service boundary
  now composes the explicit command/read/status, leader-routed admin,
  bootstrap, and node-lifecycle traits directly, so there is no separate
  bootstrap/lifecycle/admin handle that callers can depend on accidentally.
- Removed the full-service authority directory boundary. Directory lookup now
  exposes only status-list, bootstrap, node-lifecycle, linearized-authority,
  and leader-routed admin capabilities; the in-memory test scaffold builds
  those narrow handles directly from concrete OpenRaft authorities before
  storing lookup entries.
- Removed the final composed authority service trait and handle. Tests and
  in-memory directory registration now build explicit linearized, status,
  bootstrap, node-lifecycle, and leader-routed admin handles
  directly from the concrete OpenRaft authority, so no remaining Phase 12
  raft capability hands callers the full command/read/status/admin/bootstrap/
  lifecycle surface by accident.
- Split status enumeration out of the bootstrap, node-lifecycle, and
  authority directory traits. Directory capabilities now perform only
  node-id lookup for their own narrow surface, while leader-routing clients take
  an explicit status-list handle when they need to discover the current serving
  authority.
- Split post-bootstrap routed command/read/status from leader-routed membership
  admin. The routing handle used by ordinary linearized clients can no longer
  replace voters, add learners, or transfer leadership; those operations route
  through a separate leader-admin directory and routing handle.
- Tightened raft status readiness so a node is not reported as a serving
  linearized authority unless it is local leader, an effective voter, and its
  state machine has applied through the committed watermark. The serving flag is
  now derived from those status fields instead of stored separately, so routed
  clients fail closed while a restarted or lagging leader catches up.
- Extended the node-lifecycle wait boundary with a full OpenRaft log-id
  applied-through wait. Tests that assert a specific committed entry was
  reflected now verify the full `(term,node,index)` identity when the applied
  cursor is exactly at that index, while also accepting later applied indexes
  because OpenRaft applies committed entries in order.
- Tightened routed linearized authority selection so the handle returned by the
  linearized-authority directory must still report the selected serving node
  and serving readiness before routed command, read, or status calls use it.
  A regression now fails closed when a directory returns a different node's
  linearized authority handle.
- Added the first non-default process integration for the OpenRaft control
  plane. `ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT=true` starts an in-memory
  single-node OpenRaft authority behind the existing Unix control-plane RPC
  boundary, explicitly initializes the single-voter membership, bootstraps the
  configured cluster map through the replicated command path, serves runtime
  maps through read-index, and submits heartbeats, ready-peering completions,
  metadata-transfer admin commands, and lease expiry through Raft. This is only
  a process-level boundary smoke for Phase 12.1; durable Raft storage,
  multi-node process networking, upgrade/migration, and production cutover
  remain later Phase 12 work. The `ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT`
  flag is temporary spike wiring and must be removed or replaced by the final
  control-plane mode selection once the durable/multi-node Raft path is ready.
- Extended the experimental process-mode coverage through the storage-node
  heartbeat refresh path. The adapter smoke now covers the startup
  incarnation/endpoint epoch bump, a subsequent Peering observation that commits
  `CompleteReadyPgPeerings` through OpenRaft, and the follow-up Active
  heartbeat/runtime-map handoff.
- Added experimental bootstrap idempotence coverage. Re-running bootstrap with
  different configured sockets/PGs after the Raft state machine is initialized
  now proves the existing replicated cluster map is preserved rather than
  replaced by startup configuration.
- Added a Unix RPC smoke for the same experimental authority. It serves
  `UnixControlPlaneClient::refresh_node_heartbeat` through the existing
  control-plane wire protocol with deterministic authority timestamps, proving
  the process-mode adapter works across the actual storage-node/front-end RPC
  framing rather than only through direct in-process trait calls.
- Added fail-closed Unix RPC coverage for rejected storage-node heartbeat
  refresh through the experimental authority. A heartbeat for an unknown node
  returns the existing remote control-plane error through the process adapter
  and leaves the replicated snapshot unchanged.
- Added the matching Unix RPC smoke for frontend-style runtime-map reads. The
  experimental authority now serves `UnixControlPlaneClient::runtime_map_snapshot`
  through OpenRaft read-index and the test asserts the wire-decoded freshness
  proof carries a nonzero Raft log id and the deterministic issued timestamp.
- Added process-helper coverage for `control-plane-runtime-map-ready` and
  `control-plane-runtime-map-diagnostics` against the experimental authority.
  These helpers now read the OpenRaft-backed runtime map through the same Unix
  RPC path used by scripts/UAT and leave the replicated snapshot unchanged.
- Added Unix RPC coverage for ordinary placement admin through the experimental
  authority. `UnixControlPlaneClient::set_pg_acting_set` now has a process
  adapter smoke that verifies the replicated command bumps the epoch and leaves
  the PG in Peering on the requested acting set.
- Added fail-closed Unix RPC coverage for deterministic command rejection
  through the experimental authority. A malformed acting-set request for an
  unknown node now returns the existing remote control-plane error through the
  process adapter and leaves the replicated snapshot unchanged.
- Added Unix RPC coverage for the experimental metadata-transfer admin path.
  The smoke drives a source PG active through Raft heartbeats, fences it through
  `FencePgForMetadataTransferRuntimeMap`, then installs a destination acting set
  through `SetPgActingSetWithMetadataTransferRuntimeMap`, proving both admin
  RPCs cross the process adapter and persist the transfer marker through Raft.
- Added process-helper coverage for the epoch-returning live metadata-transfer
  admin helpers against the experimental authority. The helpers now fence an
  active source PG and install the transfer-backed destination acting set
  through OpenRaft-backed Unix RPC, matching the CLI admin path rather than only
  the lower-level client calls.
- Added direct experimental Raft coverage for heartbeat lease expiry. The smoke
  first proves a pre-deadline scan is a replicated no-op, then expires the lease
  at the committed deadline and verifies the node becomes unavailable, the lease
  is cleared, and the active PG moves back to Peering with its metadata floor.

1. define the replicated control-plane state machine:
   - state includes cluster epoch, PG count, PG state, PG acting sets, node
     membership, node incarnation/endpoint/liveness metadata, retained
     cluster-map history, storage-owned history floors, metadata-transfer
     markers/fences, and source lease deadlines;
   - commands include node membership changes, acting-set changes, peering
     completion, metadata-transfer fence/install, history pruning, and
     bootstrap/admin operations;
   - heartbeat/liveness updates should not automatically become full replicated
     log entries for every refresh. The first design should make durable
     membership, incarnation, endpoint, and PG proof/floor changes linearized
     state-machine updates, while allowing high-frequency lease renewal to be
     leader-local derived state only when it is bounded by a committed term,
     read-index/lease proof, monotonic-clock assumptions, and explicit
     fail-closed restart behavior;
   - bucket and object metadata remain in PG-sharded metadata stores.
2. split the authority interface from implementation:
   - keep the Phase 11 single-authority API semantics as the contract;
   - introduce a linearized command/read trait boundary that the existing
     single-authority implementation and the new replicated implementation can
     both satisfy;
   - keep the file-backed single-authority implementation for focused tests and
     local debugging until the replicated path fully replaces it.
3. select and integrate the consensus mechanism:
   - use a small Raft-style replicated log for a 3-5 node control-plane group;
   - make the replicated log contain logical control-plane commands, not SQLite
     or filesystem bytes;
   - preliminary dependency decision: spike OpenRaft first. It appears to match
     the desired shape best because Argmin can provide application-defined
     control-plane commands, storage, state-machine application, snapshots, and
     network/auth integration while leaving Raft mechanics to the library. Pin a
     specific OpenRaft release for the spike, track the 0.10/1.0 stabilization
     path before committing long-term on-disk formats, and keep the
     authority-interface split above as the fallback boundary if the integration
     becomes too opinionated;
   - initial OpenRaft spike target: `openraft 0.10.0-alpha.26`, pinned exactly,
     with default features disabled and only the required async runtime feature
     enabled. The dependency has been added to the storage crate for the Phase
     12 spike after an explicit production-dependency decision, but no on-disk
     OpenRaft storage format is committed yet;
   - use OpenRaft's application-data hooks for `ControlPlaneCommand` and an
     application response that can represent both applied and deterministic
     rejected command outcomes. Do not propagate deterministic command rejection
     as a storage/apply error, because that would stall replay of a committed
     log entry;
   - use OpenRaft's state-machine snapshot hooks with the existing
     CRC-protected `ClusterControlSnapshot` artifact, while treating OpenRaft
     snapshot metadata as the authority for the snapshot's last-included log id.
     Argmin's snapshot install guard still rejects rollback relative to the
     current applied position, but mutual consistency between a transferred
     payload and OpenRaft's `last_log_id` metadata is part of the consensus
     storage contract;
   - start with OpenRaft `ReadPolicy::ReadIndex` for serving runtime maps. The
     read path must wait until the state machine has applied the read barrier,
     then publish a `RuntimeMapFreshnessProof::ReadIndex` for the actual
     reflected `last_applied` id, not merely the barrier captured at read
     arrival. Lease reads remain deferred until Phase 12's monotonic-clock,
     skew, and restart-sleep contract is specified;
   - persist or gate recovery around OpenRaft's committed-index contract.
     OpenRaft exposes `save_committed`/`read_committed` specifically to avoid
     serving reads from a restarted transient state machine before it catches up
     to a commit previously observed by clients;
   - keep `raft-rs` as the fallback candidate if OpenRaft cannot preserve the
     Argmin-owned command encoding, snapshot, diagnostic, or stale-map
     invalidation model without awkward workarounds;
   - if the dependency owns term/vote/log metadata, membership configuration,
     committed/applied index, snapshots, and joint consensus/reconfiguration
     state, document that contract and test it through the integration harness;
     otherwise Phase 12 must persist and validate those fields explicitly.
4. define command-log and snapshot formats:
   - stable command encoding with versioning/checksums;
   - durable snapshots that include current state, retained cluster-map history,
     storage floors, transfer/fence state, and any active source lease
     deadlines;
   - consensus metadata required for safety after restart, including current
     term, voted-for state, committed/applied index, log membership
     configuration, and any joint-reconfiguration state, is durable either in
     the chosen consensus library's storage or in Argmin's own control-plane
     store;
   - replay must reject corrupt, truncated, reordered, or incompatible state
     fail-closed;
   - cluster epochs are derived only from committed state-machine transitions.
5. define linearized read semantics:
   - runtime-map reads used by frontends and storage nodes must be leader
     linearized or use a consensus read-index/lease-read equivalent;
   - stale followers must not publish runtime maps as current serving
     authority;
   - an all-Active runtime map is serving only while its control-plane freshness
     proof remains valid. When a newer committed epoch has any Peering or
     non-serving PG, frontends with an older all-Active map must stop accepting
     mutating work once their map/read lease expires or a refresh observes the
     newer epoch; storage-node stale-epoch rejection remains the last line of
     defense, not the primary invalidation mechanism;
   - diagnostic reads may be explicitly stale, but their output must include
     leader/term/index/freshness information.
6. implement leader, follower, and control-plane membership behavior:
   - bootstrap a control-plane quorum with persistent control-plane node
     identity;
   - leader loss and follower restart must not regress cluster epoch, node
     incarnation, transfer fences, or retained-history floors;
   - control-plane membership changes are separate from storage-node membership
     changes and must themselves be committed before taking effect.
7. preserve the storage-node heartbeat contract:
   - heartbeats become replicated commands or leader-linearized updates;
   - future observed epochs still reject before any mutation;
   - stale observed epochs may update non-serving liveness/incarnation/endpoint
     only under the Phase 11 rules;
   - current PG observations remain epoch-scoped, acting-set-scoped, and proof
     checked before they can contribute to serving authority;
   - storage-reported cluster-map history floors are validated before
     persistence.
8. publish runtime maps only from committed state:
   - every epoch change is a committed state-machine transition;
   - storage nodes reject stale control-plane state by epoch/incarnation;
   - frontends only start or refresh onto maps where every PG route is Active
     with a serving lease, and they must stop mutating work when that lease or
     read-index freshness expires before a replacement all-Active map is
     installed;
   - storage-node refresh may still receive non-serving maps for convergence,
     but those maps must not be exported as frontend-serving authority.
9. define clock and lease semantics:
   - all control-plane lease deadlines use a monotonic clock source, never wall
     clock time;
   - lease-read/read-index freshness, storage-node heartbeat lease deadlines,
     frontend runtime-map freshness, and metadata-transfer source lease waits
     must state their clock-skew assumptions and restart behavior explicitly;
   - `RecordNodeHeartbeat` currently commits `heartbeat_at_ms` and
     `lease_deadline_ms` for deterministic replay and validates their internal
     relationship. `ExpireHeartbeatLeases` commits the expiry timestamp used
     to decide which leases become unavailable, and single-PG peering
     completion commits the timestamp used for lease/proof validation. Phase 12
     must still define how a replicated leader chooses and bounds these
     timestamps across leader changes, restarts, clock jumps, and stale
     lease-read/read-index publication;
   - after control-plane restart or leader change, any leader-local lease state
     that is not committed must be treated as expired until re-established
     through the consensus protocol;
   - tests must cover clock jumps, delayed lease publication, stale lease
     reads, and restart while leases or source-deadline waits are active.
10. add real internal identity and transport authentication:
    - Phase 10 intentionally deferred real internal identity here. Phase 12 must
      give control-plane peers, storage nodes, and frontends authenticated
      identities on internal RPCs;
    - membership changes bind node identity to allowed roles and endpoints, not
      just numeric ids supplied by the peer;
    - admin commands, heartbeat RPCs, runtime-map reads, and storage-node
      control-plane refresh paths must reject unauthenticated or wrong-role
      callers before applying or returning authority-bearing state.
11. preserve peering and metadata-transfer safety:
    - peering proof floors, transfer fences, imported-proof provenance,
      source-route epochs, source node ids, and source lease deadlines are
      replicated state;
    - retry after leader failover resumes from committed markers rather than
      recomputing from volatile route state;
    - non-overlap metadata migration still fails closed unless the committed
      transfer/checkpoint artifact path proves safety.
12. make retained-history pruning replicated and deterministic:
    - pruning considers metadata-transfer source epochs, storage-node
      history-floor reports, durable backfill source/desired epochs, and live
      payload placement epochs;
    - pruning is either an explicit committed command or a deterministic
      state-machine side effect that all replicas apply identically.
13. update control-plane RPC/admin tooling:
    - existing Unix admin commands target the replicated leader;
    - followers return leader redirection or a typed not-leader error;
    - runtime-map diagnostics include leader id, term, committed index, applied
      index, snapshot index, current epoch, runtime-map freshness deadline,
      lease/read-index basis, and retained-history/storage-floor state;
    - UAT failure diagnostics print those fields before teardown.
14. add replicated-control-plane test coverage:
    - state-machine unit/model tests for command application, replay, snapshot
      install, corrupt-log rejection, stale/future heartbeat rejection, epoch
      monotonicity, and history-floor validation;
    - consensus integration tests for leader restart, follower restart, leader
      loss, stale leader rejection, snapshot catch-up, persisted term/vote
      handling, and safe membership reconfiguration;
    - lease/freshness tests for clock jumps, leader restart, stale read-index or
      lease publication, old all-Active frontend maps during a newer Peering
      epoch, and source-deadline waits across retry;
    - identity tests for unauthenticated, wrong-role, stale-node-incarnation,
      and wrong-endpoint callers;
    - UAT smokes that kill or restart a control-plane node during storage-node
      startup, frontend startup, route change, metadata-transfer fence/install,
      checkpoint-backed metadata transfer, shard backfill discovery, and repair;
    - correctness soak variants that run the Phase 11 route-change,
      metadata-migration, PG-backfill, loss, and shard-repair smokes through the
      replicated control-plane path.

Exit criteria:

1. committed control-plane state survives leader and follower restart
2. leader failover cannot lose, reorder, or duplicate epoch changes
3. durable consensus metadata, including term/vote, committed/applied index,
   snapshot index, and membership/reconfiguration state, survives restart and
   rejects stale leaders
4. runtime-map reads used by frontends/storage nodes are linearized or otherwise
   proven current by the consensus protocol, with explicit monotonic-clock and
   restart semantics for any lease-read/read-index freshness
5. stale followers and stale leaders cannot publish serving maps as
   authoritative
6. old all-Active frontend maps are invalidated before accepting new mutating
   work once a newer committed epoch has Peering/non-serving PGs; storage-node
   stale-epoch rejection is a backstop, not the only safety mechanism
7. storage nodes reject stale control-plane state and stale sender writes after
   epoch changes
8. control-plane peers, storage nodes, frontends, and admin clients use
   authenticated internal identities with role/endpoint checks before authority
   state is mutated or returned
9. peering, metadata-transfer fences, source lease deadlines, and retained
   history floors survive leader failover and snapshot/replay
10. route changes, metadata transfer, repair/backfill, and restart UAT smokes
   pass with at least one control-plane node killed or restarted mid-operation
11. local multi-process tests no longer depend on static startup-only cluster
   membership
12. replicated-control-plane diagnostics expose enough
   leader/index/epoch/lease/history state to debug UAT failures without
   inspecting private process memory

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
13. operations crossing epoch changes, including started-before/finished-after
    command publication, cleanup, retry, and response paths
14. deterministic commit-boundary faults before shard write, after shard write
    before metadata publish, after pending-command install before apply, after
    apply before response, and during cleanup/finalization
15. separate soak profiles for overload/backpressure and distributed
    correctness, with the latter using real process/node boundaries plus
    controlled restarts and route/epoch changes

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
