# Storage Cluster Invariants

This guide pins down the storage-cluster boundary during the multihost
transition. Phase 6.1 is moving metadata reads and writes from the
metadata-primary bridge to cluster-owned PG-primary routing; later Phase 6 work
will turn those routed operations into replicated commands.

## Operation Classes

Every public `StorageCluster` operation must fit one of these classes:

| Class | Stale-handle behavior |
|---|---|
| Construction | Creates a handle for the current local map epoch, except explicit test hooks that can create stale handles. |
| Read-only topology/config | May read local map or static topology/config without an epoch fence. It must not read object metadata, payload bytes, or worker queues. |
| Epoch-fenced routed metadata read | Routes through the active PG primary or, while Peering, through the exact replica named by the control-plane metadata-read certificate. Stale handles and uncertified routes fail with typed route errors before metadata is read. |
| Epoch-fenced routed metadata mutation | Must route through the Active PG primary selected by the cluster map. Stale handles, inactive routes, and non-primary nodes fail with typed route errors before metadata is mutated. |
| Epoch-fenced metadata bridge test hook | Test-only bridge helpers must fail with `StoreError::StaleMetadataPrimaryBridge` when the handle epoch is stale. Production metadata paths must not use this class. |
| Payload placement/read/write/delete | Must use `StorageCluster` placed payload APIs. Stale placement becomes `StalePayloadOperation`; stale shard IO becomes `StaleShardOperation` or `StaleShardLocation`. |
| Best-effort cleanup/worker queue | May suppress stale-handle and cleanup failures only where the API is explicitly best effort. Suppressed payload cleanup failures must emit typed trace context when tracing is active. |
| Active token release | Releasing already-acquired state is not new work. Release must go to the node where the token was acquired even if the creating cluster handle is stale by release time. |
| Test hook | Must be `#[cfg(any(test, feature = "test-hooks"))]` or `#[cfg(feature = "test-hooks")]`. Test hooks must either be read-only topology helpers, current-handle bridge helpers, command-consistent acting-set seed helpers, or explicitly named contention/failure hooks. |

## Invariants

- Single-PG metadata mutations must route through the cluster map's Active PG
  primary. They must not fall back to the process-wide metadata-primary node or
  to a read-certified replica.
- Single-PG metadata reads route through the Active primary in the ordinary
  case. While a PG is `Peering`, they may instead use the exact node and
  metadata proof in the current control-plane `PgMetadataReadRoute`. The
  authority may issue that certificate only when the replica is healthy,
  reports `Peering`, has no pending metadata command, and its complete metadata
  proof exactly equals the committed Peering floor or the exact imported
  transfer proof. The broader provenance-aware progression rules used by
  Peering recovery do not grant read authority. A replica ahead of the
  committed floor remains unavailable for metadata reads until the authority
  durably commits or otherwise certifies that progress.
- Storage-node handlers and embedded read routes bind each read authorization
  to its exact PG, then recheck the current route, certificate, exact metadata
  proof, and empty pending slot while holding that PG's lock used for the
  SQLite read. A certificate cannot be substituted between PGs with equal
  proofs, and a runtime-map or local metadata change cannot race certificate
  validation and read publication.
  The capability exposes bucket/object point reads, read-only listings,
  multipart classification and ListParts only; it cannot construct a metadata
  mutation route.
- `PgState::Active` remains the only state that accepts new mutations.
  `Peering` without a valid metadata-read certificate, and all metadata reads
  in `Degraded`, `Backfilling`, or `Inconsistent`, fail closed with typed route
  errors. Payload reads may separately use their retained-route EC
  reconstruction capability when at least `k` valid shards are reachable.
- Phase 6 writes are strict. A successful write must apply to every required
  metadata acting-set replica and write every required payload shard. The local
  implementation must not acknowledge quorum writes, degraded writes, or
  missing-shard writes.
- The remaining metadata-primary bridge is temporary, epoch-fenced, and allowed
  only for current-handle test hooks. Production metadata paths must route by
  PG primary or use explicit local-cluster runtime state for in-memory
  coordination.
- Payload bytes are always accessed through placed IO. Production request paths
  must not call node or PG shard read/write/delete APIs directly.
- Payload shard ack rows are metadata rows in the data PG and must route
  through that PG's primary. They hold shard checksums/sizes while payload
  files are placed through the cluster map.
- Local metadata commands are serialized per `(PG, bucket)` command stream.
  Bucket metadata commands use the bucket PG; object metadata commands use the
  object PG for the target object. A pending command for a stream must block
  later metadata mutations on that same stream until the pending command
  converges or the bucket incarnation is finalized away.
- Direct mutation of command-owned metadata is not a production boundary.
  Legacy `PgMetadataStore` and `SharedStorageNode` mutators for digest-covered
  bucket, object, upload, stream, reclaim, and allocator rows must be test-only,
  explicit test hooks, or private `PgStore` helpers used during metadata command
  apply. `PgMetadataStore::delete_finalized_bucket` is the explicit bucket
  finalization exception and may only be called by the cluster acting-set fanout
  after `MarkBucketDeleting`, write drain, visible-data checks, and reclaim
  checks have completed. The PgStore implementation must fail closed if the
  local row still exists in any state other than `Deleting`.
- Bucket write-drain authority is durable and bucket-PG-primary owned.
  Production write admission uses `bucket_write_reservations`; DeleteBucket
  begin uses `bucket_write_drains`. The legacy bucket-row write-drain counters
  have been removed and are not production fence authority. The Phase 9.4 model
  and publisher audit live in [bucket-write-drain.md](bucket-write-drain.md).
- Best-effort cleanup may suppress cleanup errors, but typed route/control-plane
  errors must not collapse into `NotFound` or generic IO before the suppression
  point.
- Concrete storage implementation errors (`StoreError`, `MetadataError`,
  `ObjectPgActionError`, and `ShardIoError`) are crate-private. Public storage
  operations return operation-specific opaque failures or bounded semantic
  classifications; coordinator and HTTP code cannot match, render, or retain
  the implementation errors. Rust visibility is the boundary. Owner-local
  tests, rather than a cross-crate source-symbol inventory, pin classification,
  redaction, error sources, and protocol mapping.
- Physical shard absence, CRC corruption, and length corruption are recoverable
  only inside the EC read paths that explicitly opt into reconstruction.
- Token release is not new work. Acquiring a token requires a current handle;
  releasing an acquired token must still decrement local-cluster runtime state
  after an epoch transition.
- Crash-durable orphan discovery is a later scavenger/reclaim responsibility.
  Injected best-effort cleanup failures may leave only explicitly documented
  orphan payload or ack state for that later process.
- Opening a `PgStore` is intended to become a recovery boundary, not just a
  schema bootstrap, so that a reopened store reconciles or rejects crash
  leftovers before it serves. This is a target invariant: the implementation
  is being introduced by the Phase 11 stabilisation slices and is not yet
  enforced at open/bind. See [PG Store Recovery
  Boundary](#pg-store-recovery-boundary) below.

## PG Store Recovery Boundary

This section states the current recovery boundary for local PG metadata stores.
`PgStore::open` remains a raw schema/open primitive, but production and local
cluster startup paths must run recovery before serving PG state.

### Current behavior

`PgStore::open` bootstraps the schema, digest triggers, and replica-state row.
It does not reconcile crash leftovers by itself because recovery needs the
owning `NodeId`. `StorageNodeServer::bind`, `StorageNodeBootstrap`, and the
local-cluster builder all run raw-node metadata-command recovery before serving
PG state. The bootstrap path holds the storage-node data-dir lock before
opening and recovering the node, then consumes itself to produce a one-shot
prepared server that couples the exact persisted runtime configuration to that
guard until bind. Bind also rejects any process configuration that differs from
an existing persisted control-plane runtime configuration. Raw `PgStore` and
shared-node construction are private storage-engine details.

Recovery is a distinct pass that runs after `PgStore::open`. The recovery epoch
is the store's own `metadata_command_replica_state.cluster_epoch`, read
internally; an external authority or config epoch must never be supplied as the
recovery epoch, since orphan detection compares the slot's epoch against the
stored replica epoch and a mismatched external value would defeat the check.

Recovery classifies anomalies into two groups and treats them differently.

Fail closed (possible corruption). Recovery returns a typed error and the store
does not serve:

- command-log hash-chain break (`MetadataCommandLogHashMismatch`);
- materialised digest mismatch: `metadata_command_replica_state.state_digest`
  disagrees with a full recomputation from rows
  (`MetadataStateDigestMismatch`);
- a forked or out-of-order command log.

Reconcile (benign crash leftovers). Recovery repairs the state and continues:

- an older-epoch orphan pending command slot: a slot whose epoch is behind the
  stored replica state epoch, for example a command prepared under an epoch
  that has since advanced and will never be applied. Future-epoch pending slots
  are not locally recoverable because they can be in-flight first commands for
  that future epoch; recovery fails closed and leaves them durable until a
  cluster-level convergence path has acting-set evidence.
- cache-only per-table digest drift: after replay validation proves
  `metadata_command_replica_state.state_digest` still matches the materialised
  rows, recovery refreshes `metadata_table_digests` from those rows so stale
  cached table digests cannot poison the next mutation.

Terminal pending command slots are different: a terminal slot proves this
replica recorded the command, but does not by itself prove that the acting set
converged. Local node recovery and heartbeat must therefore preserve terminal
slots. Cluster-level recovery, or an explicit command path that owns the
pending command, may remove a terminal slot only after it has acting-set
evidence or equivalent command-specific convergence proof.

The orphan cleanup is order-dependent: older-epoch orphan cleanup must precede
replay validation, because replay validation reads the pending slot through the
epoch-checked path and would otherwise reject the orphan before cleanup can run.

Heartbeat and other serving-time observation paths must not delete or advance
pending slots: a legitimate command can install a future-epoch pending slot
before apply/record advances the durable replica state to that epoch, and a
terminal same-epoch slot still needs acting-set cleanup evidence. Heartbeat
therefore reports the durable metadata proof plus whether any pending slot is
present. The distinction between fail-closed, local reconcile, and
cluster-level terminal cleanup is deliberate: corruption is surfaced, local
crash leftovers that are provably orphaned are healed idempotently before
serving, and ambiguous terminal evidence remains visible until a convergence
path consumes it. The full recovery contract and its implementation slices live in
[multihost-phase-11-stabilisation-plan.md](../plans/completed/multihost-phase-11-stabilisation-plan.md).

## Route Transition And Primary-Lease Fencing

Every storage-node RPC frame acquires a route-admission permit before it reads
the current process config and retains that permit until its response has been
written. Runtime-config installation first closes ordinary admission, drains
all admitted frames, persists and publishes the replacement config
exclusively, and then reopens admission. Metadata PG-lock release is the only
frame admitted during the drain: it is cleanup needed to let an already
admitted waiter finish, and it cannot create new PG state.

Metadata mutations recheck route expiry after acquiring the per-PG lock.
Visible metadata command application additionally checks its request-bound
route fence inside the SQLite transaction immediately before commit. A current
command uses the installed map validity. A historical Active command requires
the current Peering runtime map to authorize its exact PG, command epoch, log
index, and checksum; retained topology alone grants no mutation authority.
That authorization is bounded by the current map's process-bound monotonic
lease and disappears when the authority no longer reports the pending command.
The route-admission permit means a newer runtime config cannot publish between
the check and commit; expiry or guard failure rolls the transaction back,
including command-log, pending, and materialized metadata changes.

Read-only metadata access uses a separate capability. In `Active`, it resolves
to the current primary. In `Peering`, it resolves only to the replica selected
by the current `PgMetadataReadRoute`; storage atomically verifies the certified
proof and absence of a pending command before reading. Route admission still
drains across runtime-config replacement, so neither the control-plane
certificate nor its lease can change between validation and response
construction. This availability path does not weaken the mutation fence.

Retained-route payload reads do not depend on the historical data-PG primary
remaining available. Each exact historical shard owner returns the stored
payload together with a size and checksum derived from that same read; the
authenticated response codec verifies that binding before exposing either
value. The frontend accepts only the expected shard size, reconstructs from any
`k` valid surviving shards, and verifies the complete segment checksum before
returning bytes. If that checksum fails and another shard is available, it
performs at most `k` single-shard-exclusion reconstructions to retain the
existing one-corrupt-shard recovery contract and identify the suspect repair
target without an unbounded combination search. Failure to record that optional
repair while the PG has no Active mutation route does not invalidate bytes that
passed the segment checksum. A connection failure to one owner is temporary
unavailability, not proof that its shard needs repair, and must not enqueue
destructive repair work. Maintenance and backfill paths may still compare a
shard against the historical primary's durable acknowledgement catalogue when
that catalogue is part of their stronger repair proof.

The control plane independently fences successor activation. Whenever a PG
leaves `Active`, it persists the previous primary's node ID, incarnation,
endpoint, and last lease deadline in the PG record before any node lease is
cleared or replaced. Peering readiness and completion remain blocked while
that deadline is in the future if the proposed primary is a different process
identity. The exact same node/incarnation/endpoint may reactivate before its
own old deadline once the ordinary peering proof checks pass because there is
no deposed storage process to fence. While that old deadline remains live,
recovery-driven peering selection prefers the exact previous primary when it
is still serving; recovery of an earlier acting-set member must not cause an
unnecessary fenced failback. Explicit acting-set, PG-state, and metadata
transfer transitions retain the safety fence but disable this preference so
the requested administrative primary selection can take effect after the old
lease expires. Successful activation clears the previous-primary identity.

The route-transition effect audit classifies the remaining storage RPC
families as follows:

- Shard writes, repair/backfill writes, and shard-ack records may create
  durable payload or local ack state, but they do not independently publish an
  S3-visible object. Visibility still requires the fenced metadata-command
  commit. A route transition drains these frames before publishing Peering, so
  transfer/backfill observes their completed state; an abandoned file or ack
  is permitted orphan state for scavenging.
- Bucket write reservations, drains, generation reservations, and stream
  staging rows are coordination state, not independent object visibility.
  Frames admitted under the old route finish before Peering publication.
  Commands that depend on a reservation carry its durable proof, while exact
  release/reap operations remain cleanup and may run after the serving route
  changes.
- Reclaim claims and physical cleanup are rooted in durable reclaim metadata.
  Physical deletion is additionally fenced by storage-node read handles and
  exact claim/shard identity. Claim release and drain/reservation cleanup must
  remain available through retained routes because refusing cleanup after an
  epoch transition would leak authority records. A frame already in progress
  is drained before config publication.
- Passing a route-map deadline does not by itself publish a successor route.
  New ordinary work fails route validation after the deadline; already
  admitted work may finish, but a later Peering config cannot publish until it
  drains, and control-plane activation cannot precede the persisted previous
  primary deadline. This is the completion guard for non-metadata effects.

This closes the DCC-1 validate-then-transition race by effect. It does not
close RPC4: retained cleanup locations from different epochs can still name
the same physical shard file, so physical shard identity must be fenced across
epochs separately. The socket-level
`storage_node_route_transition_orders_old_command_before_successor_activation`
regression pins an admitted old command behind the session PG lock, starts the
Peering transition, releases the lock through the drain-safe cleanup frame,
and proves that the command is included before Peering while a later old-epoch
command cannot change the metadata proof after deadline and successor
activation.

## Node-runtime compiler boundary

The raw embedded node, PG stores, and aggregate local node client are
descendants of the private `node_runtime` module. `SharedStorageNode` is
available to cluster code only through the test-hook facade, and
`LocalStorageNodeClient` uses descendant-only visibility. Production cluster
routing therefore receives only the role-specific node-client traits exposed
by `LocalNodeStore`. Stateful operations live on subject-bound route traits
returned by the matching role opener, so the compiler rejects an operation on
the wrong role or without its scoped subject/PG authority. Embedded and RPC
implementations satisfy the same traits; the boundary does not depend on a
source-level receiver-name inventory.

Command minting is audited separately from method visibility. CreateBucket is
the unique bucket-root command with no existing durable subject from which its
timestamp and execution generation can be reconstructed. Its record is
private and its constructor consumes an unforgeable authority whose mint is
visible only inside `node_runtime`. Canonical bytes may be validated without
minting a command, but returning a decoded `MetadataCommandEnvelope` requires
`MetadataCommandDecodeAuthority`. Its constructor is likewise confined to the
private node runtime, and every envelope-producing storage-RPC decoder must
receive that authority, so cluster publishers cannot bypass a typed factory
through either the raw decoder or an indirect response codec. The remaining
focused semantic rule protects CreateMultipartUpload, the object-root
exception, by rejecting cluster-side direct construction outside its
subject-scoped node-runtime builder. Other migrated object command builders
are backed by apply-time validation of their exact reservation, durable
preimage, generation, or cleanup snapshot; those invariants remain covered by
recovery and malformed-envelope tests rather than route-method name scans.

## Storage operation-class matrix

The table below records the storage operation classes and representative
owner-side entry points used by the architecture. It is not an exhaustive
public-API or source-file inventory: an entry may be a private implementation
detail behind a typed route or opaque operation, and moving or renaming one
does not require a boundary-check allowlist update. Rust visibility, scoped
capability types, and the focused semantic checks described below define the
actual boundary.

| Methods | Class |
|---|---|
| `open_local_nodes`, `from_local_map` | Construction |
| `cluster_epoch`, `operation_epoch`, `metadata_node_id`, `local_node_count`, `local_node_ids`, `local_pg_route`, `local_pg_routes`, `process_local_registry_key`, `default_payload_ec_shape` | Read-only topology/config |
| `place_payload_shards`, `payload_shard_node`, `write_payload_shard`, `read_payload_shard`, `read_payload_shard_into`, `delete_payload_shard`, `write_direct_put_segment_payload_shards`, `write_stream_segment_payload_shards`, `read_segment_payload_stored_bytes_into` | Payload placement/read/write/delete |
| `delete_direct_put_segment_payload_shards` | Best-effort cleanup/worker queue |
| `reserve_put_object_generation`, `release_object_generation_reservation`, `commit_direct_put_object_from_payload_shards` | Epoch-fenced routed metadata PG command apply |
| `create_put_object_stream_session_record`, `create_put_object_stream_session`, `commit_stream_segment_append`, `abort_stream_upload_session`, `finalize_put_object_stream` | Epoch-fenced routed metadata PG command apply for stream session creation, segment append, abort, object-generation reservation, and standard-object publication. The shared append/abort methods use the command path for both PutObject and UploadPart stream sessions |
| `load_stream_upload_session`, `prepare_stream_segment_append` | Epoch-fenced routed metadata PG; prepare drains pending object command state before allocating the next staged segment generation |
| `list_stream_upload_sessions_best_effort` | Best-effort routed metadata PG fanout |
| `create_bucket_with_config_and_load_info`, `put_bucket_versioning_and_load_info`, `put_bucket_acl_and_load_info`, `put_bucket_object_lock_and_load_info`, `put_bucket_encryption_and_load_info`, `put_bucket_public_access_block_and_load_info`, `delete_bucket_public_access_block_and_load_info`, `put_bucket_ownership_controls_and_load_info`, `delete_bucket_ownership_controls_and_load_info`, `put_bucket_abac_enabled_and_load_info`, `put_bucket_subresource_and_load_info`, `delete_bucket_subresource_and_load_info` | Epoch-fenced routed metadata PG command apply |
| `try_probe_bucket_pg_available`, `load_bucket_snapshot`, `head_bucket_info`, `get_bucket_subresource` | Epoch-fenced routed metadata PG |
| `with_bucket_write_snapshot` | Epoch-fenced routed metadata PG plus durable bucket-PG-primary write reservation acquire/release |
| `list_buckets_for_owner`, `list_lifecycle_sweep_buckets` | Epoch-fenced routed metadata PG fanout |
| `begin_bucket_delete` | Epoch-fenced routed metadata PG fanout |
| `try_finalize_bucket_delete` | Epoch-fenced routed metadata PG fanout plus storage-node object-payload read-handle checks and local runtime worker queue |
| `load_available_bucket_execution_generation_batches` | Best-effort routed metadata PG |
| `try_probe_object_pg_available`, `load_object_if`, `load_existing_live_object`, `load_object_read_snapshot_if`, `load_leased_object_read_snapshot_if`, `payload_reclaim_exists`, `get_object_tags_if`, `get_object_legal_hold_if`, `get_object_retention_if` | Epoch-fenced routed metadata PG. Admitted object-subresource reads load their exact authorization subject through `ActiveObjectReadRoute`, which binds bucket/key/version/PG and rechecks the immutable request deadline immediately before node access. The leased snapshot helper additionally acquires a broad payload-generation lease before exact snapshot validation, retries if the authorization subject changed, and returns an opaque non-cloneable handoff binding that snapshot, its route provenance, and the lease |
| `put_object_tags_if`, `delete_object_tags_if`, `put_object_retention_if`, `put_object_legal_hold_if`, `put_object_acl_if`, `delete_specific_object_version_if`, `delete_current_object_if`, `insert_current_delete_marker_if`, `expire_current_object_if_due`, `delete_noncurrent_live_versions_if_due`, `delete_expired_delete_marker_if_due` | Epoch-fenced routed metadata PG command apply. Production object-subresource mutations use `ActiveObjectMetadataMutationRoute`; the corresponding raw cluster entry points are test-only |
| `list_all_objects_for_bucket`, `list_all_object_versions_for_bucket`, `list_all_multipart_uploads_for_bucket`, `list_objects_for_bucket`, `list_object_versions_for_bucket` | Epoch-fenced routed metadata PG fanout |
| `acquire_object_payload_lease`, `acquire_object_payload_lease_for_shard_locations` | Epoch-fenced routed metadata PG plus volatile storage-node-owned object-payload read handles. Copy-source and response-body snapshot loading acquire the coarse generation lease through the actual storage-node clients, so it remains visible to refreshed runtime maps and other frontends, then hand off without a gap to the shard-location helper through an opaque leased-snapshot token. Payload reads acquire all-or-release handles for every shard-owner node of the selected segments, including parity/recovery candidates. Every recorded placement epoch is validated and read through its exact retained route while acquisition remains authorized by the current admitted request epoch; the boundary script rejects direct payload-byte read bypasses |
| `enqueue_object_payload_reclaim`, `enqueue_bucket_delete_finalize`, `wait_for_reclaim_work`, `wake_reclaim_workers` | Best-effort local runtime worker queue |
| `reclaim_object_payload_if_unleased` | Epoch-fenced routed object metadata PG command apply for reclaim-row deletion, storage-node-owned read-handle/delete fencing, and placed payload cleanup |
| `complete_multipart_upload_commit_serialized_with_route_validation` | Admitted and deadline-fenced bucket-PG-primary multipart completion barrier, then routed object metadata PG command apply for completed-object publication, deterministic object write sequencing, selected streamed part segment metadata, object-version-scoped replay metadata, and omitted staging cleanup. The barrier advances one fixed-size bucket scalar and retains no upload history |
| `create_multipart_upload` | Epoch-fenced routed object metadata PG command apply for multipart upload row creation and upload generation reservation |
| `abort_multipart_upload` | Epoch-fenced routed object metadata PG command apply for multipart upload row deletion, upload generation reservation release, and part staging metadata removal; the command carries the abort-preparation cleanup snapshot, and cluster-owned best-effort payload cleanup uses those retryable refs |
| `abort_multipart_upload_if_due` | Bucket-lifecycle recheck under bucket-PG-primary lock, then epoch-fenced routed object metadata PG command apply through `abort_multipart_upload` |
| `create_upload_part_stream_session_with_route_validation`, `prepare_stream_segment_append_with_route_validation`, `finalize_upload_part_stream_with_route_validation` | Epoch-fenced routed object metadata PG effects for streamed UploadPart staging creation, segment-VID allocation, and finalization. Ordinary UploadPart and UploadPartCopy carry their immutable admitted effect fence to reservation, allocator, shard-write, and pending-command boundaries. `CreateStreamUpload` apply revalidates UploadPart targets against the current in-progress MPU row on each acting node |
| `load_multipart_upload`, `load_in_progress_multipart_upload`, `try_load_in_progress_multipart_upload`, `load_in_progress_multipart_upload_for_listing`, `load_multipart_completion_snapshot`, `load_multipart_completion_preflight`, `list_multipart_parts_for_upload`, `lookup_abort_multipart_upload` | Epoch-fenced routed metadata PG |
| `list_multipart_uploads_for_bucket` | Epoch-fenced routed metadata PG fanout |

## Associated Token Types

`ActiveObjectReadRoute` is the non-cloneable active authority for one
bucket/key/requested-version/object-metadata-PG tuple. Metadata-only subject
loads power GetObjectTagging, GetObjectAcl, GetObjectRetention, and
GetObjectLegalHold without granting mutation authority; snapshot and leased
snapshot loads use the same route for HEAD, object attributes, response-body
GET, and copy-source reads. Every node access rechecks the admission's
immutable deadline, and the coordinator accepts the route only from its own
publication domain.

`ActiveObjectMetadataMutationRoute` is the non-cloneable active mutation
authority for one bucket/key/requested-version/object-metadata-PG tuple.
PutObjectTagging, DeleteObjectTagging, PutObjectAcl, PutObjectRetention, and
PutObjectLegalHold use the buffered request's existing admission for both
policy context and the mutation command. The route rechecks that admission's
immutable deadline before snapshot loading and command construction. It also
carries an `AdmittedRouteEffectFence` through bucket-write reservation
acquisition and pending-command installation, where the embedded or RPC
storage node revalidates the request's original epoch and clock health
immediately before inserting the durable row. Embedded calls retain the exact
conservatively bound monotonic deadline. RPC requests contain no monotonic
timestamp: they carry a portable wall-clock upper bound which Unix and TLS/TCP
receivers shorten for inter-host skew and bind to their own monotonic clock.
The authority timestamp remains available for diagnostics, but is not used as
the later cutoff. A same-epoch route renewal therefore cannot extend authority
already handed to a request. Once the
authorized command is installed, applying that exact command is convergence
and may complete while a successor map waits to publish. Raw cluster
metadata-mutation entry points are available only to storage unit tests.

`ActivePutObjectRoute` is the non-cloneable authority for one complete direct
or promoted-stream PutObject workflow, including the destination side of
CopyObject. It fixes the bucket, key,
bucket-metadata PG, object-metadata PG, publication domain, and immutable
request deadline before
authorization. The same route then owns current-object loading, the durable
bucket-write snapshot, object-generation reservation, staged data-PG shard
writes, stream-session creation, segment-append publication, finalization, and
final object metadata publication. Reservation acquisition, shard writes, and
every create/append/finalize pending-command insertion carry the route's
`AdmittedRouteEffectFence` to the actual storage-node effect. The shard-write
RPC delegates only portable wall-clock authority and the receiver
conservatively binds it to its own
monotonic clock; repair writes cannot carry that frontend authority. Expiry at
any pre-publication boundary releases transient reservations and removes only
payload known to belong to the rejected attempt, including shards written
earlier in a partially completed direct placement.

Stream heartbeats use that same route rather than a renewable raw cluster
handle. Both durable effects—the bucket reservation lease renewal and the
stream session's persisted proof update—receive and revalidate the original
effect fence immediately beside mutation. The route epoch binds the new
effect; the reservation proof may retain its older acquisition epoch across a
route transition. The node-client heartbeat interfaces derive that authorizing
route epoch from the effect fence itself; callers cannot provide a second epoch
which disagrees with the fence. Unix and TLS/TCP carry only the portable wall
deadline and rebind it to storage-local monotonic time. The raw unbounded
heartbeat adapter is test/test-hook-only.

`ActiveMultipartObjectRoute` is the corresponding non-cloneable authority for
one multipart bucket/key/object-metadata-PG tuple. In addition to multipart
control and completion, UploadPartCopy uses it for destination session
creation, encrypted shard placement, segment append, and part finalization.
Multipart authorization candidates and the operation-specific capabilities
derived from them carry a private non-`Clone` marker, so consuming a candidate
cannot accidentally become reusable merely because its retained durable
record is cloneable. The multipart-upload-ID authority is intentionally
cloneable for bucket snapshot/response sharing, but carries a separate
non-comparable marker so equality cannot expose whether two authorities retain
the same hidden signing key.
The route binds the authorized upload row to the destination subject and
carries the immutable request effect fence through every fresh durable
reservation, segment-VID allocation, shard-file, and pending-command effect.
The cleanup deadline is derived from the admission inside the capability and
cannot be omitted by its caller, so recovery can remove abandoned state after
prompt retained cleanup releases frontend admission. Unbounded create and
finalize wrappers exist only for test support.

`LeasedObjectReadSnapshot` is the non-cloneable broad-to-narrow handoff token.
Its private fields bind the exact object snapshot, requested version, metadata
route, originating cluster, and broad generation lease; callers may inspect the
snapshot but cannot pair another snapshot with that lease. An
`ActiveObjectReadRoute` can consume it only when all provenance matches.
CopyObject and UploadPartCopy reconstruct that exact admitted source route
before consuming the handoff. Both forms derive every shard owner from the
token's recorded placement epochs,
acquire the narrow storage-node leases, and only then release the broad lease.
Copy-source snapshot loading itself uses the request admission, so its retained
repair fence captures the publication generation and immutable request
deadline. The authorization result and token share one immutable snapshot
allocation, so large multipart part and segment vectors are not cloned during
handoff.

`RetainedObjectPayloadRead` is the resulting response-body or copy-source
authority. It carries no long-lived publication admission and cannot perform
object metadata reads. It binds one bucket/key/generation and a private
allowlist of complete segment descriptors; range, part, ordinary GET, and copy
bodies can read only those descriptors. Every payload read uses exact
retained-route inspection, including when the segment's placement epoch equals
the originating frontend's epoch. If recovery discovers a corrupt shard, it
may reacquire a short publication permit solely to record repair work, but
only if the originating publication generation remains current and the
admission's immutable captured deadline is still valid. After map publication
or deadline expiry it skips that stale active-route mutation. The payload read
therefore remains valid if the storage nodes install a successor map before
the body's first read. Dropping the final body reference releases the captured
node sessions and performs the ordinary reclaim follow-up.

`ObjectPayloadLease::release` is the current active token release path. It must
release against the actual storage-node sessions captured at acquisition time
and must not depend on the frontend's current cluster epoch or runtime-map
generation. Unix session disconnect releases the server-side generation lease.
Long-lived Unix generation-lease sessions count against both the aggregate RPC
admission limit and its shared non-control budget. All lease acquisition stops
one slot short of that shared budget, preserving narrow-lease to read-handle
progress. Broad acquisition stops one additional slot earlier, preventing new
broad leases from consuming the broad-to-narrow transition slot during
one-at-a-time handoff to shard-scoped leases. Releasing each broad lease after
its successor is acquired preserves the same slot for the next handoff. The
remaining reserved capacity allows short reclaim/control operations.
Phase 9.5 keeps these handles volatile: if the process/node serving the read
fails, the client retries from a fresh snapshot.

Storage-node read-handle and reclaim-fence primitives are crate-local. The
production surface is `StorageCluster` read-handle acquisition plus the
placed-delete reclaim helper; Rust visibility prevents downstream use of the
low-level payload-read and storage-node handle/fence APIs. Temporary semantic
caller checks additionally keep physical shard deletion behind the placed
reclaim fence, raw shard reads inside the storage segment reader or publish
validator, and payload-byte reads behind `ReadRuntime` after handle
acquisition. The write-side publish validator performs its explicit placed
read to prove acknowledged shard files still match their `WriteAck` before
publishing metadata.

`ReleasedObjectPayloadLease::remaining`, `ReleasedObjectPayloadLease::payload_reclaim_exists`,
and `ReleasedObjectPayloadLease::enqueue_object_payload_reclaim` are release
follow-up helpers for the already-acquired token. `payload_reclaim_exists`
routes through the object PG primary for the handle epoch captured at acquire
time; stale or unavailable routing is treated by callers as a conservative
reason to enqueue. `enqueue_object_payload_reclaim` uses the captured
local-cluster runtime state because enqueueing after the final release is part
of the already-acquired token workflow, not new metadata work. Durable reclaim
metadata remains the retry source, and read handles only decide whether storage
nodes may physically delete local shard files now.
