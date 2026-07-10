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
| Epoch-fenced routed metadata PG | Must route through the PG primary selected by the cluster map. Stale handles fail with `StoreError::StaleMetadataOperation`; inactive or missing routes fail with typed route errors before metadata is read or mutated. |
| Epoch-fenced metadata bridge test hook | Test-only bridge helpers must fail with `StoreError::StaleMetadataPrimaryBridge` when the handle epoch is stale. Production metadata paths must not use this class. |
| Payload placement/read/write/delete | Must use `StorageCluster` placed payload APIs. Stale placement becomes `StalePayloadOperation`; stale shard IO becomes `StaleShardOperation` or `StaleShardLocation`. |
| Best-effort cleanup/worker queue | May suppress stale-handle and cleanup failures only where the API is explicitly best effort. Suppressed payload cleanup failures must emit typed trace context when tracing is active. |
| Active token release | Releasing already-acquired state is not new work. Release must go to the node where the token was acquired even if the creating cluster handle is stale by release time. |
| Test hook | Must be `#[cfg(any(test, feature = "test-hooks"))]` or `#[cfg(feature = "test-hooks")]`. Test hooks must either be read-only topology helpers, current-handle bridge helpers, command-consistent acting-set seed helpers, or explicitly named contention/failure hooks. |

## Invariants

- Single-PG metadata reads and writes must route through the cluster map PG
  primary. They must not fall back to the process-wide metadata-primary node.
- `PgState::Active` is the only serving PG state in the local Phase 6
  implementation. `Peering`, `Degraded`, `Backfilling`, and `Inconsistent` are
  placeholders for later failure/repair work and must fail closed with typed
  route/control-plane errors before metadata or payload state is read or
  mutated.
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
owning `NodeId`. `StorageNodeServer::bind`, the control-plane-managed pre-bind
startup heartbeat path, and the local-cluster builder all call
`SharedStorageNode::recover_pg_metadata_command_state(node_id)` before serving
PG state. The pre-bind startup path holds the storage-node data-dir lock before
opening and recovering the node, then transfers that guard into bind.

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
an explicit recovery permit created when this process drains and replaces that
exact current Active route with a retained historical route; retained topology
alone grants no mutation authority. The permit is keyed by route epoch and PG,
bound to the exact primary/acting-set shape, expires with the old map, and is
not reconstructed from persisted historical topology after restart. The
route-admission permit means a newer runtime config cannot publish between the
check and commit; expiry or guard failure rolls the transaction back,
including command-log, pending, and materialized metadata changes.

The control plane independently fences successor activation. Whenever a PG
leaves `Active`, it persists the deposed primary's last lease deadline in the
PG record before any node lease is cleared or replaced. Peering readiness and
completion remain blocked while that deadline is in the future. Activation at
or after the deadline is allowed only when the proposed primary has a current,
unexpired lease and the ordinary peering proof checks pass, and successful
activation clears the old-primary deadline.

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

## StorageCluster Method Matrix

The method list below is exhaustive for the public `StorageCluster` surface.
Additions to `StorageCluster` should update this matrix and the boundary check
script in the same change.

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
| `try_probe_bucket_pg_available`, `load_bucket_snapshot`, `load_bucket_snapshot_pair`, `head_bucket_info`, `get_bucket_subresource` | Epoch-fenced routed metadata PG |
| `with_bucket_write_snapshot` | Epoch-fenced routed metadata PG plus durable bucket-PG-primary write reservation acquire/release |
| `list_buckets_for_owner`, `list_lifecycle_sweep_buckets` | Epoch-fenced routed metadata PG fanout |
| `begin_bucket_delete` | Epoch-fenced routed metadata PG fanout |
| `try_finalize_bucket_delete` | Epoch-fenced routed metadata PG fanout plus storage-node object-payload read-handle checks and local runtime worker queue; completed-MPU tombstone cleanup uses metadata command apply before bucket row deletion |
| `load_available_bucket_execution_generation_batches` | Best-effort routed metadata PG |
| `try_probe_object_pg_available`, `load_object_if`, `load_existing_live_object`, `load_object_read_snapshot_if`, `payload_reclaim_exists`, `get_object_tags_if`, `get_object_legal_hold_if`, `get_object_retention_if` | Epoch-fenced routed metadata PG |
| `put_object_tags_if`, `delete_object_tags_if`, `put_object_retention_if`, `put_object_legal_hold_if`, `put_object_acl_if`, `delete_specific_object_version_if`, `delete_current_object_if`, `insert_current_delete_marker_if`, `expire_current_object_if_due`, `delete_noncurrent_live_versions_if_due`, `delete_expired_delete_marker_if_due` | Epoch-fenced routed metadata PG command apply |
| `list_all_objects_for_bucket`, `list_all_object_versions_for_bucket`, `list_all_multipart_uploads_for_bucket`, `list_objects_for_bucket`, `list_object_versions_for_bucket` | Epoch-fenced routed metadata PG fanout |
| `acquire_object_payload_lease`, `acquire_object_payload_lease_for_shard_locations` | Epoch-fenced routed metadata PG plus volatile storage-node-owned object-payload read handles. Production reads acquire all-or-release handles for every shard-owner node of the selected segments, including parity/recovery candidates; the coarse generation helper is test-hooks-only and the boundary script rejects production callers and direct payload-byte read bypasses |
| `enqueue_object_payload_reclaim`, `enqueue_bucket_delete_finalize`, `wait_for_reclaim_work`, `wake_reclaim_workers` | Best-effort local runtime worker queue |
| `reclaim_object_payload_if_unleased` | Epoch-fenced routed object metadata PG command apply for reclaim-row deletion, storage-node-owned read-handle/delete fencing, and placed payload cleanup |
| `complete_multipart_upload_commit_serialized` | Bucket-PG-primary serialized completed-upload order allocation, then epoch-fenced routed object metadata PG command apply for completed-object publication, deterministic object write sequencing, selected streamed part segment metadata, completed-upload idempotence rows, replica bucket completed-upload sequence advancement, and omitted staging cleanup |
| `create_multipart_upload` | Epoch-fenced routed object metadata PG command apply for multipart upload row creation and upload generation reservation |
| `abort_multipart_upload` | Epoch-fenced routed object metadata PG command apply for multipart upload row deletion, upload generation reservation release, and part staging metadata removal; the command carries the abort-preparation cleanup snapshot, and cluster-owned best-effort payload cleanup uses those retryable refs |
| `abort_multipart_upload_if_due` | Bucket-lifecycle recheck under bucket-PG-primary lock, then epoch-fenced routed object metadata PG command apply through `abort_multipart_upload` |
| `begin_upload_part_stream_session`, `create_upload_part_stream_session`, `finalize_upload_part_stream` | Epoch-fenced routed object metadata PG command apply for streamed UploadPart staging creation and finalization. `CreateStreamUpload` apply revalidates UploadPart targets against the current in-progress MPU row on each acting node |
| `load_multipart_upload`, `load_in_progress_multipart_upload`, `try_load_in_progress_multipart_upload`, `load_in_progress_multipart_upload_for_listing`, `load_multipart_completion_snapshot`, `load_multipart_completion_preflight`, `list_multipart_parts_for_upload`, `lookup_abort_multipart_upload` | Epoch-fenced routed metadata PG |
| `list_multipart_uploads_for_bucket` | Epoch-fenced routed metadata PG fanout |
| `prune_completed_multipart_uploads_for_bucket_with_limit` | Epoch-fenced routed metadata PG fanout for global ordering, then metadata command apply for each completed-MPU tombstone delete |
| `test_from_local_map_with_epoch`, `test_install_before_stream_abort_storage_hook`, `test_install_after_direct_put_metadata_publish_hook`, `test_install_before_placed_payload_shard_delete_hook`, `test_install_before_metadata_primary_payload_ack_delete_hook`, `test_install_best_effort_payload_cleanup_error_hook`, `test_install_before_metadata_command_apply_hook`, `test_install_before_abort_multipart_pending_install_hook`, `test_install_before_stream_put_create_pending_install_hook`, `test_install_before_stream_put_create_command_id_hook`, `test_install_before_bucket_delete_command_id_hook`, `test_install_before_metadata_command_apply_context_hook`, `test_apply_metadata_command_to_acting_set_from_origin`, `test_reserve_completed_multipart_upload_order`, `test_pg_ids`, `object_payload_lease_count`, `bucket_object_payload_lease_count`, `try_take_reclaim_work`, `test_ec_scratch_allocation_count`, `test_bucket_pg_id_for`, `test_head_bucket_raw`, `test_object_pg_id_for`, `test_data_pg_id_for`, `test_object_generation_reservation_for`, `test_multipart_part_data_pg_id_for`, `test_get_object_meta`, `test_get_multipart_upload`, `test_get_multipart_part`, `test_list_multipart_parts`, `test_list_multipart_uploads_for_bucket`, `test_get_object_segments`, `test_replace_live_object_segments`, `test_get_object_parts`, `test_replace_object_parts`, `test_get_object_version`, `test_get_object_segments_reclaim`, `test_put_object_segments_reclaim`, `test_put_multipart_reclaim`, `test_payload_reclaim_exists`, `test_list_bucket_payload_reclaim_roots`, `test_force_became_noncurrent_at`, `test_create_deleting_bucket`, `test_delete_bucket_metadata`, `test_get_all_multipart_part_segments_for_upload`, `test_set_upload_state`, `test_list_stream_segments`, `test_force_stream_upload_created_at`, `test_list_all_stream_uploads`, `test_create_stream_upload`, `test_shard_exists`, `test_lock_bucket_pg`, `test_payload_shard_file_path`, `test_payload_shard_file_exists` | Test hook |

## Associated Token Types

`ObjectPayloadLease::release` is the current active token release path. It must
release against the local storage-node handle state captured at acquisition time
and must not depend on the current cluster epoch. Phase 9.5 keeps these handles
volatile: if the process/node serving the read fails, the client retries from a
fresh snapshot.

Storage-node read-handle and reclaim-fence primitives are crate-local. The
production surface is `StorageCluster` read-handle acquisition plus the
placed-delete reclaim helper; boundary checks reject public low-level payload
read APIs and public storage-node handle/fence APIs. Production payload-byte
reads stay inside segment readers after acquiring read handles, except for the
write-side publish validator, which performs an explicit placed read to prove
acknowledged shard files still match their `WriteAck` before publishing
metadata.

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
