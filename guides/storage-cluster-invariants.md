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
| Test hook | Must be `#[cfg(any(test, feature = "test-hooks"))]` or `#[cfg(feature = "test-hooks")]`. Test hooks must either be read-only topology helpers, current-handle bridge helpers, or explicitly named contention/failure hooks. |

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
| `try_probe_bucket_pg_available`, `load_bucket_snapshot`, `load_bucket_snapshot_pair`, `with_bucket_write_snapshot`, `head_bucket_info`, `get_bucket_subresource` | Epoch-fenced routed metadata PG |
| `list_buckets_for_owner`, `list_lifecycle_sweep_buckets` | Epoch-fenced routed metadata PG fanout |
| `begin_bucket_delete` | Epoch-fenced routed metadata PG fanout |
| `try_finalize_bucket_delete` | Epoch-fenced routed metadata PG fanout plus local runtime lease bookkeeping and worker queue |
| `load_available_bucket_execution_generation_batches` | Best-effort routed metadata PG |
| `try_probe_object_pg_available`, `load_object_if`, `load_existing_live_object`, `load_object_read_snapshot_if`, `payload_reclaim_exists`, `get_object_tags_if`, `get_object_legal_hold_if`, `get_object_retention_if` | Epoch-fenced routed metadata PG |
| `put_object_tags_if`, `delete_object_tags_if`, `put_object_retention_if`, `put_object_legal_hold_if`, `put_object_acl_if`, `delete_specific_object_version_if`, `delete_current_object_if`, `insert_current_delete_marker_if`, `expire_current_object_if_due`, `delete_noncurrent_live_versions_if_due`, `delete_expired_delete_marker_if_due` | Epoch-fenced routed metadata PG command apply |
| `list_all_objects_for_bucket`, `list_all_object_versions_for_bucket`, `list_all_multipart_uploads_for_bucket`, `list_objects_for_bucket`, `list_object_versions_for_bucket` | Epoch-fenced routed metadata PG fanout |
| `acquire_object_payload_lease` | Epoch-fenced routed metadata PG plus local runtime lease bookkeeping |
| `enqueue_object_payload_reclaim`, `enqueue_bucket_delete_finalize`, `wait_for_reclaim_work`, `wake_reclaim_workers` | Best-effort local runtime worker queue |
| `reclaim_object_payload_if_unleased` | Epoch-fenced routed object metadata PG command apply for reclaim-row deletion, local runtime lease bookkeeping, and placed payload cleanup. Payload files are deleted before the retryable metadata command removes reclaim rows from the acting set |
| `complete_multipart_upload_commit_serialized` | Bucket-PG-primary serialized completed-upload order allocation, then epoch-fenced routed object metadata PG command apply for completed-object publication, deterministic object write sequencing, selected streamed part segment metadata, completed-upload idempotence rows, replica bucket completed-upload sequence advancement, and omitted staging cleanup |
| `create_multipart_upload` | Epoch-fenced routed object metadata PG command apply for multipart upload row creation and upload generation reservation |
| `abort_multipart_upload` | Epoch-fenced routed object metadata PG command apply for multipart upload row deletion, upload generation reservation release, and part staging metadata removal; the command carries the abort-preparation cleanup snapshot, and cluster-owned best-effort payload cleanup uses those retryable refs |
| `abort_multipart_upload_if_due` | Bucket-lifecycle recheck under bucket-PG-primary lock, then epoch-fenced routed object metadata PG command apply through `abort_multipart_upload` |
| `begin_upload_part_stream_session`, `create_upload_part_stream_session`, `finalize_upload_part_stream` | Epoch-fenced routed object metadata PG command apply for streamed UploadPart staging creation and finalization. `CreateStreamUpload` apply revalidates UploadPart targets against the current in-progress MPU row on each acting node |
| `load_multipart_upload`, `load_in_progress_multipart_upload`, `try_load_in_progress_multipart_upload`, `load_in_progress_multipart_upload_for_listing`, `load_multipart_completion_snapshot`, `load_multipart_completion_preflight`, `list_multipart_parts_for_upload`, `lookup_abort_multipart_upload` | Epoch-fenced routed metadata PG |
| `list_multipart_uploads_for_bucket`, `prune_completed_multipart_uploads_for_bucket_with_limit` | Epoch-fenced routed metadata PG fanout |
| `test_from_local_map_with_epoch`, `test_install_before_stream_abort_storage_hook`, `test_install_after_direct_put_metadata_publish_hook`, `test_install_before_placed_payload_shard_delete_hook`, `test_install_before_metadata_primary_payload_ack_delete_hook`, `test_install_best_effort_payload_cleanup_error_hook`, `test_install_before_metadata_command_apply_hook`, `test_install_before_abort_multipart_pending_install_hook`, `test_install_before_stream_put_create_pending_install_hook`, `test_install_before_metadata_command_apply_context_hook`, `test_apply_metadata_command_to_acting_set_from_origin`, `test_pg_ids`, `object_payload_lease_count`, `bucket_object_payload_lease_count`, `try_take_reclaim_work`, `test_ec_scratch_allocation_count`, `test_bucket_pg_id_for`, `test_head_bucket_raw`, `test_object_pg_id_for`, `test_data_pg_id_for`, `test_object_generation_reservation_for`, `test_multipart_part_data_pg_id_for`, `test_get_object_meta`, `test_get_multipart_upload`, `test_get_multipart_part`, `test_list_multipart_parts`, `test_list_multipart_uploads_for_bucket`, `test_get_object_segments`, `test_replace_live_object_segments`, `test_get_object_parts`, `test_replace_object_parts`, `test_get_object_version`, `test_get_object_segments_reclaim`, `test_put_object_segments_reclaim`, `test_put_multipart_reclaim`, `test_payload_reclaim_exists`, `test_list_bucket_payload_reclaim_roots`, `test_force_became_noncurrent_at`, `test_create_deleting_bucket`, `test_delete_bucket_metadata`, `test_get_all_multipart_part_segments_for_upload`, `test_set_upload_state`, `test_list_stream_segments`, `test_force_stream_upload_created_at`, `test_list_all_stream_uploads`, `test_create_stream_upload`, `test_shard_exists`, `test_lock_bucket_pg`, `test_lock_bucket`, `test_lock_multipart_completion_bucket`, `test_payload_shard_file_path`, `test_payload_shard_file_exists` | Test hook |

## Associated Token Types

`ObjectPayloadLease::release` is the active token release path. It must release
against the local-cluster runtime state captured at acquisition time and must
not depend on the current cluster epoch.

`ReleasedObjectPayloadLease::remaining`, `ReleasedObjectPayloadLease::payload_reclaim_exists`,
and `ReleasedObjectPayloadLease::enqueue_object_payload_reclaim` are release
follow-up helpers for the already-acquired token. `payload_reclaim_exists`
routes through the object PG primary for the handle epoch captured at acquire
time; stale or unavailable routing is treated by callers as a conservative
reason to enqueue. `enqueue_object_payload_reclaim` uses the captured
local-cluster runtime state because enqueueing after the final release is part
of the already-acquired token workflow, not new metadata work.
