# Storage Cluster Invariants

This guide pins down the storage-cluster boundary during the multihost
transition. It is intentionally stricter than the current implementation
requires, because Phase 6 will replace the metadata-primary bridge with routed
and replicated metadata commands.

## Operation Classes

Every public `StorageCluster` operation must fit one of these classes:

| Class | Stale-handle behavior |
|---|---|
| Construction | Creates a handle for the current local map epoch, except explicit test hooks that can create stale handles. |
| Read-only topology/config | May read local map or static topology/config without an epoch fence. It must not read object metadata, payload bytes, or worker queues. |
| Epoch-fenced metadata bridge | Must call the metadata-primary bridge and fail with `StoreError::StaleMetadataPrimaryBridge` before metadata mutation or metadata reads when the handle epoch is stale. |
| Payload placement/read/write/delete | Must use `StorageCluster` placed payload APIs. Stale placement becomes `StalePayloadOperation`; stale shard IO becomes `StaleShardOperation` or `StaleShardLocation`. |
| Best-effort cleanup/worker queue | May suppress stale-handle and cleanup failures only where the API is explicitly best effort. Suppressed payload cleanup failures must emit typed trace context when tracing is active. |
| Active token release | Releasing already-acquired state is not new work. Release must go to the node where the token was acquired even if the creating cluster handle is stale by release time. |
| Test hook | Must be `#[cfg(any(test, feature = "test-hooks"))]` or `#[cfg(feature = "test-hooks")]`. Test hooks must either be read-only topology helpers, current-handle bridge helpers, or explicitly named contention/failure hooks. |

## Invariants

- The metadata-primary bridge is temporary and epoch-fenced. Production
  metadata reads and writes must not bypass `metadata_primary_bridge_node` or
  `metadata_primary_bridge_node_arc`.
- Payload bytes are always accessed through placed IO. Production request paths
  must not call node or PG shard read/write/delete APIs directly.
- Metadata-primary shard rows are an ack bridge only. They hold shard
  checksums/sizes while payload files are placed through the cluster map.
- Best-effort cleanup may suppress cleanup errors, but typed route/control-plane
  errors must not collapse into `NotFound` or generic IO before the suppression
  point.
- Physical shard absence, CRC corruption, and length corruption are recoverable
  only inside the EC read paths that explicitly opt into reconstruction.
- Token release is not new work. Acquiring a token requires a current handle;
  releasing an acquired token must still decrement the acquired node state
  after an epoch transition.
- Crash-durable orphan discovery is a later scavenger/reclaim responsibility.
  Injected best-effort cleanup failures may leave only explicitly documented
  orphan payload or ack state for that later process.

## StorageCluster Method Matrix

The method list below is exhaustive for the public `StorageCluster` surface in
Phase 5.6. Additions to `StorageCluster` should update this matrix and the
boundary check script in the same change.

| Methods | Class |
|---|---|
| `open_local_nodes`, `from_local_map` | Construction |
| `cluster_epoch`, `operation_epoch`, `metadata_node_id`, `local_node_count`, `local_node_ids`, `local_pg_route`, `local_pg_routes`, `process_local_registry_key`, `default_payload_ec_shape` | Read-only topology/config |
| `place_payload_shards`, `payload_shard_node`, `write_payload_shard`, `read_payload_shard`, `read_payload_shard_into`, `delete_payload_shard`, `write_direct_put_segment_payload_shards`, `write_stream_segment_payload_shards`, `read_segment_payload_stored_bytes_into` | Payload placement/read/write/delete |
| `delete_direct_put_segment_payload_shards` | Best-effort cleanup/worker queue |
| `reserve_put_object_generation`, `release_object_generation_reservation`, `commit_direct_put_object_from_payload_shards` | Epoch-fenced metadata bridge |
| `create_put_object_stream_session_record`, `load_stream_upload_session`, `prepare_stream_segment_append`, `commit_stream_segment_append`, `abort_stream_upload_session`, `create_put_object_stream_session`, `finalize_put_object_stream` | Epoch-fenced metadata bridge |
| `list_stream_upload_sessions_best_effort` | Best-effort cleanup/worker queue |
| `create_bucket_with_config_and_load_info`, `load_bucket_snapshot`, `with_bucket_write_snapshot`, `load_bucket_snapshot_pair`, `begin_bucket_delete`, `try_finalize_bucket_delete`, `head_bucket_info`, `get_bucket_subresource`, `put_bucket_versioning_and_load_info`, `put_bucket_object_lock_and_load_info`, `put_bucket_encryption_and_load_info`, `put_bucket_public_access_block_and_load_info`, `delete_bucket_public_access_block_and_load_info`, `put_bucket_ownership_controls_and_load_info`, `delete_bucket_ownership_controls_and_load_info`, `put_bucket_abac_enabled_and_load_info`, `put_bucket_acl_and_load_info`, `put_bucket_subresource_and_load_info`, `delete_bucket_subresource_and_load_info`, `list_buckets_for_owner`, `prune_completed_multipart_uploads_for_bucket_with_limit`, `list_lifecycle_sweep_buckets` | Epoch-fenced metadata bridge |
| `load_available_bucket_execution_generation_batches` | Best-effort cleanup/worker queue |
| `list_all_objects_for_bucket`, `list_all_object_versions_for_bucket`, `list_all_multipart_uploads_for_bucket`, `list_objects_for_bucket`, `list_object_versions_for_bucket`, `load_object_if`, `load_existing_live_object`, `load_object_read_snapshot_if`, `payload_reclaim_exists`, `get_object_tags_if`, `put_object_tags_if`, `delete_object_tags_if`, `put_object_retention_if`, `put_object_legal_hold_if`, `put_object_acl_if`, `get_object_legal_hold_if`, `get_object_retention_if`, `delete_specific_object_version_if`, `delete_current_object_if`, `insert_current_delete_marker_if`, `expire_current_object_if_due`, `delete_noncurrent_live_versions_if_due`, `delete_expired_delete_marker_if_due` | Epoch-fenced metadata bridge |
| `acquire_object_payload_lease` | Epoch-fenced metadata bridge |
| `enqueue_object_payload_reclaim`, `enqueue_bucket_delete_finalize`, `wait_for_reclaim_work`, `wake_reclaim_workers` | Best-effort cleanup/worker queue |
| `reclaim_object_payload_if_unleased` | Epoch-fenced metadata bridge plus payload cleanup |
| `create_multipart_upload`, `load_multipart_upload`, `begin_upload_part_stream_session`, `create_upload_part_stream_session`, `load_in_progress_multipart_upload`, `load_in_progress_multipart_upload_for_listing`, `load_multipart_completion_snapshot`, `load_multipart_completion_preflight`, `complete_multipart_upload_commit_serialized`, `finalize_upload_part_stream`, `list_multipart_uploads_for_bucket`, `list_multipart_parts_for_upload`, `lookup_abort_multipart_upload`, `abort_multipart_upload`, `abort_multipart_upload_if_due` | Epoch-fenced metadata bridge |
| `test_from_local_map_with_epoch`, `test_install_before_stream_abort_storage_hook`, `test_install_after_direct_put_metadata_publish_hook`, `test_install_before_placed_payload_shard_delete_hook`, `test_install_before_metadata_primary_payload_ack_delete_hook`, `test_install_best_effort_payload_cleanup_error_hook`, `test_pg_ids`, `try_probe_bucket_pg_available`, `try_probe_object_pg_available`, `object_payload_lease_count`, `bucket_object_payload_lease_count`, `try_take_reclaim_work`, `try_load_in_progress_multipart_upload`, `test_ec_scratch_allocation_count`, `test_bucket_pg_id_for`, `test_head_bucket_raw`, `test_object_pg_id_for`, `test_data_pg_id_for`, `test_object_generation_reservation_for`, `test_multipart_part_data_pg_id_for`, `test_get_object_meta`, `test_get_multipart_upload`, `test_get_multipart_part`, `test_list_multipart_parts`, `test_list_multipart_uploads_for_bucket`, `test_get_object_segments`, `test_replace_live_object_segments`, `test_get_object_parts`, `test_replace_object_parts`, `test_get_object_version`, `test_get_object_segments_reclaim`, `test_put_object_segments_reclaim`, `test_put_multipart_reclaim`, `test_payload_reclaim_exists`, `test_list_bucket_payload_reclaim_roots`, `test_force_became_noncurrent_at`, `test_create_deleting_bucket`, `test_delete_bucket_metadata`, `test_get_all_multipart_part_segments_for_upload`, `test_set_upload_state`, `test_list_stream_segments`, `test_force_stream_upload_created_at`, `test_list_all_stream_uploads`, `test_create_stream_upload`, `test_shard_exists`, `test_lock_bucket_pg`, `test_lock_bucket`, `test_lock_multipart_completion_bucket`, `test_payload_shard_file_path`, `test_payload_shard_file_exists` | Test hook |

## Associated Token Types

`ObjectPayloadLease::release` is the active token release path. It must release
against the `SharedStorageNode` captured at acquisition time and must not depend
on the current cluster epoch.

`ReleasedObjectPayloadLease::remaining`, `ReleasedObjectPayloadLease::payload_reclaim_exists`,
and `ReleasedObjectPayloadLease::enqueue_object_payload_reclaim` are release
follow-up helpers for the already-acquired token. They are allowed to use the
captured node directly because they do not acquire new payload leases or publish
new object state.
