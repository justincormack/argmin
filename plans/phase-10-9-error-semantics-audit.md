# Phase 10.9 Error Semantics Audit

This audit is the starting point for the Phase 10.9 multihost stabilization
gate. It records which storage/coordinator errors are expected races or
overload signals and which ones should remain internal failures.

The target rule is:

1. expected request contention must return an S3-shaped retryable response, not
   a generic `InternalError`
2. overload must return an S3-shaped retryable overload response only before a
   side effect is accepted, or after an idempotent command/session/reservation
   identity makes retry safe
3. true invariant, corruption, routing, and storage failures remain fail-closed
   internal errors, with structured diagnostics

## Current HTTP Shapes

`ServerError::Store`, `ServerError::Metadata`, `ServerError::Ec`,
`ServerError::InternalError`, and integrity errors all serialize as
`InternalError`/HTTP 500. Request paths should therefore avoid returning
`Store` or `Metadata` directly for normal races that can happen under
concurrent S3 clients or multihost command-stream contention.

`ServerError::OperationAborted` serializes as S3 `OperationAborted`/HTTP 409.
This is the current retryable S3 shape used for expected metadata command
contention and stale command-generation races.

`ServerError::SlowDown` serializes as S3 `SlowDown`/HTTP 503. Phase 10.9
backpressure work should use this or a deliberately chosen equivalent only at
the side-effect-safe boundary described in the phase plan.

## Status

Complete for the Phase 10.9 error-semantics audit slice. The request-path
mapper regressions found during the audit have focused coverage, and the
boundary script now rejects the known drift classes for object-PG mappers,
bucket snapshot mappers, bucket-write drain mappers, and payload read storage
error mapping. Remaining Phase 10.9 work should move to diagnostics,
observability, and backpressure implementation rather than this mapper audit,
unless a new concrete request-path HTTP 500 regression is found.

## Error Classification

| Source error | Classification | HTTP shape | Notes |
| --- | --- | --- | --- |
| `StoreError::MetadataCommandLogConflict` | expected command-stream contention when emitted from request-shaped metadata mutation paths | `OperationAborted` | The storage layer often drains/retries this internally. If it escapes to a public request handler, it should not be HTTP 500. Divergent command bytes still fail closed before reaching this mapper. |
| `StoreError::MetadataCommandPendingConflict` | expected same-PG pending-slot contention | `OperationAborted` | Normal cross-frontend race while another command owns the pending slot. |
| `MetadataError::ObjectGenerationReservationConflict` | expected stale generation reservation race | `OperationAborted` | Direct/stream PUT should retry internally where possible; escaped contention is still client-retryable, not internal. |
| `MetadataError::ObjectVersionReservationConflict` | expected version allocator race in command-owned allocator paths | `OperationAborted` | Storage retries the exact allocator race internally where possible; an escaped typed conflict is still retryable client contention, not an internal failure. |
| `MetadataError::StaleBucketMetadataCommand` | expected stale bucket execution generation race | `OperationAborted` | Bucket subresource/control-plane mutations and request-time bucket snapshot helpers should use this mapping consistently. |
| `MetadataError::BucketWriteDraining` | expected delete-bucket drain race for writes | internal retry or `OperationAborted` candidate | Most write paths should wait/retry while the drain is active. Any escaped public request shape needs operation-specific review. |
| `MetadataError::BucketWriteReservationConflict` / `BucketWriteDrainConflict` | durable reservation/drain identity conflict | fail closed unless exact-idempotent retry is proven | Usually indicates wrong or stale proof identity. Do not blanket-map to retryable HTTP without a request-specific idempotency proof. |
| `MetadataError::BucketNotFound` / `ObjectNotFound` / `NoSuchUpload` / `PartNotFound` | client-visible absence when the operation can reveal it | operation-specific 4xx | Auth paths may intentionally convert missing objects to `AccessDenied`. |
| `StoreError::StorageRpcResourceExhausted` | storage-node overload before a safe side-effect boundary | `SlowDown` | The storage-node server only emits this for bounded request/session/read-handle limits before accepting the side effect. Generic RPC decode/route/protocol failures remain internal. |
| `StoreError::StorageRpcShardDeleteInProgress` | physical shard reclaim/read-handle contention | internal/recoverable only | Read-handle acquire delete fences are treated as recoverable missing shards during EC read reconstruction. Delete-side fences are cleanup/delete contention and are not currently mapped to public `SlowDown`. |
| `StoreError::StorageRpc` | transport or remote protocol failure | internal unless structured code says otherwise | Phase 10.9 should split typed overload and typed contention from generic transport failures. |
| route, epoch, placement, digest, checksum, corruption, IO, DB, EC errors | invariant/storage failure | internal/fail closed | Must emit structured diagnostics rather than being hidden as retryable client contention. |

## Current Mapper Inventory

Centralized or mostly centralized:

1. `Coordinator::map_object_pg_action_error`
   - maps metadata command log/pending conflicts and object generation
     reservation conflicts to `OperationAborted`
   - preserves operation-specific `NoSuchUpload`
   - treats storage/invariant failures as internal through `Store`/`Metadata`
2. `Coordinator::map_bucket_snapshot_load_error`
   - maps stale bucket metadata commands to `OperationAborted`
   - maps missing bucket and bucket-not-empty to S3-visible shapes
3. `BucketHandleLoader::map_bucket_snapshot_error`
   - request-scoped bucket handle loads now map stale bucket metadata commands
     to `OperationAborted`

Updated in this audit slice:

1. stream PUT session creation now delegates to
   `Coordinator::map_object_pg_action_error`
2. delete-object mutation paths now delegate to
   `Coordinator::map_object_pg_action_error`
3. object-read snapshot authorization fallback now preserves its
   object-not-found access semantics and delegates all other object-PG errors
   to the central mapper
4. bucket handle, policy/tag, cached-policy test helper, and lifecycle runtime
   bucket snapshot helpers now map `StaleBucketMetadataCommand` to
   `OperationAborted`
5. active bucket summary helpers now delegate to the central bucket snapshot
   mapper, so create-bucket lost/recreate checks, bucket-exists, expected-owner
   validation, and delete authorization do not leak stale bucket command races
   as raw metadata failures
6. delete-bucket drain/finalize request helpers now delegate to the central
   bucket-write drain mapper, and that mapper converts escaped metadata command
   log/pending conflicts plus stale bucket metadata command generations to
   `OperationAborted`
7. object version reservation conflicts now map through the central object-PG
   mapper to `OperationAborted`, matching the existing generation reservation
   conflict behavior
8. direct PUT now has request-path regressions that inject metadata command
   log conflict during `ReserveObjectGeneration`, `ReserveObjectVersion`, and
   `CommitDirectPutObject` apply and verify the public request returns
   `OperationAborted`
9. delete object now has request-path regressions that inject metadata command
   log conflict during both `DeleteObjectVersion` and `InsertDeleteMarker`
   apply and verify the public request returns `OperationAborted`
10. streamed PUT now has request-path regressions for session creation, segment
   append, abort, and finalization. The create regression covers
   `CreateStreamUpload` contention escaping through the bucket snapshot mapper,
   append covers `AppendStreamSegment`, abort covers `AbortStreamUpload`, and
   finalize covers `CommitDirectPutObject` contention escaping through the
   object-PG mapper; all verify the public request returns `OperationAborted`.
11. bucket snapshot store-contention mappers now classify
   `MetadataCommandLogConflict` and `MetadataCommandPendingConflict` as
   `OperationAborted`, including the bucket handle, runtime, and bucket
   subresource policy/tag loading paths. The boundary script now rejects new
   ad hoc production `BucketSnapshotLoadError::Store` arms that map directly to
   `ServerError::Store` outside the central mappers.
12. bucket metadata write request paths now have regressions for each bucket-PG
   command family: `PutBucketVersioning`, `PutBucketAcl`, `PutBucketProperty`,
   and `PutBucketSubresource`. Each injects a metadata command log conflict at
   apply time and verifies the public request returns `OperationAborted`.
   `CreateBucket` has the same request-path apply-time regression.
   Object-metadata writes are covered through a `PutObjectMetadata` regression
   using the public `PutObjectTagging` path.
13. multipart creation, completion, abort, and streamed upload-part commit now
   have request-path regressions that inject metadata command log conflict
   during `CreateMultipartUpload`, `CommitMultipartObject`,
   `AbortMultipartUpload`, `AppendStreamSegment`, and `CommitStreamPart` apply
   and verify the public request returns `OperationAborted`.
14. lifecycle worker mutation paths now have regressions for current-object
   expiry and incomplete-MPU abort. Each injects metadata command log conflict
   during the lifecycle-owned command apply and verifies the worker-facing
   result is `OperationAborted` rather than a raw internal store error.
15. `StorageRpcErrorCode::ResourceExhausted` now decodes to typed
   `StoreError::StorageRpcResourceExhausted` in the normal Unix client,
   read-handle session client, and metadata-command session client. Central
   object-PG, bucket snapshot, bucket-write drain, and read-payload paths
   convert that pre-side-effect overload outcome to S3 `SlowDown`, including
   the routed shard-read shape where it is wrapped as
   `StoreError::ShardStore { source: StorageRpcResourceExhausted, .. }`.
   The public body-consumption paths (`GetObject`, `GetObjectRange`, and
   `GetObjectPart`) and coordinator-consumed copy source paths (`CopyObject`
   plus full-source and ranged `UploadPartCopy`) now have request-shaped
   regressions that inject this nested shard-read overload and verify it
   returns `SlowDown`. Generic `StoreError::StorageRpc` remains an internal
   transport/protocol failure.
16. `BucketWriteDraining` was audited across the storage acquire sites. Public
   create/PUT/stream PUT/MPU object-mutation paths wait for the durable drain
   and retry before returning to server-core. Lifecycle uses the explicit
   non-waiting acquire path to stop work for an old bucket incarnation rather
   than mutating through the drain. Delete-bucket drain ownership paths use the
   central bucket-write drain mapper. There is still no blanket public HTTP
   mapping for `BucketWriteDraining`.
17. `StorageRpcShardDeleteInProgress` was audited as a distinct typed RPC
   outcome. The read path already treats read-handle acquire delete fences as
   recoverable shard absence while reconstructing from other shards. Delete-side
   occurrences are cleanup/delete contention and remain internal or
   best-effort cleanup diagnostics; `map_store_error` intentionally does not
   convert this outcome to public `SlowDown`.
18. The boundary script now rejects production coordinator payload reads that
   call `read_segment_payload_stored_bytes_into(...)` and use `?` without first
   mapping through `map_store_error`. This protects the nested shard-read
   overload shape from drifting back to HTTP 500.

Open audit items:

None for this error-semantics mapper audit. No remaining request-path-only
mapper regressions are known after the direct PUT, streamed PUT, delete object,
bucket metadata, MPU, lifecycle worker, payload read, and typed storage RPC
coverage above.

## Follow-up Notes

- Object version reservation conflicts are now mapped to `OperationAborted`.
  `StorageCluster::reserve_next_object_version` still retries the exact
  stale-version race internally first; the mapper handles only conflicts that
  escape that storage retry loop.
- Bucket write draining is currently handled inside the storage write-snapshot
  and object-mutation helpers by waiting for the durable drain and retrying.
  That is the preferred shape. Public request mappers should not grow a blanket
  `BucketWriteDraining` to HTTP mapping unless a specific operation proves the
  side-effect boundary and desired S3 response.
- `scripts/check-storage-cluster-boundaries` now rejects new production
  coordinator `ObjectPgActionError` match arms that directly map
  `Store(error)` to `ServerError::Store(error)` or `Metadata(error)` to
  `ServerError::Metadata(error)` outside `Coordinator::map_object_pg_action_error`.
  The check tracks the raw `Store`/`Metadata` arm across block and multiline
  formatting, not only the current one-line spelling. Operation-specific
  mappers should preserve their special 4xx cases first, then delegate all
  remaining object-PG errors to the central mapper.
- The same boundary script now rejects production coordinator bucket snapshot
  mappers that fall back from `BucketSnapshotLoadError::Metadata(...)` to raw
  `ServerError::Metadata(...)` unless the same mapper segment also handles
  `StaleBucketMetadataCommand` explicitly.
- It also rejects production coordinator `BucketSnapshotLoadError::Store` arms
  that map directly to `ServerError::Store` outside the central bucket snapshot
  mappers, because bucket snapshot loads can wrap object-PG command-stream
  contention in stream create and bucket subresource paths.
- The boundary script also rejects production coordinator bucket-write drain
  error arms that map raw `BucketWriteDrainError::Store` or `Metadata` variants
  directly to `ServerError::Store` or `ServerError::Metadata` outside
  `Coordinator::map_bucket_write_drain_error`.
- The same guardrail now requires production coordinator payload read calls to
  route storage errors through `map_store_error` before `?`, so nested
  `ShardStore { source: StorageRpcResourceExhausted, .. }` continues to map to
  S3 `SlowDown`.
