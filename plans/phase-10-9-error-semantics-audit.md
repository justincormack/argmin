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

## Error Classification

| Source error | Classification | HTTP shape | Notes |
| --- | --- | --- | --- |
| `StoreError::MetadataCommandLogConflict` | expected command-stream contention when emitted from request-shaped metadata mutation paths | `OperationAborted` | The storage layer often drains/retries this internally. If it escapes to a public request handler, it should not be HTTP 500. Divergent command bytes still fail closed before reaching this mapper. |
| `StoreError::MetadataCommandPendingConflict` | expected same-PG pending-slot contention | `OperationAborted` | Normal cross-frontend race while another command owns the pending slot. |
| `MetadataError::ObjectGenerationReservationConflict` | expected stale generation reservation race | `OperationAborted` | Direct/stream PUT should retry internally where possible; escaped contention is still client-retryable, not internal. |
| `MetadataError::ObjectVersionReservationConflict` | expected version allocator race in command-owned allocator paths | `OperationAborted` candidate | Storage currently handles many of these internally. Audit remaining request crossings before enabling a blanket mapper. |
| `MetadataError::StaleBucketMetadataCommand` | expected stale bucket execution generation race | `OperationAborted` | Bucket subresource/control-plane mutations and request-time bucket snapshot helpers should use this mapping consistently. |
| `MetadataError::BucketWriteDraining` | expected delete-bucket drain race for writes | internal retry or `OperationAborted` candidate | Most write paths should wait/retry while the drain is active. Any escaped public request shape needs operation-specific review. |
| `MetadataError::BucketWriteReservationConflict` / `BucketWriteDrainConflict` | durable reservation/drain identity conflict | fail closed unless exact-idempotent retry is proven | Usually indicates wrong or stale proof identity. Do not blanket-map to retryable HTTP without a request-specific idempotency proof. |
| `MetadataError::BucketNotFound` / `ObjectNotFound` / `NoSuchUpload` / `PartNotFound` | client-visible absence when the operation can reveal it | operation-specific 4xx | Auth paths may intentionally convert missing objects to `AccessDenied`. |
| `StoreError::StorageRpcShardDeleteInProgress` | expected physical shard reclaim/read-handle contention | operation-specific retry/overload candidate | Needs side-effect-boundary review before exposing as `SlowDown` or another retryable response. |
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

Open audit items:

1. decide whether `MetadataError::ObjectVersionReservationConflict` should be
   added to the central object mapper for all request-shaped object mutation
   paths, or only for allocator-specific paths with proof of retry safety
2. classify escaped `BucketWriteDraining` for create/PUT/MPU/copy/delete paths:
   internal wait/retry is preferred, but any public escape needs an
   operation-specific S3 shape
3. split `StoreError::StorageRpc` into typed overload/contention outcomes versus
   generic transport/protocol failures
4. add guardrail checks for ad hoc request-path mappings that return
   `ServerError::Store` or `ServerError::Metadata` for expected contention
5. add request-path regressions, not only mapper tests, for direct PUT,
   streamed PUT, delete object, bucket subresources, lifecycle, and MPU
   completion/abort contention

