## Tiny Single-Segment Write Fast Path

### Problem

Small `PutObject` requests are still paying the full streamed upload lifecycle:

- `begin_stream_put`
- `append_stream_segment`
- `finalize_stream_put`

The recent bucket-reservation work removed the coarse bucket mutex from that
path, but tiny uploads still do multiple bucket-PG round trips and create
stream-session rows even when the body fits in a single internal segment.

For the `warp mixed --obj.size=4KiB` benchmark, traces now show:

- shard durability is only part of the remaining `PUT` cost
- the rest is fixed session/setup/finalize overhead
- this overhead is avoidable for bodies that fit in one internal segment

### Goal

Add a direct single-segment `PutObject` path that:

- uses one bucket write reservation
- writes one segment shard batch
- commits the object directly
- skips stream-session rows entirely

Streaming uploads should promote to the existing begin/append/finalize path
only once the request body exceeds one internal segment.

### Intended design

#### Core

Add a direct `PutObject` helper in `server-core` that:

1. acquires a bucket write reservation
2. performs the same requester / ACL / bucket-owner-enforced checks as
   `begin_stream_put`
3. applies the same conditional write checks and overwrite handling as
   `finalize_stream_put`
4. writes shard files for the committed segment using the final
   `(bucket, key, generation_id, segment_index)` placement
5. publishes shard rows and object metadata
6. performs the same stale-payload cleanup as streamed finalize

This path must reuse the existing streamed finalize semantics rather than
creating a divergent overwrite/versioning implementation.

#### HTTP streaming `PutObject`

The streaming handler should switch to buffer-then-decide:

1. authenticate and prepare request context, but do not create a stream session
2. read body into the first pooled `8 MiB` segment buffer
3. if EOF arrives before the buffer fills:
   - call the direct single-segment `PutObject` helper
4. if the first buffer fills and more body remains:
   - create a stream session lazily
   - append the already-filled first segment as segment `0`
   - continue on the current streamed path

That preserves bounded memory while removing the session lifecycle from tiny
uploads.

#### Buffered non-streaming `PutObject`

The buffered fallback path should use the same direct helper for one-segment
objects, and only fall back to the streamed session path for multi-segment
bodies.

### Scope for this pass

Done in this pass:

1. direct single-segment `PutObject` core helper
2. lazy stream-session creation in the streaming `PutObject` handler
3. buffered fallback `PutObject` using the same direct helper
4. regression coverage for:
   - tiny direct `PutObject`
   - no leaked stream-session rows on tiny direct `PutObject`
   - exact one-segment `PutObject` avoiding stream-session rows

Implementation notes:

- the direct path reuses the streamed finalize overwrite/versioning helpers so
  object visibility and stale-payload cleanup stay aligned with the existing
  streamed semantics
- streaming `PutObject` now buffers the first segment and promotes lazily only
  when more body bytes arrive; exact-EOF-at-`8 MiB` stays on the direct path
- `UploadPart` is still on the existing streamed session path in this pass

Deferred:

1. direct single-segment `UploadPart`
2. inline tiny-object storage

`UploadPart` should follow the same body-side pattern later, but it is a
separate follow-up because its MPU state and checksum rules differ from
`PutObject`.

### Validation

Completed:

- unit/regression coverage for direct tiny `PutObject`
- existing `PutObject` and streaming upload tests
- `cargo fmt --all`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features --no-fail-fast`

Follow-up:

- rerun the tiny-object warp benchmark and compare trace shape
- if tiny `PUT` is still dominated by fixed overhead after this change, the next
  likely step is direct single-segment `UploadPart`, followed by inline tiny
  object storage

### Outcome

The direct single-segment `PutObject` path is now the steady-state path for
tiny and exact-one-segment uploads:

- no stream-session rows for one-segment `PutObject`
- one bucket write reservation instead of streamed begin/finalize
- one shard batch write and direct metadata publish

This materially reduced small-object `PUT` fixed cost and improved the `warp`
tiny-object benchmark, but it did not remove the remaining gap to rustfs. The
next major write-path steps, if we return to this area, are:

1. direct single-segment `UploadPart`
2. inline tiny-object / tiny-part storage
3. lower-cost shard durability, potentially via group commit
