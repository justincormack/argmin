# Bounded AWS-Chunked Streaming Plan

Status: planned

## Context

Streaming `PutObject` and `UploadPart` support AWS SigV4 `aws-chunked` request
bodies. The current incremental decoder parses chunk headers and then buffers
the entire current chunk in memory before yielding decoded payload bytes to the
streaming ingest path.

That behavior is compatible for ordinary SDK chunk sizes, but it violates the
server's bounded-memory requirement. AWS accepts at least a 128 MiB single
signed `aws-chunked` data chunk, and likely allows much larger chunks. A small
fixed chunk-size cap such as 16 MiB would therefore be visibly
AWS-incompatible.

The original security finding overstated the allocation behavior: a chunk
header claiming 5 GiB does not allocate 5 GiB by itself. Memory grows with bytes
actually received. The real issue is still serious: a valid request can send a
very large single chunk, and the server will retain that chunk until it
completes and its signature is verified. Multiple concurrent clients can
therefore drive unbounded memory pressure and potentially OOM the process.

Relevant current code:

- `crates/server-http/src/http/chunked.rs`: `IncrementalChunkedDecoder::feed`
  appends wire bytes to `self.buf` and yields payload only after the complete
  chunk is present.
- `crates/server-http/src/http/serve.rs`: streaming `PutObject` and
  `UploadPart` call `decoder.feed(wire_data)` and only account decoded payload
  size after decoded bytes are returned.
- `MIN_CHUNK_SIZE` validates that non-final chunks are at least 8 KiB. It is
  not a memory bound.

## Goals

1. Keep memory bounded independently of client-chosen aws-chunked chunk size.
2. Preserve AWS-compatible acceptance of large valid chunk sizes.
3. Preserve signed chunk verification semantics.
4. Abort and clean up staged upload state if a streamed chunk later fails
   signature verification.
5. Apply the same bounded behavior to streaming `PutObject` and streaming
   `UploadPart`.
6. Keep checksum, payload SHA256, MD5, trailing checksum, segment append, and
   finalization behavior unchanged for valid requests.

## Non-Goals

1. Do not add a small fixed per-chunk payload cap unless AWS oracle testing
   later shows that AWS enforces the same cap.
2. Do not relax per-chunk signature verification.
3. Do not change public S3 semantics for valid uploads.
4. Do not rely on body idle timeout as the memory bound.

## AWS Compatibility Notes

Temporary AWS probes in `us-east-1` showed that single signed aws-chunked chunks
of 8 MiB, 16 MiB, 32 MiB, 64 MiB, and 128 MiB are accepted with HTTP 200.

This rules out a default 16 MiB cap and strongly suggests the fix should stream
large chunks with bounded memory instead of rejecting them.

## Design Direction

### 1. Early Protocol and Object-Bound Validation

These checks are useful hygiene, but they are not the main mitigation:

- reject `x-amz-decoded-content-length` values greater than `MAX_OBJECT_SIZE`
  before body streaming starts
- track remaining decoded content length inside the aws-chunked decoder
- reject a chunk header whose declared size exceeds remaining decoded length
- reject decoded body completion if the total decoded length differs from
  `x-amz-decoded-content-length`
- keep the existing non-final minimum chunk-size check

These checks prevent impossible declarations from entering the stream, but a
valid 5 GiB object sent as one chunk can still exist. The main mitigation must
therefore avoid whole-chunk buffering.

### 2. Streaming Decoder Output

Replace the current `feed(&[u8]) -> Vec<u8>` shape with an interface that can
emit bounded decoded payload slices before the current chunk completes.

The API shape must avoid holding a borrow of the decoder across `.await` in the
streaming ingest loops. Returning `Payload(&[u8])` can work only if the caller
copies or consumes the slice synchronously before awaiting. Prefer an API that
returns owned bounded payload chunks, or an API that invokes a synchronous
callback to hand off decoded bytes before returning control to the async loop.

One possible owned-output shape:

```rust
pub enum ChunkedEvent {
    Payload(bytes::Bytes),
    TrailersReady,
    Done,
}
```

Another possible shape is `feed_with(&[u8], FnMut(&[u8]) -> Result<_, _>)`,
where the callback updates checksum/hash state and appends to bounded staging
buffers synchronously, and the async append work happens after `feed_with`
returns. The implementation should choose the shape that keeps borrow lifetimes
simple and makes it impossible to hold `&mut IncrementalChunkedDecoder` across
an `.await`.

The decoder should:

- buffer only chunk headers, CRLF delimiters, and trailer lines
- when in a data-chunk state, consume at most the currently available data and
  return bounded payload slices
- decrement current-chunk remaining byte count as slices are emitted
- update current chunk SHA256 incrementally for signed modes
- for unsigned trailer mode, emit payload slices directly without chunk-data
  buffering
- verify the chunk signature only when the chunk boundary is reached
- advance `prev_sig` only after successful signature verification
- retain trailer parsing and trailer signature verification behavior

The slice size can be bounded by the incoming HTTP frame slice and the remaining
chunk bytes. It does not need to allocate a new `Vec` for the full chunk.

### 3. Signed Chunk Hashing

For signed modes, the string-to-sign needs `sha256(chunk_data)`. Instead of
holding `chunk_data`, the decoder should keep a `ring::digest::Context` for the
current chunk:

- create a new SHA256 context after parsing a nonzero chunk header
- update it for every emitted payload slice
- finalize it when the chunk reaches zero remaining bytes
- compute and verify the chunk signature from the finalized digest

This preserves signature semantics without retaining chunk bytes.

### 4. Failure After Emitting Payload

With streaming emission, payload bytes may already have been appended to a
staging session before the chunk signature is checked.

That is acceptable only if failure handling is explicit:

- on chunk signature failure, abort the streaming staging session immediately
- ensure no metadata commit/finalization can occur after a decoder error
- keep existing abort paths for `PutObject` and `UploadPart`
- add tests that inject a bad signature after some payload bytes have already
  been streamed and verify the object/part is not committed
- explicitly include bad terminal zero-length chunk signature coverage, where
  all payload bytes have already been streamed before the final chunk signature
  fails
- verify cleanup beyond public invisibility: staged sessions, staged segment
  records, and temporary/staged payload data must be removed or unreachable
  after bad data-chunk signatures, bad terminal signatures, and decoded-length
  mismatch failures
- treat `InvalidChunkSize` after an already-emitted too-small non-final chunk
  as the same staged-then-abort failure class; it must abort and clean up just
  like a bad signature

This moves signed streaming from "verify before staging" to "stage then abort on
verification failure", which is acceptable because staged data is not visible
until finalization.

### 5. Integration With Streaming Ingest

Both streaming paths currently do roughly:

```rust
let payload = decoder.feed(wire_data)?;
ingest_streaming_*_payload(&payload).await?;
```

They should instead loop over decoder events/slices and call the existing
ingest function for each emitted payload slice.

Important details:

- avoid allocating a full decoded `Vec`
- preserve existing segment buffering through `PooledSegmentBuffer`
- preserve CRC64/SHA/MD5/trailing checksum updates in the ingest functions
- keep body timing metrics, adding chunked-decoder emitted-byte and buffered
  byte metrics if useful
- ensure `decoder.into_trailers()` or equivalent remains available only after
  `Done`

### 6. Memory Bound

After the refactor, per-request aws-chunked decoder memory should be bounded by:

- maximum header line size
- maximum trailer section size
- small parser state
- current incoming HTTP frame slice borrowed from hyper
- existing `PooledSegmentBuffer` capacity

It should not be proportional to the declared or actual chunk size.

If header/trailer line limits are not currently explicit, add them as part of
this work.

## Work Items

1. Add early decoded-length validation:
   - reject decoded length above `MAX_OBJECT_SIZE`
   - pass expected decoded length into the decoder
   - reject chunk declarations larger than remaining decoded length
2. Refactor `IncrementalChunkedDecoder`:
   - replace full-chunk buffering with a state machine that emits bounded
     payload slices
   - add incremental per-chunk SHA256 state for signed modes
   - keep unsigned trailer mode streaming without full-chunk buffers
3. Update streaming `PutObject`:
   - consume decoder payload events in a loop
   - call existing ingest on each bounded slice
   - abort session on decoder error after partial staging
4. Update streaming `UploadPart`:
   - mirror the `PutObject` integration
   - abort part staging on decoder error after partial staging
5. Preserve trailer handling:
   - keep checksum trailer extraction semantics
   - verify signed trailers after terminal chunk
   - ensure trailers are available before finalization
6. Add observability:
   - maximum decoder buffered bytes per request
   - decoded bytes emitted from chunked decoder
   - decoder abort reason for malformed body/signature failure
7. Decide the future of `decode_chunked_body`:
   - either keep it explicitly test-only and whole-buffered, with comments that
     it is not a production memory model
   - or refactor it to share the new parser/decoder logic so test-only and
     production paths cannot diverge semantically
8. Remove or update any tests that assume `feed()` returns only whole chunks.

## Tests

### Unit Tests

1. oversized decoded length is rejected before body streaming
2. chunk size greater than remaining decoded length is rejected at header parse
3. a large single chunk fed in small pieces emits bounded payload slices and
   never accumulates the full chunk in decoder-owned memory
4. signed chunk verification succeeds when data is emitted incrementally
5. signed chunk verification fails at the chunk boundary when data was emitted
   incrementally
6. trailers and signed trailers still verify after incremental payload emission
7. non-final chunks smaller than `MIN_CHUNK_SIZE` still fail when followed by
   another data chunk

### Integration Tests

1. streaming `PutObject` with a large single signed aws-chunked chunk succeeds
   and stores the expected object
2. streaming `UploadPart` with a large single signed aws-chunked chunk succeeds
   and completes the expected object
3. bad chunk signature after partially emitted payload aborts `PutObject` and
   leaves no committed object
4. bad chunk signature after partially emitted payload aborts `UploadPart` and
   leaves no committed part usable by `CompleteMultipartUpload`
5. bad terminal zero-length chunk signature after all payload bytes have been
   streamed aborts `PutObject` and leaves no committed object
6. bad terminal zero-length chunk signature after all payload bytes have been
   streamed aborts `UploadPart` and leaves no committed part usable by
   `CompleteMultipartUpload`
7. decoded-length mismatch after partial payload emission aborts `PutObject`
   and leaves no committed object
8. decoded-length mismatch after partial payload emission aborts `UploadPart`
   and leaves no committed part usable by `CompleteMultipartUpload`
9. `InvalidChunkSize` after an already-emitted too-small non-final chunk aborts
   `PutObject` and leaves no committed object
10. `InvalidChunkSize` after an already-emitted too-small non-final chunk aborts
   `UploadPart` and leaves no committed part usable by `CompleteMultipartUpload`
11. bad signature, decoded-length mismatch, and post-emission
   `InvalidChunkSize` tests also assert staged sessions/segments are cleaned up
   or unreachable, not only that public object/part commit failed
12. object too large is rejected without allowing decoder memory to grow with
   the oversized declaration

### AWS Oracle Tests

Do not add huge AWS-facing tests to the normal suite. Keep a manual or ignored
probe for compatibility if needed. Current evidence already shows AWS accepts
single signed chunks through at least 128 MiB.

## Exit Criteria

1. Decoder-owned memory is bounded and demonstrably independent of chunk size.
2. Valid large aws-chunked chunks still succeed.
3. Signature failures after partial staging reliably abort and cannot commit.
4. `PutObject` and `UploadPart` share the same bounded decoder behavior.
5. Focused tests cover signed, unsigned-trailer, trailer-signature, bad
   signature, and oversized declaration paths.
6. No permanent AWS-facing test uploads very large objects by default.
