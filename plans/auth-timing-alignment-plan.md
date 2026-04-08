# Authorization Timing Alignment Plan

## Scope

Align request handling with the repository policy in
[guides/authorization-timing.md](/home/justin/src/github.com/justincormack/argmin/guides/authorization-timing.md).

This is a narrow consistency plan:

- make auth timing explicit and uniform
- fix paths where a single external request is authorized too late or more
  than once
- add regression coverage so future write-path refactors do not silently move
  authorization boundaries

This plan does not cover:

- broad bucket-policy feature work
- changing multipart's per-request authorization model
- redesigning unrelated concurrency or storage mechanics

## Current mismatches

### 1. Streaming `PutObject` can authorize after body ingestion

The current HTTP streaming `PutObject` path prepares request context first and
only reaches `begin_stream_put` after the first internal segment promotion.

That means:

- unauthorized large streamed `PutObject` requests can be rejected after the
  first full segment is buffered
- unauthorized small streamed `PutObject` requests can be rejected after the
  entire body is read by the direct single-segment path

This violates the intended request-entry authorization rule.

### 2. Streamed `PutObject` re-authorizes at finalize

`finalize_stream_put` currently re-checks object write authorization for the
same external `PutObject` request.

That makes one client request depend on both:

- authorization at stream-session creation time
- authorization again at finalize time

This is inconsistent with the intended "authorize once per external request"
policy.

### 3. `PutObject` paths do not share one auth model

Today:

- direct single-segment `put_object` effectively authorizes once near commit
- buffered large `put_object` authorizes early and then again through
  `begin_stream_put` and `finalize_stream_put`
- HTTP streaming `PutObject` currently authorizes later than either of the
  above

These paths should converge on one request-entry authorization model.

## Target end state

For a single external `PutObject` request:

1. authorization happens once before meaningful body ingestion
2. internal streaming/direct-path decisions do not change that boundary
3. finalize only checks commit-time conflicts, validation, and session/storage
   state
4. refactors cannot move the auth boundary without failing tests

Multipart remains per external request:

- create
- each part upload
- complete
- abort

## Work items

### 1. Add an explicit entry-time write authorization step for streaming `PutObject`

Introduce a header-only/core-side authorization step during streaming
`PutObject` preparation so unauthorized requests fail before body ingestion.

This step should not require immediate stream-session creation.

### 2. Carry authorized write intent through streamed `PutObject`

Once the request is authorized, internal stream state should represent an
already-authorized write intent for that request.

`begin_stream_put` should become session/materialization setup, not a second
permission boundary for the same request.

### 3. Remove same-request re-authorization from streamed finalize

Restructure `finalize_stream_put` so it still performs:

- conditional write checks
- overwrite/versioning/object-lock checks
- session consistency checks
- metadata commit checks

but does not re-run the request's write authorization decision.

### 4. Audit adjacent single-request write paths

Audit these paths against the guide and adjust if needed:

- buffered `PutObject`
- copy destination writes
- streamed `PostObject`, if it has internal multi-phase write auth boundaries

The expected result is one clear authorization boundary per external request.

### 5. Add regression coverage

Add tests that lock in the intended semantics:

- unauthorized streaming `PutObject` is rejected before meaningful body
  ingestion
- direct and promoted streaming `PutObject` share the same authorization timing
  semantics
- mid-request internal promotion does not introduce a second auth boundary
- finalize can still reject on conflicts/validation without using `AccessDenied`
  for the same already-authorized request

## Validation

At minimum, run:

- `cargo fmt --all`
- `cargo clippy --all-targets --all-features -- -D warnings`
- targeted `server-http` and `server-core` tests covering streamed/direct
  `PutObject`
- the relevant `s3-tests` coverage for `PutObject` authorization behavior
