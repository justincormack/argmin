# Authorization Timing Alignment Plan

## Scope

Align request handling with the repository policy in
[guides/authorization-timing.md](/home/justin/src/github.com/justincormack/argmin/guides/authorization-timing.md).

This is a narrow consistency plan:

- make auth timing explicit and uniform
- fix paths where a single external request is authorized too late or more
  than once
- use type/API shape to bind auth-sensitive request state to the authorization
  decision
- add regression coverage so future write-path refactors do not silently move
  authorization boundaries

This plan does not cover:

- broad bucket-policy feature work
- changing multipart's per-request authorization model
- redesigning unrelated concurrency or storage mechanics

## Status

The original `PutObject` alignment work is now implemented.

Implemented changes:

- streaming `PutObject` now authorizes before meaningful body ingestion
- direct and streamed `PutObject` now share the same entry-time authorization
  model
- same-request re-authorization was removed from streamed finalize
- multi-phase `PutObject` now carries an explicit authorized token so later
  phases consume bound auth-sensitive request state rather than accepting a
  second caller-controlled copy
- regressions cover early denial and token-bound direct/streamed commit state

The plan remains open because adjacent write paths should still be reviewed
against the same rules.

## Implemented pattern

For a single external multi-phase write request, the intended pattern is now:

1. authorize once at request entry in `server-core`
2. produce an opaque authorized token that binds the auth-sensitive request
   state used for that decision
3. let later internal phases consume that token
4. restrict later phases to commit-time inputs such as body state, checksums,
   conditional-write data, and session references
5. keep finalize/commit checks focused on conflicts, validation, and storage
   invariants rather than re-authorizing the same request

For `PutObject`, this is the `AuthorizedPutObjectWrite` pattern and the
follow-on helpers that consume it.

## Resolved mismatches

### 1. Streaming `PutObject` authorized after body ingestion

Resolved by adding an explicit entry-time authorization step during streaming
request preparation.

### 2. Streamed `PutObject` re-authorized at finalize

Resolved by making streamed finalize consume the bound authorized token and by
limiting finalize checks to commit-time validation and conflict/state checks.

### 3. `PutObject` paths did not share one auth model

Resolved by converging direct and streamed `PutObject` on the same entry-time
authorization contract.

## Target end state

For any single external write request that spans multiple internal phases:

1. authorization happens once before meaningful body ingestion
2. an authorized token binds the auth-sensitive request state for that request
3. internal streaming/direct-path decisions do not change that boundary
4. finalize only checks commit-time conflicts, validation, and session/storage
   state
5. refactors cannot move the auth boundary without failing tests

Multipart remains per external request:

- create
- each part upload
- complete
- abort

## Work items

### 1. Keep `PutObject` token use narrow and explicit

Preserve the current pattern that later helpers consume a bound authorized
token instead of a second auth-sensitive request description.

Follow-up checks:

- avoid widening helper signatures to re-accept bucket/key/requester/ACL/tag/
  object-lock inputs after authorization
- prefer making invalid combinations unrepresentable in API shape
- keep raw lower-level finalize helpers internal when they bypass the safe
  token contract

### 2. Audit adjacent single-request write paths

Audit these paths against the same guide and convert them to the same pattern
where appropriate:

- copy destination writes
- any other future multi-phase single-request write path

`PostObject` now follows the safe streamed finalize helper and should stay
aligned with `PutObject`.

The expected result is one clear authorization boundary per external request
and one explicit bound token for any internal multi-phase write.

### 3. Keep regression coverage aligned with the contract

Tests should continue to lock in:

- unauthorized streaming `PutObject` is rejected before meaningful body
  ingestion
- direct and streamed `PutObject` share the same authorization timing semantics
- authorized-token helpers commit the token-bound ACL/tag/object-lock/
  requester state rather than caller-supplied substitutes
- mid-request internal promotion does not introduce a second auth boundary
- finalize can still reject on conflicts/validation without using
  `AccessDenied` for the same already-authorized request

## Validation

At minimum, run:

- `cargo fmt --all`
- `cargo clippy --all-targets --all-features -- -D warnings`
- targeted `server-http` and `server-core` tests covering streamed/direct
  `PutObject`
- the relevant `s3-tests` coverage for `PutObject` authorization behavior
