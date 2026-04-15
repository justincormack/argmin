# Multipart, Reclaim, and Stream-Session Testing Plan

## Scope

This plan adds stateful, property-style, and narrowly targeted concurrency
coverage for the lifetime-sensitive parts of the storage and upload pipeline.

In scope:

- multipart completion and abort state transitions
- reclaim queue and payload-lease retry behavior
- stream-session cleanup across success, abort, and failure paths
- deterministic race coverage using existing test hooks plus new narrow hooks
  where needed
- bounded stateful/property tests over short traces
- loom-style micro-models only for small synchronization units if they are
  clearly justified

Out of scope:

- HTTP/request parser fuzzing
- broad performance benchmarking or soak testing
- a whole-system loom model of the coordinator, SQLite layer, and filesystem
- replacing existing targeted regressions; this plan is meant to generalize
  them into reusable invariants

## Motivation

Several of the more important security regressions here were not parser bugs.
They were lifetime and concurrency bugs:

- `security/codex-470aa8a`: streamed `UploadPart` reuploads left displaced shard
  data behind
- `security/codex-ecef3a4`: reclaim retries and an unbounded queue allowed
  read-driven memory pressure
- `security/codex-195d31f`: failure paths leaked streaming sessions until
  cleanup was made explicit

Those were fixed, but the current protection is still mostly a collection of
incident-specific regressions.

The codebase already has the right direction elsewhere:

- `server-core` uses a bounded reference model in `lifecycle_prop_tests`
- targeted race hooks already exist for multipart metadata and stream append
  races

This plan extends that style to the security-sensitive lifetime surfaces.

## Current State

- deterministic race helpers already exist in `crates/server-core/src/coordinator.rs`:
  - `install_multipart_metadata_race_hooks`
  - `install_stream_append_race_hooks`
- HTTP cleanup regressions already assert that failed `PutObject` and
  `UploadPart` requests do not leak sessions
- `plans/reclaim-queue-hardening-plan.md` covers design hardening for reclaim
  queue behavior, but not a broader stateful testing strategy
- there is no dedicated property/stateful model for multipart completion,
  reclaim retry, or stream-session lifetime invariants

## Goals

- encode the important lifetime invariants explicitly instead of rediscovering
  them one bug at a time
- cover failure, abort, retry, and race paths rather than only happy paths
- make concurrency failures reproducible with short traces or named
  interleavings
- keep the first implementation dependency-light by using existing hooks and
  `proptest` before considering new concurrency tooling

## Non-Goals

- do not introduce a broad concurrent-test framework without a concrete need
- do not attempt to explore every possible scheduler interleaving in the full
  coordinator
- do not hide externally visible behavioral regressions only inside unit tests;
  promote key cases upward when they affect S3-visible behavior

## Core Invariants

The first stateful suite should make the following invariants explicit:

- no stream session is leaked after abort, finalize failure, checksum failure,
  precondition failure, or scavenging
- no live object or completed multipart result is visible unless its referenced
  payload or segments are durable
- reuploading a part or completing a multipart upload does not orphan displaced
  part shards or streamed segments
- active payload leases prevent reclaim, but once the final lease drops,
  reclaim retries only if durable reclaim state still exists
- abort and complete are terminal for the targeted upload/session; later
  append/list/finalize operations fail predictably
- reclaim queue contents correspond to actual pending reclaim work rather than
  ordinary reads or duplicate noise

## Testing Layers

### 1. Deterministic Race and Failure Coverage

Start with narrow tests that use barriers and hooks to force the interleavings
we care about.

Existing hooks already cover useful edges:

- multipart metadata snapshot and delete windows
- duplicate stream-segment append races

Add more narrow hooks only where a missing interleaving blocks a concrete test.
Likely candidates:

- between multipart completion snapshot and live-object commit
- between stale-payload enqueue and best-effort cleanup
- around reclaim worker dequeue and lease checks
- around stream-session transition from in-progress to terminal state

Immediate scenarios to cover:

- `CompleteMultipartUpload` racing with `AbortMultipartUpload`
- streamed part reupload cleanup after a prior streamed generation
- final lease drop racing with reclaim worker retry
- session cleanup after finalize failures on object and multipart paths
- duplicate or delayed terminal operations after an upload/session is already
  completed or aborted

### 2. Bounded Stateful Model with `proptest`

Follow the style of `lifecycle_prop_tests`: small state, short traces, and a
clear rendered trace on failure.

Use a tiny model over one bucket, one or two keys, and a handful of parts or
sessions.

Candidate operations:

- create multipart upload
- upload plain part
- begin stream put / begin stream part
- append stream segment
- finalize stream put / finalize stream part
- complete multipart upload
- abort multipart upload
- delete or overwrite object
- start and finish a read that acquires/releases payload leases
- advance reclaim worker by one step
- scavenge stale sessions

Model outputs should track:

- which uploads and sessions exist
- which objects are live and which payload generations they reference
- which generations are pending reclaim
- whether a session or upload is terminal

The concrete harness should compare the model against observable coordinator and
storage state after each step.

### 3. Narrow Loom-Style Micro-Models Where Needed

There is no current `loom` dependency, and this repo is careful about adding
new dependencies. Keep the bar high.

If existing hooks plus `proptest` still leave a genuinely important
synchronization gap, use loom-style testing only for tiny in-memory state
machines such as:

- reclaim queue dedup and wake/sleep behavior
- lease-count handoff into reclaim retry
- stream-session terminal-state transitions

Do not attempt to loom the full coordinator, storage node, or filesystem.

### 4. Promote S3-Visible Failures Upward

When a stateful failure changes externally visible behavior, keep or add a
higher-level regression in `server-http`, `s3-local-tests`, or another
integration harness where the externally visible contract is easiest to assert.

These promotions should feed the dedicated local security suite proposed in
`plans/security-test-suite-plan.md`.

They should not be driven primarily by whether a path moves the
`./scripts/coverage` number.

Representative cases:

- leaked session after `UploadPart` checksum failure
- leaked session after `PutObject` precondition failure
- multipart part reupload cleanup
- terminal behavior after complete versus abort races, where the outcome is
  externally observable

## File Layout

Keep the test structure auditable and avoid burying everything inside
`coordinator.rs`.

Recommended additions:

- `crates/server-core/src/coordinator/multipart_stateful_tests.rs`
- `crates/server-core/src/coordinator/reclaim_stateful_tests.rs`

Keep HTTP cleanup regressions in `crates/server-http/src/http` where the
observable contract is easiest to assert.

## Phase 1: Consolidate Invariants and Deterministic Hook Tests

Deliver:

- a small shared test-support layer for inspecting:
  - active stream sessions
  - pending multipart uploads
  - pending reclaim roots
  - displaced segment cleanup where needed
- deterministic regressions for the known bug classes above, expressed in terms
  of explicit invariants rather than bug-specific anecdotes
- a short list of any missing hook points that block phase 2

Success criteria:

- the existing fixed regressions are re-expressed as stable invariants
- failures identify the violated invariant, not just the symptom

## Phase 2: Multipart and Session Trace Model

Deliver:

- a bounded trace generator for multipart and stream-session operations
- a small reference model for:
  - upload/session state
  - visible object state
  - terminal transitions
- comparison against the real coordinator after each step

Focus the first trace model on:

- create
- append
- finalize
- complete
- abort
- duplicate terminal operations

Success criteria:

- short generated traces can reproduce multipart/session regressions
- every failure prints the exact operation sequence and final state mismatch

## Phase 3: Reclaim and Lease Trace Model

Deliver:

- a bounded trace model for:
  - reads acquiring and releasing payload leases
  - delete/overwrite operations creating reclaim work
  - reclaim-worker steps
  - bucket-delete or cleanup follow-on work where relevant
- checks for:
  - no reclaim while an active lease still protects a generation
  - retry after the final lease drop only when reclaim metadata remains
  - queue contents staying aligned with actual pending reclaim work

This phase should complement, not replace,
`plans/reclaim-queue-hardening-plan.md`.
That plan is about design hardening; this one is about proving the behavior
repeatedly under short stateful traces.

Success criteria:

- reclaim invariants are covered by generated traces rather than only static
  examples
- the known lease/retry bug class is guarded by a broader model

## Phase 4: Targeted Loom-Style Models and Coverage Promotion

Deliver:

- loom-style micro-models only for the smallest synchronization units that
  still lack trustworthy coverage
- promotion of the highest-value trace failures into HTTP or local integration
  regressions where they affect user-visible behavior

Success criteria:

- there is a clear reason for every added concurrency-specific helper or
  dependency
- important lifetime regressions are represented in both low-level and
  externally observable coverage where appropriate

## Validation

Each phase should add runnable coverage with straightforward entry points:

1. targeted `cargo test -p server-core ... -- --nocapture` runs for the new
   stateful and hook-driven modules
2. targeted `cargo test -p server-http ... -- --nocapture` runs for HTTP-level
   cleanup regressions
3. inclusion in the dedicated local security suite once
   `plans/security-test-suite-plan.md` is implemented
4. optional `./scripts/coverage` checks only when a promoted local integration
   test naturally affects the `s3-tests` coverage signal

## Success Criteria

- multipart completion, reclaim retry, and stream-session cleanup have explicit
  invariant-based coverage
- failures print short traces or named interleavings that a reviewer can reason
  about
- the plan stays dependency-light unless a narrow loom-style model is clearly
  warranted
- future lifetime regressions are more likely to be caught as generalized model
  failures rather than new one-off bug reports

## Recommended Order

1. consolidate the existing incident-specific regressions into invariant-driven
   hook tests
2. add the multipart/session trace model
3. add the reclaim/lease trace model
4. only then decide whether any tiny loom-style micro-models are still needed
