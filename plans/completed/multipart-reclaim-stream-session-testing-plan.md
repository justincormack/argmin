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
  justified

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
- `plans/completed/reclaim-queue-hardening-plan.md` covers the historical
  design hardening for reclaim queue behavior, but not a broader stateful
  testing strategy
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

If a loom dependency is needed, that can be added.

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
`plans/completed/security-test-suite-plan.md`.

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

## Stage 0: API Hygiene Before Test Expansion

Before adding broader stateful coverage, tighten the streaming-session API
surface enough that the later tests are exercising the right boundaries rather
than large public implementation-detail bags.

Focus this stage narrowly on the HTTP/session seam, not on redesigning storage
or coordinator interfaces that are already typed and reasonably scoped.

Deliver:

- reduce unnecessary `pub` visibility on streaming HTTP/session context and
  binding types where they do not need to be externally constructed
- prefer narrow constructors and helper methods over open field bags for the
  streaming context objects that cross async/blocking boundaries
- separate raw request provenance from derived/authorized state more clearly
  where those are currently mixed into a single context type
- document any remaining intentionally wide test-only or crate-internal seams
  that Phase 1 will rely on

Non-goals:

- do not redesign the typed storage trait streaming methods
- do not introduce a new session service abstraction
- do not broaden this into a general coordinator refactor

Success criteria:

- the main streaming HTTP/session context types expose only the construction and
  access surface actually needed by production callers
- later invariant/stateful tests can be written against clear boundaries rather
  than depending on broad public field access

Current state:

- complete
- `server-http` streaming helper methods are now internal rather than part of a
  wider helper surface
- streaming session/binding/checksum helper types are no longer public field
  bags
- `StreamingPutContext` and `StreamingPostContext` no longer retain duplicated
  object identity or unused raw request provenance once authorization has
  completed
- async/blocking streaming paths now mostly access object/session/upload state
  through narrow helper methods instead of open struct layout
- the typed storage and coordinator stream-session boundaries were left intact,
  as intended by this stage

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

Current state:

- complete
- new invariant-focused coordinator coverage lives in
  `crates/server-core/src/coordinator/multipart_stateful_tests.rs`
- the first moved regressions now assert explicit lifetime invariants for:
  - streamed multipart part reupload displacing prior shards without orphaning
  - aborting a streamed multipart upload cleaning committed segment rows and
    shard data
  - stale-session scavenging removing abandoned session state
  - stale-session scavenging leaving committed objects and payloads intact
  - staged stream-put data remaining invisible until finalize succeeds
  - duplicate stream-segment append races preserving exactly one staged winner
    across same-PG and cross-PG interleavings
  - later multipart management operations failing predictably once an upload is
    already aborting or completing
  - failed stream-put and stream-part finalize attempts leaving no visible
    object or committed part, with stale-session scavenging removing the
    abandoned staged shards afterwards
  - a real `CompleteMultipartUpload` versus `AbortMultipartUpload` interleaving
    where complete has already snapshotted state but abort wins before the
    final commit, leaving no visible object or leaked multipart state
  - lease-gated reclaim retry behavior, including the no-op case for dropping
    a read-only lease without reclaim metadata and the retry case where the
    final lease drop re-enqueues work only while durable reclaim metadata still
    exists
- the helper layer in that module now supports direct inspection of:
  - active stream sessions
  - pending multipart uploads
  - pending reclaim roots
  - committed multipart part segments for shard-cleanup assertions
- no additional narrow hook point is currently blocking phase 2's bounded
  multipart/session trace model

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

Current state:

- complete
- new bounded trace coverage lives in
  `crates/server-core/src/coordinator/multipart_trace_tests.rs`
- the first Phase 2 property is intentionally narrow:
  - one bucket and one key
  - one active multipart upload at a time
  - one streamed part with constant payload bytes
  - operations covering create, begin-stream-part, append, finalize-part,
    complete, abort-active, abort-session, duplicate abort against the last
    upload id, and duplicate complete replay against the last successfully
    completed upload id
- the current model checks after every step that:
  - pending multipart upload presence matches the model
  - active stream-session presence matches the model
  - `ListParts` visibility matches whether a part is committed
  - object visibility matches whether completion has happened
- the trace now also pins the live-session abort edge:
  - aborting an upload with a live stream-part session removes the upload
  - the session remains until explicit session cleanup
  - no object becomes visible while that orphaned session still exists
- a second bounded Phase 2 property now extends the model to the first
  concurrent same-key multipart shape:
  - up to two pending uploads for the same key
  - one distinct streamed part payload per upload, plus a live stream-part
    session on the newest upload
  - begin/append/finalize/abort-session on the newest upload and
    complete/abort on newest versus oldest upload, including aborting the
    newest upload while its session remains orphaned until explicit cleanup,
    and completing the current upload while a live replacement session exists
    now also preserving that session as orphaned until explicit cleanup,
    without widening to a general N-upload model yet
  - duplicate abort replay against the last terminated upload id and duplicate
    complete replay against the last successfully completed upload id are now
    included in this same-key trace as inert replays
  - checks that pending upload ordering, active session presence, per-upload
    `ListParts`, and visible object winner identity stay aligned with the
    model after each step
- a third bounded Phase 2 property now covers real multipart composition for a
  single upload without widening to arbitrary part sets:
  - one buffered head part at the AWS minimum non-final part size, with
    distinct overwrite payloads so same-part winner identity is observable
  - one streamed tail part with explicit session begin/append/finalize/abort
    and distinct refinalize payloads so streamed same-part winner identity is
    observable too
  - complete-head-only versus complete-head-and-tail terminal choices
  - abort-upload while the tail session is still live, with orphaned session
    cleanup modeled explicitly afterward
  - duplicate abort replay against the last terminated upload id and duplicate
    complete replay against the last successfully completed composed upload are
    now included in this head-plus-tail trace too
  - checks that pending upload presence, `ListParts` part-number visibility,
    active tail-session presence, and visible object body stay aligned with the
    model after each step

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

This phase complemented, rather than replaced,
`plans/completed/reclaim-queue-hardening-plan.md`.
That plan was about design hardening; this one was about proving the behavior
repeatedly under short stateful traces.

Current state:

- complete
- a first bounded reclaim/lease trace now lives in
  `crates/server-core/src/coordinator/multipart_reclaim_trace_tests.rs`
- the initial property is intentionally narrow:
  - one bucket, one key, and one payload generation
  - one optional active payload lease
  - one durable simple-payload reclaim record
  - one deduplicated object-reclaim queue slot plus one bucket-delete finalize
    queue slot
- operations currently cover:
  - seeding durable reclaim metadata
  - acquiring and releasing the payload lease
  - explicitly enqueueing object reclaim
  - consuming object reclaim worker steps
  - consuming bucket-delete finalize worker steps
  - asserting that no work is queued
- the current model checks after every step that:
  - active lease presence matches the live lease count
  - durable reclaim metadata presence matches the model
- when explicit worker or no-work observation steps are taken, the
  worker-visible queue order matches the real storage-node priority
  (`ObjectPayload` before `BucketDelete`)
- this already generalizes the existing fixed regressions for:
  - no reclaim work from dropping a read-only lease without durable metadata
  - retry only after the final lease drop while reclaim metadata still exists
- a second bounded reclaim/lease property now extends that model to two
  generations on the same key without widening to arbitrary histories:
  - separate durable reclaim metadata and lease state for an older and newer
    payload generation
  - FIFO object-reclaim queue ordering across generations
  - bucket-delete finalize remaining deduplicated at the bucket level even when
    multiple generations reclaim successfully
  - explicit deleting-bucket finalization steps, including the real behavior
    where bucket-delete work follows the current bucket reclaim root and can
    enqueue only that next generation’s object reclaim before the bucket itself
    can disappear
  - checks after every step that per-generation metadata presence and lease
    counts stay aligned with the model while worker steps validate the expected
    queue order
- a third bounded reclaim/lease property now covers cross-key bucket-root
  ordering without widening to arbitrary object sets:
  - two keys in the same bucket with one payload generation each
  - independent lease state and durable reclaim metadata per key
  - bucket-delete finalization following the real `key ASC` reclaim-root order
    across keys
  - object-reclaim worker steps validating the expected per-key dequeue order
    while post-step assertions keep metadata presence, lease counts, and bucket
    existence aligned with the model
- a fourth bounded reclaim/lease property now varies the reclaim root kind for
  one key and one generation:
  - `simple_payload_reclaims`
  - `object_segments_reclaims`
  - `multipart_reclaims`
  - the model checks that lease-gating, background object reclaim, and inert
    bucket-delete follow-on work behave the same across all three durable
    reclaim surfaces

Success criteria:

- reclaim invariants are covered by generated traces rather than only static
  examples
- the known lease/retry bug class is guarded by a broader model

## Phase 4: Targeted Loom-Style Models and Coverage Promotion

Deliver:

- promotion first:
  - convert a small number of the highest-value saved shrink cases from
    `crates/server-core/proptest-regressions/coordinator/multipart_trace_tests.txt`
    and
    `crates/server-core/proptest-regressions/coordinator/multipart_reclaim_trace_tests.txt`
    into named deterministic regressions
  - prefer local coordinator/invariant regressions over broader new modeling
    when a shrink already captures the behavior clearly
- at most one loom-style micro-model unless promotion still leaves a real
  synchronization concern:
  - the default candidate is the smallest lease-count / reclaim-worker handoff
    that could still hide an interleaving bug
  - only add a second micro-model if the first one proves valuable or still
    leaves distrust
- do not widen back out to new broad trace families in this phase
- only promote to HTTP-level coverage when the trace failure clearly maps to a
  user-visible contract

Success criteria:

- 2-4 promoted deterministic regressions land before any new concurrency helper
  is introduced
- there is a clear reason for every added concurrency-specific helper or
  dependency
- if a loom-style helper is added at all, it is tiny, local, and justified by
  a concrete remaining interleaving risk

Current state:

- complete on the promotion-first path, with no new concurrency helper needed
- promoted deterministic regressions now directly cover:
  - completing an upload while a live replacement stream session still exists,
    with the replacement session left orphaned until explicit cleanup
  - completing the current same-key upload and publishing its payload while
    clearing pending multipart state
  - completing the older and then newer same-key uploads, with the newer
    completion overwriting visible object state and leaving no pending uploads
  - object reclaim work taking priority over stale bucket-delete follow-on work
  - deleting-bucket finalization advancing from an older generation root to a
    newer generation root
  - deleting-bucket finalization not skipping an older generation root that is
    still lease-blocked
  - deleting an empty bucket without any follow-on reclaim work
- no loom-style helper was added because the promoted regressions and existing
  Phase 3 traces left no concrete remaining interleaving risk that justified a
  new concurrency-specific dependency or helper

## Validation

Each phase should add runnable coverage with straightforward entry points:

1. targeted `cargo test -p server-core ... -- --nocapture` runs for the new
   stateful and hook-driven modules
2. targeted `cargo test -p server-http ... -- --nocapture` runs for HTTP-level
   cleanup regressions
3. inclusion in the dedicated local security suite once
   `plans/completed/security-test-suite-plan.md` is implemented
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
