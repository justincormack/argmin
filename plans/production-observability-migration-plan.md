# Production Observability Migration Plan

## Context

Our current observability path is a custom in-process trace/event system. It is
useful for local diagnosis, but it is too expensive to rely on in production
for performance work.

The current design does work on the request path:

- request-scoped trace IDs are created in
  `crates/server-http/src/http/serve.rs`
- nested `trace_scope!` spans are emitted throughout hot code in
  `crates/server-core/src/coordinator.rs`,
  `crates/storage/src/pg_store.rs`, and `crates/storage/src/node.rs`
- events are formatted into strings and synchronously written or enqueued in
  `crates/observability/src/lib.rs`

This is the wrong cost model for production performance visibility. eBPF-based
OpenTelemetry profiling is a better fit there because it samples execution from
outside the process instead of asking the application to format high-volume
span/event text on hot paths.

However, profiling is not a full replacement for semantic application
telemetry. We still need a small amount of in-process observability for:

- request identity and request outcome
- auth and policy failure summaries
- lock contention and queue pressure summaries
- rare correctness-debugging events that a profiler cannot infer

## Status

Not started.

## Goals

- use eBPF/OpenTelemetry Profiles for production performance analysis
- stop depending on deep custom tracing in production
- keep enough semantic telemetry to debug correctness and operational failures
- preserve current observability hardening for escaping and redaction
- reduce hot-path observability overhead to a bounded, low-rate cost

## Non-Goals

- replacing profiling with another broad in-process tracing framework
- adding new production dependencies before we agree they are necessary
- preserving every existing trace line or internal span name
- removing all local deep tracing on day one if it still helps development

## Target State

We should end up with three distinct layers:

1. **Production profiling**
   Linux eBPF continuous profiling exported through OpenTelemetry Profiles via
   the Collector path.

2. **Minimal semantic telemetry**
   Low-volume in-process events for request lifecycle, failures, slow requests,
   and resource contention. These should be thresholded and summarized rather
   than emitted for every internal function call.

3. **Local deep tracing**
   An explicitly local-only diagnostic mode for narrow investigations. This is
   not a production observability mechanism.

## Current Hot-Path Observability Surface

The current system is concentrated in a few files and should be reduced there
first:

- `crates/observability/src/lib.rs`
  - trace sink setup
  - request-local trace stack
  - `TraceScope`
  - `event_in_context`
- `crates/server-http/src/http/serve.rs`
  - request start events
  - streaming PUT and UploadPart lifecycle events
- `crates/server-http/src/http/mod.rs`
  - response body lifecycle events
- `crates/server-core/src/coordinator.rs`
  - broad operation-level span coverage across many S3 paths
  - read-path tracing such as `ReadHandle::next_chunk`
- `crates/storage/src/node.rs`
  - bucket lock wait/acquire timing
- `crates/storage/src/pg_store.rs`
  - dense storage-layer span coverage
- `crates/auth/src/request.rs` and `crates/auth/src/post.rs`
  - request auth trace scopes

## Keep vs Remove

### Keep as semantic telemetry

These still provide value that profiling cannot:

- request start and request finish summary
- response status, total latency, and body size summary
- auth rejection summaries with stable error classes
- slow-request summaries
- lock wait summaries when a threshold is exceeded
- queue-pressure or background-work delay summaries when a threshold is exceeded
- streaming upload summary events at session start/finish/failure, not per chunk

### Remove or demote out of production

These should not remain as always-on production instrumentation:

- most nested `trace_scope!` spans in `coordinator.rs`
- dense storage-level spans in `pg_store.rs`
- per-call read spans such as `ReadHandle::next_chunk`
- auth entry spans whose only value is timing internal function boundaries
- per-segment or per-chunk streaming ingestion events

## Phase 1: Gate Deep Tracing Out of Production

The current deep tracing code is not suitable for production. The first step is
to make that true in the build and runtime model, not just in guidance.

### Work

1. Feature-gate deep tracing behind a non-default cargo feature such as
   `deep-tracing`.
2. Compile deep tracing APIs and dense `trace_scope!` callsites only when that
   feature is enabled.
3. Keep test and local-debug workflows able to opt in explicitly.
4. Do not use raw `#[cfg(test)]` as the primary mechanism, because integration
   tests compile library crates as normal dependencies and would not see test-
   only code paths.
5. Remove deep tracing from normal production builds and production guidance.

### Exit Criteria

- production builds do not compile in deep tracing by default
- test and local-debug workflows can still enable deep tracing explicitly
- the codebase no longer treats deep tracing as production-eligible

## Phase 2: Profiling Pilot

Stand up eBPF/OpenTelemetry profiling next, so we have production-appropriate
performance visibility while slimming the remaining in-process telemetry.

### Work

1. Deploy the OpenTelemetry Collector eBPF profiling receiver in a Linux
   staging environment.
2. Confirm Rust release builds symbolize well enough to identify coordinator,
   storage, auth, and HTTP hotspots.
3. Verify the profile backend and retention path are acceptable for our
   operational model.
4. Run representative workloads:
   - large PUT and UploadPart
   - range GET and multipart GET
   - bucket listing
   - overwrite/delete workloads
5. Compare profiler output against the hotspots that originally motivated the
   custom tracing work.

### Exit Criteria

- profiler output clearly identifies hot functions and lock/contention regions
- symbolization is good enough to act on profiles
- overhead is low enough for production canaries

## Phase 3: Define the Minimal Semantic Event Set

Before editing code, define the events that remain allowed in production.

### Required Event Types

- `request_start`
- `request_finish`
- `request_error`
- `auth_failure`
- `slow_request`
- `lock_wait_exceeded`
- `background_queue_delay_exceeded`
- `streaming_upload_summary`

### Event Rules

- no nested internal span trees in production
- events must be summary-oriented, not step-oriented
- events must be low-cardinality except where bucket/key context is essential
- thresholds and sampling must be explicit
- existing escaping/redaction rules remain mandatory

## Phase 4: Refactor the In-Process Observability Crate

Reduce `crates/observability` from a general tracing system to a minimal event
and formatting layer.

### Work

1. Keep:
   - `escaped`
   - `redacted`
   - `query_summary`
   - lightweight request ID / context support
2. Stop using `TraceScope` for production paths.
3. Replace `event_in_context` usage patterns with a smaller semantic event API
   that is intended for summary events only.
4. Add thresholded helpers for:
   - slow requests
   - lock waits
   - queue delays
5. Keep the remaining crate focused on production-safe summary telemetry.

### Notes

This phase does not need a new production dependency. We can keep the reduced
crate internal and small.

## Phase 5: Remove Dense Hot-Path Instrumentation

Delete or demote the highest-volume callsites first.

### First removals

- `crates/server-core/src/coordinator.rs`
  - broad `trace_scope!` coverage on S3 operation implementations
  - `ReadHandle::next_chunk`
- `crates/storage/src/pg_store.rs`
  - storage-level internal span coverage
- `crates/auth/src/request.rs`
  - auth trace entry spans
- `crates/auth/src/post.rs`
  - POST auth trace entry spans

### Convert to summaries

- `crates/server-http/src/http/serve.rs`
  - keep `request_start`
  - add `request_finish` and `slow_request`
  - collapse streaming PUT and UploadPart events into per-request summaries
- `crates/server-http/src/http/mod.rs`
  - keep response lifecycle only as summarized outcome data
- `crates/storage/src/node.rs`
  - keep lock wait reporting only above threshold

### Validation

- request throughput and tail latency improve measurably with semantic events
  enabled
- flamegraphs still show the real hotspots after trace removal
- incident debugging remains possible from request summaries plus profiles

## Phase 6: Keep a Narrow Local Debug Mode

The existing deep tracing may still be useful locally. If so, keep it only as a
deliberate debug path with a clearly different contract.

### Options

1. Keep deep tracing behind the non-default `deep-tracing` feature for tests
   and explicit local debugging only.
2. If the maintenance burden is not justified after the profiling rollout,
   delete it entirely.

The decision can be made after phases 4 and 5. It does not need to block the
production migration.

## Testing and Rollout

### Code validation

- unit tests for any new summary/threshold helpers in `crates/observability`
- integration tests proving request summaries remain correctly escaped/redacted
- regression tests for slow-request and lock-wait event thresholds

### Performance validation

- benchmark representative request mixes before and after removing dense spans
- compare p50/p95/p99 latency with production-style semantic events enabled
- confirm profiler overhead remains acceptable in canaries

### Operational rollout

1. gate deep tracing out of production builds
2. staging profiler pilot
3. canary profiler rollout
4. remove dense production trace callsites
5. keep local deep tracing temporarily if still useful
6. reassess whether the remaining internal observability crate can be reduced
   further

## Recommended Order

1. gate deep tracing behind a non-default feature immediately
2. pilot eBPF/OpenTelemetry profiling in staging
3. define and document the minimal semantic event set
4. slim `crates/observability` to summary-oriented production telemetry
5. remove dense hot-path spans from coordinator, storage, and auth
6. retain or delete local deep tracing based on actual remaining value
