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

In progress.

Completed:

- deep tracing is behind the non-default `deep-tracing` cargo feature and no
  longer part of the default production build path
- production/local docs now treat deep tracing as local/test-only
- the always-on production event surface has been reduced to:
  - `request_finish`
  - `request_error`
  - `slow_request`
  - `bucket_lock_wait_exceeded`
- dense request/stream/layout observability is now `deep-tracing` only
- `crates/observability` now provides small helper APIs for the remaining
  production events
- `crates/observability` now maintains a minimal in-process metrics snapshot
  with:
  - `inflight_requests`
  - `request_finish_total`
  - `request_error_total`
  - `slow_request_total`
  - `bucket_lock_wait_exceeded_total`

Remaining:

- choose how to expose `metrics_snapshot()` for explicit local ops/debugging
- run the eBPF/OpenTelemetry profiling pilot in staging
- remove or demote the remaining dense `trace_scope!` callsites in
  coordinator/storage/auth paths that are still compiled for local deep tracing

## Current State

The code is already much closer to the target state than the original plan
assumed.

### Production path today

- deep tracing is off by default and not documented as a production mechanism
- always-on semantic telemetry is limited to request summaries, slow-request
  summaries, and thresholded lock-wait summaries
- the production event formatting and metric increments are centralized in
  `crates/observability`

### Local-only deep tracing today

- dense request/stream/layout detail remains available only behind the
  non-default `deep-tracing` feature
- this is intended for local debugging and tests, not production deployments

### Main gap to close next

- production-safe local visibility into the new metric snapshot
- production profiling via eBPF/OpenTelemetry
- further cleanup of remaining dense tracing callsites now that the production
  event surface is stable

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

## Current Observability Concentration

The remaining observability code is still concentrated in a few files. Some of
this is now production summary telemetry, and some is local-only deep tracing
that still needs further cleanup.

- `crates/observability/src/lib.rs`
  - trace sink setup for local deep tracing
  - request-local trace stack
  - `TraceScope`
  - production summary-event helpers and metrics snapshot
- `crates/server-http/src/http/serve.rs`
  - request summary emission
  - slow-request summaries
  - local-only streaming PUT and UploadPart detail
- `crates/server-http/src/http/mod.rs`
  - request terminal summary emission
  - in-flight request guard lifetime handling
- `crates/server-core/src/coordinator.rs`
  - broad operation-level local trace coverage across many S3 paths
  - read-path tracing such as `ReadHandle::next_chunk`
- `crates/storage/src/node.rs`
  - thresholded bucket lock wait summaries
- `crates/storage/src/pg_store.rs`
  - dense storage-layer local trace coverage
- `crates/auth/src/request.rs` and `crates/auth/src/post.rs`
  - auth trace scopes still primarily useful only for local deep tracing

## Keep vs Remove

### Keep as semantic telemetry

These still provide value that profiling cannot:

- request finish summary
- request transport/body error summary
- response status, total latency, and body size summary
- slow-request summaries
- lock wait summaries when a threshold is exceeded
- cheap in-process counters/gauges for inflight request count and key abnormal
  event totals
- queue-pressure or background-work delay summaries when a threshold is exceeded
- auth rejection summaries with stable error classes if they prove necessary in
  production

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

Status: completed in commits `af23fa0` and `3e8ab11`.

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
performance visibility while we finish removing the remaining dense local trace
coverage.

Status: not started.

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

Status: mostly completed in commits `e83680a`, `0af4140`, `b2224ed`, and
`f78d191`.

### Required Event Types

- `request_finish`
- `request_error`
- `slow_request`
- `lock_wait_exceeded`
- optional future additions if needed:
  - `auth_failure`
  - `background_queue_delay_exceeded`

### Event Rules

- no nested internal span trees in production
- events must be summary-oriented, not step-oriented
- events must be low-cardinality except where bucket/key context is essential
- thresholds and sampling must be explicit
- existing escaping/redaction rules remain mandatory

## Phase 4: Refactor the In-Process Observability Crate

Reduce `crates/observability` from a general tracing system to a minimal event,
formatting, and metric layer.

Status: mostly completed in commit `2249c45`.

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
5. Add minimal counters/gauges for the production telemetry surface.
6. Keep the remaining crate focused on production-safe summary telemetry.

### Notes

This phase does not need a new production dependency. We can keep the reduced
crate internal and small.

Completed in this phase:

- summary helper APIs now exist for request finish/error, slow requests, and
  thresholded lock waits
- minimal production-safe counters/gauges now exist

Remaining work in this phase:

- decide whether to expose `metrics_snapshot()` via a local debug endpoint,
  periodic dump, or another explicit local-only path
- add queue-delay helpers only if/when we introduce queue-pressure summaries

## Phase 5: Remove Dense Hot-Path Instrumentation

Delete or demote the highest-volume callsites first. The production event
surface has already been reduced; this phase is now about shrinking the
remaining dense local-only trace coverage.

Status: in progress. Event-surface reductions landed in commits `e83680a`,
`0af4140`, `b2224ed`, and `f78d191`, but many dense `trace_scope!` callsites
still exist in coordinator/storage/auth code.

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
  - `request_start` is now `deep-tracing` only
  - `request_finish`, `request_error`, and `slow_request` remain always-on
  - streaming PUT and UploadPart detail events are now `deep-tracing` only
- `crates/server-http/src/http/mod.rs`
  - response lifecycle has been collapsed to request-level summaries
- `crates/storage/src/node.rs`
  - keep lock wait reporting only above threshold
  - lock acquire/release chatter has been removed

### Validation

- request throughput and tail latency improve measurably with semantic events
  enabled
- flamegraphs still show the real hotspots after trace removal
- incident debugging remains possible from request summaries plus profiles

## Phase 6: Keep a Narrow Local Debug Mode

The existing deep tracing may still be useful locally. If so, keep it only as a
deliberate debug path with a clearly different contract.

Status: effectively completed for now. Deep tracing remains available only via
the non-default feature and local/test docs.

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
- unit tests for the minimal metrics snapshot / guard behavior
- integration tests proving request summaries remain correctly escaped/redacted
- regression tests for slow-request and lock-wait event thresholds

### Performance validation

- benchmark representative request mixes before and after removing dense spans
- compare p50/p95/p99 latency with production-style semantic events enabled
- confirm profiler overhead remains acceptable in canaries

### Operational rollout

1. choose a local-only exposure path for the metrics snapshot
2. stage the profiler pilot
3. canary the profiler rollout if the staging results are good
4. remove remaining dense production-adjacent trace callsites
5. keep local deep tracing temporarily if still useful
6. reassess whether the remaining internal observability crate can be reduced
   further

## Recommended Order

1. choose how to expose local metrics snapshots
2. pilot eBPF/OpenTelemetry profiling in staging
3. remove remaining dense hot-path spans from coordinator, storage, and auth
4. retain or delete local deep tracing based on actual remaining value
