# Production Logging Plan

## Context

We currently have several observability mechanisms, but not a coherent
production logging contract.

Existing pieces:

- `crates/observability` provides request trace context, a bounded flight
  recorder, summary metrics, and local/deep tracing support.
- `plans/production-observability-migration-plan.md` covers production
  profiling, metrics, and the move away from dense always-on tracing.
- `guides/observability-formatting.md` defines escaping and redaction rules for
  trace and future log output.
- many operational messages still use direct `eprintln!`, especially in
  `argmin-s3`, frontend listener setup, storage-node serving, control-plane
  loops, and some diagnostic paths.

Direct `eprintln!` is acceptable for quick local debugging and for early
process-start failures before logging is initialized. It is not a good
long-term production interface because:

- tests have no standard way to capture or suppress expected log output
- call sites decide formatting, redaction, and structure independently
- future JSON/systemd/journald output would require rewriting scattered prints
- severity, component, request ID, node ID, PG ID, and error kind are not
  consistently represented
- operator-facing logs and test diagnostics are mixed together

## Goal

Define a small structured logging layer that all production log events use,
while keeping the default deployment behavior simple: write logs to stderr so
systemd, containers, and existing scripts can collect them.

The logging layer should make it possible to switch formatting and sinks later
without changing every call site.

## Non-Goals

- implementing the logging subsystem in this plan
- replacing metrics, flight recorder, or local deep tracing
- adding a broad logging dependency before we decide it is needed
- logging every request or every internal operation by default
- changing S3-visible protocol errors or response bodies

## Relationship to Existing Observability

Logging should be distinct from the existing layers:

- **Metrics:** counters/gauges for dashboards, alerts, and soak health gates.
- **Flight recorder:** bounded in-memory recent-event context for panics and
  failed local/UAT runs.
- **Deep tracing:** explicit local/test-only dense traces.
- **Production logs:** durable operator-facing records for process lifecycle,
  configuration problems, background worker failures, retry exhaustion,
  security-relevant denials where useful, and rare correctness/health events.

Some events may feed more than one layer. For example, a storage RPC timeout may
increment a metric, emit a flight-recorder record, and log a warning only when
it crosses a threshold or reaches retry exhaustion.

## Logging Contract

All production log call sites should eventually use a shared API, not direct
`eprintln!`.

Each log record should have:

- timestamp
- level: `error`, `warn`, `info`, `debug`
- component: for example `frontend`, `storage_node`, `control_plane`,
  `coordinator`, `metadata_store`
- stable event name
- optional trace/request ID from the current observability context
- stable fields for node, PG, bucket, key, operation, error code, status, and
  latency where relevant
- a short message intended for humans

The stable event name and fields matter more than the prose message. Tests and
production tooling should match on event names and fields, not on message text.

## Formatting Rules

Logging must follow `guides/observability-formatting.md`.

In particular:

- non-secret attacker-controlled text must be escaped or represented through
  hardened `Debug`
- secrets and bearer material must be redacted
- raw query strings, credentials, SSE-C material, wrapped keys, signatures, and
  session tokens must not be logged
- large blobs and config documents should be summarized by count, length, or
  presence
- errors should prefer stable error codes or typed classifications when raw
  error text may include request-derived values

## Sinks and Formats

The initial target should support:

1. **Plain text stderr**
   Default production mode. One line per record, suitable for systemd,
   containers, and existing local scripts.

2. **Test sink**
   Test-only capture sink so tests can assert, suppress, or inspect expected
   log events without relying on global stderr behavior.

   The test sink must be safe under `nextest` and parallel `cargo test`.
   It must not globally capture or suppress unrelated test output. Capture
   should be scoped to a request/task/log context where possible, with explicit
   nesting semantics. If expected logs are emitted from spawned worker threads,
   tests must pass an explicit capture context or use an API that records only
   matching structured events; installing process-global stderr or panic-hook
   suppression is not acceptable.

Future possible sinks:

- JSON stderr for log shippers, one JSON object per line
- file output with rotation delegated to deployment tooling
- journald-native fields if this becomes valuable
- OpenTelemetry logs if the deployment stack settles on it

The shared API should hide the sink and format choice from call sites.

## Initialization

Logging should initialize early in `argmin-s3`, but there are unavoidable
pre-initialization cases:

- root-user rejection and other hard process-start guardrails
- invalid configuration that prevents even building the runtime/config object
- logger initialization failure itself

Those early paths may continue to write directly to stderr, but they should be
few, documented, and easy to audit.

After logging initialization, production code should not call `eprintln!`
directly except for local/debug-only features explicitly marked as such.

## API Shape

The first implementation can be deliberately small. Possible shape:

```rust
observability::log_event(LogEvent {
    level: LogLevel::Warn,
    component: "storage_node",
    event: "metadata_command_lock_wait_blocked",
    fields: &[("node_id", node_id), ("pg_id", pg_id), ...],
    message: "metadata command lock wait exceeded diagnostic threshold",
});
```

Call sites should not build ad hoc JSON or parseable text strings themselves.

Important implementation details:

- use low-cardinality stable event names
- keep field keys stable and documented
- allow cheap no-allocation fast paths for disabled debug-level records
- avoid holding locks while formatting log records
- never panic from logging
- expose a test sink that is scoped and restores previous state on drop
- make test capture parallel-safe: nested captures must be deterministic, and
  unrelated threads/tests must continue to log normally

## Severity Guidance

Use severity consistently:

- `error`: operation failed permanently or process/service cannot continue
- `warn`: abnormal but retryable/degraded behavior, retry exhaustion, unexpected
  state recovered by fallback, or slow contention above a threshold
- `info`: process lifecycle, configured mode, listener startup, clean shutdown,
  major control-plane transitions
- `debug`: local-only or explicitly enabled diagnostic detail

Routine expected client errors should generally not be production logs unless
they are rate-limited/security-relevant summaries. They should be metrics and
request summaries instead.

## Migration Inventory

Initial audit targets:

- `crates/argmin-s3/src/main.rs`
  - process startup and configuration errors
  - control-plane socket accept/worker errors
  - OpenRaft control-plane errors
  - storage-node server startup/runtime failures
  - frontend listener startup failures
- `crates/server-http/src/http/serve.rs`
  - accept errors
  - TCP setup errors
  - TLS handshake errors
  - local debug flight-recorder dumps
- `crates/server-http/src/http/mod.rs`
  - panic-on-500 flight-recorder dump path
- `crates/storage/src/storage_node_server.rs`
  - storage-node connection/session failures
  - metadata-command lock wait diagnostics
- `crates/storage/src/cluster.rs` and `crates/storage/src/pg_store.rs`
  - remaining direct diagnostic `eprintln!` calls

Each migration should decide whether the existing output is:

- production log
- metric only
- flight-recorder only
- local/debug-only output
- early startup stderr exception
- intentional user-visible CLI stdout output
- benchmark/example output
- test/UAT helper diagnostic output
- obsolete and removable

## Phase 1: Define the Logging Surface

Status: not started.

Work:

1. Add a small logging API in `crates/observability`.
2. Define `LogLevel`, event name rules, and field representation.
3. Add plain-text stderr sink as the default.
4. Add test sink support for capture/suppression.
5. Add focused tests for redaction, escaping, one-line output, and test-sink
   scoping.

Exit criteria:

- new production logging call sites can avoid `eprintln!`
- tests can capture expected logs without suppressing unrelated panics or
  process-wide stderr
- formatting follows `guides/observability-formatting.md`

## Phase 2: Convert High-Value Runtime Logs

Status: not started.

Work:

1. Convert storage-node session and lock-wait diagnostics.
2. Convert frontend listener/TLS accept errors.
3. Convert control-plane and storage-node runtime loop errors.
4. Convert process lifecycle `info` logs after logger initialization.
5. Leave documented early-startup stderr exceptions in place.

Exit criteria:

- runtime services use structured logging for operator-facing events
- remaining direct `eprintln!` use is either early startup or explicitly
  local/debug-only

## Phase 3: Add JSON Output Mode

Status: not started.

This is a later extension, not required for the first logging API.

Work:

1. Add an environment/config option for log format, for example
   `ARGMIN_LOG_FORMAT=plain|json`.
2. Emit one JSON object per line.
3. Keep field names stable across plain and JSON formats.
4. Add tests that verify escaping/redaction in JSON mode.

Exit criteria:

- production deployments can choose JSON without changing code
- plain stderr remains the default

## Phase 4: Audit and Enforce

Status: not started.

Work:

1. Audit direct `eprintln!` and `println!` call sites.
2. Document allowed exceptions.
3. Add a lightweight check, likely script-based, that flags new production
   `eprintln!` call sites unless they are in an allowlist.
4. Update review guidance so new operator-facing diagnostics use the logging
   API.

Exit criteria:

- new scattered production `eprintln!` use is caught during review/CI
- the allowlist is small and intentional
- intentional `println!` output from admin/status commands, benchmark/example
  binaries, test helpers, and protocol/stdout command output is not treated as
  production logging and is not migrated to the logging API

## Open Questions

- Should we use the existing custom observability module only, or introduce a
  small dependency such as `tracing` once the API shape is agreed?
- Should JSON logging be enabled only in binaries, or should libraries know
  about sink format for tests?
- Should request summaries eventually be logs, metrics-only events, or both?
- How much log sampling/rate limiting is needed for high-frequency warnings?

## Recommendation

Do not start by converting every `eprintln!`.

Start by adding the shared API and test sink, then convert one or two noisy
runtime paths such as metadata-command lock wait diagnostics and frontend accept
errors. That proves the API shape before migrating the larger `argmin-s3`
startup/control-plane surface.
