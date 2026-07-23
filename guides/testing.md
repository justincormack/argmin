# Testing Guide

This guide collects the main testing workflows for the repository:

- targeted crate tests
- workspace and integration coverage
- AWS-backed compatibility runs
- golden response-shape assertions
- HTTP-only and local-only test crates
- parser fuzzing

For the current compatibility target and explicitly documented AWS gaps, see
[aws-compatibility.md](aws-compatibility.md).

## Quick reference

Common commands:

```bash
# Targeted crate tests while changing auth or HTTP parsing.
cargo test -p auth -- --nocapture
cargo test -p server-http -- --nocapture

# Broad local verification.
cargo nextest run
./scripts/check-storage-cluster-boundaries
cargo clippy --all-targets --all-features -- -D warnings

# Release-mode control-plane invariant and process gate.
./scripts/ci-control-plane-release

# Integration coverage.
./scripts/coverage

# Sweep all fuzz targets for a fixed time budget each.
./scripts/fuzz

# Local-only S3 behavior tests.
cargo test -p s3-local-tests

# AWS-backed S3 tests.
./scripts/aws-tests

# Explicit STS discovery oracle (not part of aws-tests).
./scripts/aws-sts-oracle

# Standalone binary UAT acceptance run.
./scripts/uat-s3-tests --test bucket_crud
```

## Deterministic Unit Tests

Unit tests, property tests, and local crate-level harnesses must be fully
deterministic.

That means:

- do not use wait-and-timeout polling in unit tests or local test harnesses
- do not spawn helper threads and then "hope" they reply within some small
  timeout
- do not use sleeps as a substitute for correct test coordination

If a local/unit test needs to observe queued work, background state, or
concurrent transitions, the harness must expose a deterministic mechanism for
doing so directly. Prefer explicit hooks, direct queue inspection/pop helpers,
barriers, or other fully controlled synchronization. A test that can fail
because the machine is busy is a broken test harness.

The exception is external compatibility/integration coverage where the system
under test is AWS or another remote S3 endpoint. In those cases, bounded
eventual checks are correct because AWS control-plane convergence is part of
the real behavior being modeled. That is acceptable in `crates/s3-tests` and
similar external suites, but not in ordinary unit/property tests.

## Release-Mode Control-Plane Gate

`./scripts/ci` runs `./scripts/ci-control-plane-release` as a distinct step
after the ordinary debug workspace suite. The release gate also remains
independently runnable for focused verification. It executes the typed
control-plane snapshot publication regressions and the complete multi-process
Raft integration test binary with release optimizations and `debug_assertions`
disabled. This catches release-only `cfg`, overflow, timeout, and
optimization-sensitive behavior at the same process boundary used by the
replicated control plane.

## Storage Cluster Boundary Checks

Run `./scripts/check-storage-cluster-boundaries` as part of broad local
verification while the multihost transition is in progress. The script fails on
boundary regressions that should break loudly during command-log and multihost
work:

- broad `self.single_node` use outside the approved `StorageCluster` bridge
  helpers
- production shard read/write/delete calls that bypass placed `StorageCluster`
  IO
- legacy metadata-primary payload write paths
- ungated production `PgMetadataStore` methods that are not in the explicit
  read-only/finalized-delete/write-drain allowlist
- route/control-plane errors converted through generic `StoreError::Io`

The operation classes and bridge-era invariants are documented in
[storage-cluster-invariants.md](storage-cluster-invariants.md).

## Storage Metadata Test Setup

Prefer production request paths when creating ordinary metadata state in tests.
Coordinator tests should normally use `Coordinator` methods, and storage-cluster
tests should normally use `StorageCluster` command APIs. This keeps the test
surface exercising epoch fencing, pending-command retry, command-log
validation, placement routing, cleanup ownership, and replica fanout.

Direct `PgMetadataStore` access is appropriate only for narrowly scoped cases:

- low-level `PgStore` tests that validate schema constraints, indexes,
  canonical row encoding, or exact table behavior
- assertions against replica-local rows after a production API call
- deliberate fault injection, divergence, corruption, or otherwise unreachable
  intermediate state
- test hooks that model a crash/retry point more precisely than a public API can

When a cluster or coordinator test uses direct `PgMetadataStore` mutation, add a
short comment explaining the unreachable or divergent state being created. Avoid
using it as a setup shortcut for normal buckets, objects, multipart uploads,
stream sessions, reclaim records, or bucket subresources.

## External Test Scripts

For external AWS-backed workflows, prefer the wrapper scripts under
`./scripts/` rather than reconstructing long `cargo test` commands by hand.

- `./scripts/aws-tests`
  - runs the external `s3-tests` package
  - loads AWS credentials from `.env`
  - sets the required service endpoint and shared `AWS_TEST_*` variables
  - forwards extra arguments to the `cargo test` command
- `./scripts/aws-sts-oracle`
  - runs the separate, explicitly selected STS discovery probes
  - is not invoked by `./scripts/aws-tests`
  - accepts `--assume-role` for its self-cleaning same-account fixture
- `./scripts/uat-s3-tests`
  - starts the standalone `argmin-s3` binary over HTTPS
  - configures UAT-only primary, alternate, same-account constrained, and
    owner-root credentials
  - runs `s3-tests` against that process as an external endpoint
  - forwards extra arguments to `cargo nextest run -p s3-tests`
- `./scripts/cleanup`
  - cleans up leftover external test buckets
  - loads the primary AWS credentials from `.env`
  - uses the same AWS user as `./scripts/aws-tests`

The AWS-backed scripts accept `--region`. `aws-tests` accepts additional
`cargo test` selectors and `-- --nocapture` style test-binary arguments.
`uat-s3-tests` accepts additional `cargo nextest run` selectors and options.

## Standalone `argmin-s3` UAT `s3-tests`

`./scripts/uat-s3-tests` is the acceptance route for running the same
external-endpoint `s3-tests` harness against the real `argmin-s3` binary
instead of the embedded in-process test server.

The wrapper:

- starts `cargo run -p argmin-s3` with a temporary data directory, or starts an
  explicit binary path when `--binary PATH` is provided
- enables HTTPS using the repository localhost test certificate
- passes `S3_TEST_TLS_CA_CERT_PATH` so the external AWS SDK clients trust that
  local certificate
- configures the primary credential through the normal production variables
  `ARGMIN_ACCOUNT_ID`, `ARGMIN_ACCESS_KEY_ID`, and
  `ARGMIN_SECRET_ACCESS_KEY`
- configures the extra identities needed by `s3-tests` through UAT-only
  variables:
  - `ARGMIN_UAT_ALT_ACCOUNT_ID`
  - `ARGMIN_UAT_ALT_ACCESS_KEY_ID`
  - `ARGMIN_UAT_ALT_SECRET_ACCESS_KEY`
  - `ARGMIN_UAT_SECOND_ACCESS_KEY_ID`
  - `ARGMIN_UAT_SECOND_SECRET_ACCESS_KEY`
  - `ARGMIN_UAT_OWNER_ROOT_ACCESS_KEY_ID`
  - `ARGMIN_UAT_OWNER_ROOT_SECRET_ACCESS_KEY`
- enables `ARGMIN_ABORT_ON_500=1` by default so internal server errors are not
  hidden by SDK retries during local acceptance runs

These UAT variables exist only to drive acceptance and conformance testing.
They are deliberately not a production account-management API.

Example targeted run:

```bash
./scripts/uat-s3-tests --test bucket_crud
```

Example run against an already-built binary:

```bash
./scripts/uat-s3-tests --binary ./target/debug/argmin-s3 --test bucket_crud
```

Example full acceptance run:

```bash
./scripts/uat-s3-tests --release
```

`--release` and `--binary` are for the ordinary external `s3-tests` acceptance
path. UAT smoke modes use debug metrics and local debug hooks, so they must use
the script-built debug binary and intentionally reject `--release` and
`--binary`.

Strict forced-overload validation uses the same standalone UAT topology but
intentionally constrains storage-node RPC admission. It is a boundedness gate,
not a throughput benchmark:

```bash
./scripts/uat-forced-overload
```

By default this runs the `atomic` and `versioning` external `s3-tests` binaries
with `ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT=8`, short storage-RPC admission
waits, and the normal 30s SDK operation-attempt timeout. This strict timeout is
not inherited from `S3_TEST_TIMEOUT_SECS`; use `--timeout-secs` or
`ARGMIN_FORCED_OVERLOAD_TIMEOUT_SECS` only for explicit diagnostic runs. The
wrapper validates that overload or metadata contention was actually observed,
storage RPC errors stayed at zero, and no HTTP 500, transport EOF,
connection-refused, panic, or SDK operation-attempt timeout shape appeared in
the retained UAT logs. Exact `SlowDown` or `OperationAborted` counts are not
benchmark targets; they are accepted S3-shaped pressure signals.

The wrapper provides deterministic default credentials. Override them with the
same environment variables if a specific test setup needs stable names or
secrets across runs.

`ARGMIN_PANIC_ON_500` and `ARGMIN_ABORT_ON_500` are diagnostic server options,
not normal production behavior. Both default to false in `argmin-s3` itself.
The UAT wrapper and embedded local `s3-tests` server enable abort-on-500 so
hidden 500s fail the whole local test process at the point the server produces
the internal error.

The UAT wrapper also enables the local debug endpoint on the frontend process
by default for debug binaries it builds itself, and builds those spawned
`argmin-s3` binaries with the non-default `local-debug-endpoints` feature when
doing so. `--release` and `--binary` runs default to the endpoint disabled
because release builds must not include the feature and the wrapper cannot add
features to an already-built binary. Smoke runs that depend on debug metrics or
hooks reject `--release` and `--binary`. A default production build rejects
`ARGMIN_LOCAL_DEBUG_ENDPOINT=1` at startup; feature-enabled test/debug builds
still require a loopback frontend listener. On any UAT failure it asks
`POST /__argmin/debug/flight-recorder/dump` to write the bounded, redacted
flight recorder to the frontend log before the process group is stopped. This
keeps intermittent full-suite races diagnosable without exposing debug state on
production binaries or non-loopback listeners. It also sets
`ARGMIN_METADATA_COMMAND_CONFLICT_DIAGNOSTICS=1` for the UAT process group so
frontend and storage-node metadata-command conflicts are visible in retained
logs without making ordinary production contention noisy.

For convenience, the UAT wrapper also maps `S3_TEST_TRACE`,
`S3_TEST_TRACE_FILTER`, `S3_TEST_TRACE_FILE`, `S3_TEST_TRACE_DIR`, and
`S3_TEST_TRACE_SYNC` onto the standalone server's `ARGMIN_TRACE*` variables.
When tracing is requested and the wrapper is launching through `cargo run`, it
adds `--features deep-tracing` automatically. With `--binary PATH`, the binary
must already have been built with `deep-tracing` support.

By default the wrapper creates and removes a temporary data directory. A
directory supplied with `--data-dir PATH` or `ARGMIN_UAT_DATA_DIR` is treated as
caller-owned and is kept after the run.

### `.env` Naming

The repository now uses role-based test configuration names in `.env`:

- `TEST_AWS_PRIMARY_ACCESS_KEY`
- `TEST_AWS_PRIMARY_SECRET_KEY`
- `TEST_AWS_PRIMARY_ACCOUNT_ID`
- `TEST_AWS_ALT_ACCESS_KEY`
- `TEST_AWS_ALT_SECRET_KEY`
- `TEST_AWS_ALT_ACCOUNT_ID`
- `TEST_AWS_OWNER_ROOT_ACCESS_KEY`
- `TEST_AWS_OWNER_ROOT_SECRET_KEY`
- `TEST_AWS_SECOND_ACCESS_KEY`
- `TEST_AWS_SECOND_SECRET_KEY`
- `TEST_S3_REGION`
- `TEST_S3_BUCKET_PREFIX`
- `TEST_S3_TIMEOUT_SECS`
- optional `TEST_S3_ENDPOINT`
- optional `TEST_S3_CONTROL_ENDPOINT`
- optional `TEST_S3_CONTROL_ALT_ENDPOINT`

`PRIMARY` is the main AWS test user, `ALT` is a user from a different AWS
account, `OWNER_ROOT` is the root credential for the primary account, and
`SECOND` is the optional same-account constrained user. The wrapper scripts map
these `.env` values to the `S3_TEST_*` variables expected by the Rust test
harnesses, and `./scripts/cleanup` maps the primary pair to the standard AWS
CLI credential variables when it invokes `aws`.

The AWS wrapper scripts default `TEST_S3_TIMEOUT_SECS` to 120 seconds when it is
not set. This is an AWS SDK per-attempt timeout, not a test-suite timeout; large
AWS-backed data-plane cases such as SSE-C multipart round trips can legitimately
need longer than the local harness default. `./scripts/uat-s3-tests` keeps its
shorter local default so local server stalls surface quickly.

## Local Deep Tracing

Deep tracing is now a non-default local/test-only facility. Production builds
must not rely on it.

The tracing code is compiled only when the relevant crate is built with the
`deep-tracing` cargo feature. That means:

- the normal production build command does not include deep tracing
- local tests can opt in with `--features deep-tracing`
- standalone local debugging can opt in explicitly when needed

### Local `s3-tests` tracing

For local embedded-server runs, enable the feature and then use the trace
environment variables:

| Variable | Description |
|---|---|
| `S3_TEST_TRACE` | Enables tracing for the local embedded test server |
| `S3_TEST_TRACE_FILTER` | Sets `ARGMIN_TRACE_FILTER` for the local embedded test server |
| `S3_TEST_TRACE_FILE` | Sets `ARGMIN_TRACE_FILE` directly |
| `S3_TEST_TRACE_DIR` | Writes one trace file per test binary as `<dir>/<binary>.trace` |

Example:

```bash
S3_TEST_TRACE=1 \
S3_TEST_TRACE_FILTER=server_http,auth,server_core,storage,ec \
S3_TEST_TRACE_DIR=/tmp/s3-test-traces \
cargo test -p s3-tests --features deep-tracing --no-fail-fast
```

### Standalone local server tracing

Use this only for local debugging, not production deployment:

```bash
ARGMIN_ACCOUNT_ID=111122223333 \
ARGMIN_ACCESS_KEY_ID=admin \
ARGMIN_SECRET_ACCESS_KEY=useasecuresecretkey \
ARGMIN_SSE_S3_WRAPPING_KEY='<base64-encoded-32-byte-secret>' \
ARGMIN_TRACE=1 \
ARGMIN_TRACE_FILTER=server_http,auth,server_core,storage,ec \
ARGMIN_TRACE_FILE=/tmp/argmin.trace \
cargo run -p argmin-s3 --features deep-tracing --release
```

## AWS-backed S3 tests and STS discovery oracle

This repo keeps the shared compatibility tests separate from discovery probes:

- `crates/s3-tests` owns S3 API compatibility tests and the reusable raw
  signing, HTTP, and golden-shape test support
- `crates/sts-tests` owns STS Query, AssumeRole, and temporary-credential
  conformance probes; it reuses the public S3 test support where those probes
  exercise temporary credentials through S3

`./scripts/aws-tests` runs the same `s3-tests` corpus against AWS that is run
against the local endpoint. It does not invoke the STS oracle. The live STS
oracle is an explicitly selected discovery tool, run with
`./scripts/aws-sts-oracle`; `--assume-role` enables its self-cleaning
same-account fixture.

External runs fail fast if the AWS-specific environment is incomplete, rather
than silently skipping coverage.

### Required environment variables

The wrapper scripts above load `.env` directly. If you need to construct a
manual command, use the `TEST_AWS_*` and `TEST_S3_*` names from `.env` as the
source values. The runtime test contract uses service-specific endpoint names
and shared `AWS_TEST_*` identity names.

The external `s3-tests` harness hard-fails if any of these are missing:

- `S3_TEST_ENDPOINT`
  - Prefer `https://...` for full coverage.
  - `http://...` is allowed for partial runs, but tests that explicitly require
    HTTPS will fail.
- `S3_CONTROL_TEST_ENDPOINT`
  - Endpoint used for the narrow S3 Control test surface. The harness does not
    infer it from the ordinary S3 endpoint or identify AWS by hostname.
  - `./scripts/aws-tests` derives the AWS account-prefixed endpoint by default;
    `./scripts/uat-s3-tests` uses the standalone local server endpoint.
  - Embedded local `cargo test` runs set it to the embedded server endpoint
    automatically.
- `S3_CONTROL_ALT_TEST_ENDPOINT`
  - Endpoint used for S3 Control requests scoped to the alternate AWS account.
  - `./scripts/aws-tests` derives it from the alternate account ID by default;
    `./scripts/uat-s3-tests` and embedded local runs use the same standalone
    server endpoint as the primary S3 Control endpoint.
- `AWS_TEST_ACCESS_KEY`
- `AWS_TEST_SECRET_KEY`
- `AWS_TEST_ACCOUNT_ID`
- `AWS_TEST_ALT_ACCESS_KEY`
- `AWS_TEST_ALT_SECRET_KEY`
- `AWS_TEST_ALT_ACCOUNT_ID`
- `S3_TEST_BUCKET_PREFIX`
  - Required for external runs. The committed IAM policy below assumes
    `claude-s3-`.

Optional external endpoint support:

- `S3_TEST_TLS_CA_CERT_PATH`
  - PEM CA certificate path for local HTTPS endpoints such as
    `./scripts/uat-s3-tests`.
- `AWS_TEST_SECOND_PRINCIPAL`
  - Exact IAM-style principal ARN for the same-account constrained test
    credential.
  - This is normally only needed for non-AWS external endpoints, where tests
    cannot discover the principal from AWS's AccessDenied message text. The
    UAT wrapper sets it automatically.

The dedicated privileged root-principal suite in
`crates/s3-tests/tests/bucket_policy_root.rs` requires:

- `AWS_TEST_OWNER_ROOT_ACCESS_KEY`
- `AWS_TEST_OWNER_ROOT_SECRET_KEY`

Those root credentials must belong to the same AWS account as
`AWS_TEST_ACCESS_KEY` / `AWS_TEST_SECRET_KEY`. A full
`cargo test -p s3-tests --no-fail-fast` run includes the `bucket_policy_root`
binary, so it will fail fast with a focused setup error if they are absent.
Targeted non-root test binaries can still be run without them.

Recommended command:

```bash
./scripts/aws-tests
```

Separate STS discovery:

```bash
./scripts/aws-sts-oracle
./scripts/aws-sts-oracle --assume-role
```

Privileged bucket-policy root-principal coverage:

```bash
./scripts/aws-tests --test bucket_policy_root -- --nocapture
```

You can override the region or forward any normal `cargo test` selectors:

```bash
./scripts/aws-tests --region us-west-2 --test versioning -- --nocapture
./scripts/aws-tests object_lock
```

The shared S3/STS HTTP support defaults to a 30 second operation-attempt
timeout. The AWS scripts default `S3_TEST_TIMEOUT_SECS` to 120 seconds; use
`--timeout-secs` to override it for the selected script. AWS and local clients
otherwise use the same AWS SDK stalled-stream protection.

### AWS convergence and retries

AWS-backed `s3-tests` already contain a number of retry and eventual-check
helpers. These are not all the same class of issue, and new tests should match
the existing patterns instead of adding blind sleeps.

The main categories currently covered are:

- authorization and policy propagation
  - Cross-account and anonymous data-plane authorization can lag behind
    control-plane writes such as `PutBucketPolicy`, `PutBucketAcl`,
    `PutBucketOwnershipControls`, and `PutPublicAccessBlock`.
  - An owner-side read-back such as `GetBucketPolicy` is not sufficient proof
    that the corresponding data-plane authorization decision has converged.
  - Existing helpers in `bucket_policy.rs`, `ownership.rs`,
    `public_access_block.rs`, `public_access_acl_matrix.rs`, and
    `bucket_anon.rs` wait for the exact operation under test (`GetObject`,
    `ListObjects`, `GetBucketPolicyStatus`, `CopyObject`, `UploadPartCopy`,
    `CreateMultipartUpload`, and similar) rather than assuming immediate
    consistency after the control-plane write.

- CORS and response-policy convergence
  - Preflight behavior can take time to reflect CORS configuration updates, so
    `cors.rs` uses eventual status checks instead of asserting on the first
    response.

- versioning and lifecycle metadata convergence
  - Some metadata surfaces do not update immediately after the enabling or rule
    write that causes them.
  - `bucket_list.rs` waits for `HeadObject` after enabling versioning.
  - `lifecycle.rs` waits for `x-amz-expiration` to appear on `PutObject`,
    `HeadObject`, and `GetObject`.
  - `versioning.rs` confirms expected version/delete-marker counts twice before
    treating the state as stable.

- object-lock policy and retention timing
  - In `object_lock.rs`, policy-based bypass permissions can lag behind bucket
    policy reads, so the tests retry the actual bypass operation until it is
    accepted.
  - Those retries build a fresh alternate client for external AWS runs because
    the propagation issue is on the authorization decision path being observed,
    not just in local test control flow.
  - Object-lock cleanup and related tests also wait for legal-hold and
    retention windows to pass when AWS is correctly enforcing them.

- cleanup races
  - Bucket deletion frequently races with multipart uploads, object version
    cleanup, and other bucket state transitions, producing transient
    `OperationAborted` or `BucketNotEmpty`.
  - Cleanup helpers in files such as `bucket_acl.rs`, `multipart.rs`, and
    `expected_bucket_owner.rs` retry those delete paths rather than treating
    them as hard failures.

- local orchestration timing
  - A few sleeps are not AWS eventual-consistency workarounds at all. For
    example, `admission.rs` uses a short delay only to ensure the local test
    server has accepted a slow request before sending the competing request.

Guideline for new AWS-backed tests:

- If the test depends on a control-plane change becoming visible to a
  data-plane operation, add an eventual helper for that exact operation.
- Prefer retrying a success predicate or a specific expected error code over a
  fixed sleep.
- Treat owner read-backs (`GetBucketPolicy`, `GetBucketAcl`, etc.) as useful
  diagnostics, not as proof that the external behavior under test has
  converged.
- Use a fixed sleep only when the test is coordinating local timing, not when
  it is waiting for AWS state to settle.

### HTTP-only transport checks

HTTP-only transport checks live in `crates/s3-http-tests`. They reuse the same
credentials and bucket prefix, but talk to an `http://` endpoint instead:

- If `S3_TEST_HTTP_ENDPOINT` is set, it must be `http://...` and is used as-is.
- Otherwise `s3-http-tests` derives `http://...` from `S3_TEST_ENDPOINT`.

Recommended command:

```bash
eval "$(grep = .env)" && \
S3_TEST_ENDPOINT=https://s3.us-east-1.amazonaws.com \
S3_CONTROL_TEST_ENDPOINT=https://111122223333.s3-control.us-east-1.amazonaws.com \
AWS_TEST_ACCESS_KEY="$TEST_AWS_PRIMARY_ACCESS_KEY" \
AWS_TEST_SECRET_KEY="$TEST_AWS_PRIMARY_SECRET_KEY" \
AWS_TEST_REGION="${TEST_S3_REGION:-us-east-1}" \
S3_TEST_BUCKET_PREFIX="${TEST_S3_BUCKET_PREFIX:-claude-s3-}" \
S3_TEST_TIMEOUT_SECS="${TEST_S3_TIMEOUT_SECS:-120}" \
cargo test -p s3-http-tests --no-fail-fast
```

### Golden response-shape assertions

The former standalone AWS-vs-local diff suite (`crates/s3-diff-tests`) has
been fully converted to golden shape assertions inside `crates/s3-tests`:

- the shape helpers live in `crates/s3-tests/src/shape.rs` (`assert_shape`,
  validated `{placeholder}` templates, complete-set header comparison, and
  `assert_shape_one_of` for divergences documented in
  [aws-compatibility.md](aws-compatibility.md))
- error-body expectations are generated from the production formatters via
  `s3_tests::shape::expected_error`, each anchored by a literal test in the
  shape module
- the bucket-policy condition matrices live in
  `crates/s3-tests/tests/bucket_policy_conditions.rs`

These assertions run against the embedded server on every
`cargo nextest run` and against AWS via `./scripts/aws-tests`; the same
expectations must hold on both endpoints, and tests never branch on the
endpoint. The full conversion history is in
plans/completed/diff-test-consolidation-plan.md.

#### Writing a golden shape test

The working pattern, in order:

1. **Probe first.** Drive the exact request against the embedded server with
   the `raw_object`/`raw_object_with`/`raw_object_query`/`raw_bucket`/
   `raw_anonymous` helpers and capture the full `RawResponse`. Do not write
   the template from documentation or memory.
2. **Pin everything except transport framing.** `assert_shape` takes the
   status, the complete header set (full-set equality after dropping
   `connection`/`date`/`server`; `content-length` and `transfer-encoding`
   are also ignored unless the spec names them, because AWS varies the
   framing by frontend), and the full body. Weak assertions — status plus
   error code, `contains` checks, header subsets — silently lose pinning;
   if a value is deterministic for the fixture and semantic (checksums of
   fixed bodies, `content-length` equal to a data GET/HEAD payload size),
   assert it literally. Do not pin the `content-length` of XML bodies.
3. **Use placeholders only for genuinely variable values.** The built-ins
   (`{request_id}`, `{host_id}`, `{etag}`, `{version_id}`, `{upload_id}`,
   `{owner_id}`, `{http_date}`, `{iso8601}`, `{ws}`, `{any}`) are
   shape-validated, and captured placeholders enforce consistency (the same
   `{etag}` across a body, and against headers). Values known to the test
   (bucket, key, region, account IDs, fixture version IDs) go in via
   `.sub(...)`, keeping the template endpoint-agnostic. `assert_shape`
   returns its captures for cross-response checks.
4. **Error bodies come from `s3_tests::shape::expected_error`.** Wrappers
   delegate to the production `xml.rs` formatters; when adding a wrapper,
   also add its literal anchor in the shape module's
   `expected_error_literal_anchors` test so a formatter change fails
   locally, not at the next AWS run. Message text is passed by the test, so
   messages stay pinned non-tautologically.
5. **Unordered content** (DeleteObjects entries, tag sets) uses
   `assert_body_with_unordered_blocks`: blocks match as a set and the
   stripped remainder must equal the envelope exactly.
6. **Divergences use `assert_shape_one_of`,** never endpoint branching, and
   only for differences documented in
   [aws-compatibility.md](aws-compatibility.md), with a comment pointing at
   the guide entry.
7. **Validate against AWS before trusting it.** Run the touched binaries via
   `./scripts/aws-tests --test <binary> -- <test names>`. One AWS
   data point covers one request shape — probe adjacent shapes (bucket vs
   object scope, existing vs missing resource, with vs without an optional
   header) before concluding anything about drift. If AWS disagrees with the
   template, treat it as a server bug first (see aws-compatibility.md).
8. **Upgrade in place.** When an existing test already covers the behaviour
   with weaker assertions, strengthen that test rather than adding a
   parallel one.

### Local-only tests

The local-only `s3-local-tests` crate currently contains:

- `bucket_naming`
- `test_list_buckets_anonymous`

Run it locally with the embedded server:

```bash
cargo test -p s3-local-tests
```

Use this crate for embedded-server behavior that should not require AWS.

### Cleanup helper

Failed AWS-backed runs can leave behind versioned test buckets, delete markers,
legal holds, or governance-retained objects under the `claude-s3-` prefix. For
that case, [`scripts/cleanup`](../scripts/cleanup) provides a manual
cleanup pass for leftover test buckets.

It currently:

- removes leaked, name-checked IAM roles under the same-account, cross-account,
  and caller-denied `/argmin-sts-oracle/` fixture paths after a one-hour safety
  window, so it does not race an active oracle run
- lists buckets with names starting `claude-s3-`
- removes object versions and delete markers
- attempts to disable legal holds and bypass governance retention
- deletes the bucket once it is empty

The script requires `aws`, `jq`, `date`, and the primary AWS credentials in `.env`.
It exports `TEST_AWS_PRIMARY_ACCESS_KEY` / `TEST_AWS_PRIMARY_SECRET_KEY` as
standard AWS CLI variables,
so it uses the same user as `./scripts/aws-tests`. It is intended as an
after-failure cleanup tool, not part of the normal test invocation.

Examples:

```bash
./scripts/cleanup
./scripts/cleanup --bucket-prefix claude-s3-
./scripts/cleanup --region us-west-2
```

### Cross-account requirements

- The alternate credentials must belong to a different AWS account.
- A second IAM user in the same AWS account is not sufficient.
- The harness probes S3 canonical owner IDs during setup and fails fast if the
  primary and alternate credentials resolve to the same owner.
- When owner-root credentials are provided, the harness also probes S3
  canonical owner IDs and fails fast unless the root credentials resolve to the
  same owner as the primary credentials.

### IAM policy

Attach [`crates/s3-tests/aws/test-user-policy.json`](../crates/s3-tests/aws/test-user-policy.json)
to both test IAM users. For the primary account, the idempotent command below
creates or updates a customer-managed policy and attaches it to the primary
test user:

```bash
./scripts/aws-apply-test-user-policy
```

The command resolves the target IAM user using the primary credential, verifies
that both credentials belong to `TEST_AWS_PRIMARY_ACCOUNT_ID`, and uses the
owner/root credential only for the required IAM policy operations. It cannot
update the alternate-account user; run the equivalent policy update with an
administrator of that account.

The committed policy assumes:

- buckets are created under the `claude-s3-` prefix
- both users can create and delete prefixed buckets
- both users can perform the bucket/object operations exercised by `s3-tests`
- bucket ABAC validation additionally requires:
  - `s3:GetBucketAbac`
  - `s3:PutBucketAbac`
  - `s3:TagResource`
  - `s3:UntagResource`
  - `s3:TagResource` and `s3:UntagResource` are currently granted on `Resource: "*"`
    in the committed test policy; scoping them to `arn:aws:s3:::claude-s3-*` was not
    sufficient for the AWS control-plane `TagResource` / `UntagResource` calls
- lifecycle validation requires:
  - `s3:GetLifecycleConfiguration`
  - `s3:PutLifecycleConfiguration`
  - `DeleteBucketLifecycle` uses `s3:PutLifecycleConfiguration`
- object-lock validation requires:
  - `s3:PutBucketObjectLockConfiguration`
  - `s3:GetBucketObjectLockConfiguration`
  - `s3:PutObjectRetention`
  - `s3:GetObjectRetention`
  - `s3:PutObjectLegalHold`
  - `s3:GetObjectLegalHold`
  - `s3:BypassGovernanceRetention`
- the live STS oracle creates and removes temporary roles and path-bearing IAM
  users below the `argmin-sts-oracle` namespace; the latter pin configured
  IAM-user `aws:PrincipalArn` resource-policy behavior without relying on an
  identity policy

If you change `S3_TEST_BUCKET_PREFIX`, update the policy resource ARNs to
match.

AWS announced on April 6, 2026 that SSE-C is being disabled by default for new
buckets, with `PutBucketEncryption` and `BlockedEncryptionTypes = NONE`
required to re-enable it. The external SSE-C fixtures call that configuration
after bucket creation so their positive SSE-C coverage remains stable while the
default-blocked behavior is asserted separately in AWS-backed bucket-encryption
tests.

If AWS-backed object-lock tests fail immediately with `AccessDenied` on
`PutBucketObjectLockConfiguration`, re-attach the committed policy after
pulling the latest version.

### Account-level S3 settings

The external suite expects account-level S3 Block Public Access to allow public
ACL and public policy coverage. For the primary account, and preferably the
alternate account as well, these must all be `false` or absent:

- `BlockPublicAcls`
- `IgnorePublicAcls`
- `BlockPublicPolicy`
- `RestrictPublicBuckets`

If any of these are enabled, tests that need public-read, public-read-write, or
public bucket policies will fail instead of skipping.

New AWS buckets still start with bucket-level public access block enabled by
default. The test suite clears bucket-level public access block on buckets that
need public ACL or public policy coverage. That means the AWS environment must
allow `PutPublicAccessBlock` on those test buckets, but you do not need to
manually pre-clear bucket-level settings before running the suite.

Also ensure there is no SCP, permission boundary, or other org/account policy
that denies bucket ACL, bucket policy, ownership-controls, or public-access
block operations needed by the suite.

## Coverage

Use [`scripts/coverage`](../scripts/coverage) to measure integration coverage.
This is the main coverage number the repo currently optimizes for, rather than
unit-test line coverage.

Current command:

```bash
./scripts/coverage
```

That script runs integration coverage against `s3-tests`, so it is best used
after targeted local testing has already narrowed down any failures.

## Fuzzing

Parser hardening work now has a dedicated `cargo-fuzz` harness under `fuzz/`.
This is aimed at parser and request-front-door targets rather than trying to
fuzz the entire HTTP server end-to-end.

Current targets:

- `auth_dates`
- `auth_post`
- `auth_request`
- `server_http_parsers`
- `server_http_post_multipart`
- `server_http_chunked_decoder`
- `server_http_streaming_frontend`

The `server-http` targets are intentionally focused on the attacker-controlled
surfaces called out in the threat model:

- `server_http_post_multipart` drives the incremental `POST Object`
  multipart/form-data parser used by the streaming POST path.
- `server_http_chunked_decoder` drives `IncrementalChunkedDecoder`, including
  incremental feeds, signed/unsigned aws-chunked modes, and trailer handling.
- `server_http_streaming_frontend` drives the streaming PUT/POST request
  classification and header parsing entry points in `serve.rs`.

Useful commands:

```bash
# Run the checked-in all-target wrapper (10m per target by default).
./scripts/fuzz
./scripts/fuzz --time 2m
./scripts/fuzz auth_bucket_policy server_http_parsers

# List targets.
cd fuzz
cargo fuzz list

# Run a short session.
cargo +nightly fuzz run auth_dates -- -max_total_time=30

# Reproduce a saved crash artifact.
cargo +nightly fuzz run auth_dates artifacts/auth_dates/<artifact>
```

Notes:

- `cargo-fuzz` requires nightly for sanitizer instrumentation.
- corpora live under `fuzz/corpus/`
- crashes and minimized artifacts live under `fuzz/artifacts/`
- these targets are for "must not panic / must reject malformed input cleanly"
  style parser invariants, not AWS behavior compatibility by themselves

## Choosing the right test type

As a rough rule:

- use targeted crate tests while implementing parser/auth/http changes
- use `s3-local-tests` for local embedded-server behavior that does not need AWS
- use `s3-http-tests` for plain-HTTP transport behavior
- use AWS-backed `s3-tests` when compatibility depends on real AWS behavior
- use golden shape assertions (`s3_tests::shape`) when the contract being
  checked is the exact response shape for a request: full header set and
  body, holding on AWS and locally alike (transport framing —
  `content-length` vs `transfer-encoding` — is ignored unless explicitly
  pinned, since AWS varies it by frontend; pin it only where the value is
  semantic, e.g. data GET/HEAD sizes)
- use `./scripts/coverage` when checking integration coverage movement
- use `cargo-fuzz` for malformed-input robustness and panic discovery
