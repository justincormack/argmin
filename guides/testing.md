# Testing Guide

This guide collects the main testing workflows for the repository:

- targeted crate tests
- workspace and integration coverage
- AWS-backed compatibility runs
- AWS-vs-local differential response-shape runs
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

# Integration coverage.
./scripts/coverage

# Sweep all fuzz targets for a fixed time budget each.
./scripts/fuzz

# Local-only S3 behavior tests.
cargo test -p s3-local-tests

# AWS-backed compatibility tests.
./scripts/aws-tests

# Standalone binary UAT acceptance run.
./scripts/uat-s3-tests --test bucket_crud

# Differential AWS-vs-local response-shape tests.
./scripts/diff-tests --test response_shape
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
  - runs the external `s3-tests` suite
  - loads AWS credentials from `.env`
  - sets the required `S3_TEST_*` variables
  - forwards extra arguments to `cargo test -p s3-tests`
- `./scripts/uat-s3-tests`
  - starts the standalone `argmin-s3` binary over HTTPS
  - configures UAT-only primary, alternate, same-account constrained, and
    owner-root credentials
  - runs `s3-tests` against that process as an external endpoint
  - forwards extra arguments to `cargo test -p s3-tests`
- `./scripts/diff-tests`
  - runs the standalone AWS-vs-local `s3-diff-tests` suite
  - loads AWS credentials from `.env`
  - sets the required `S3_TEST_*` variables
  - forwards extra arguments to `cargo test --manifest-path crates/s3-diff-tests/Cargo.toml`
- `./scripts/cleanup`
  - cleans up leftover external test buckets
  - loads the primary AWS credentials from `.env`
  - uses the same AWS user as `./scripts/aws-tests`

The AWS-backed scripts accept `--region`, and `aws-tests` / `diff-tests` also accept
additional `cargo test` selectors and `-- --nocapture` style test-binary
arguments.

## Standalone `argmin-s3` UAT `s3-tests`

`./scripts/uat-s3-tests` is the acceptance route for running the same
external-endpoint `s3-tests` harness against the real `argmin-s3` binary
instead of the embedded in-process test server.

The wrapper:

- starts `cargo run -p argmin-s3` with a temporary data directory
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

These UAT variables exist only to drive acceptance and conformance testing.
They are deliberately not a production account-management API.

Example targeted run:

```bash
./scripts/uat-s3-tests --test bucket_crud -- --nocapture
```

Example full acceptance run:

```bash
./scripts/uat-s3-tests --release
```

The wrapper provides deterministic default credentials. Override them with the
same environment variables if a specific test setup needs stable names or
secrets across runs.

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

`PRIMARY` is the main AWS test user, `ALT` is a user from a different AWS
account, `OWNER_ROOT` is the root credential for the primary account, and
`SECOND` is the optional same-account constrained user. The wrapper scripts map
these `.env` values to the `S3_TEST_*` variables expected by the Rust test
harnesses, and `./scripts/cleanup` maps the primary pair to the standard AWS
CLI credential variables when it invokes `aws`.

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

## AWS-backed `s3-tests`

This repo uses `crates/s3-tests` for AWS compatibility checks. External runs
now fail fast if the AWS-specific environment is incomplete, rather than
silently skipping coverage.

### Required environment variables

The wrapper scripts above load `.env` directly. If you need to construct a
manual command, use the `TEST_AWS_*` and `TEST_S3_*` names from `.env` as the
source values that you map into `S3_TEST_*`.

The external `s3-tests` harness hard-fails if any of these are missing:

- `S3_TEST_ENDPOINT`
  - Prefer `https://...` for full coverage.
  - `http://...` is allowed for partial runs, but tests that explicitly require
    HTTPS will fail.
- `S3_TEST_ACCESS_KEY`
- `S3_TEST_SECRET_KEY`
- `S3_TEST_ACCOUNT_ID`
- `S3_TEST_ALT_ACCESS_KEY`
- `S3_TEST_ALT_SECRET_KEY`
- `S3_TEST_ALT_ACCOUNT_ID`
- `S3_TEST_BUCKET_PREFIX`
  - Required for external runs. The committed IAM policy below assumes
    `claude-s3-`.

Optional external endpoint support:

- `S3_TEST_TLS_CA_CERT_PATH`
  - PEM CA certificate path for local HTTPS endpoints such as
    `./scripts/uat-s3-tests`.
- `S3_TEST_SECOND_PRINCIPAL`
  - Exact IAM-style principal ARN for the same-account constrained test
    credential.
  - This is normally only needed for non-AWS external endpoints, where tests
    cannot discover the principal from AWS's AccessDenied message text. The
    UAT wrapper sets it automatically.

The dedicated privileged root-principal suite in
`crates/s3-tests/tests/bucket_policy_root.rs` requires:

- `S3_TEST_OWNER_ROOT_ACCESS_KEY`
- `S3_TEST_OWNER_ROOT_SECRET_KEY`

Those root credentials must belong to the same AWS account as
`S3_TEST_ACCESS_KEY` / `S3_TEST_SECRET_KEY`. A full
`cargo test -p s3-tests --no-fail-fast` run includes the `bucket_policy_root`
binary, so it will fail fast with a focused setup error if they are absent.
Targeted non-root test binaries can still be run without them.

Recommended command:

```bash
./scripts/aws-tests
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

When `S3_TEST_ENDPOINT` is set, `s3-tests` defaults to a 30 second client
timeout instead of the local 5 second timeout, and disables the AWS SDK
stalled-stream watchdog. This keeps slower remote runs from failing with
`ThroughputBelowMinimum` while preserving the stricter local defaults for the
embedded server.

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
    `public_access_block.rs`, `access_matrix.rs`, and `bucket_anon.rs` wait
    for the exact operation under test (`GetObject`, `ListObjects`,
    `GetBucketPolicyStatus`, `CopyObject`, `UploadPartCopy`,
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
S3_TEST_ACCESS_KEY="$TEST_AWS_PRIMARY_ACCESS_KEY" \
S3_TEST_SECRET_KEY="$TEST_AWS_PRIMARY_SECRET_KEY" \
S3_TEST_REGION="${TEST_S3_REGION:-us-east-1}" \
S3_TEST_BUCKET_PREFIX="${TEST_S3_BUCKET_PREFIX:-claude-s3-}" \
S3_TEST_TIMEOUT_SECS="${TEST_S3_TIMEOUT_SECS:-30}" \
cargo test -p s3-http-tests --no-fail-fast
```

### Differential AWS-vs-local response-shape checks

Differential response-shape checks live in `crates/s3-diff-tests`. Each test
in `crates/s3-diff-tests/tests/response_shape.rs` sends the same request to
real AWS and to the embedded local server, then compares the response status,
headers, and body shape.

This is different from the other dedicated test crates:

- unlike `s3-local-tests`, it is not local-only
- unlike ordinary AWS-backed `s3-tests`, it is not exercising only one backend
- it always needs the full AWS-backed `s3-tests` environment, including both
  AWS credential sets and the bucket prefix, because the local response is
  only half of the comparison
- it is outside the main workspace so normal workspace test runs do not include
  it
- `cargo test --manifest-path crates/s3-diff-tests/Cargo.toml` is expected to
  fail fast if the external AWS comparison environment is incomplete

Missing external AWS comparison configuration is a hard failure rather than a
silent skip.

Recommended command:

```bash
./scripts/diff-tests --test response_shape -- --nocapture
```

Like `./scripts/aws-tests`, this wrapper accepts region overrides and forwards
additional `cargo test` selection arguments:

```bash
./scripts/diff-tests --region us-west-2 --test bucket_policy -- --nocapture
./scripts/diff-tests response_shape
```

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

- lists buckets with names starting `claude-s3-`
- removes object versions and delete markers
- attempts to disable legal holds and bypass governance retention
- deletes the bucket once it is empty

The script requires `aws`, `jq`, and the primary AWS credentials in `.env`.
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
to both test IAM users.

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
- use `s3-diff-tests` when the contract being checked is "AWS response shape vs
  our response shape for the same request"
- use `./scripts/coverage` when checking integration coverage movement
- use `cargo-fuzz` for malformed-input robustness and panic discovery
