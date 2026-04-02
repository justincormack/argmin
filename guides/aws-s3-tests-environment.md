# AWS `s3-tests` Environment

This guide describes the AWS environment required to run the external
`crates/s3-tests` suite without silent skips.

## Required environment variables

Use `eval "$(grep = .env)"` to read `.env` without exporting `AWS_ACCESS_KEY`
and `AWS_SECRET_KEY` into the process environment.

The external `s3-tests` harness now hard-fails if any of these are missing:

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

Recommended command:

```bash
eval "$(grep = .env)" && \
S3_TEST_ENDPOINT=https://s3.us-east-1.amazonaws.com \
S3_TEST_ACCESS_KEY="$AWS_ACCESS_KEY" \
S3_TEST_SECRET_KEY="$AWS_SECRET_KEY" \
S3_TEST_ACCOUNT_ID="$AWS_ACCOUNT_ID" \
S3_TEST_ALT_ACCESS_KEY="$AWS_ALT_ACCESS_KEY" \
S3_TEST_ALT_SECRET_KEY="$AWS_ALT_SECRET_KEY" \
S3_TEST_ALT_ACCOUNT_ID="$AWS_ALT_ACCOUNT_ID" \
S3_TEST_REGION=us-east-1 \
S3_TEST_BUCKET_PREFIX=claude-s3- \
S3_TEST_TIMEOUT_SECS=30 \
cargo test -p s3-tests --no-fail-fast
```

When `S3_TEST_ENDPOINT` is set, `s3-tests` defaults to a 30 second client
timeout instead of the local 5 second timeout, and disables the AWS SDK
stalled-stream watchdog. This keeps slower remote runs from failing with
`ThroughputBelowMinimum` while preserving the stricter local defaults for the
embedded server.

You can also point `S3_TEST_ENDPOINT` at `http://...` for exploratory or
transport-specific runs. This is not the recommended configuration for the main
suite, because HTTPS-sensitive coverage such as SSE-C checks will fail in that
mode.

HTTP-only transport checks live in `crates/s3-http-tests`. They reuse the same
credentials and bucket prefix, but talk to an `http://` endpoint instead:

- If `S3_TEST_HTTP_ENDPOINT` is set, it must be `http://...` and is used as-is.
- Otherwise `s3-http-tests` derives `http://...` from `S3_TEST_ENDPOINT`.

Recommended command:

```bash
eval "$(grep = .env)" && \
S3_TEST_ENDPOINT=https://s3.us-east-1.amazonaws.com \
S3_TEST_ACCESS_KEY="$AWS_ACCESS_KEY" \
S3_TEST_SECRET_KEY="$AWS_SECRET_KEY" \
S3_TEST_REGION=us-east-1 \
S3_TEST_BUCKET_PREFIX=claude-s3- \
S3_TEST_TIMEOUT_SECS=30 \
cargo test -p s3-http-tests --no-fail-fast
```

## Cleanup helper

Failed AWS-backed runs can leave behind versioned test buckets, delete markers,
legal holds, or governance-retained objects under the `claude-s3-` prefix. For
that case, [`scripts/cleanup.sh`](../scripts/cleanup.sh) provides a manual
cleanup pass for leftover test buckets.

It currently:

- lists buckets with names starting `claude-s3-`
- removes object versions and delete markers
- attempts to disable legal holds and bypass governance retention
- deletes the bucket once it is empty

The script requires `aws`, `jq`, and AWS credentials in the environment that
are allowed to delete those buckets and objects. It is intended as an
after-failure cleanup tool, not part of the normal test invocation.

## Cross-account requirements

- The alternate credentials must belong to a different AWS account.
- A second IAM user in the same AWS account is not sufficient.
- The harness probes S3 canonical owner IDs during setup and now fails fast if
  the primary and alternate credentials resolve to the same owner.

## IAM policy

Attach [`crates/s3-tests/aws/test-user-policy.json`](../crates/s3-tests/aws/test-user-policy.json)
to both test IAM users.

The committed policy assumes:

- buckets are created under the `claude-s3-` prefix
- both users can create and delete prefixed buckets
- both users can perform the bucket/object operations exercised by `s3-tests`
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

If you change `S3_TEST_BUCKET_PREFIX`, update the policy resource ARNs to match.

If AWS-backed object-lock tests fail immediately with `AccessDenied` on
`PutBucketObjectLockConfiguration`, re-attach the committed policy after pulling
the latest version.

## Account-level S3 settings

The external suite expects account-level S3 Block Public Access to allow public
ACL and public policy coverage. For the primary account, and preferably the
alternate account as well, these must all be `false` or absent:

- `BlockPublicAcls`
- `IgnorePublicAcls`
- `BlockPublicPolicy`
- `RestrictPublicBuckets`

If any of these are enabled, tests that need public-read, public-read-write, or
public bucket policies will now fail instead of skipping.

New AWS buckets still start with bucket-level public access block enabled by
default. The test suite now clears bucket-level public access block on buckets
that need public ACL or public policy coverage. That means the AWS environment
must allow `PutPublicAccessBlock` on those test buckets, but you do not need to
manually pre-clear bucket-level settings before running the suite.

Also ensure there is no SCP, permission boundary, or other org/account policy
that denies bucket ACL, bucket policy, ownership-controls, or public-access
block operations needed by the suite.

## Local-only tests

The local-only `s3-local-tests` crate currently contains:

- `bucket_naming`
- `test_list_buckets_anonymous`

These do not run as part of the AWS-backed `s3-tests` package command above.

Run it locally with the embedded server:

```bash
cargo test -p s3-local-tests
```

The separate `s3-http-tests` crate covers the small set of transport-sensitive
AWS checks that must run over plain HTTP, such as rejecting SSE-C on insecure
connections.
