## Overall guidance

We are building a server that much exactly match AWS S3 behaviour at all times. Do not work around this, always
fix non conforming behaviour.

This is a high quality codebase. If changes to add new features or fix issues are extensive we will do these
rather than finding shortcuts that are less maintainable long term.

## Repository structure

The guides/ folder has guides about specific technical or other issues and coding guidelines. These include
important areas like concurrency.

The plans/ folder is for work plans. Remember to adjust these if during implementation things change and the
plan detail needs correcting.

Under tmp/ but not committed to repo are clones of dependencies so we can read code

## Stability of interfaces and backwards compatibility

We are in pre-release state. That means no code needs to be added to migrate database schemas at present, as
there are no existing users. The public API can be changed as necessary, there are no external consumers outside
this repository.

## Dependencies

We are trying to not have too many dependencies and to keep code simple and understandable. Ask before adding
new dependencies.

## Testing

We are designing a highly reliable system so we need to have full trust in it. We need a very comprehensive
set of tests, and will look at different test methodologies, formal methods, fuzz testing and so on as needed.

We are using the Ceph test suite and porting these to native tests 1:1, these tests are in ./tmp/s3/tests

Use `./scripts/coverage` to measure integration test coverage, which is the main measure we want to improve,
not unit test coverage.

Always run the full test suite before comitting in case something unexpected breaks.

## Running tests against AWS

AWS credentials are in `.env` (not committed) with `AWS_ACCESS_KEY`,
`AWS_SECRET_KEY`, `AWS_ACCOUNT_ID`, `AWS_ALT_ACCESS_KEY`,
`AWS_ALT_SECRET_KEY`, and `AWS_ALT_ACCOUNT_ID`.

The external `s3-tests` harness now requires all of the primary and alternate
credentials/account IDs and hard-fails if the alternate credentials are not a
different AWS account. Buckets must be prefixed `claude-s3-`.

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

**Important:** Do NOT use `set -a && source .env` — exporting `AWS_ACCESS_KEY` and
`AWS_SECRET_KEY` into the environment interferes with the AWS Rust SDK's credential
resolution, causing `AuthorizationHeaderMalformed` errors. Use `eval "$(grep = .env)"`
instead to read the values without exporting them.

See [`guides/aws-s3-tests-environment.md`](guides/aws-s3-tests-environment.md)
for the committed IAM policy, required account-level S3 Block Public Access
settings, the separate HTTP-only `s3-http-tests` crate, and the separate
local-only `s3-local-tests` crate.

## Cleanliness

Make sure `cargo clippy --all-targets --all-features -- -D warnings` is clean, even if it is pedantic.
Always run tests after making changes and make sure they still pass. Review your code to make sure it
is clear, correct and secure. Always run `cargo fmt` after making any edits. Aim to make illegal states
unrepresnetable versus having checks.

## Diary

We keep a diary os the work we did. This is a historical record, so only append to it. We will update this at
the end of the day, not after every work session.
