

## Repository structure

The notes/ path has rough design notes and is not part of this repo. It also contains some useful papers. Do
not edit anything here.

The guides/ folder has guides about specific technical or other issues and coding guidelines. These include
important areas like concurrency.

The plans/ folder is for work plans. Remember to adjust these if during implementation things change and the
plan detail needs correcting.

Under tmp/ but not committed to repo are clones of dependencies so we can read code

## Dependencies

We are trying to not have too many dependencies and to keep code simple and understandable. Ask before adding
new dependencies.

## Testing

We are designing a highly reliable system so we need to have full trust in it. We need a very comprehensive
set of tests, and will look at different test methodologies, formal methods, fuzz testing and so on as needed.

We are using the Ceph test suite and porting these to native tests 1:1, these tests are in ./tmp/s3/tests

## Running tests against AWS

AWS credentials are in `.env` (not committed) with `AWS_ACCESS_KEY` and `AWS_SECRET_KEY`.
The IAM user is `claude-s3` and buckets must be prefixed `claude-s3-`.

```bash
eval "$(grep = .env)" && \
S3_TEST_ENDPOINT=https://s3.us-east-1.amazonaws.com \
S3_TEST_ACCESS_KEY="$AWS_ACCESS_KEY" \
S3_TEST_SECRET_KEY="$AWS_SECRET_KEY" \
S3_TEST_REGION=us-east-1 \
S3_TEST_BUCKET_PREFIX=claude-s3- \
S3_TEST_TIMEOUT_SECS=30 \
cargo test -p s3-tests --no-fail-fast
```

**Important:** Do NOT use `set -a && source .env` — exporting `AWS_ACCESS_KEY` and
`AWS_SECRET_KEY` into the environment interferes with the AWS Rust SDK's credential
resolution, causing `AuthorizationHeaderMalformed` errors. Use `eval "$(grep = .env)"`
instead to read the values without exporting them.

## Cleanliness

Make sure `cargo clippy` is clean, even if it is pedantic. Always run tests after making changes and make sure
they still pass. Review your code to make sure it is clear, correct and secure. Always run `cargo fmt` after
making any edits.

## Diary

We keep a diary os the work we did. This is a historical record, so only append to it. We will update this at
the end of the day, not after every work session.
