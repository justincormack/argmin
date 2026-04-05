# SigV2 Compatibility Note

This is a short future-work note, not an implementation plan.

## Current position

We do not want to implement AWS SigV2 authentication now.

That remains the right call for the current roadmap:

- SigV2 is deprecated by AWS documentation.
- SigV4 is the compatibility target for current Argmin work.
- Adding SigV2 would widen the auth surface substantially.

## Why keep this note

Live compatibility checks showed that SigV2 is not as fully obsolete in AWS
behavior as the deprecation messaging suggests.

Observed against AWS on April 5, 2026:

- valid raw SigV2 bucket `GET`/`ListObjects` requests still succeeded for fresh
  buckets in `us-east-1`
- valid raw SigV2 bucket `GET`/`ListObjects` requests still succeeded for fresh
  buckets in `us-west-2`
- the same valid raw SigV2 requests were rejected in `eu-central-1` with
  `400 InvalidRequest` and the message:
  `The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.`

So the useful compatibility rule is:

- SigV2 still appears to work in at least some older regions
- SigV2 is rejected in later regions such as `eu-central-1`
- we should not assume a blanket "new buckets always reject SigV2" rule without
  rechecking AWS directly

## Client ecosystem note

This still matters because some client libraries and test suites continue to
exercise SigV2 code paths:

- botocore integration tests still contain `signature_version='s3'` cases
- some older S3-compatible tooling still sends `Authorization: AWS ...`

That means SigV2 can still show up as an interoperability issue even if we do
not plan to support it by default.

## If we ever implement it

Treat it as explicit legacy compatibility work, ideally behind a clearly marked
flag or compatibility mode.

If revisited later, scope should start narrow:

- verify exact AWS behavior per region before implementing anything
- prefer bucket/object operations actually seen in client ecosystems
- do not infer behavior from deprecation posts alone
- keep SigV4 as the default and preferred auth path
