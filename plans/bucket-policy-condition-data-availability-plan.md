## Bucket Policy Condition Data Availability Plan

Status: planned

## Goal

Map and lock down the AWS rule that bucket-policy condition evaluation depends
on data already available to the action being authorized.

The concrete trigger for this plan is the observed AWS behavior that
`s3:ExistingObjectTag/*` can authorize `GetObject`, but does not authorize
`GetObjectAttributes` even when the statement explicitly names
`s3:GetObjectAttributes`.

The working hypothesis is:

- AWS accepts the policy statement
- AWS does not fetch extra object state solely to satisfy condition evaluation
- if an action does not have the relevant data available, the condition does
  not match for that action

This plan turns that hypothesis into explicit AWS-pinned tests.

## Scope

Focus first on bucket-policy condition families that depend on object state or
request state:

- `s3:ExistingObjectTag/*`
- `s3:RequestObjectTag/*`
- request-header conditions already modeled in auth
  - `s3:x-amz-copy-source`
  - `s3:x-amz-metadata-directive`
  - ACL/grant header condition keys
  - SSE request headers

The main target is the action-specific evaluability matrix, not generic policy
CRUD or parser validation.

## Non-goals

- do not broaden bucket-policy language support beyond what AWS-backed tests
  justify
- do not replace the existing differential suites; use them only for the
  narrow wire-shape cases that need exact AWS output matching
- do not guess unsupported/evaluable combinations without an AWS-backed test

## Phase 1: ExistingObjectTag Matrix

Add AWS-facing `s3-tests` that probe `s3:ExistingObjectTag/*` across adjacent
object actions.

Start with the highest-value candidates:

- `GetObjectVersionAttributes`
- `GetObjectRetention`
- `GetObjectLegalHold`
- `PutObjectRetention`
- `BypassGovernanceRetention`
- `DeleteObject`
- `DeleteObjectVersion`
- `DeleteObjectTagging`
- `DeleteObjectVersionTagging`

Test shape:

- create an object or object version
- set `security=public` object tags
- install a bucket policy with the relevant action(s) and
  `StringEquals { "s3:ExistingObjectTag/security": "public" }`
- compare the target action result against a closely-related control action
  where evaluability is already known

Acceptance criteria:

- every action above is classified by AWS-backed test as one of:
  - evaluable and matching
  - accepted but not evaluable for that action
  - another concrete AWS behavior that must be modeled explicitly

## Phase 2: Versioned Pair Matrix

Check whether current-version and version-specific actions differ in condition
evaluability.

Primary pairs:

- `GetObject` vs `GetObjectVersion`
- `GetObjectAttributes` vs `GetObjectVersionAttributes`
- `DeleteObject` vs `DeleteObjectVersion`
- `DeleteObjectTagging` vs `DeleteObjectVersionTagging`

Acceptance criteria:

- versioned and non-versioned actions are explicitly pinned where AWS differs
- auth support tables no longer assume “versioned behaves like current” without
  a test

## Phase 3: RequestObjectTag Matrix

Map request-tag condition evaluability for actions that may or may not carry
request tags.

Priority targets:

- `PutObject`
- `PutObjectTagging`
- `PutObjectVersionTagging`
- `PutObjectAcl`
- `PutObjectRetention`
- `PutObjectLegalHold`

Acceptance criteria:

- request-tag-dependent actions are distinguished from actions that never
  surface request tags
- no auth rule treats missing request-tag data as available merely because the
  action mutates an existing object

## Phase 4: Copy and Header-conditioned Paths

Expand the same model to request-header-backed condition keys.

Priority targets:

- `CopyObject` source-read vs destination-write checks
- `UploadPartCopy`
- ACL/grant header conditions on ACL mutation operations
- SSE request-header conditions where the action does not actually use that
  header family

Acceptance criteria:

- source-side and destination-side condition data are only evaluated where AWS
  makes them available
- composite operations keep distinct sub-action behavior where AWS does

## Phase 5: Auth Model Cleanup

Once the AWS-backed matrix is mapped, refactor auth support to encode
evaluability centrally rather than via per-action ad hoc checks.

Implementation goal:

- model condition-family-by-action evaluability explicitly
- keep “accepted but not evaluable” distinct from “unsupported condition”
- use the same matrix to drive any prefetch decisions for object tags or other
  policy inputs

Acceptance criteria:

- the auth layer can explain every special-case action by a small evaluability
  table
- no current behavior depends on hidden “don’t load this field” shortcuts

## Verification

For each phase:

- run `cargo fmt`
- run targeted `cargo test` for the touched `s3-tests` files
- run `cargo clippy --all-targets --all-features -- -D warnings`

Before commit:

- run `/bin/bash -lc 'S3_TEST_TIMEOUT_SECS=30 cargo test --workspace --no-fail-fast'`

For any behavior that changes auth semantics:

- add or update an AWS-backed `s3-tests` regression first
- only then adjust auth support tables or evaluability logic
