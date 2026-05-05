## Bucket Policy Condition Data Availability Plan

Status: in progress

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

Early AWS-backed results have already shown that the rule is more nuanced than
the initial hypothesis:

- some actions accept the policy statement but do not evaluate the condition
  for that action
- some actions reject the policy at `PutBucketPolicy` time because the
  condition does not apply to that action at all

So the matrix needs to classify at least three distinct outcomes:

- evaluable and matching
- accepted but not evaluable for the action
- rejected at policy write time as an invalid condition/action combination

There is now a second modeling distinction to preserve inside auth:

- data unavailable to this request path
- data available, but no matching value present

Those are not the same state. In particular:

- `ExistingObjectTag` on `GetObjectAttributes` is an AWS semantic:
  - policy accepted
  - condition family accepted
  - action does not make tag data available for evaluation
- forgetting to preload tags for an action that is supposed to evaluate them is
  an internal bug:
  - the request should carry an explicit `Unavailable` state
  - the evaluator must not collapse that to “available but empty”

So the auth model has to represent three layers separately:

- policy validity
- request-time condition data availability
- condition value matching

## Findings So Far

The first `s3:ExistingObjectTag/*` row is now AWS-pinned:

- `GetObject`
  - evaluable and matching
- `HeadObject`
  - same shape as `GetObject`
  - evaluable and matching
- `GetObjectVersion`
  - same shape as `GetObject`
  - evaluable and matching
- `HeadObject` with explicit `GetObjectTagging`
  - still the same shape as `GetObject`
  - separate permission to read tags does not change evaluability
- `GetObjectVersion` with explicit `GetObjectVersionTagging`
  - still the same shape as `GetObjectVersion`
  - separate permission to read tags does not change evaluability
- `HeadObjectVersion`
  - same shape as `GetObjectVersion`
  - evaluable and matching
- `HeadObjectVersion` with explicit `GetObjectVersionTagging`
  - still the same shape as `GetObjectVersion`
  - separate permission to read tags does not change evaluability
- `GetObjectAttributes`
  - policy accepted
  - condition not evaluated for the action
  - runtime result is `AccessDenied`
- `GetObjectVersionAttributes`
  - same shape as `GetObjectAttributes`
  - policy accepted
  - condition not evaluated for the action
  - runtime result is `AccessDenied`
- `GetObjectRetention`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy` with
    `Conditions do not apply to combination of actions and resources in statement`
- `GetObjectLegalHold`
  - same shape as `GetObjectRetention`
  - policy rejected at `PutBucketPolicy` with `MalformedPolicy`
- `DeleteObjectTagging`
  - policy accepted
  - evaluable and matching
  - `s3:ExistingObjectTag/*` can authorize deletion of the current object's
    tag set
- `DeleteObjectVersionTagging`
  - same shape as `DeleteObjectTagging`
  - policy accepted
  - evaluable and matching for the requested version's existing tags

That means `ExistingObjectTag` is not a single “supported or unsupported”
family. Its behavior is action-specific in at least three ways:

- fully evaluable
- accepted but non-evaluable
- policy-invalid

The `HeadObject` result narrows one possible explanation:

- this is not simply “operations that do not return tags cannot evaluate
  tag-based conditions”
- `HeadObject` does not return tags either, but AWS evaluates
  `ExistingObjectTag` for it just like `GetObject`
- even granting explicit tag-read access does not make
  `GetObjectAttributes`/`GetObjectVersionAttributes` evaluate the condition

So the current best model is narrower:

- `HeadObject` belongs to the `GetObject` authorization family for tag-based
  condition evaluation
- `GetObjectAttributes` and `GetObjectVersionAttributes` are distinct,
  accepted-but-non-evaluable actions for `ExistingObjectTag`

The next delete/tagging row is also now AWS-pinned:

- `PutObject`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- `DeleteObject`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- `DeleteObjectVersion`
  - same shape as `DeleteObject`
  - policy rejected at `PutBucketPolicy` with `MalformedPolicy`
- `DeleteObjectTagging`
  - policy accepted
  - condition is fully evaluable
- `DeleteObjectVersionTagging`
  - same shape as `DeleteObjectTagging`
  - policy accepted
  - condition is fully evaluable

So even closely-related mutation actions can split sharply:

- plain overwrite via `PutObject`: policy-invalid
- delete actions: policy-invalid
- delete-tagging actions: fully evaluable

The versioned ACL row is now also AWS-pinned:

- `GetObjectAcl`
  - policy accepted
  - condition is fully evaluable
  - pinned with same-policy public/private tag controls
- `PutObjectAcl`
  - policy accepted
  - condition is fully evaluable
  - pinned with same-policy public/private tag controls
- `GetObjectVersionAcl`
  - policy accepted
  - condition is fully evaluable
  - pinned with same-policy public/private tag controls
- `PutObjectVersionAcl`
  - policy accepted
  - condition is fully evaluable
  - pinned with same-policy public/private tag controls

So at least in the ACL family, the versioned actions match the current-version
shape rather than splitting the way `GetObjectAttributes` did.

The versioned tagging row is now AWS-pinned too:

- `GetObjectVersionTagging`
  - policy accepted
  - condition is fully evaluable
- `PutObjectVersionTagging`
  - policy accepted
  - condition is fully evaluable

So the tagging family now looks consistent across:

- `GetObjectTagging`
- `PutObjectTagging`
- `DeleteObjectTagging`
- `GetObjectVersionTagging`
- `PutObjectVersionTagging`
- `DeleteObjectVersionTagging`

with all six currently behaving as fully evaluable for
`s3:ExistingObjectTag/*`.

The object-lock mutation row is now partially AWS-pinned too:

- `PutObjectRetention`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- `PutObjectLegalHold`
  - same shape as `PutObjectRetention`
  - policy rejected at `PutBucketPolicy` with `MalformedPolicy`
- `BypassGovernanceRetention`
  - same shape as `PutObjectRetention`
  - policy rejected at `PutBucketPolicy` with `MalformedPolicy`

This reinforces the current pattern:

- object-lock actions so far are policy-invalid for `ExistingObjectTag`
- tagging actions remain fully evaluable

The first `RequestObjectTag` result is now AWS-pinned too:

- `PutObjectAcl`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- `PutObjectRetention`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- `PutObjectLegalHold`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`

These are still only a few pinned request-side results. They are enough to
prove that `RequestObjectTag` is not generically valid across all object
actions, but they are still not enough to justify a broad allowlist or
denylist yet.

Until more actions are AWS-pinned, auth should only encode the specific invalid
pairs that are already proven:

- `PutObjectAcl` + `s3:RequestObjectTag/*` is policy-invalid
- `PutObjectRetention` + `s3:RequestObjectTag/*` is policy-invalid
- `PutObjectLegalHold` + `s3:RequestObjectTag/*` is policy-invalid

Other request-tag/action combinations still need explicit AWS-backed tests
before they are narrowed or rejected in production validation.

The mixed-action rejection rule is now also pinned for request-tag conditions:

- `["s3:PutObjectAcl", "s3:PutObjectTagging"]`
  - with `s3:RequestObjectTag/*`
  - is rejected at `PutBucketPolicy` with `MalformedPolicy`

So for request-side conditions too:

- if any action in the statement makes the condition/action combination
  invalid, AWS rejects the whole statement at policy write time

The first copy/header-conditioned `UploadPartCopy` row is now AWS-pinned too:

- destination-side `s3:x-amz-copy-source`
  - policy accepted on `s3:PutObject`
  - `UploadPartCopy` evaluates it on the destination write path
  - same-account non-initiator part-copy from `public/*` succeeds
  - same policy denies part-copy from `private/*`
- destination-side `s3:x-amz-metadata-directive`
  - policy accepted on `s3:PutObject`
  - `UploadPartCopy` still evaluates the missing header as an absent request
    value on the destination write path
  - a `StringNotEquals { "s3:x-amz-metadata-directive": "COPY" }` deny blocks
    `UploadPartCopy`

So for `UploadPartCopy` specifically:

- destination bucket policy is not limited to plain `CreateMultipartUpload`
  and `UploadPart`
- `s3:x-amz-copy-source` is fully evaluable on the part-copy call
- `s3:x-amz-metadata-directive` is also accepted and operative, even though the
  part-copy request does not send that header

The corresponding `CopyObject` source/destination split is now AWS-pinned too:

- mixed `["s3:GetObject", "s3:PutObject"]` with
  `StringLike { "s3:x-amz-copy-source": "src/public/*" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObject", "s3:PutObject"]` with
  `StringEquals { "s3:x-amz-metadata-directive": "COPY" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObject", "s3:PutObject"]` with
  `Null { "s3:x-amz-server-side-encryption": "true" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObject", "s3:PutObject"]` with
  `Null { "s3:x-amz-server-side-encryption-customer-algorithm": "true" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObject", "s3:PutObject"]` with
  `StringEquals { "s3:x-amz-acl": "private" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObject", "s3:PutObject"]` with
  `StringEquals { "s3:x-amz-grant-read": "uri=http://acs.amazonaws.com/groups/global/AllUsers" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObjectVersion", "s3:PutObject"]` with
  `StringLike { "s3:x-amz-copy-source": "src/public/*" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObjectVersion", "s3:PutObject"]` with
  `StringEquals { "s3:x-amz-acl": "private" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObjectVersion", "s3:PutObject"]` with
  `Null { "s3:x-amz-server-side-encryption": "true" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObjectVersion", "s3:PutObject"]` with
  `StringEquals { "s3:x-amz-metadata-directive": "COPY" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObjectVersion", "s3:PutObject"]` with
  `Null { "s3:x-amz-server-side-encryption-customer-algorithm": "true" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObject", "s3:PutObject"]` with
  `StringEquals { "s3:x-amz-grant-read-acp": "uri=http://acs.amazonaws.com/groups/global/AllUsers" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObject", "s3:PutObject"]` with
  `StringEquals { "s3:x-amz-grant-full-control": "uri=http://acs.amazonaws.com/groups/global/AllUsers" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObjectVersion", "s3:PutObject"]` with
  `StringEquals { "s3:x-amz-grant-read-acp": "uri=http://acs.amazonaws.com/groups/global/AllUsers" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`
- mixed `["s3:GetObjectVersion", "s3:PutObject"]` with
  `StringEquals { "s3:x-amz-grant-full-control": "uri=http://acs.amazonaws.com/groups/global/AllUsers" }`
  - policy rejected at `PutBucketPolicy`
  - AWS returns `MalformedPolicy`

So for the copy/header family:

- `s3:x-amz-copy-source` and `s3:x-amz-metadata-directive` are destination-write
  condition keys
- destination SSE request headers are also not accepted on the source-read
  `GetObject` side of mixed `CopyObject` statements
- at least the representative ACL/grant destination headers we tested
  (`s3:x-amz-acl`, `s3:x-amz-grant-read`, `s3:x-amz-grant-read-acp`,
  `s3:x-amz-grant-full-control`) are also not accepted on the source-read
  `GetObject` side of mixed `CopyObject` statements
- the same mixed-statement rejection also applies on the versioned source-read
  `GetObjectVersion` side for copy-specific, ACL, grant, and SSE rows
- AWS does not accept them on the source-read `GetObject` or `GetObjectVersion`
  side of `CopyObject`
- if a single statement mixes a valid destination action with an invalid
  source-read action for those keys, AWS rejects the whole statement

The first `ExistingObjectTag` copy probes are now AWS-pinned, and they do not
behave symmetrically:

- source-side `CopyObject`
  - a source bucket policy on `s3:GetObject` with
    `StringEquals { "s3:ExistingObjectTag/security": "public" }` still allows
    plain `GetObject`
  - the same policy does not authorize `CopyObject` source reads, even for the
    same public-tagged object
  - local coordinator auth now models that by treating `CopyObject` source
    reads as not having existing tags available for policy evaluation
- source-side `UploadPartCopy`
  - the same `s3:GetObject` `ExistingObjectTag` policy does authorize
    multipart copy-source reads for the public-tagged object
  - private-tagged source reads are still denied
- mixed `["s3:GetObject", "s3:PutObject"]` with
  `s3:ExistingObjectTag/*`
  - is rejected at `PutBucketPolicy`
  - the destination `PutObject` side remains policy-invalid even though the
    source `GetObject` side is valid

The first multipart destination-encryption row is also now AWS-pinned:

- destination-side `s3:x-amz-server-side-encryption = AES256`
  - `CreateMultipartUpload` can satisfy the bucket-policy condition at
    initiation time
  - `UploadPartCopy` reuses that destination SSE-S3 context even without
    resending the header on the part-copy request
  - `CompleteMultipartUpload` also reuses that destination SSE-S3 context
    without resending the header
  - local coordinator regressions now pin the exact reuse point at
    `with_multipart_upload_managed_encryption_policy_context(...)`
- destination-side
  `s3:x-amz-server-side-encryption-customer-algorithm = AES256`
  - `CreateMultipartUpload` accepts the header and the multipart upload is
    created under SSE-C
  - `UploadPartCopy` does not reuse the destination SSE-C header value from
    initiation; the part-copy request still fails under a deny-on-missing-header
    policy
  - `CompleteMultipartUpload` does not reuse the destination SSE-C header value
    from initiation; the request still fails under a deny-on-missing-header
    policy

So the multipart destination-encryption behavior now appears split by
encryption family:

- SSE-S3 context is reused across multipart lifecycle steps
- this likely reflects durable multipart state: the upload records managed
  encryption mode without any secret-bearing request material
- SSE-C context is not reused uniformly across multipart lifecycle steps
- this likely reflects request-scoped key material: later operations do not
  inherit customer-provided encryption headers just because initiation used
  SSE-C

The mixed invalid/valid action rejection shape is now also pinned:

- mixed-action statements that combine one policy-invalid action with one valid
  action, for example:
  - `["s3:DeleteObject", "s3:DeleteObjectTagging"]`
  - with `s3:ExistingObjectTag/*`
- these are rejected at `PutBucketPolicy` if any matched action in the
  statement makes the condition/action combination invalid

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

- `PutObject`
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
  - rejected at policy write time for that action
  - another concrete AWS behavior that must be modeled explicitly

## Phase 2: Versioned Pair Matrix

Check whether current-version and version-specific actions differ in condition
evaluability.

Primary pairs:

- `GetObject` vs `GetObjectVersion`
- `HeadObject` vs `HeadObject` with separate tag-read permission
- `HeadObjectVersion` vs `HeadObjectVersion` with separate tag-read permission
- `GetObjectAttributes` vs `GetObjectVersionAttributes`
- `DeleteObject` vs `DeleteObjectVersion`
- `DeleteObjectTagging` vs `DeleteObjectVersionTagging` (covered; both are
  evaluable and matching)

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

Status:

- complete

## Phase 5: Auth Model Cleanup

Once the AWS-backed matrix is mapped, refactor auth support to encode
evaluability centrally rather than via per-action ad hoc checks.

Status:

- first cleanup slice complete:
  - condition-key rows now identify the runtime input family they require
    when evaluable
  - policy validity, accepted-but-not-evaluable, and evaluable action states
    are exposed from the central resolver table
  - existing-object-tag, request-tag, and bucket-tag preload decisions now use
    the central table instead of local key-prefix checks
- second cleanup slice complete:
  - request object tags now use an explicit available/unavailable request
    input state
  - missing request tags on an available request remain distinct from an auth
    path that did not provide request-tag data
  - `s3:RequestObjectTag/*`, `aws:RequestTag/*`,
    `s3:RequestObjectTagKeys`, and `aws:TagKeys` share that distinction
- third cleanup slice complete:
  - scalar request fields now use an explicit available/unavailable state
  - request-header, list-parameter, object-ownership, and version-ID
    conditions can distinguish an available-but-absent value from an auth path
    that did not provide that request field
  - `s3:ObjectCreationOperation` now uses the same explicit availability
    distinction
  - context-less bucket auth paths now explicitly provide available-but-absent
    request-header context for currently supported bucket-policy header
    conditions
- fourth cleanup slice completed:
  - `authz/policy.rs` now centralizes object and bucket `PolicyRequest`
    construction through local builders
  - repeated manual request-construction chains in the main legacy/object and
    bucket policy helpers now share the same availability baseline
- fifth cleanup slice complete:
  - the BOE modern write path now uses the shared object `PolicyRequest`
    builder so scalar and tag availability behavior cannot diverge from the
    legacy/object path

Implementation goal:

- model condition-family-by-action evaluability explicitly
- model request-time condition inputs explicitly as available vs unavailable
- keep “accepted but not evaluable” distinct from “unsupported condition”
- keep “policy-invalid for this action” distinct from both of the above
- keep “input unavailable at runtime” distinct from “available but missing”
- use the same matrix to drive any prefetch decisions for object tags or other
  policy inputs

Acceptance criteria:

- the auth layer can explain every special-case action by a small evaluability
  table
- no current behavior depends on hidden “don’t load this field” shortcuts
- request construction cannot silently encode “unavailable” as an empty set

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
