# Bucket Policy Root Principal Compatibility Plan

## Scope

This plan covers one specific AWS S3 compatibility gap:
- `GetBucketPolicy`
- `PutBucketPolicy`
- `DeleteBucketPolicy`

AWS documents that the bucket owner's account `root` principal can still perform
those three bucket-policy APIs even if the bucket policy explicitly denies that
root principal. That carveout does not extend to arbitrary IAM principals in the
same account.

This plan does not cover:
- `GetBucketPolicyStatus`
- VPC endpoint policy behavior
- AWS Organizations / SCP behavior
- a full IAM identity-policy model

## Problem

Current argmin behavior is too permissive for bucket-policy CRUD.

Today, those APIs all go through the generic bucket-admin path in
[coordinator.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/coordinator.rs),
and bucket-admin reduces to exact principal equality with the stored
`owner_principal`:
- [coordinator.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/coordinator.rs#L3924)
- [coordinator.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/coordinator.rs#L6605)
- [coordinator.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/coordinator.rs#L6659)
- [coordinator.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/coordinator.rs#L6711)

That means:
- if the stored bucket owner principal is an IAM user or role, that principal
  can never lock itself out of bucket-policy CRUD
- argmin does not distinguish the bucket owner's account root principal from
  other principals in the same account

That is broader than AWS.

## Why This Needs Its Own Plan

This is not just a one-line auth tweak.

There are three coupled gaps:
- bucket ownership is stored as an arbitrary `owner_principal`, not as durable
  account ownership plus a distinct creating principal
- `Requester` / `AccountIdentity` does not currently model principal kind
  strongly enough to distinguish account root from a same-account IAM principal
- the internal bucket-policy action enum does not yet include
  `s3:GetBucketPolicy`, `s3:PutBucketPolicy`, or `s3:DeleteBucketPolicy`, so
  there is no way to evaluate explicit bucket-policy deny/allow for those APIs
  in the first place

Those are account-model and authorization-model changes, not a narrow
request-handler patch.

## Desired AWS-Compatible Behavior

For the three bucket-policy CRUD APIs:

1. The bucket owner account root principal may perform the API even if the
   bucket policy explicitly denies that root principal.

2. A non-root IAM principal in the bucket owner's account may still be denied by
   the bucket policy.

3. Other existing non-bucket-policy controls still apply:
   - expected bucket owner checks
   - public-access-block checks relevant to `PutBucketPolicy`
   - any future VPC endpoint / Organizations policy model, when implemented

4. `GetBucketPolicyStatus` should keep its existing behavior. The AWS carveout
   is specifically for `GetBucketPolicy`, `PutBucketPolicy`, and
   `DeleteBucketPolicy`.

## Current Modeling Gaps

### 1. Bucket ownership is principal-scoped, not account-scoped

`AccountIdentity` currently stores:
- principal string
- canonical user ID
- display name

See [lib.rs](/home/justin/src/github.com/justincormack/argmin/crates/s3-types/src/lib.rs#L274).

That is enough for the current pre-account-service model, but it is not enough
to answer:
- what AWS account owns this bucket?
- what is that account's root principal?
- is this requester that root principal or just another same-account principal?

### 2. Bucket-policy CRUD bypasses bucket policy entirely

The current bucket-policy CRUD methods authorize through the generic bucket
admin path before parsing or loading bucket policy. That means explicit bucket
policy deny is not part of the decision at all for those APIs.

### 3. Policy actions are missing

`PolicyAction` in
[bucket_policy.rs](/home/justin/src/github.com/justincormack/argmin/crates/auth/src/bucket_policy.rs)
currently includes `GetBucketPolicyStatus`, but not:
- `GetBucketPolicy`
- `PutBucketPolicy`
- `DeleteBucketPolicy`

Without those actions, argmin cannot express or test the compatibility rule
"non-root owner principal can be explicitly denied, root principal cannot".

## Proposed Design

### 1. Move bucket ownership toward account ownership

Introduce a bucket-owner account identity distinct from the creating principal.

Minimum durable state needed for this behavior:
- bucket owner account identifier
- bucket owner canonical ID
- a stable way to derive or resolve the bucket owner account root principal

The current `owner_principal` should remain useful for audit and compatibility
surfaces, but it should stop being the sole source of truth for bucket
administrative identity.

This should align with the longer-term account-model work rather than creating a
special-case bucket-policy-only ownership mechanism.

### 2. Add principal classification helpers

Add explicit helper semantics for:
- requester is exact bucket owner principal
- requester is in the bucket owner's account
- requester is the bucket owner's account root principal

These helpers should be built on durable account metadata, not string-matching
ad hoc policy logic inside the bucket-policy CRUD handlers.

### 3. Add bucket-policy CRUD actions to policy evaluation

Extend `auth::PolicyAction` with:
- `GetBucketPolicy`
- `PutBucketPolicy`
- `DeleteBucketPolicy`

Then wire dedicated bucket-resource policy evaluation for those actions.

This is required so argmin can represent:
- explicit deny of a same-account IAM principal
- explicit deny of the owner root principal, which must then be bypassed only
  by the special AWS carveout

### 4. Replace generic admin bypass with a dedicated bucket-policy CRUD authorizer

Do not keep using the generic `authorize_bucket_admin_requester()` behavior for
these three APIs.

Instead, add a dedicated authorization path with this shape:

1. Load bucket summary and expected-owner checks.
2. Evaluate bucket policy for the specific CRUD action.
3. If requester is the bucket owner's account root principal:
   - ignore explicit bucket-policy deny for these three APIs
   - continue to enforce non-policy controls such as `BlockPublicPolicy` for
     `PutBucketPolicy`
4. Otherwise:
   - if bucket policy explicitly denies the action, deny
   - if bucket policy explicitly allows the action, allow
   - if bucket policy has no matching statement, fall back to the repo's
     simplified "bucket owner/admin by default" model for now

That last fallback is intentionally a compatibility approximation until a fuller
identity-policy model exists. The key AWS correction is that explicit
bucket-policy deny must be able to block non-root owner IAM principals.

### 5. Keep `GetBucketPolicyStatus` separate

Do not apply the root-principal carveout to `GetBucketPolicyStatus`.

That API should continue using its own existing logic unless AWS documentation
or testing shows a distinct special-case rule there too.

## Implementation Phases

### Phase 1: Ownership / Principal Modeling Prerequisite

Deliver:
- bucket owner account identity separate from creator principal
- requester helpers for same-account and root-principal classification

This phase may land as part of the broader account-structure work rather than
as an isolated bucket-policy change.

### Phase 2: Bucket Policy Action Support

Deliver:
- `GetBucketPolicy`, `PutBucketPolicy`, `DeleteBucketPolicy` in
  `auth::PolicyAction`
- bucket-resource policy evaluation coverage for those actions

### Phase 3: CRUD Authorization Split

Deliver:
- dedicated authorizer for bucket-policy CRUD
- explicit root-principal bypass for bucket-policy deny on those three APIs
- explicit deny now able to block non-root owner principals
- existing `BlockPublicPolicy` behavior preserved for `PutBucketPolicy`

### Phase 4: Conformance / Regression Coverage

Deliver:
- local regression tests for the intended authorization split
- deferred AWS verification once the harness can represent the required
  principal distinctions

## Test Plan

### What We Can Test Early

Once the account model is in place, add local tests in `server-core` for:
- owner-account root principal can `GetBucketPolicy` despite explicit deny
- owner-account root principal can `PutBucketPolicy` despite explicit deny
- owner-account root principal can `DeleteBucketPolicy` despite explicit deny
- same-account non-root owner principal is denied by explicit deny for those
  APIs
- `GetBucketPolicyStatus` does not inherit the CRUD carveout
- `PutBucketPolicy` is still blocked by `BlockPublicPolicy` even for the owner
  root principal

These can be deterministic local tests once the model can represent:
- owner account root principal
- same-account non-root principal
- cross-account principal

### What We Should Defer

AWS-backed verification is awkward in the current harness because `s3-tests`
uses IAM user credentials, not true account root credentials.

So the external part should be deferred until we have one of:
- a dedicated AWS manual verification procedure using real root credentials
- a separate privileged harness for these cases
- a fuller local account-structure model we trust enough to cover the rule
  without immediate AWS automation

Until then, the most useful thing is to have the local regression structure and
to document the exact AWS rule we are matching.

## Exit Criteria

This plan is done when:
- bucket-policy CRUD no longer relies on exact `owner_principal` equality alone
- non-root owner principals can be denied by bucket policy for
  `GetBucketPolicy`, `PutBucketPolicy`, and `DeleteBucketPolicy`
- the bucket owner account root principal bypass exists only for those three
  APIs
- local regression coverage exists for the root-vs-non-root split
- the remaining AWS external verification gap is either executed or explicitly
  documented as deferred due to harness limitations
