# ACL Compatibility Follow-Up Plan

## Scope

This plan covers the remaining AWS S3 ACL compatibility work after the recent
Ceph test ports exposed the current gaps more clearly.

It focuses on:
- missing ACL product behavior
- missing ACL conformance coverage in `crates/s3-tests`
- alignment of ACL write-path behavior with already-modeled ACL read/authz
  behavior
- AWS verification for the supported ACL surface

It does not cover:
- full IAM policy language
- non-ACL bucket policy features except where they directly interact with ACLs
- account-level identity/credential modeling beyond what existing ACL behavior
  already requires
- email grantees

## Why This Needs Its Own Plan

The current repository is no longer missing ACL support in a generic sense.
Bucket ACLs, object ACLs, object ownership controls, public access block
interaction, and several canned ACLs are already implemented.

What remains is a narrower but still important compatibility gap:
- some ACL variants are modeled but intentionally rejected
- some ACL grantee types are understood by authz but cannot be written
- some Ceph ACL scenarios are still only present in the Python suite and not in
  the Rust conformance suite

This is best tracked as a dedicated follow-up rather than folded into the older
auth or ownership plans.

## Current State

Implemented today:
- bucket `private`, `public-read`, and `public-read-write` canned ACLs
- object `private`, `public-read`, `public-read-write`,
  `authenticated-read`, `bucket-owner-read`, and
  `bucket-owner-full-control` canned ACLs
- bucket and object ACL XML rendering
- bucket and object ACL authorization checks for canonical-user grants,
  `AllUsers`, and `AuthenticatedUsers`
- public access block interaction for public canned ACLs
- bucket-owner-enforced ACL restrictions
- explicit ACL grant headers for canonical users and group URIs

Missing or intentionally rejected today:
- bucket `authenticated-read`
- explicit `AuthenticatedUsers` ACL grants via XML or `x-amz-grant-*`
- object `aws-exec-read`
- `LogDelivery` grantee / `log-delivery-write` (deferred until bucket logging
  itself is in scope)
- Rust conformance coverage for much of the Ceph bucket-grant matrix

Out of scope:
- email grantees / `AmazonCustomerByEmail`

Reason:
- AWS is discontinuing email-grantee ACL creation
- implementing it would require additional user/account data we do not yet
  persist
- it is not a good use of implementation effort relative to the remaining AWS
  ACL surface

## Confirmed Gaps

### 1. Bucket `authenticated-read`

The HTTP layer accepts `authenticated-read`, but the coordinator rejects it as
`NotImplemented` for bucket ACL creation and update.

That blocks:
- `CreateBucket` with `x-amz-acl: authenticated-read`
- `PutBucketAcl` with canned `authenticated-read`
- Ceph scenarios that depend on authenticated bucket read through ACL
- bucket-policy-status scenarios that treat authenticated-read as public

This is the most obvious currently-commented ACL gap in the Rust suite.

### 2. Explicit `AuthenticatedUsers` grants

The ACL model already includes `AuthenticatedUsers`, and authorization already
checks that grant for signed callers. But bucket and object ACL validation
reject explicit `AuthenticatedUsers` grants on write.

That means we currently have a read/evaluation model that is broader than the
write model.

Practical consequences:
- ACL XML with `AuthenticatedUsers` cannot be stored
- `x-amz-grant-*` headers targeting the `AuthenticatedUsers` group cannot be
  stored
- canned behavior can only cover the object path today, not the general grant
  path

### 3. `LogDelivery` grantee and `log-delivery-write`

The current ACL grantee model only represents:
- canonical users
- `AllUsers`
- `AuthenticatedUsers`

AWS also exposes the log-delivery group and bucket canned ACL
`log-delivery-write`.

Without that modeled grantee, we cannot represent:
- canned `log-delivery-write`
- explicit grants to the log-delivery group
- the parts of logging compatibility that depend on ACL shape rather than
  bucket policy alone

### 4. Object `aws-exec-read`

`PutObjectAcl` parses `aws-exec-read`, but the coordinator returns
`NotImplemented`.

This is part of the documented object canned ACL surface. It is less important
than the `AuthenticatedUsers` and `LogDelivery` gaps, but it is still a known
compatibility hole.

### 5. Conformance coverage gaps

Several Ceph ACL cases are still not ported into Rust, even where the current
implementation likely already supports them.

The largest remaining gaps are:
- bucket canonical-user grant matrix:
  - `FULL_CONTROL`
  - `READ`
  - `READ_ACP`
  - `WRITE`
  - `WRITE_ACP`
- bucket ACL XML/group-grant coverage beyond the currently added cases
- create-time ACL coverage for the remaining bucket variants
- targeted ACL interaction cases that currently only exist in Ceph

These are important because this codebase depends on conformance tests, not on
implementation confidence alone.

## AWS Reference Surface

The relevant AWS docs for this plan are:
- `PutBucketAcl`:
  `https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html`
- `PutObjectAcl`:
  `https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html`
- ACL overview:
  `https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html`
- Object ownership / ACL-disabled behavior:
  `https://docs.aws.amazon.com/AmazonS3/latest/userguide/about-object-ownership.html`
- Block Public Access:
  `https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-block-public-access.html`

This plan should continue to treat those documents as the primary reference,
with Ceph `s3-tests` used as the conformance corpus to port and validate
against.

## Design Constraints

### 1. Match AWS behavior, do not simplify the surface away

If AWS supports an ACL variant that is still active and relevant, the default
goal is to implement it rather than invent a narrower product rule.

### 2. Keep the ACL model coherent

We should not continue growing the gap where authz understands a grantee type
that the write path refuses to store. The model for supported ACL grantees
should be consistent across:
- request parsing
- validation
- persistence
- XML rendering
- authorization

### 3. Preserve existing ownership and public-access-block behavior

No ACL expansion should regress:
- bucket-owner-enforced ACL rejection rules
- block-public-acls rejection rules
- ignore-public-acls behavior
- bucket-owner-preferred / object-writer ownership semantics

### 4. Defer log-delivery ACL work until bucket logging exists

`LogDelivery` is part of the ACL surface, but it is tightly coupled to a larger
open feature area: bucket logging.

We should not prioritize `log-delivery-write` or explicit log-delivery grants
ahead of:
- finishing the active ACL gaps that already affect implemented features
- porting the remaining high-value Ceph ACL conformance cases
- deciding the logging feature shape itself

The right time to implement that ACL surface is when bucket logging work starts,
so the ACL behavior can be validated as part of the end-to-end logging model.

### 5. Keep email grants out

Even if AWS still documents legacy email-grantee behavior in some form, this
plan explicitly excludes it.

## Implementation Phases

### Phase 1: Finish `AuthenticatedUsers` ACL support

Deliver:
- implement bucket canned `authenticated-read`
- allow explicit `AuthenticatedUsers` group grants for bucket ACLs
- allow explicit `AuthenticatedUsers` group grants for object ACLs where the
  permission is otherwise valid
- port the commented Rust test for bucket `authenticated-read`
- add Rust tests for explicit `AuthenticatedUsers` grant persistence through
  ACL XML and grant headers

Success criteria:
- no `NotImplemented` for bucket `authenticated-read`
- explicit `AuthenticatedUsers` grants round-trip through `GetBucketAcl` /
  `GetObjectAcl`
- signed non-owner callers gain access exactly where AWS grants it

### Phase 2: Decide and implement `aws-exec-read`

Deliver:
- verify current AWS behavior for `aws-exec-read`
- either implement the documented behavior or explicitly document why the repo
  will keep returning `NotImplemented`
- add a conformance test for whichever behavior is correct

This phase is intentionally later because it is lower-value than
`AuthenticatedUsers`, and it may matter only in narrow AWS
consumer scenarios.

### Phase 3: Port the remaining high-value Ceph ACL cases

Deliver:
- bucket canonical-user grant matrix in `crates/s3-tests`
- missing bucket ACL create/update round-trip coverage
- explicit bucket group-grant coverage
- remaining ACL interaction tests that exercise real authorization behavior,
  not just storage shape

Priority order:
1. cases that prove authorization semantics
2. cases that prove write-path acceptance/rejection semantics
3. cases that only restate already-covered response shape

### Phase 4: AWS verification pass

Deliver:
- rerun the narrowed ACL subset against AWS
- confirm exact error codes and status codes for:
  - bucket `authenticated-read`
  - explicit `AuthenticatedUsers` grants
  - `aws-exec-read` if implemented
- record any AWS/Ceph divergence discovered during verification and decide
  whether the target is AWS or Ceph for that case

### Deferred Follow-On: `LogDelivery` ACL support with bucket logging

Deliver when bucket logging work begins:
- extend the ACL grantee model with the log-delivery group
- implement bucket canned `log-delivery-write`
- allow explicit log-delivery group grants in ACL XML and grant headers
- add conformance coverage as part of the logging test plan

Success criteria:
- `log-delivery-write` is representable, persisted, and rendered correctly
- logging behavior that depends on ACLs matches AWS
- logging-specific ACL behavior is validated end to end rather than in
  isolation

## Test Plan

Unit and local coordinator coverage:
- add targeted `server-core` tests for ACL grant normalization and validation
- add parser tests for new group URIs and grant-header acceptance
- add XML parse/render round-trip coverage for new grantee types

Primary integration coverage:
- `cargo test -p s3-tests --test bucket_acl`
- `cargo test -p s3-tests --test object_crud`
- `cargo test -p s3-tests --test bucket_crud`
- `cargo test -p s3-tests --test public_access_block`
- `cargo test -p s3-tests --test bucket_policy`
- `cargo test -p s3-tests --test ownership`
- `cargo test -p s3-tests --test access_matrix`

Full validation before merge:
- `cargo fmt`
- `cargo clippy --all-targets --all-features -- -D warnings`
- targeted ACL integration suite above
- full test suite before commit

AWS-backed verification:
- run the narrowed ACL subset against AWS using the existing `.env`-driven test
  setup
- prefer exact AWS observation over Ceph behavior where they differ

## Open Decisions

### 1. `aws-exec-read`

We should confirm whether AWS still meaningfully supports this ACL in the paths
we care about, or whether it should remain a documented unsupported corner.

### 2. `LogDelivery` rollout breadth

This should stay deferred until bucket logging implementation starts.

When that work begins, the likely minimum useful rollout is:
- canned `log-delivery-write`
- explicit group grant support

If logging compatibility later needs more ACL-specific behavior, this plan
should be updated then rather than widened ad hoc before logging exists.

## Exit Criteria

This plan is done when:
- bucket `authenticated-read` works
- explicit `AuthenticatedUsers` grants work for the supported bucket/object ACL
  surface
- `aws-exec-read` is either implemented or explicitly documented as a deliberate
  unsupported case after AWS verification
- the high-value remaining Ceph ACL cases are ported into `crates/s3-tests`
- email grants remain explicitly out of scope and undocumented as a supported
  feature

`LogDelivery` is intentionally not part of the exit criteria for this plan. It
should be tracked as follow-on work when bucket logging is implemented.
