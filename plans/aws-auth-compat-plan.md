# AWS Auth Compatibility Follow-Up Plan

## Scope

This plan now tracks the remaining AWS auth/authz compatibility work after the
main SigV4 and principal-propagation implementation.

Completed work is retained here only as context. The actual implementation work
remaining is limited and should be treated as a follow-up plan, not a greenfield
auth design.

This follow-up covers:
- remaining account-level public-access/authz gaps
- credential-scope / same-account authorization compatibility
- conformance tests and documentation for the above

This still does not cover:
- full IAM policy language
- full bucket policy evaluation
- STS AssumeRole API implementation

Carry-forward from the completed Ceph authz diff review:
- keep the remaining account-level IAM questions visible here:
  `CreateBucket`, `ListBuckets`, and any future `DeleteBucket` IAM-policy
  investigation
- keep the cross-cutting `s3:ResourceTag/*` object-policy investigation visible
  as deferred policy-evaluator follow-up rather than reopening the completed
  Ceph review plan

## Current Status

Completed:
- Header SigV4 auth
- Presigned SigV4 query auth
- POST SigV4 form auth
- Temporary/session credential checks (`session_token`, credential expiry)
- Constant-time signature/token comparison
- Principal propagation from auth into request handling
- Authorization moved into `server-core`
- Principal-based bucket ownership (`owner_principal`)
- Durable bucket `owner_canonical_id`
- Owner/private/public-read authorization behavior
- Bucket-level `PutBucketAcl` support
- Bucket-level `GetBucketAcl` support
- Bucket-level public write semantics for anonymous/public `PUT Object`
- Bucket-level public write semantics for anonymous/public `POST Object`
- S3 XML owner `<ID>` fields using canonical owner IDs for current bucket/list
  and version-list surfaces
- Schema-level length checks for bucket auth/identity fields
- Public access block and ownership-controls integration
- Auth, presigned, public-access, and owner-XML integration coverage

Still open:
- Account-level Block Public Access controls
- Final compatibility/conformance documentation
- Decision on where to track and validate the remaining `s3:ResourceTag/*`
  investigation once policy-evaluator expansion resumes

## Remaining Work

### 1. Account-Level Block Public Access

Still missing:
- account-level Block Public Access controls
- account-scoped enforcement of `BlockPublicAcls`, `IgnorePublicAcls`,
  `BlockPublicPolicy`, and `RestrictPublicBuckets`
- AWS-matching interaction between account-level controls and the already
  implemented bucket-level ACL / public-access behavior

Explicitly still out of scope for this plan:
- full bucket policy evaluation
- full IAM policy language

### 2. Constrained Same-Account Authorization Shape

Completed since the previous revision:
- credentials now carry an explicit `AuthorizationProfile`
- auth, HTTP, and coordinator request propagation preserve that scope
- default `Requester` construction is now least-privileged
- owner-account admin behavior must be opted into explicitly
- same-account constrained credentials are denied for ordinary object writes and
  multipart write flows unless separately allowed
- same-account root / broad owner-account credentials retain the AWS-aligned
  bucket-admin and object-admin behavior covered by the current fixture

This closes the review-driven gap where local behavior had started to treat any
same-account principal as a generic implicit writer.

Longer-term identity note:
- canonical owner IDs should ultimately come from durable account metadata, not
  be derived from principal strings
- `AuthorizationProfile` is the current coarse credential-scope model, not the
  end-state account/credential service

### 3. Conformance Cleanup

Still missing:
- a short documented auth/public-access compatibility subset against AWS
- folding in the remaining auth/header-related Ceph `s3-tests` porting work so
  it is tracked here rather than in the old integration-framework bootstrap plan
- a documented follow-up decision for account-level IAM coverage on
  `CreateBucket` / `ListBuckets`, which the Ceph authz review identified as
  account-policy work rather than bucket-policy work
- a documented owner for the deferred `s3:ResourceTag/*` investigation

Already covered:
- wrong-region header auth coverage
- wrong-service header auth coverage
- duplicate `Authorization` rejection coverage
- AWS validation for bucket-level public write and admin-operation denial
- AWS validation for anonymous public-write ownership behavior on plain `PUT`
  and `POST`
- AWS validation for same-account root/non-root bucket-admin and object-admin
  behavior
- AWS validation for constrained same-account write denial

## Target Behavior

### Authentication

1. Accept and validate:
- Header SigV4
- Presigned SigV4 query auth
- POST SigV4 form auth

2. Accept temporary credentials:
- require matching session token when bound to the credential
- reject expired credentials

3. Enforce AWS-like header auth rules:
- strict header scope validation for configured region/service
- every header listed in `SignedHeaders` must be present
- all `x-amz-*` headers must be signed
- malformed/duplicate auth headers rejected consistently

### Authorization

Minimal intended behavior after this follow-up:
- owner: full bucket/object access
- anonymous: read-only on explicitly public-read buckets
- anonymous/public write only when explicitly enabled by the supported ACL model
- anonymous public-write uploads must retain AWS-compatible owner/read/delete
  behavior for the bucket owner
- same-account constrained credentials must remain denied unless an implemented
  ACL or policy path explicitly allows them
- same-account owner-account admin credentials may perform only the
  AWS-compatible bucket/object admin operations that are explicitly modeled
- account-level Block Public Access controls override bucket-level public ACL
  behavior where AWS does
- non-owner authenticated callers remain denied unless allowed by supported ACL or later policy work

## Implementation Phases

### Phase 1: Header Auth Hardening

Status: complete.

### Phase 2: ACL Surface Completion

Status: complete for bucket-level ACL behavior.

### Phase 3: Owner Identity Compatibility

Status: complete for current bucket/list/version-list XML surfaces.

### Phase 4: Ownership And ACL Compatibility

Status: complete for the current supported surface.

Completed:
- explicit object-level owner identity on stored objects, delete markers, and
  multipart uploads
- object ACL API support and the required owner/grantee authorization behavior
- AWS-aligned anonymous public-write ownership behavior for plain `PUT` and
  plain `POST`
- AWS-aligned `bucket-owner-full-control` behavior for anonymous `PUT` and
  anonymous `POST`
- same-account root/non-root bucket-admin and object-admin coverage
- same-account constrained write denial coverage

### Phase 5: Account-Level Public Access Block

Status: open.

Deliver:
- account-level Block Public Access controls
- AWS-aligned interaction between account-level and bucket-level public-access
  enforcement

### Phase 6: Conformance Cleanup

Status: open.

Deliver:
- rerun targeted auth/public-access `s3-tests`
- account for the remaining auth/header-related Ceph test coverage that is not
  yet ported into `crates/s3-tests`
- document remaining intentional incompatibilities:
  - account-level Block Public Access, if still deferred
  - policy evaluation, if still deferred

## Test Plan

Unit tests:
- `cargo test -p auth`

Targeted integration tests:
- `cargo test -p s3-tests --test headers`
- `cargo test -p s3-tests --test presigned`
- `cargo test -p s3-tests --test public_access_block`
- `cargo test -p s3-tests --test bucket_anon`
- `cargo test -p s3-tests --test ownership`
- `cargo test -p s3-tests --test post_object`
- `cargo test -p s3-tests --test versioning`
- `cargo test -p s3-tests --test bucket_admin_root`
- `cargo test -p s3-tests --test object_admin_root`
- `cargo test -p s3-tests --test object_write_root`
- `cargo test -p s3-tests --test object_write_constrained`

AWS checks:
- rerun the narrowed auth/public-access subset against AWS while working on
  Phase 5 account-level public-access behavior

## Open Decisions

1. Account-level Block Public Access rollout shape
- whether to mirror the AWS account-level control surface directly in the
  current local/server model, or defer part of that to later durable account
  management work

2. Authorization-profile evolution
- whether `AuthorizationProfile` remains the long-lived compatibility mechanism
  for current credentials, or becomes an implementation detail once durable
  account / credential metadata exists

3. Deferred authz follow-up ownership
- whether the remaining `s3:ResourceTag/*` compatibility investigation should
  live under a future policy-evaluator plan or stay tracked here until that
  work is started

## Recommended Default Decisions

- Keep region strictness enabled
- Keep bucket-level public write as the current supported bucket-ACL surface
- Keep `AuthorizationProfile` explicit at auth boundaries rather than inferring
  broad rights from same-account identity
- Keep `owner_canonical_id` explicitly stored rather than deriving it ad hoc in XML rendering
