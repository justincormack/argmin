# AWS Auth Compatibility Follow-Up Plan

## Scope

This plan now tracks the remaining AWS auth/authz compatibility work after the
main SigV4 and principal-propagation implementation.

Completed work is retained here only as context. The actual implementation work
remaining is limited and should be treated as a follow-up plan, not a greenfield
auth design.

This follow-up covers:
- remaining ACL/authz gaps
- owner identity compatibility gaps
- full object ownership compatibility gaps
- conformance tests for the above

This still does not cover:
- full IAM policy language
- full bucket policy evaluation
- STS AssumeRole API implementation

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
- Full object ownership compatibility
- Object ACL APIs / semantics
- Account-level Block Public Access controls
- Final compatibility/conformance documentation

## Remaining Work

### 1. Full Ownership Model Compatibility

Still missing:
- durable object-level owner identity distinct from bucket owner
- object-owner semantics for anonymous/public-write uploads
- bucket-owner access rules that match AWS for objects written by other principals
- object ACL semantics sufficient to express owner/grantee behavior

Confirmed AWS behavior that still differs locally:
- bucket-level `public-read-write` allows anonymous `PUT` and `POST`
- the bucket owner cannot subsequently `HEAD`/`GET` that uploaded object
- the bucket owner can still delete it

Current argmin behavior:
- anonymous/public write itself is implemented
- argmin does not yet model per-object owner identity for these writes
- local bucket-owner readback therefore still differs from AWS

Longer-term identity note:
- canonical owner IDs should ultimately come from durable account metadata, not
  be derived from principal strings
- that implies a real account/account-metadata service in the later ownership
  design, rather than treating principal strings as the permanent identity substrate

### 2. ACL Surface Beyond Bucket ACLs

Still missing:
- object ACL APIs / semantics
- any ACL-driven authorization beyond the current bucket-level ACL model
- account-level Block Public Access controls and their interaction with
  bucket-level ACL/public-access behavior

Explicitly still out of scope for this plan:
- full bucket policy evaluation
- full IAM policy language

### 3. Conformance Cleanup

Still missing:
- a short documented auth/public-access compatibility subset against AWS
- a short written record of the confirmed AWS ownership behavior for anonymous
  public-write uploads

Already covered:
- wrong-region header auth coverage
- wrong-service header auth coverage
- duplicate `Authorization` rejection coverage
- AWS validation for bucket-level public write and admin-operation denial

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
- account-level Block Public Access controls override bucket-level public ACL
  behavior where AWS does
- non-owner authenticated callers remain denied unless allowed by supported ACL or later policy work

This is still intentionally weaker than full AWS ownership behavior until the
object-ownership work lands.

## Implementation Phases

### Phase 1: Header Auth Hardening

Status: complete.

### Phase 2: ACL Surface Completion

Status: complete for bucket-level ACL behavior.

### Phase 3: Owner Identity Compatibility

Status: complete for current bucket/list/version-list XML surfaces.

### Phase 4: Full Ownership Model

Status: open.

Deliver:
- explicit object-level owner identity
- correct bucket-owner behavior for anonymously/publicly uploaded objects
- enough object ACL support to make those semantics coherent

Success criteria:
- external AWS-aligned tests for anonymous public-write uploads match locally:
  - upload succeeds
  - bucket-owner read is denied where appropriate
  - bucket-owner cleanup still succeeds

### Phase 5: Conformance Cleanup

Status: open.

Deliver:
- rerun targeted auth/public-access `s3-tests`
- document remaining intentional incompatibilities:
  - object ACLs, if still deferred
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
- `cargo test -p s3-tests --test post_object`
- `cargo test -p s3-tests --test versioning`

AWS checks:
- rerun the narrowed auth/public-access subset against AWS while working on
  Phase 4 ownership behavior

## Open Decisions

1. Object owner identity source
- whether to persist authenticated writer principal first, or introduce a
  canonical object-owner identifier at the same time

2. Object ACL rollout shape
- whether to start with the minimum object-owner semantics needed for the AWS
  public-write behavior, or implement a broader object-ACL surface together

## Recommended Default Decisions

- Keep region strictness enabled
- Keep bucket-level public write as the current supported bucket-ACL surface
- Finish object ownership semantics before widening object ACL scope
- Keep `owner_canonical_id` explicitly stored rather than deriving it ad hoc in XML rendering
