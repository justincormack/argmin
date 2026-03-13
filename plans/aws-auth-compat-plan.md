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

Implemented:
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
- Bucket-level `PutBucketAcl` support for current public-read behavior
- Bucket-level `GetBucketAcl` support
- Bucket-level public write semantics for anonymous/public `PUT Object`
- Bucket-level public write semantics for anonymous/public `POST Object`
- S3 XML owner `<ID>` fields using canonical owner IDs for current bucket/list
  surfaces
- Schema-level length checks for bucket auth/identity fields
- Public access block and ownership-controls integration
- Auth, presigned, and public-access integration coverage

## Remaining Gaps

### 1. Header SigV4 Hardening

Completed:
- Header SigV4 credential scope region validation
- Header SigV4 credential scope service validation
- Strict enforcement that every header named in `SignedHeaders` is present
- Explicit auth parsing size/count limits
  - `Authorization` header length
  - presigned query length
  - token length
  - signed-header count
- Duplicate `Authorization` handling
  - AWS-compatible `501 NotImplemented` for duplicate `Authorization`

### 2. Auth Error Mapping

Completed:
- `ExpiredToken` maps explicitly
- `InvalidToken` maps explicitly
- malformed header scope failures route through `AuthorizationHeaderMalformed`

### 3. ACL and Authorization Surface

Still missing:
- Object ACL APIs / semantics
- Any ACL-driven authorization beyond the current bucket-level ACL model
- Bucket policy / IAM policy evaluation

Notes:
- `PutBucketAcl` is implemented; the old plan text saying otherwise is stale.
- `GetBucketAcl` is now implemented.
- Public-read is no longer create-time only; bucket ACL updates already affect it.
- Bucket-level public write for anonymous/public `PUT` and `POST` is now implemented.

### 4. Owner Identity Compatibility

Completed:
- durable `owner_canonical_id`
- S3 XML owner `<ID>` using canonical owner id instead of raw principal string
  on the current bucket/list surfaces
- schema-level length checks for bucket auth/identity fields

### 5. Full Ownership Model Compatibility

Still missing:
- durable object-level owner identity distinct from bucket owner
- object-owner semantics for anonymous/public-write uploads
- bucket-owner access rules that match AWS for objects written by other principals
- object ACL semantics sufficient to express owner/grantee behavior

Notes:
- AWS behavior was confirmed directly for bucket-level `public-read-write`:
  - anonymous `PUT`/`POST` succeeds
  - the bucket owner cannot subsequently `HEAD`/`GET` that object
  - the bucket owner can still delete it
- Argmin does not yet model per-object owner identity for these writes, so local
  behavior still differs here.
- Longer term, canonical owner IDs should come from durable account metadata,
  not be derived from principal strings. That implies a real account/account-
  metadata service during the later ownership/account design work, rather than
  treating principal strings as the permanent identity substrate.

### 6. Conformance and Integration Coverage

Still missing:
- a documented auth/public-access compatibility subset against AWS
- a short written record of the confirmed AWS ownership behavior for anonymous
  public-write uploads

Completed:
- header-auth wrong-region coverage
- header-auth wrong-service coverage
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
- non-owner authenticated callers remain denied unless allowed by supported ACL or later policy work

This is still intentionally weaker than full AWS ownership behavior until the
object-ownership work lands.

## Implementation Phases

### Phase 1: Header Auth Hardening

Deliver:
- Completed

Success criteria:
- the formerly ignored header-auth tests are enabled and passing
- targeted AWS checks match the local behavior for:
  - wrong region
  - wrong service
  - duplicate `Authorization`

### Phase 2: ACL Surface Completion

Deliver:
- Completed

Success criteria:
- `GetBucketAcl` is implemented
- bucket-level public write for anonymous/public `PUT` and `POST` is implemented
- integration coverage exists for:
  - anonymous/public `PUT`
  - anonymous/public `POST`
  - public-write buckets still denying admin operations

### Phase 3: Owner Identity Compatibility

Deliver:
- `owner_canonical_id` storage
- canonical owner XML rendering
- schema `CHECK(length(...))` constraints for auth/identity fields

Success criteria:
- owner XML no longer exposes raw principal strings as canonical IDs

### Phase 4: Full Ownership Model

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

Deliver:
- rerun targeted auth/public-access `s3-tests`
- document remaining intentional incompatibilities:
  - object ACLs, if still deferred
  - policy evaluation, if still deferred

## Test Plan

Unit tests:
- `cargo test -p auth`

Targeted integration tests:
- `cargo test -p s3-tests --test headers`
- `cargo test -p s3-tests --test presigned`
- `cargo test -p s3-tests --test public_access_block`
- add/update dedicated tests for anonymous/public `PUT` and `POST`

AWS checks:
- rerun the auth/public-access subset against AWS once the remaining header-scope
  and public-write behavior is implemented

## Open Decisions

1. Public write representation
- whether to model public write as a bucket-level ACL flag first, or jump directly
  to fuller ACL/object-ACL representation

2. Owner canonical id derivation
- whether to store a configured/generated canonical id directly or derive it
  deterministically from an existing stable principal identity

3. Object owner identity source
- whether to persist the authenticated writer principal directly as object owner
  first, or introduce a separate canonical object-owner identifier at the same
  time

## Recommended Default Decisions

- Keep region strictness enabled
- Finish bucket-level public write before object ACLs
- Store `owner_canonical_id` explicitly rather than deriving it ad hoc in XML rendering
