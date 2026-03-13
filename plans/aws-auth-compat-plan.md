# AWS Auth Compatibility Follow-Up Plan

## Scope

This plan now tracks the remaining AWS auth/authz compatibility work after the
main SigV4 and principal-propagation implementation.

Completed work is retained here only as context. The actual implementation work
remaining is limited and should be treated as a follow-up plan, not a greenfield
auth design.

This follow-up covers:
- header SigV4 hardening gaps
- remaining auth error mapping gaps
- remaining ACL/authz gaps
- owner identity compatibility gaps
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
- Owner/private/public-read authorization behavior
- Bucket-level `PutBucketAcl` support for current public-read behavior
- Public access block and ownership-controls integration
- Auth, presigned, and public-access integration coverage

## Remaining Gaps

### 1. Header SigV4 Hardening

Still missing:
- Header SigV4 credential scope region validation
- Header SigV4 credential scope service validation
- Strict enforcement that every header named in `SignedHeaders` is present
  - current behavior is still lenient for non-required signed headers
- Explicit auth parsing size/count limits
  - `Authorization` header length
  - presigned query length
  - token length
  - signed-header count
- Duplicate `Authorization` header rejection

### 2. Auth Error Mapping

Still missing or incomplete:
- `ExpiredToken` should map explicitly, not collapse to generic `AccessDenied`
- `InvalidToken` should map explicitly, not collapse to generic `AccessDenied`
- `AuthorizationHeaderMalformed` remains coarse; the remaining region/service
  scope failures should map through it consistently once implemented

### 3. ACL and Authorization Surface

Still missing:
- `GetBucketAcl`
- Object ACL APIs / semantics
- Bucket-level public write semantics
  - anonymous/public `PUT Object`
  - anonymous/public `POST Object`
- Any ACL-driven write authorization beyond the current owner-only write model
- Bucket policy / IAM policy evaluation

Notes:
- `PutBucketAcl` is implemented; the old plan text saying otherwise is stale.
- Public-read is no longer create-time only; bucket ACL updates already affect it.
- Public write is still absent and is now explicitly in scope for this follow-up.

### 4. Owner Identity Compatibility

Still missing:
- durable `owner_canonical_id`
- S3 XML owner `<ID>` using canonical owner id instead of raw principal string
- schema-level length checks for auth/identity fields

### 5. Conformance and Integration Coverage

Still missing:
- unignore and pass the header-auth wrong-region test
- unignore and pass the header-auth wrong-service test
- unignore and pass duplicate `Authorization` header rejection
- explicit integration coverage for anonymous/public write behavior once added
  - `PUT Object`
  - `POST Object`
- a documented auth/public-access compatibility subset against AWS

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

## Implementation Phases

### Phase 1: Header Auth Hardening

Deliver:
- header region/service scope validation
- strict signed-header enforcement
- duplicate `Authorization` rejection
- auth size/count limits
- explicit error mapping for token failures

Success criteria:
- the currently ignored header-auth tests are enabled and passing

### Phase 2: ACL Surface Completion

Deliver:
- `GetBucketAcl`
- bucket-level public write model sufficient for anonymous/public `PUT` and `POST`
- any required core/storage representation changes for that ACL state

Success criteria:
- anonymous/public write behavior is explicit and integration-tested

### Phase 3: Owner Identity Compatibility

Deliver:
- `owner_canonical_id` storage
- canonical owner XML rendering
- schema `CHECK(length(...))` constraints for auth/identity fields

Success criteria:
- owner XML no longer exposes raw principal strings as canonical IDs

### Phase 4: Conformance Cleanup

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

## Recommended Default Decisions

- Keep region strictness enabled
- Finish bucket-level public write before object ACLs
- Store `owner_canonical_id` explicitly rather than deriving it ad hoc in XML rendering
