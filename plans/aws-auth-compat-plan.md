# AWS-Compatible Auth Handling Plan

## Scope

Add AWS-compatible request authentication and basic authorization for the S3 API in `argmin-s3`.

Assumption from product direction:
- No backward compatibility for persisted metadata is required.
- We can change schema and in-memory interfaces directly.

This plan covers:
- SigV4 `Authorization` header auth.
- SigV4 presigned query auth (`X-Amz-*`).
- Temporary/session credentials (`x-amz-security-token`).
- Principal propagation from auth to authz.
- Minimal AWS-like authorization behavior for owner and anonymous/public access.

This plan does not cover:
- Full IAM policy language.
- Full bucket policy language evaluator.
- STS AssumeRole API implementation.

## Current Status (Implemented)

Completed so far:
- Header SigV4 auth and presigned SigV4 auth.
- Temporary credential checks (`session_token`, credential expiry).
- `AuthContext` propagation through HTTP dispatch.
- Principal-based bucket ownership (`owner_principal`) and owner-scoped bucket listing/creation.
- Minimal authz checks in HTTP layer (owner read/write, anonymous/non-owner read on public buckets).
- Bucket-level public-read flag (`public_read`) with create-time support via `x-amz-acl: public-read`.

## Remaining Gaps (Still Missing)

Auth correctness and compatibility gaps:
- Header SigV4 credential scope region/service validation is not enforced yet.
- Signed header enforcement is still lenient for non-required signed headers.
- Signature comparison is not constant-time yet.
- Auth parsing size limits are not enforced yet (auth header/query/token lengths and counts).
- `AuthError`/S3 error mapping is still incomplete for AWS-compatible codes/messages:
  - `InvalidToken`/`ExpiredToken`/`AuthorizationHeaderMalformed` handling is partial.
  - Many auth failures still collapse to generic `AccessDenied`.

Authorization and ACL gaps:
- Public-read is currently create-time only (`x-amz-acl` on CreateBucket).
- `PutBucketAcl` / `GetBucketAcl` APIs are not implemented.
- No object ACL support.
- No bucket policy or IAM policy evaluation.

Identity/data-model compatibility gaps:
- `owner_canonical_id` is not stored yet.
- S3 XML owner `<ID>` currently uses principal string, not AWS-style canonical owner id.
- No DB `CHECK(length(...))` constraints for principal/canonical-id limits yet.

Conformance/testing gaps:
- Need dedicated auth/public-read `s3-tests` subset reruns against current code and documented expected failures.
- Need explicit integration tests for anonymous access behavior across private vs public buckets.

## Target Behavior

1. Accept and validate both:
- Header SigV4.
- Presigned SigV4 query authentication.

2. Accept temporary credentials:
- Validate session token when credential is token-bound.
- Enforce credential expiration.

3. Propagate principal identity to request handling:
- Every request runs with an `AuthContext`.
- Bucket ownership and access checks use principal identity.

4. Support AWS-like minimal authorization:
- Owner access to owned buckets/objects.
- Anonymous access only for explicitly public data.
- Return appropriate S3 authz errors (`AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch`, `RequestTimeTooSkewed`, `ExpiredToken`, `InvalidToken`, `AuthorizationHeaderMalformed`).

## Proposed Interfaces

## `auth` crate

Add authentication entrypoint:

```rust
pub fn authenticate_request(
    method: &str,
    path: &str,
    query_string: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    store: &CredentialStore,
    expected_region: &str,
    expected_service: &str, // "s3"
    now_epoch_secs: u64,
) -> Result<AuthContext, AuthError>
```

Add context types:

```rust
pub enum AuthMode {
    HeaderSigV4,
    PresignedSigV4,
    Anonymous,
}

pub struct AuthContext {
    pub mode: AuthMode,
    pub access_key_id: Option<String>,
    pub principal: Option<String>, // principal id string
    pub request_epoch_secs: Option<u64>,
}
```

Add credential model:

```rust
pub struct CredentialRecord {
    pub access_key_id: String,
    pub secret_key: SecretKey,
    pub principal: String,
    pub session_token: Option<String>,
    pub expires_at_epoch_secs: Option<u64>,
    pub enabled: bool,
}
```

Replace `CredentialStore` internals with `HashMap<String, CredentialRecord>`.

Add helpers:

```rust
pub fn verify_header_sigv4(...) -> Result<AuthContext, AuthError>
pub fn verify_presigned_sigv4(...) -> Result<AuthContext, AuthError>
```

## `server` crate

Change auth integration:

```rust
fn authenticate(&self, req: &S3Request) -> Result<AuthContext, ServerError>
fn dispatch(&self, req: &S3Request, auth: &AuthContext) -> Result<S3Response, ServerError>
```

In `handle_request`, call `authenticate`, then `dispatch(req, &auth)`.

Change coordinator signatures to take principal:

```rust
pub fn create_bucket(&self, owner: &str, name: &str) -> Result<(), ServerError>
pub fn list_buckets(&self, owner: &str) -> Result<Vec<BucketInfo>, ServerError>
```

Add authorization checks at HTTP layer for bucket/object operations:
- Resolve bucket owner via `head_bucket`.
- Compare `auth.principal` for owner-only operations.
- Allow anonymous only for public resources.

## Data Model Changes (No Migration)

Because compatibility is not required, replace bucket owner integer with principal text.

Current implemented bucket schema:

```sql
CREATE TABLE buckets (
    name            TEXT PRIMARY KEY,
    owner_principal TEXT NOT NULL,
    created_at      INTEGER NOT NULL,
    region          INTEGER NOT NULL DEFAULT 0,
    versioning      INTEGER NOT NULL DEFAULT 0,
    public_read     INTEGER NOT NULL DEFAULT 0
)
```

Still missing in schema:
- `owner_canonical_id` (AWS-style canonical owner id).
- explicit `CHECK(length(...))` limits for auth/identity fields.

Update:
- `crates/storage/src/schema.rs`
- `crates/storage/src/types.rs` (`BucketInfo.owner_id: u64` -> `owner_principal: String`)
- `crates/storage/src/traits.rs` (`create_bucket/list_buckets` owner arg types to `&str`)
- `crates/storage/src/bucket_db.rs`
- `crates/server/src/coordinator.rs`
- `crates/server/src/http/xml.rs` list owner serialization (owner ID/display should be principal or stable derived canonical id)

## Data Size Limits (Required)

All auth and identity fields must have explicit limits in both parsing code and schema constraints.

Recommended limits:
- `owner_principal`: `1..=2048` bytes.
- `owner_canonical_id`: exactly `64` lowercase hex chars (AWS-style canonical owner ID).
- `access_key_id`: `1..=128` bytes.
- `secret_access_key`: `1..=256` bytes.
- `session_token`: `1..=4096` bytes when present.
- `Authorization` header value: `<=8192` bytes.
- Query string length for presigned auth: `<=16384` bytes.
- `X-Amz-Credential`: `1..=2048` bytes.
- `X-Amz-SignedHeaders`: `1..=2048` bytes.
- `X-Amz-Signature`: exactly `64` lowercase hex chars.
- Signed header count: `<=128`.

Enforcement points:
- `auth` parsing layer rejects over-limit values with explicit auth errors.
- `server/http/request.rs` enforces request-line/query/header size ceilings.
- SQLite schema adds `CHECK(length(...))` for principal/canonical id fields.

Notes for AWS compatibility:
- Bucket/object owner identity in S3 XML should use `owner_canonical_id` (64-hex), not raw principal/ARN.
- `owner_principal` remains internal for authorization decisions.

## Authorization Policy (Minimal)

Implement a small policy module in `crates/server/src/authz.rs`:

```rust
pub enum ResourceVisibility {
    Private,
    PublicRead,
}

pub fn can_read_bucket(auth: &AuthContext, owner: &str, visibility: ResourceVisibility) -> bool
pub fn can_write_bucket(auth: &AuthContext, owner: &str) -> bool
```

Initial behavior:
- Owner: full access.
- Anonymous: read-only if `public_read`.
- Non-owner authenticated: denied unless public read and operation is read.

This is enough to align with current test focus around owner/private/public-read behavior without implementing full IAM.

## SigV4 Requirements To Implement

Header SigV4:
- Existing functionality retained.
- Enforce all headers listed in `SignedHeaders` are present.
- Constant-time signature comparison.
- Validate scope service=`s3`.
- Validate scope region equals server configured region (or support wildcard config if desired).

Presigned SigV4:
- Parse and validate:
  - `X-Amz-Algorithm` == `AWS4-HMAC-SHA256`
  - `X-Amz-Credential`
  - `X-Amz-Date`
  - `X-Amz-Expires` (reject outside AWS range; usually `1..=604800`)
  - `X-Amz-SignedHeaders`
  - `X-Amz-Signature`
  - optional `X-Amz-Security-Token`
- Canonical query for signing must exclude `X-Amz-Signature`.
- For presigned requests use payload hash `UNSIGNED-PAYLOAD` unless explicitly signed otherwise.
- Validate expiration against current time.

Temporary credentials:
- If credential has `session_token`, require matching header or query token.
- Reject missing/mismatched token.
- Reject expired credentials.

## Error Model

Extend `AuthError` with explicit variants:

```rust
InvalidToken
ExpiredToken
AuthorizationHeaderMalformed { region: String, service: String }
PresignMissingParam { param: &'static str }
PresignInvalidParam { param: &'static str }
```

Map to S3 errors in `ServerError` mapping:
- `InvalidAccessKeyId`
- `SignatureDoesNotMatch`
- `RequestTimeTooSkewed`
- `AccessDenied`
- `ExpiredToken`
- `InvalidToken`
- `AuthorizationHeaderMalformed`

Keep HTTP status aligned with AWS behavior (mostly `403`; malformed request pieces may be `400`).

## Implementation Phases

### Phase 1: Auth Context + Credential Model
Status: completed.
- Add `CredentialRecord`, update `CredentialStore`.
- Load credentials in `main` from config source.
- Update `authenticate` to return `AuthContext`.
- Thread `AuthContext` through dispatch.

Deliverable:
- Header SigV4 still passes current tests with no behavior regression.

### Phase 2: Presigned SigV4
Status: completed.
- Implement query parser + verifier.
- Integrate into `authenticate_request` mode selection:
  - Header auth first if present.
  - Else query auth if `X-Amz-Algorithm` present.
  - Else `MissingAuth` (converted to anonymous context at server layer).
- Add exhaustive unit tests with AWS documentation vectors.

Deliverable:
- Presigned GET/PUT compatible with AWS SDK and `s3-tests` presign coverage.

### Phase 3: Principal-Based Ownership
Status: completed.
- Replace owner `0` usage with principal string.
- Update bucket schema and bucket DB methods.
- Update XML owner fields and coordinator owner propagation.

Deliverable:
- Buckets listed/owned per principal, no hardcoded owner.

### Phase 4: Minimal Authorization
Status: partially completed.
- Implemented: owner/private/public-read authorization checks in HTTP layer.
- Missing: ACL APIs (`PutBucketAcl`/`GetBucketAcl`), object ACLs, policy evaluation.
- Add `authz.rs`.
- Enforce owner/public/anonymous checks on relevant endpoints in `http/mod.rs`.
- Return AWS-like errors for denied operations.

Deliverable:
- Expected behavior for private/public-read and anonymous requests.

### Phase 5: Hardening and Conformance
Status: pending.
- Tighten signed-header enforcement.
- Constant-time compare.
- Region/service scope validation errors.
- Normalize error responses to match AWS messages where practical.

Deliverable:
- Stable auth behavior against selected `s3-tests` auth subset and aws-cli/boto3 scenarios.

## Test Plan

Add tests in:
- `crates/auth/src/sigv4.rs`:
  - Header SigV4 positive/negative.
  - Presigned positive/negative.
  - Session token required/mismatch.
  - Expired token/expired presign.
- `crates/server/src/http/mod.rs`:
  - Anonymous vs authenticated routing.
  - Owner checks and access denied.
  - Public-read allow path.
- Integration (`tmp/s3-tests` subsets):
  - Auth-only subset.
  - Anonymous/public/private access subset.
  - Presigned URL subset.

Commands:

```bash
cargo test -p auth
cargo test -p server
cargo clippy --workspace --all-targets -- -D warnings
```

Then run targeted `s3-tests` auth markers.

## Open Decisions

1. Region strictness:
- Strictly require credential scope region == `ARGMIN_REGION`, or allow any region for compatibility with some clients.

2. Principal identifier:
- Use access-key id as principal for now, or a separate principal/user id in credentials.

3. Public-read representation:
- Keep as a simple bucket-level flag now, or implement object-level ACL metadata immediately.

## Recommended Default Decisions

- Region strictness: strict by default, optional relax flag later.
- Principal: separate principal string in `CredentialRecord`.
- Public-read: bucket-level first, then object-level ACL extension later.
