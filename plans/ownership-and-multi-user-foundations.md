# Ownership And Multi-User Foundations

## Scope

This plan covers the identity and ownership substrate needed for the remaining
ignored Ceph tests that currently depend on multi-user or multi-tenant
behavior.

In scope:
- durable account/requester identity beyond a bare principal string
- durable object owner identity per version and per delete marker
- multipart upload owner and initiator identity
- authorization changes that distinguish bucket owner, object owner, grantee,
  and anonymous caller
- compatibility work for the current ignored multi-user and multi-tenant tests

Out of scope:
- full IAM policy language
- full bucket policy evaluation
- full ACL surface beyond the ownership behavior needed as a foundation
- STS APIs or external identity federation

## Why This Is Separate

ACLs and bucket policies both depend on correct ownership semantics. Today the
core authorization model is still bucket-centric, so adding ACL or policy
checks first would force another authz rewrite later.

This plan should land before:
- `plans/object-and-bucket-acls.md`
- `plans/bucket-policies.md`

## Current State

The current code already has:
- authenticated account identity propagated into `server-core`
- durable bucket owner principal and canonical ID
- durable object owner principal and canonical ID on live object rows and
  delete markers
- durable multipart effective owner identity plus optional initiator identity
- alternate test credentials in `crates/s3-tests`
- limited ownership-controls support at the bucket level

The current code does not yet have:
- complete object-level authorization decisions across grant-based ACL and
  policy cases
- XML and API surfaces that consistently render stored object and multipart
  owner identity where AWS expects it
- tenant/account modeling beyond synthetically derived account identity on
  configured credentials

Important current shortcuts:
- write authorization still compares the requester primarily to the bucket
  owner plus bucket public-write flags
- object ACL handling only covers the minimal stored public-read/public-read-write
  behavior needed for current tests; grant-based ACLs remain later work
- multipart list and future ACL/XML owner surfaces do not yet consistently use
  the stored owner and initiator identity

## Current Status

Phase 1 is complete. Phase 2 durable persistence is also in place in storage
and coordinator paths.

Completed in Phase 1:
- shared `AccountIdentity` type added for auth and request handling
- credential records now resolve access keys to typed account identity rather
  than a bare principal string
- `AuthContext` now carries account identity, including canonical user ID,
  across header auth, presigned auth, and POST auth
- `Requester` now models anonymous versus authenticated account callers instead
  of only `Option<&str>` principal state
- HTTP request dispatch now constructs coordinator requesters from the typed
  auth context

Completed in Phase 2:
- live object rows and delete markers now persist owner principal and canonical
  ID explicitly
- multipart upload rows now persist effective owner identity plus optional
  initiator identity explicitly
- coordinator write paths compute effective object ownership from the requester
  and bucket ownership-controls rules
- multipart completion reuses stored upload owner identity rather than
  re-deriving it from the bucket
- storage read paths now return stored owner identity for live objects, delete
  markers, and multipart uploads without inference

Still open after Phase 2:
- the server config and local test harness still synthesize account identity
  directly from configured access keys and fixture principals
- object-scoped authorization is still incomplete on write-side cleanup edges
  and ACL-grant cases
- XML and API surfaces still need later phases to render stored owner identity
  for object and multipart responses where AWS expects it

## Target Behavior

### Identity Model

Introduce an explicit account identity model used consistently by auth, HTTP,
coordinator, and storage.

Minimum required identity fields:
- account ID or durable principal key
- canonical user ID
- display name or principal name used in XML where required

Requirements:
- canonical IDs must be durable and no longer recomputed ad hoc on object paths
- access keys authenticate to an account identity, not directly to a bucket
  ownership check
- presigned and header-authenticated requests must resolve to the same account
  identity

### Ownership Model

Buckets:
- keep existing durable bucket owner identity

Objects:
- every live object version stores an explicit owner identity
- every delete marker stores an explicit owner identity
- owner identity is set from the actual requester at write time, subject to
  ownership-controls rules

Multipart uploads:
- store both initiator and effective owner identity
- expose owner and initiator in list responses where AWS does

### Authorization Model

Move from bucket-only authorization to layered authorization:

1. Bucket-scoped checks
- bucket existence
- bucket owner admin operations
- bucket-level public-read/public-write gates where still relevant

2. Object-scoped checks
- object owner access
- bucket owner access when AWS grants it
- later ACL and policy decisions

Required AWS-compatible behavior to unlock current tests:
- creating an existing bucket as a non-owner must fail as non-owner
- a bucket owner must not automatically gain read access to objects written by
  other principals when AWS would deny it
- a bucket owner must still be able to perform the bucket-owner operations AWS
  allows, including cleanup paths
- multipart listing must report correct owner and initiator information

### Tenant Compatibility

The ignored presigned tenant tests should be treated as account-isolation tests,
not as a separate parallel auth stack.

Recommended shape:
- define tenant/account identity in the shared auth model
- keep the external request surface unchanged unless a test proves AWS requires
  an additional namespace dimension
- avoid adding a second incompatible notion of "user" vs "tenant"

## Design Changes

### 1. Auth And Requester Context

Expand authenticated request context to resolve a durable account identity:
- add account identity fields to auth credential records
- carry canonical user ID through authentication output
- replace `Requester` as a thin optional principal wrapper with a typed caller
  identity that can still represent anonymous requests

The resulting caller type should support:
- anonymous
- authenticated account caller

Tests should use real caller identities wherever practical. If a test needs
privileged fixture setup, that setup should live in test-only helpers or
harness code rather than in the request authorization model.

### 2. Storage Schema

Add explicit ownership fields to object metadata:
- live object rows: owner principal and owner canonical ID
- delete markers: owner principal and owner canonical ID
- multipart uploads: initiator principal/canonical ID and effective owner
  principal/canonical ID

Do not leave ownership implicit in bucket metadata once object ACLs and bucket
policies begin to rely on per-object semantics.

### 3. Core Authorization

Refactor authorization entry points to separate:
- bucket admin authorization
- bucket list/read authorization
- object read authorization
- object write authorization
- object owner vs bucket owner cleanup/delete authorization

The coordinator should stop treating "can read bucket" as equivalent to "can
read any object in bucket".

### 4. XML And API Surfaces

Update XML renderers and list surfaces to use stored owner identity rather than
bucket owner identity by default where AWS expects object or upload ownership.

This includes:
- list multipart uploads owner/initiator output
- future object ACL owner output
- version-list owner output once per-version owner data exists

### 5. Test Infrastructure

The test harness already exposes owner and alternate clients. Extend it only as
needed for tenant/account-isolation cases, keeping the identity story shared
with the production auth path.

## Implementation Phases

### Phase 1: Durable Account Identity

Status: complete. Typed account identity now flows through auth, HTTP, and core
requester handling.

Deliver:
- typed account identity in auth and request handling
- canonical ID carried through request auth
- stable mapping from access key to account identity

Success criteria:
- all existing auth tests still pass
- no call sites depend on raw principal strings alone for future ownership work

### Phase 2: Durable Object And Multipart Ownership

Status: complete in storage and coordinator paths. Explicit owner identity is
now stored and read back for live objects, delete markers, and multipart
uploads.

Deliver:
- schema changes for object and multipart owner identity
- write paths populate owner identity from the requester
- read paths can retrieve object owner identity without inference

Success criteria:
- PUT, COPY, multipart initiation, complete, and delete-marker creation all
  persist explicit owner identity

### Phase 3: Object-Scoped Authorization

Status: in progress. Private-object read, head, range, attributes, tagging,
and copy-source authorization now consult stored object owner identity instead
of only bucket read access. This phase also persists the minimal object
`public-read`/`public-read-write` bit needed to preserve AWS-compatible
anonymous read behavior once bucket-level read shortcuts are removed. Full
grant-based object ACL semantics remain for later ACL work.

Deliver:
- object read and object metadata APIs authorize against object ownership rules
- bucket-owner cleanup/delete behavior matches AWS for the currently targeted
  cases

Success criteria:
- multi-user private-object read and copy authorization tests can be unignored
- ACL-grant cases stay explicitly tracked for later ACL work rather than
  relying on bucket-level shortcuts

### Phase 4: Multipart Owner And Initiator Surfaces

Deliver:
- list multipart uploads result includes correct owner/initiator semantics
- multipart ownership checks use stored upload owner identity

Success criteria:
- multipart owner test can be unignored

### Phase 5: Tenant Compatibility Cleanup

Deliver:
- presigned tests that require account isolation use the shared identity model
- remaining multi-tenant ignored tests are either passing or reduced to a
  narrower documented gap

## Test Plan

Targeted integration tests:
- `cargo test -p s3-tests --test bucket_crud test_bucket_create_exists_nonowner -- --ignored`
- `cargo test -p s3-tests --test copy_object test_object_copy_not_owned_bucket -- --exact`
- `cargo test -p s3-tests --test multipart test_list_multipart_upload_owner -- --ignored`
- `cargo test -p s3-tests --test presigned test_object_presigned_put_object_with_acl_tenant -- --ignored`
- `cargo test -p s3-tests --test presigned test_object_raw_get_x_amz_expires_not_expired_tenant -- --ignored`

Still blocked on later ACL work:
- `cargo test -p s3-tests --test copy_object test_object_copy_not_owned_object_bucket -- --ignored`

Regression coverage to keep green while working:
- `cargo test -p s3-tests --test ownership`
- `cargo test -p s3-tests --test bucket_anon`
- `cargo test -p s3-tests --test presigned`
- `cargo test -p s3-tests --test multipart`
- `cargo test -p s3-tests --test copy_object`

AWS validation:
- rerun a narrowed ownership subset against AWS before finalizing object-owner
  semantics for public-write and cross-account behavior

## Open Decision

1. Account identity source
- whether to introduce a small durable local account registry now, or keep the
  source in the credential store while still exposing a stable typed identity

Resolved in implementation:
- delete-marker owner identity is stored in the same row shape as live objects
- multipart uploads store effective owner and initiator as separate durable
  fields

## Recommended Defaults

- Introduce a typed account identity now rather than continuing to pass raw
  principal strings through `server-core`
- Store owner principal and canonical ID directly on every committed object
  version and delete marker
- Store multipart initiator and effective owner explicitly rather than inferring
  either from the bucket
- Keep tenant compatibility work inside the same account identity model instead
  of creating a second parallel abstraction
