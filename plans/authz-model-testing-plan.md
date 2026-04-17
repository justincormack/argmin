# Authorization Model Testing Plan

## Scope

This plan adds an executable authorization spec in `server-core` tests for the
security-sensitive authz surface currently implemented in
`crates/server-core/src/coordinator/authz.rs`.

In scope:
- a pure test-only authz model over a finite scenario matrix
- an adapter that materializes each modeled scenario into real coordinator state
- differential comparison between the model and public coordinator operations
- explicit coverage for:
  - existing-object authorization
  - missing-object discovery behavior (`403` vs `404` / `VersionNotFound`)
  - bucket-owner-enforced (`BOE`) semantics
  - public-access-block interactions
  - narrow bucket-policy allow/deny interactions
  - selected write-authorization and transition semantics
  - bucket-scoped ACL, versioning, and listing authorization
  - bounded `CopyObject` and `UploadPartCopy` authorization and
    request-context behavior
  - object-lock read/update authorization plus governance-bypass profile
    boundaries
  - delete authorization for current, versioned, and missing-version deletes
  - object-scoped multipart upload authorization for:
    - `BeginStreamPart`
    - `CompleteMultipartUpload`
    - `AbortMultipartUpload`
    - `ListParts`
  - request-context exactness for narrow write condition keys such as
    `s3:x-amz-acl`, `s3:x-amz-grant-*`, and copy tagging replacement
  - bucket-scoped request-context exactness for `PutBucketAcl`
  - bucket metadata/discovery invariants for `HeadBucket` and
    `GetBucketLocation`

Out of scope:
- replacing AWS-backed `s3-tests` as the external compatibility oracle
- modeling the full bucket-policy parser/evaluator
- IAM policy language expansion
- HTTP/parser fuzzing and transport-specific coverage
- measuring coverage with diff-tests

## Why This Is Worth Doing

`authz.rs` now has good targeted regressions, but it is still primarily
example-driven. The risky cases are not isolated parser bugs; they are awkward
combinations of:

- requester identity versus canonical ID
- `AuthorizationProfile`
- bucket ownership mode
- object owner identity
- ACL grants and public ACL suppression
- bucket policy allow/deny outcomes
- missing-object discovery masking

The current code is correct in many of the recently reported areas, but the
state-space is large enough that example tests alone are a weak long-term guard.

This should follow the same strategy already used successfully elsewhere in the
repo:

- `auth` uses `proptest` for canonicalization invariants
- `server-core` lifecycle behavior uses a small reference model

Authorization should have the same level of executable specification.

## Principles

### 1. Keep AWS as the external oracle

The model is not a replacement for AWS-backed tests. It is an internal,
earlier-failing spec for the subset of behavior that has already been
established by:

- existing `s3-tests`
- existing coordinator regressions
- current threat-model assumptions

When the model disagrees with the implementation and there is no already-known
AWS answer, add or update the relevant AWS-backed test rather than “fixing” the
model by intuition.

### 2. Compare against public coordinator operations

The model should not compare directly against private boolean helpers such as
`requester_can_*`.

Instead, compare against the public coordinator operations that shape the real
contract:

- `get_object`
- `get_object_attributes`
- `get_object_acl`
- `get_object_tags`
- `put_object_tags`
- `delete_object_tags`
- selected write, copy, delete/object-lock, multipart, and bucket operations
  in later phases
- bounded bucket-meta checks in later phases

This avoids encoding the current internal factoring as the expected behavior.

### 3. Start with an exhaustive finite matrix

The initial suite should use small enums and exhaustive iteration over a bounded
set of scenarios. Do not start with `proptest`.

Reasons:

- the main authz state-space is finite and should be fully covered
- failures need to print a clear, reviewable scenario description
- deterministic exhaustive tests make it easier to trust the spec

Once the matrix is stable, bounded stateful transition properties can be added
on top.

### 4. Separate principal identity from canonical identity

The model must not collapse “same canonical ID” and “same owner principal” into
one concept.

Current behavior explicitly distinguishes them. In particular:

- object-owner matching is principal-based for ordinary access
- canonical-ID matching carries special meaning only in owner-account-admin
  paths

This is an easy place to accidentally encode the wrong rule.

### 5. Model discovery behavior explicitly

For missing keys and missing versions, the important contract is often not just
allow versus deny. It is whether the caller is allowed to discover that the
target is missing.

The model should therefore use distinct outcomes such as:

- `Allow`
- `Deny`
- `RevealMissing`
- `HideMissing`

The harness can then map those to concrete `ServerError` shapes.

## Current Anchors

The first iteration should be grounded in behavior already covered by existing
tests rather than exploring the entire S3 surface at once.

Important local anchors:

- `bucket_owner_enforced_same_account_standard_requester_cannot_read_acl_attributes_or_discover`
- `bucket_owner_enforced_same_account_standard_requester_cannot_manage_object_tags`
- `get_object_bucket_owner_enforced_disables_legacy_public_read_acl`
- existing `PutObjectAcl` bucket-policy condition regressions
- `RestrictPublicBuckets` tests

Important AWS-backed anchors:

- `crates/s3-tests/tests/boe_constrained.rs`
- `crates/s3-tests/tests/boe_admin_root.rs`
- `crates/s3-tests/tests/ownership.rs`
- `crates/s3-tests/tests/object_admin_root.rs`
- `crates/s3-tests/tests/public_access_block.rs`
- `crates/s3-tests/tests/object_crud.rs`
- `crates/s3-tests/tests/multipart.rs`
- `crates/s3-tests/tests/bucket_policy.rs`
- `crates/s3-tests/tests/bucket_policy_root.rs`
- `crates/s3-tests/tests/object_lock.rs`

## File Layout

Add a new test-only module:

- `crates/server-core/src/coordinator/authz_model_tests.rs`

Wire it from the coordinator module with:

- `#[cfg(test)] mod authz_model_tests;`

Keep the new file split into two internal sections:

1. `mod model`
- pure, side-effect-free authz spec
- no coordinator calls
- no reuse of `requester_can_*` helpers

2. `mod harness`
- materialization of modeled scenarios into real coordinator state
- operation runners and result classification
- exhaustive matrix drivers

This keeps the model auditable and prevents the harness from leaking current
implementation assumptions back into the spec.

## Model Types

Use small enums instead of free-form booleans wherever possible.

### Scenario

```rust
struct Scenario {
    action: Action,
    presence: Presence,
    requester: RequesterShape,
    bucket: BucketShape,
    object: ObjectShape,
    policy: PolicyShape,
}
```

### Action

Start with:

- `GetObject`
- `GetObjectAttributes`
- `GetObjectAcl`
- `GetObjectTagging`
- `PutObjectTagging`
- `DeleteObjectTagging`

Later phases add:

- `PutObject`
- `CreateMultipartUpload`
- `BeginStreamPut`
- `BeginStreamPart`
- `CopyObject`
- `UploadPartCopy`
- `CompleteMultipartUpload`
- `AbortMultipartUpload`
- `ListParts`
- `PutObjectAcl`
- `PutObjectVersionAcl`
- `PutObjectRetention`
- `GetObjectRetention`
- `PutObjectLegalHold`
- `GetObjectLegalHold`
- `DeleteObject`
- `DeleteObjectVersion`
- `GetBucketAcl`
- `PutBucketAcl`
- `GetBucketVersioning`
- `PutBucketVersioning`
- `ListBucketVersions`
- `ListBucketMultipartUploads`
- `HeadBucket`
- `GetBucketLocation`

### Presence

Use an explicit presence enum rather than overloading object state:

- `ExistingCurrent`
- `ExistingVersion`
- `MissingKey`
- `MissingVersion`

### RequesterShape

Recommended initial shape:

```rust
struct RequesterShape {
    relation_to_bucket_owner: PrincipalRelation,
    relation_to_object_owner: PrincipalRelation,
    canonical_relation_to_object_owner: CanonicalRelation,
    profile: ProfileShape,
    is_root_principal: bool,
}
```

With:

- `PrincipalRelation`
  - `Anonymous`
  - `ExactPrincipal`
  - `SameAccountOtherPrincipal`
  - `CrossAccountPrincipal`
- `CanonicalRelation`
  - `Matches`
  - `Different`
- `ProfileShape`
  - `Standard`
  - `OwnerAccountAdmin`

Notes:

- `is_root_principal` is only meaningful for same-account admin requesters
- anonymous plus canonical match should usually be invalid and filtered out
- same-account principal and canonical match are intentionally separate knobs

### BucketShape

Recommended initial shape:

```rust
struct BucketShape {
    ownership: OwnershipShape,
    ignore_public_acls: bool,
    block_public_acls: bool,
    restrict_public_buckets: bool,
    bucket_public_read: bool,
    bucket_public_write: bool,
}
```

With:

- `OwnershipShape`
  - `ObjectWriter`
  - `BucketOwnerPreferred`
  - `BucketOwnerEnforced`

### ObjectShape

Recommended initial shape:

```rust
struct ObjectShape {
    owner_kind: ObjectOwnerKind,
    acl: ObjectAclShape,
}
```

With:

- `ObjectOwnerKind`
  - `BucketOwnerPrincipal`
  - `SameAccountOtherPrincipal`
  - `CrossAccountPrincipal`
  - `AnonymousUpload`
- `ObjectAclShape`
  - `Private`
  - `PublicRead`
  - `AuthenticatedRead`
  - `GrantReadToRequester`
  - `GrantReadAcpToRequester`
  - `FullControlToRequester`

The first iteration does not need to model arbitrary ACL graphs. It only needs
enough shape to represent the decision boundaries already known to matter.

### PolicyShape

Do not model full JSON policy parsing in the first version. Model the evaluated
policy class instead:

- `NoPolicy`
- `NoMatch`
- `ExplicitAllowPrivate`
- `ExplicitAllowPublic`
- `ExplicitDeny`

The harness can materialize these into simple bucket policies using the current
helper patterns already present in coordinator tests.

## Harness Design

### 1. Fixed real identities

Create a small stable identity fixture set:

- bucket-owner root principal
- bucket-owner non-root admin principal
- same-account non-owner principal
- cross-account principal
- anonymous requester

Use fixed principal strings and canonical IDs so failures print readable
scenarios.

### 2. Stable materialization helpers

Build narrow adapters that reuse the existing test helpers for:

- bucket creation
- ownership controls
- public-access-block
- bucket policy
- object creation
- object ACL updates
- object tagging

Do not reimplement those setup flows ad hoc inside each matrix test.

### 3. Operation runner

Add one runner per modeled action that returns a normalized result:

```rust
enum ClassifiedResult {
    Allow,
    AccessDenied,
    NoSuchKey,
    VersionNotFound,
}
```

Then map this into the model outcome space:

- existing object success -> `Allow`
- missing object visible -> `RevealMissing`
- missing object hidden -> `HideMissing`
- unauthorized existing object -> `Deny`

### 4. Impossible-scenario filtering

Exhaustive enumeration is only practical if obviously invalid combinations are
filtered before execution.

Examples to filter:

- anonymous requester with non-anonymous canonical identity
- `is_root_principal = true` with cross-account or anonymous requester
- requester-specific ACL grant shapes when the requester is anonymous
- object owner marked `AnonymousUpload` for operations not using the anonymous
  upload owner path

Filtering rules should live in one helper so the matrix boundary is reviewable.

## Phase Plan

### Phase 1: Scaffolding and Existing-Object Read Core

Status: completed

Implemented:

- `crates/server-core/src/coordinator/authz_model_tests.rs`
- pure phase-1 authz model for existing-object `GetObject` and
  `GetObjectAttributes`
- shared-coordinator harness with per-scenario bucket isolation for fast
  exhaustive matrix execution
- explicit coverage for:
  - BOE root versus non-root bucket-owner principal behavior
  - same-account canonical-match versus distinct-principal behavior
  - split `GetObject` versus `GetObjectAttributes` bucket-policy gates
  - `RestrictPublicBuckets` interactions on public allows

Deliver:

- new `authz_model_tests.rs`
- `Scenario` and the initial model enums
- scenario formatter for failure output
- real identity fixture builder
- materialization for:
  - bucket ownership controls
  - bucket public-access-block
  - narrow bucket policy variants
  - object owner identity
  - a minimal ACL shape
- runners for:
  - `GetObject`
  - `GetObjectAttributes`

Matrix focus:

- same-account `Standard` versus `OwnerAccountAdmin`
- BOE versus non-BOE
- object owner principal versus bucket-owner principal
- no-policy versus explicit allow/deny

Acceptance criteria:

- local matrix matches the current coordinator behavior
- matrix agrees with the already-established regressions around BOE read and
  BOE object-attributes behavior

### Phase 2: Existing-Object ACL and Tagging Surface

Status: completed

Implemented:

- extended `crates/server-core/src/coordinator/authz_model_tests.rs` to cover:
  - `GetObjectAcl`
  - `GetObjectTagging`
  - `PutObjectTagging`
  - `DeleteObjectTagging`
- reused the existing-object shared-coordinator harness and scenario substrate
  from phase 1
- added an explicit non-owner `ReadAcp` ACL-grant shape for `GetObjectAcl`
  outside BOE so requester-role versus object-owner-role mismatches are modeled
- encoded BOE tagging as bucket-owner-account-admin authorization rather than
  inferring it from read-object behavior

Expand the matrix to:

- `GetObjectAcl`
- `GetObjectTagging`
- `PutObjectTagging`
- `DeleteObjectTagging`

Important rules to encode explicitly:

- BOE `GetObject` and BOE `GetObjectAttributes` are not the same contract
- BOE tagging remains bucket-owner-account-admin only
- explicit ACL grants can remain relevant outside BOE

Acceptance criteria:

- tagging and ACL operations share the same scenario materialization substrate
- the matrix catches mismatches in requester role versus object-owner role
- the known BOE tagging/admin semantics are encoded, not inferred from helpers

### Phase 3: Missing-Object Discovery Matrix

Status: completed

Implemented:

- extended `crates/server-core/src/coordinator/authz_model_tests.rs` with a
  separate missing-object matrix covering:
  - `GetObject`
  - `GetObjectAttributes`
  - `GetObjectAcl`
  - `GetObjectTagging`
  - `PutObjectTagging`
  - `DeleteObjectTagging`
- added explicit missing-target modeling for both:
  - missing current keys
  - missing version IDs on otherwise-existing keys
- modeled missing discovery outcomes directly as:
  - `RevealMissing`
  - `HideMissing`
- added harness classification for:
  - `NoSuchKey`
  - `VersionNotFound`
  - `AccessDenied`
- encoded the action-specific discovery rules rather than inferring them from
  existing-object authorization:
  - `GetObject` discovery via bucket read and `ListBucket`
  - `GetObjectAttributes` discovery via the conjunction of read action,
    attributes action, and `ListBucket`
  - `GetObjectAcl` discovery via bucket-admin semantics
  - object-tagging discovery via bucket-owner-account-admin semantics
- added missing-only bucket-shape coverage for public bucket read and
  `IgnorePublicAcls`, without expanding the existing-object phase-1/2 matrix
- kept the phase-1/2 shared-coordinator approach and materialized missing
  version cases by creating an existing object and probing a non-existent
  version ID

Add missing-object and missing-version cases for the phase-1/2 actions.

Model outputs:

- `RevealMissing`
- `HideMissing`

Harness classification:

- `NoSuchKey`
- `VersionNotFound`
- `AccessDenied`

Important rules to encode:

- missing-object discovery is action-specific
- BOE discovery is not equivalent to existing-object read authorization
- ACL discovery and attribute discovery differ

Acceptance criteria:

- current local coordinator behavior is fully specified for missing-object
  masking on the modeled actions
- the scenario output makes it obvious which discovery rule regressed

### Phase 4: Write-Authorization Matrix

Status: completed

Implemented:

- extended `crates/server-core/src/coordinator/authz_model_tests.rs` with a
  separate phase-4 write model and harness covering:
  - `PutObject`
  - `CreateMultipartUpload`
  - `BeginStreamPut`
  - `PutObjectAcl`
  - `PutObjectVersionAcl`
- split phase 4 into two bounded matrices instead of forcing write entrypoints
  and ACL updates through one oversized scenario type:
  - a creation-path write matrix for `PutObject`, multipart creation, and
    streaming session creation
  - an existing-object ACL-update matrix for current and versioned
    `PutObjectAcl`
- kept the write-entry matrix focused on the authz-relevant creation contract:
  - BOE-allowed versus BOE-rejected ACL write shapes
  - `BlockPublicAcls` rejection of public canned ACLs and explicit grants
  - `IgnorePublicAcls` suppression of bucket public-write fallback without
    turning into `BlockPublicAcls`
  - `RestrictPublicBuckets` falling back to the non-policy path when a public
    allow does not survive
  - the current anonymous `CreateMultipartUpload` carveout as distinct from
    `PutObject` and `BeginStreamPut`
- added a dedicated ACL-update matrix that models:
  - current versus versioned `PutObjectAcl` policy actions
  - owner/fallback authorization versus requester-specific `WriteAcp` grants
  - same-account owner-account-admin requesters for both exact-owner and
    canonical-owner fallback paths
  - BOE `AccessControlListNotSupported` ordering after authorization succeeds
  - `BlockPublicAcls` on ACL updates
  - `PutObjectAcl` policy-context exactness and conflicting-header rejection as
    explicit `InvalidArgument` outcomes
- materialized the ACL-policy regressions through narrow policy snippets rather
  than reusing the existing one-off tests only:
  - unconditional private/public allows
  - public-allow plus `RestrictPublicBuckets`
  - public-write fallback staying distinct from `BlockPublicAcls` on private
    ACL requests
  - allow-plus-conditional-deny on `s3:x-amz-acl`
  - allow conditioned on exact `s3:x-amz-grant-read`

Add a narrower write-focused matrix for:

- `PutObject`
- `CreateMultipartUpload`
- `BeginStreamPut`
- `PutObjectAcl`
- `PutObjectVersionAcl`

This phase should focus on the authz-relevant contract, not payload handling.

Important rules to encode:

- BOE allowed versus rejected ACL shapes
- `BlockPublicAcls` handling for canned ACLs and explicit grants
- `IgnorePublicAcls` is distinct from `BlockPublicAcls`
- `RestrictPublicBuckets` only constrains public-policy allows
- `PutObjectAcl` policy-context exactness remains enforced

For policy materialization in this phase, continue using narrow hand-authored
JSON snippets rather than trying to model the full policy language.

Acceptance criteria:

- the suite covers the recent `PutObjectAcl` condition regressions through the
  modeled harness, not only dedicated one-off tests
- stream and multipart entrypoints share the same modeled write-decision
  substrate where their authz rules are supposed to match

### Phase 5: Transition Semantics

Status: completed

Implemented:

- extended `crates/server-core/src/coordinator/authz_model_tests.rs` with a
  deterministic phase-5 transition model and harness for authz state changes on
  an unchanged object
- encoded bounded step traces instead of a randomized state machine so each
  transition failure reports the exact mutation/probe sequence that regressed
- covered legacy ACL suppression and restoration across BOE transitions for:
  - anonymous `public-read` object ACL access
  - explicit cross-account grantee `READ` access
- covered `IgnorePublicAcls` toggles after object creation, including restoring
  the legacy public-read path once the public-access-block config is removed
- covered bucket-policy transition behavior on an existing private object for:
  - allow -> deny replacement
  - deny -> no policy removal
  - policy-based `GetObject` access surviving BOE enable and later BOE removal
- kept the policy transition trace focused on same-coordinator authz semantics;
  the separate cross-coordinator bucket-policy cache invalidation test remains
  the place that pins replacement invalidation behavior directly

Once the static matrices are stable, add bounded transition scenarios for:

- object created under legacy ACL behavior, then BOE enabled
- BOE removed after prior legacy ACL state
- public-access-block toggles after object creation
- bucket policy replacement / removal

Keep the first version deterministic and explicit rather than randomized.

This phase exists to lock down the transition behavior already represented in
tests such as the BOE restore/read semantics in `ownership.rs`.

Acceptance criteria:

- legacy ACL suppression and restoration across BOE transitions are encoded in
  one place
- transition failures produce a short trace, not only a final-state mismatch

### Phase 6: Copy and Multipart Copy Matrix

Status: complete

Add a bounded copy-focused matrix for:

- `CopyObject`
- `UploadPartCopy`

The first pass should hold the source side narrow and stable:

- materialize a readable fixed source object
- vary the destination-side authorization and request context
- vary the multipart-upload target state for `UploadPartCopy`
- keep broader source-read differentials for a later expansion if needed

Important rules to encode:

- `x-amz-tagging-directive=REPLACE` plus inline tags requires
  `s3:PutObjectTagging`
- destination canned ACLs and explicit grant headers are evaluated using the
  copy request context rather than being inferred from `PutObject`
- `RestrictPublicBuckets`, BOE, and public-access-block still apply to the
  destination-side write decision
- exact copy condition keys remain distinct:
  - `s3:x-amz-acl`
  - `s3:x-amz-grant-read`
  - `s3:x-amz-grant-read-acp`
  - `s3:x-amz-grant-write`
  - `s3:x-amz-grant-write-acp`
- `UploadPartCopy` destination authorization is evaluated against the target
  multipart upload state and write context, not as a generic object-copy write
- copy-to-object and copy-to-upload request contexts must stay distinct even
  when they share the same readable source object

Acceptance criteria:

- recent AWS-backed `CopyObject` and `UploadPartCopy` authz regressions are
  covered by the modeled harness, not only by `s3-tests`
- copy-specific request-context failures print the exact scenario fields that
  diverged

Current state:

- `server-core` now has a dedicated Phase 6 authz model matrix for
  `CopyObject` and `UploadPartCopy`.
- The `CopyObject` matrix covers:
  - copy-source policy conditions
  - explicit `COPY` metadata-directive context
  - tagging replacement needing `PutObjectTagging`
  - exact copy request-context handling for:
    - `s3:x-amz-acl`
    - `s3:x-amz-grant-read`
    - `s3:x-amz-grant-read-acp`
    - `s3:x-amz-grant-write`
    - `s3:x-amz-grant-write-acp`
  - destination BOE / BlockPublicAcls / RestrictPublicBuckets behavior
- The `UploadPartCopy` matrix covers:
  - readable versus unreadable source objects
  - in-progress versus completed multipart targets
  - requester-owned versus bucket-owner-owned uploads
  - copy-source-conditioned destination policy
  - the deliberate request-context distinction from `CopyObject`
    (`UploadPartCopy` does not satisfy metadata-directive conditions)

### Phase 7: Delete and Object-Lock Matrix

Status: complete

Add a bounded delete/object-lock matrix for:

- `PutObjectRetention`
- `GetObjectRetention`
- `PutObjectLegalHold`
- `GetObjectLegalHold`
- `DeleteObject`
- `DeleteObjectVersion`

Cover:

- object-lock read/update authorization separately from delete authorization
- current-object versus version-specific delete authorization
- missing-key versus missing-version targets where the authz result still
  matters
- object-lock enabled versus disabled buckets
- bypass header absent versus present
- same-account `Standard` versus `OwnerAccountAdmin` requesters on
  object-lock-enabled buckets
- narrow bucket-policy grants for:
  - `s3:PutObjectRetention`
  - `s3:GetObjectRetention`
  - `s3:PutObjectLegalHold`
  - `s3:GetObjectLegalHold`
  - `s3:DeleteObject`
  - `s3:DeleteObjectVersion`
  - `s3:BypassGovernanceRetention`

Important rules to encode:

- same-account `Standard` requesters must not inherit implicit owner-account
  object-lock management or governance-bypass rights that belong only to
  `OwnerAccountAdmin`
- same-account `OwnerAccountAdmin` requesters can exercise the implicit
  owner-account object-lock and governance-bypass paths, subject to explicit
  bucket-policy deny
- object-lock read/update APIs and delete-with-bypass must not be collapsed
  into one generic "object-lock allowed" rule
- version-specific delete of a missing version with the bypass header on an
  object-lock-enabled bucket requires `s3:BypassGovernanceRetention`
- non-version delete with the bypass header does not require bypass permission
  purely because the header is present
- missing-version and existing-version versioned deletes should share the same
  modeled permission structure where AWS does

Acceptance criteria:

- same-account constrained-versus-admin governance-bypass behavior is encoded
  in the model, not left only to targeted regressions or AWS-backed tests
- the missing-version bypass-governance rule is encoded in the model, not only
  in dedicated regressions
- non-version and version-specific delete behavior cannot be accidentally
  collapsed together

Current state:

- `server-core` now has a dedicated Phase 7 authz model matrix for object-lock
  read/update APIs and delete authorization.
- The object-lock matrix covers:
  - same-account `Standard` versus `OwnerAccountAdmin` behavior on
    object-lock-enabled buckets
  - narrow cross-account bucket-policy allow for:
    - `s3:GetObjectRetention`
    - `s3:PutObjectRetention`
    - `s3:GetObjectLegalHold`
    - `s3:PutObjectLegalHold`
  - `s3:BypassGovernanceRetention` as a separate requirement for governance
    retention updates that request bypass
  - explicit bucket-policy deny of bypass for same-account owner-account admin
  - plain-bucket `InvalidRequest` for authorized callers versus `AccessDenied`
    for unauthorized callers, so lock configuration is not leaked
- The delete matrix covers:
  - current-object delete-marker insertion versus version-specific delete
  - current-missing versus missing-version authorization
  - object-lock-enabled versus object-lock-disabled buckets on the
    version-specific delete path
  - same-account `Standard` versus `OwnerAccountAdmin` governance-bypass
    behavior
  - cross-account bucket-policy allow for:
    - `s3:DeleteObject`
    - `s3:DeleteObjectVersion`
    - `s3:BypassGovernanceRetention`
  - the missing-version rule that `DeleteObjectVersion` alone is insufficient
    when the bypass header is present on an object-lock-enabled bucket
  - the non-version delete path treating the bypass header as irrelevant to the
    delete-marker authorization decision
  - compliance-retention and legal-hold delete denial staying distinct from
    governance-bypass authorization

### Phase 7A: Object-Scoped Multipart Management Matrix

Status: complete

Add a bounded multipart-management matrix for:

- `BeginStreamPart`
- `CompleteMultipartUpload`
- `AbortMultipartUpload`
- `ListParts`

Cover:

- initiator versus upload owner versus bucket-owner exact principal
- same-account `Standard` versus `OwnerAccountAdmin`
- in-progress versus completed upload records where the API distinguishes them
- narrow bucket-policy allow on multipart write paths that reuse `PutObject`
  semantics
- absence of a bucket-policy fallback on management-only paths unless an
  AWS-backed baseline later proves otherwise

Important rules to encode:

- `BeginStreamPart` and `CompleteMultipartUpload` reuse multipart write
  authorization with the upload's stored context, rather than generic
  bucket-admin-only management
- `AbortMultipartUpload` and `ListParts` are initiator/owner/admin management
  paths and must not accidentally inherit `PutObject` bucket-policy allows
- completed-upload abort follows `requester_can_manage_completed_multipart_upload`
  rather than the in-progress upload management path

Acceptance criteria:

- object-scoped multipart authorization no longer lives only in one-off tests
- the distinction between multipart write paths and multipart management paths
  is encoded explicitly and cannot collapse accidentally

Current state:

- `server-core` now has a dedicated Phase 7A authz model matrix for
  object-scoped multipart authorization.
- The multipart write matrix covers:
  - `BeginStreamPart` and `CompleteMultipartUpload`
  - initiator/upload-owner exact callers on private buckets with and without
    explicit `s3:PutObject` bucket-policy allow
  - bucket-owner exact principal behavior on both object-writer and
    bucket-owner-enforced upload ownership shapes, including the object-writer
    case without any explicit `s3:PutObject` policy allow
  - same-account `Standard` versus `OwnerAccountAdmin` behavior
  - the completed-upload `NoSuchUpload` behavior on write paths that only
    operate on in-progress uploads
  - the current shipped rule that explicit `s3:PutObject` bucket-policy allow
    does permit non-initiator write-path continuation
- The multipart management matrix covers:
  - `AbortMultipartUpload` on both in-progress and completed upload records
  - `ListParts` on in-progress uploads
  - initiator versus upload owner versus bucket-owner exact principal
  - same-account `Standard` versus `OwnerAccountAdmin`
  - object-writer versus bucket-owner-enforced ownership shapes
  - the absence of `s3:PutObject` bucket-policy fallback on management-only
    paths, including completed-upload abort

### Phase 8: Request-Context Exactness Matrix

Status: complete

Add a narrow request-context matrix shared across the write/copy surfaces for:

- `PutObject`
- `CreateMultipartUpload`
- `CopyObject`
- `PutObjectAcl`
- `PutObjectVersionAcl`

This phase should focus on condition-key exactness rather than the full write
state-space.

Important rules to encode:

- absent versus present `x-amz-acl` must remain observable to policy conditions
  such as `Null` and `StringNotEquals`
- each `x-amz-grant-*` header must remain independently modeled rather than
  collapsed into a generic “explicit grants present” flag
- copy tagging replacement plus inline tags is distinct from plain copy and
  from `PutObject`
- do not collapse “header omitted” and “default private behavior” into one
  modeled state

Acceptance criteria:

- condition-key exactness regressions fail the modeled harness before they are
  rediscovered only through AWS-backed tests
- the recent absent/present `x-amz-acl` and explicit `x-amz-grant-*` baselines
  have corresponding model cases

Current state:

- Added a bounded Phase 8 model harness in
  `crates/server-core/src/coordinator/authz_model_tests.rs` covering
  `PutObject`, `CreateMultipartUpload`, `CopyObject`, `PutObjectAcl`, and
  `PutObjectVersionAcl`.
- The local matrix now pins absent versus explicit `x-amz-acl=private` for all
  five request surfaces, so policy-visible ACL headers are no longer allowed to
  collapse into “default private behavior”.
- The local matrix also keeps the `x-amz-grant-full-control`,
  `x-amz-grant-read-acp`, `x-amz-grant-write`, and
  `x-amz-grant-write-acp` headers distinct on `PutObject`, instead of treating
  them as a generic “explicit grants present” state.
- `CopyObject` tagging replacement is now modeled separately from plain copy,
  and the model pins the existing rule that request object tags require the
  tagging policy half as well as `s3:PutObject`.
- The implementation under test was tightened so `PutObject`,
  `CreateMultipartUpload`, and `CopyObject` no longer synthesize policy-visible
  ACL headers from structured ACL input when those headers were absent on the
  request; the authz path now uses only the explicit request context after
  validating it against the parsed ACL input.
- AWS-facing coverage in `crates/s3-tests/tests/bucket_policy.rs` now also
  includes the missing versioned ACL absent/present cases for `Null` and
  `StringNotEquals` on `s3:x-amz-acl`, in addition to the already-existing
  current-object ACL and multipart/upload-side exactness coverage.

### Phase 9: Bucket Action Matrix

Status: complete

Add a bounded bucket-action matrix for:

- `GetBucketAcl`
- `PutBucketAcl`
- `GetBucketVersioning`
- `PutBucketVersioning`
- `ListBucketVersions`
- `ListBucketMultipartUploads`

Important rules to encode:

- dedicated bucket-policy actions can grant cross-account access for these
  APIs where AWS allows
- `GetBucketAcl` and `PutBucketAcl` preserve the existing owner/bucket-admin/ACL
  fallback behavior, with dedicated policy allow layered on top
- `ListBucketVersions` and `ListBucketMultipartUploads` keep their existing
  bucket-read/public-read fallback; the dedicated policy action is additive,
  not a replacement
- explicit deny on the dedicated action must still override the fallback path

Acceptance criteria:

- the recent AWS-backed bucket ACL/versioning/listing regressions are covered
  by the modeled harness, not only by `s3-tests`
- the additive fallback behavior for versions and multipart listings is encoded
  explicitly rather than being left as accidental implementation detail

Current state:

- Added a bounded Phase 9 bucket-action model matrix in
  `crates/server-core/src/coordinator/authz_model_tests.rs` covering
  `GetBucketAcl`, `PutBucketAcl`, `GetBucketVersioning`,
  `PutBucketVersioning`, `ListBucketVersions`, and
  `ListBucketMultipartUploads`.
- The matrix now pins the dedicated bucket-policy allow path for all six
  actions, so the recent AWS-backed `s3-tests` coverage is no longer the only
  place that exercises those cross-account grants.
- `GetBucketAcl` and `PutBucketAcl` now have explicit modeled fallback cases
  instead of relying on scattered coordinator regressions:
  exact-owner fallback, `READ_ACP`/`WRITE_ACP` grant fallback, the
  bucket-owner-enforced owner-account-admin read fallback, dedicated policy
  allow, and deny-overrides-fallback.
- The ACL slice also records the narrower-than-expected `PutBucketAcl` rule:
  same-account owner-account-admin does not gain a write fallback on a standard
  bucket without exact-owner or `WRITE_ACP`, while `GetBucketAcl` on a
  bucket-owner-enforced bucket still uses the owner-account-admin path.
- `GetBucketVersioning` and `PutBucketVersioning` now explicitly model their
  owner-account-admin fallback plus dedicated policy allow for cross-account
  callers.
- `ListBucketVersions` and `ListBucketMultipartUploads` now explicitly model
  their additive bucket-read/public-read fallback, dedicated policy allow, and
  the rule that explicit deny on the dedicated action still overrides the read
  fallback.

### Phase 10: Bucket ACL Request-Context Exactness

Status: planned

Add a narrow request-context matrix for:

- `PutBucketAcl`

Cover:

- canned ACL requests
- grant-header requests
- XML-body ACL requests

Important rules to encode:

- canned ACL requests expose only `s3:x-amz-acl`
- grant-header requests expose only the original `s3:x-amz-grant-*` headers
- XML-body ACLs do not synthesize grant headers into bucket-policy context
- absent versus present `x-amz-acl` must remain observable to `Null` and
  `StringNotEquals` bucket-policy conditions
- each `x-amz-grant-*` header remains independently modeled rather than
  collapsing to a generic “grants present” flag

Acceptance criteria:

- the recent `PutBucketAcl` conditioned-policy regressions are covered by the
  modeled harness
- request provenance mismatches fail locally with a concrete scenario diff

### Phase 11: Bucket Meta and Discovery Slice

Status: planned

Add a small bounded slice for:

- `HeadBucket`
- `GetBucketLocation`

This slice should stay intentionally small and explicit; it exists to pin
non-obvious bucket-policy exceptions rather than to turn bucket metadata into a
large matrix.

Important rules to encode:

- `ListBucket` bucket-policy allow is not sufficient for `HeadBucket`
- `ListBucket` bucket-policy allow is not sufficient for `GetBucketLocation`
- dedicated `GetBucketLocation` bucket-policy allow is sufficient for
  cross-account `GetBucketLocation` under the current AWS-backed baseline

Acceptance criteria:

- the `HeadBucket` and `GetBucketLocation` bucket-policy sufficiency and
  non-sufficiency cases are encoded in the same framework rather than only in
  standalone `s3-tests`
- failures print the exact negative scenario that regressed

### Phase 12: Optional Stateful Expansion

After phases 1-11 are solid, consider a bounded stateful generator using
`proptest` for short authz traces.

Possible generated operations:

- set ownership controls
- set public-access-block
- set or delete narrow bucket policy
- put object with a constrained ACL shape
- copy object with constrained destination headers
- change object ACL
- update bucket ACL with constrained request context
- change bucket versioning state
- delete object or object version with bounded object-lock state
- read or mutate object tags

This is explicitly follow-up work. Do not start here.

## Model Rules That Must Be Explicit

The first implementation should write these rules down in code comments next to
the model itself.

### 1. BOE `GetObject` differs from BOE `GetObjectAttributes`

Current behavior intentionally differs:

- `GetObject` aligns to bucket-owner-account-admin behavior under BOE
- `GetObjectAttributes` follows object-owner principal semantics under BOE

This distinction must be encoded directly in the model and covered in the first
matrix. Do not simplify them into one generic “read” rule.

### 2. `IgnorePublicAcls` differs from `BlockPublicAcls`

The model must preserve the AWS-compatible split:

- `IgnorePublicAcls` suppresses the anonymous `AllUsers` path
- `BlockPublicAcls` rejects writing public ACL shapes, including
  `AuthenticatedUsers` exposure where AWS does

These controls cannot be modeled as one generic `public_acls_disabled` flag.

### 3. `RestrictPublicBuckets` depends on public-policy status

The decision is not just “policy allow or deny”. It depends on whether the
policy is considered public.

That is why the initial model uses:

- `ExplicitAllowPrivate`
- `ExplicitAllowPublic`

rather than a single `ExplicitAllow`.

### 4. Owner matching is principal-first, not canonical-first

For ordinary object-owner matching, principal equality and canonical-ID equality
are not interchangeable. The model must keep these separate and only let
canonical-ID matching carry owner-account-admin meaning where the implementation
does.

## Concrete Implementation Steps

### Step 1: Add the new test module

Create:

- `crates/server-core/src/coordinator/authz_model_tests.rs`

And wire it into the coordinator module.

### Step 2: Build the pure model

Implement:

- scenario structs and enums
- impossible-scenario filter
- `expected_existing_outcome(&Scenario) -> Outcome`
- `expected_missing_outcome(&Scenario) -> Outcome`

Keep these functions small and table-like.

### Step 3: Build the harness

Implement:

- fixed identity fixtures
- bucket/object materialization
- operation runners
- result classifier
- scenario formatter for assertion failures

### Step 4: Add the first exhaustive matrices

Add deterministic exhaustive tests for:

- BOE object reads
- BOE object attributes
- same-account admin versus constrained behavior
- narrow bucket-policy allow/deny combinations

### Step 5: Expand action coverage

Once the first matrices are stable, add:

- ACL surface
- tagging surface
- missing-object discovery surface

### Step 6: Add write, copy, delete, bucket-action, and transition slices

Only after the read/discovery surface is stable, add:

- write auth matrix
- bounded `CopyObject` and `UploadPartCopy` matrix
- delete/object-lock matrix
- object-scoped multipart-management matrix
- bucket-action matrix
- BOE/public-access-block/policy transition traces

### Step 7: Add request-context exactness matrices

After the base write/copy/delete/bucket-action slices are stable, add bounded matrices for:

- absent versus present canned ACL headers
- exact `x-amz-grant-*` header matching
- tagging replacement plus inline-tag request context
- `PutBucketAcl` provenance-sensitive canned ACL versus grant-header versus XML context

### Step 8: Add bounded bucket-meta exception slices

Once the bucket-action matrix is stable, add small explicit negative slices for:

- `HeadBucket` bucket-policy non-sufficiency
- `GetBucketLocation` bucket-policy non-sufficiency

## Validation

For each phase:

- `cargo fmt`
- `cargo clippy --all-targets --all-features -- -D warnings`
- targeted `cargo test -p server-core ...`

Suggested targeted test runs while developing:

- `cargo test -p server-core authz_model_ -- --nocapture`
- `cargo test -p server-core bucket_owner_enforced_ -- --nocapture`
- `cargo test -p server-core restrict_public_buckets_ -- --nocapture`
- `cargo test -p server-core put_object_acl_ -- --nocapture`

AWS-backed follow-up runs when a modeled rule is unclear or changes:

- `cargo test -p s3-tests --test boe_constrained -- --nocapture`
- `cargo test -p s3-tests --test boe_admin_root -- --nocapture`
- `cargo test -p s3-tests --test ownership -- --nocapture`
- `cargo test -p s3-tests --test public_access_block -- --nocapture`
- `cargo test -p s3-tests --test object_crud -- --nocapture`
- `cargo test -p s3-tests --test multipart -- --nocapture`
- `cargo test -p s3-tests --test bucket_policy -- --nocapture`
- `cargo test -p s3-tests --test bucket_policy_root -- --nocapture`
- `cargo test -p s3-tests --test object_lock -- --nocapture`

## Risks

### 1. Encoding the current implementation rather than the intended contract

Mitigation:

- keep the model pure and independent
- compare against public coordinator APIs
- use existing AWS-backed tests as anchors

### 2. Collapsing principal and canonical identity

Mitigation:

- model them as separate fields from the start
- keep dedicated same-canonical / same-principal scenarios

### 3. Combinatorial explosion

Mitigation:

- start with a narrow action set
- filter impossible combinations centrally
- split matrices by concern instead of one universal mega-test

### 4. Over-modeling bucket policy too early

Mitigation:

- keep the initial policy model abstract and narrow
- materialize only the simple policy forms already used in current tests

## Success Criteria

This plan is complete when:

1. `server-core` has a dedicated authz model test module with a pure executable
   spec and a coordinator-backed harness
2. existing-object read, ACL, tagging, and missing-object discovery behavior are
   covered by matrix-style authz differentials
3. BOE, public-access-block, and narrow public-policy interactions are encoded
   in the model rather than only as isolated regressions
4. selected write-auth, bucket ACL/versioning/listing actions, copy,
   delete/object-lock, object-scoped multipart authorization, and
   BOE/public-access transitions are covered by the same framework
5. narrow request-context exactness for key write condition keys and
   `PutBucketAcl` request provenance is modeled rather than pinned only by
   isolated regressions
6. bucket-meta policy exceptions such as `HeadBucket` and
   `GetBucketLocation` are encoded rather than left only to standalone
   AWS-backed tests
7. future authz regressions fail with a small scenario diff rather than only an
   AWS-backed integration failure
