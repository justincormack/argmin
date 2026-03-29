# Object And Bucket ACLs

## Scope

This plan covers completion of the S3 ACL surface after the ownership and
multi-user foundations are in place.

In scope:
- bucket ACLs beyond the current boolean public flags
- object ACLs, including per-version ACLs
- canned ACLs on PUT, POST, COPY, multipart initiation, and presigned PUT
- header grant parsing via `x-amz-grant-*`
- `GetObjectAcl` and `PutObjectAcl`
- interaction with bucket ownership controls
- interaction with bucket-level public access block for ACL-related behavior

Out of scope:
- bucket policy evaluation
- IAM policy language
- account-level public access block

Dependency:
- this plan assumed `plans/completed/ownership-and-multi-user-foundations.md`
  had landed first

## Current State

The current implementation now supports a substantial ACL subset:
- bucket ACLs are stored as structured grants and rendered from stored ACL state
- `CreateBucket`, `GetBucketAcl`, and `PutBucketAcl` accept structured ACL
  inputs, including header grants
- object ACLs are stored durably per version
- `GetObjectAcl` and `PutObjectAcl` are implemented, including version-aware
  reads and writes
- direct `PutObject` supports canned ACLs and `x-amz-grant-*` headers on both
  small-body and promoted streaming paths
- CopyObject canned destination ACLs, multipart initiation canned ACL
  persistence, and presigned PUT ACL handling are implemented
- bucket-level public access block now rejects public object ACL attempts under
  `BlockPublicAcls` and suppresses public object ACL effects under
  `IgnorePublicAcls`
- unsupported states such as object `WRITE` grants and `AuthenticatedUsers`
  enforcement are rejected rather than silently persisted

## Current Status

Phases 1 through 5 are complete for the scoped ACL behavior in this plan. The
object and bucket ACL surface covered here is implemented and exercised by live
integration tests.

Remaining follow-up work is outside this plan:
- bucket-policy evaluation belongs to `plans/completed/bucket-policies.md`
- account-level public access block remains out of scope
- `AuthenticatedUsers` parsing and serialization exist, but those grants are
  still rejected on write because enforcement semantics remain a separate
  follow-up

## Goals

Implement a maintainable ACL model that matches AWS semantics for the covered
feature set rather than continuing to special-case public and private behavior.

The resulting design should make these states explicit:
- owner grants
- canonical-user grants
- group grants
- per-version object ACLs
- ownership-controls restrictions
- public-access-block overrides of public ACL effects

## Target Behavior

### Bucket ACLs

Bucket ACLs should become a durable structured document rather than a pair of
booleans.

Requirements:
- preserve current public-read and public-read-write behavior
- support header grants and XML ACL documents
- render `GetBucketAcl` from stored grants, not a synthetic reconstruction
- derive fast-path public flags from the structured ACL where useful, but do not
  treat those flags as the source of truth

### Object ACLs

Object ACLs must be first-class and version-specific.

Requirements:
- each committed live object version stores its own ACL
- each version remains readable through `GetObjectAcl` even after newer versions
  exist
- requests without `versionId` operate on the current version's ACL, matching
  AWS semantics

### Canned ACLs And Header Grants

Support the request forms AWS uses for the targeted test set:
- `x-amz-acl`
- `acl` form field for POST object
- `x-amz-grant-read`
- `x-amz-grant-write`
- `x-amz-grant-read-acp`
- `x-amz-grant-write-acp`
- `x-amz-grant-full-control`

Coverage requirements:
- direct PUT object
- POST object / form upload ACL handling
- CopyObject destination ACL
- multipart initiation where ACL is part of the upload state
- presigned PUT where ACL headers are signed and enforced

### Ownership Controls

Ownership-controls behavior must remain correct:
- `BucketOwnerEnforced` continues to reject ACL usage where AWS rejects it
- `BucketOwnerPreferred` and `ObjectWriter` determine owner and ACL defaults
  coherently
- ACL-focused compatibility tests must explicitly create or convert buckets to
  `ObjectWriter` or `BucketOwnerPreferred`, because fresh AWS buckets default to
  `BucketOwnerEnforced`
- `bucket-owner-full-control` must work across direct PUT, POST object, COPY,
  and presigned PUT

### Public Access Block Interaction

The bucket-level public access block already parses:
- `BlockPublicAcls`
- `IgnorePublicAcls`

ACL behavior must respect those settings for both bucket and object ACLs:
- `BlockPublicAcls` rejects new public ACL attempts
- `IgnorePublicAcls` keeps stored ACLs but removes their public effect during
  authorization

## Design Changes

### 1. Internal ACL Model

Introduce typed ACL structures in core and storage:
- owner identity
- grants
- grantee type
- permission set

Recommended grantee support:
- canonical user
- `AllUsers`
- `AuthenticatedUsers`

Avoid representing ACL state as free-form XML or ad hoc booleans in core logic.
XML should remain a boundary format, not the internal source of truth.

### 2. Storage

Bucket metadata:
- replace boolean-only ACL storage with a durable structured ACL column or
  normalized ACL tables
- keep derived public-read/public-write fast-path projections only if they are
  maintained from the structured ACL deterministically

Object metadata:
- add object ACL storage keyed by `(bucket, key, version_id)`
- ensure writes, copies, and version deletes cannot orphan ACL state

Multipart upload state:
- persist the ACL requested at initiation so complete and abort flows retain the
  correct semantics

### 3. HTTP Surface

Add or complete:
- `GET ?acl` and `PUT ?acl` for object paths
- ACL XML parsing and rendering
- `x-amz-grant-*` parsing
- canned ACL parsing for bucket and object APIs

The HTTP layer should normalize request forms into typed ACL structures before
handing them to `server-core`.

### 4. Authorization Engine

Extend authorization to evaluate ACL permissions explicitly:
- object read
- object write
- read ACP
- write ACP
- bucket read/write where bucket ACLs apply

This should be separate from bucket-policy evaluation so later policy work can
compose cleanly on top.

### 5. Copy And Presigned Paths

ACL handling must be consistent across:
- direct PUT
- CopyObject
- UploadPartCopy where relevant state is inherited or rejected
- presigned PUT with signed ACL headers

Do not leave copy and presigned ACL behavior as separate special cases.

## Implementation Phases

### Phase 1: Typed ACL Representation

Status: complete.

Deliver:
- internal ACL types
- XML and header normalization into typed ACLs
- permission and grantee enums that make illegal states hard to represent

Success criteria:
- bucket ACL paths can operate on the typed model without changing external
  behavior yet

### Phase 2: Bucket ACL Completion

Status: complete.

Deliver:
- create-bucket ACL normalization onto the structured bucket ACL model
- deterministic projection of current public flags from stored bucket ACL
- `CreateBucket`, `GetBucketAcl`, and `PutBucketAcl` powered by the structured ACL
- explicit ACL rejection under `BucketOwnerEnforced` where AWS rejects it

Success criteria:
- current bucket ACL tests continue to pass
- header grant bucket ACL test can be unignored

### Phase 3: Direct PutObject ACL Header Grants

Status: complete.

Deliver:
- direct `PutObject` `x-amz-grant-*` normalization
- grant persistence on initial object writes for both small-body and streaming PUT paths
- AWS-compatible rejection of mixed canned and grant ACL inputs
- object-grant validation aligned with current enforcement rules

Success criteria:
- object header ACL grant test can be unignored
- existing object ACL and versioned object ACL coverage continues to pass

### Phase 4: Canned ACLs On Write Paths

Status: complete.

Deliver:
- end-to-end integration coverage for multipart initiation ACL persistence
- AWS validation for the CopyObject, multipart, and presigned canned ACL paths

Success criteria:
- copy canned ACL, multipart canned ACL, and presigned PUT ACL tests pass

### Phase 5: Public Access Block Integration

Status: complete.

Deliver:
- public object ACL attempts rejected under `BlockPublicAcls`
- public object ACL effects suppressed under `IgnorePublicAcls`

Success criteria:
- object public-access-block ACL tests pass without `--ignored`

## Test Plan

Targeted integration tests:
- `cargo test -p s3-tests --test bucket_crud test_bucket_header_acl_grants`
- `cargo test -p s3-tests --test object_crud test_object_header_acl_grants`
- `cargo test -p s3-tests --test object_crud test_object_header_acl_grants_streaming_put`
- `cargo test -p s3-tests --test copy_object test_object_copy_canned_acl`
- `cargo test -p s3-tests --test multipart test_multipart_upload_canned_acl_persists_to_completed_object`
- `cargo test -p s3-tests --test presigned test_object_presigned_put_object_with_acl`
- `cargo test -p s3-tests --test public_access_block test_block_public_object_canned_acls`
- `cargo test -p s3-tests --test versioning test_versioned_object_acl`
- `cargo test -p s3-tests --test versioning test_versioned_object_acl_no_version_specified`

Regression coverage:
- `cargo test -p s3-tests --test ownership`
- `cargo test -p s3-tests --test public_access_block`
- `cargo test -p s3-tests --test copy_object`
- `cargo test -p s3-tests --test post_object`
- `cargo test -p s3-tests --test presigned`
- `cargo test -p s3-tests --test versioning`

AWS validation:
- targeted ownership-default and object ACL compatibility runs have been
  checked against AWS; broader ACL matrix comparison remains ongoing

## Resolved Decisions

1. Storage layout
- bucket, object, and multipart ACLs live in typed serialized `acl_grants`
  columns, with derived public projections stored alongside them

2. Fast-path projections
- `public_read` and `public_write` remain as derived metadata projections for
  hot authorization paths, with structured ACL grants as the source of truth

3. Authenticated users group
- boundary parsing and serialization support `AuthenticatedUsers`, but write
  paths currently reject those grants until enforcement semantics are added in
  a separate follow-up

## Recommended Defaults

- adopt a typed internal ACL model and keep XML as a boundary format only
- make structured ACL state the source of truth for bucket ACLs, with public
  flags derived from it if still needed
- store object ACLs per version, not per key
- keep ACL evaluation as a separate layer from bucket-policy evaluation so the
  later policy work can compose rather than replace it
