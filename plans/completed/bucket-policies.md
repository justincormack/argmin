# Bucket Policies

## Scope

This plan covers bucket policy support needed for AWS-compatible bucket policy
behavior, including the Ceph-derived parity gaps we still have after the first
five phases landed.

In scope:
- `PutBucketPolicy`, `GetBucketPolicy`, and `DeleteBucketPolicy`
- durable bucket policy storage
- bucket policy parsing, validation, and evaluation
- public-policy classification for public access block interaction
- request-context extraction for the condition keys exercised by current tests
- bucket-level policy evaluation for listing and policy-status APIs
- multipart policy enforcement and the additional `PutObject` condition keys
  exercised by the remaining Ceph bucket-policy tests

Out of scope:
- IAM user and role policy APIs
- STS APIs
- account-level public access block
- RGW tenant-prefixed bucket names and tenant-specific ARN/resource syntax that
  AWS S3 does not expose
- a generic external policy engine dependency without prior approval

Dependencies:
- `plans/completed/ownership-and-multi-user-foundations.md`
- `plans/completed/object-and-bucket-acls.md` for the tests that involve
  `GetObjectAcl` or ACL-conditioned policy behavior

## Current State

Current implementation status:
- bucket metadata now stores raw bucket policy JSON in `buckets.bucket_policy`
- bucket metadata also stores a compact `bucket_policy_public` summary used by
  hot auth paths
- bucket metadata also stores a shared bucket policy generation used to
  invalidate parsed-policy caches across coordinators
- storage exposes `put/get/delete_bucket_policy`
- the router and HTTP handler support `PUT ?policy`, `GET ?policy`, and
  `DELETE ?policy`, plus `GET ?policyStatus`
- `PUT ?policy` requires a UTF-8 body, parses the policy into a typed internal
  representation, and stores the provided JSON string only after validation
- `GET ?policy` returns the stored policy JSON, or `NoSuchBucketPolicy` when
  no policy is configured
- `GET ?policyStatus` returns AWS-compatible XML with
  `PolicyStatus.IsPublic` derived from bucket-policy publicness, and returns
  `NoSuchBucketPolicy` when no bucket policy is configured
- `DELETE ?policy` removes the stored policy
- bucket policy administration is wired through bucket-admin authorization
- the public access block XML parser already supports `BlockPublicPolicy` and
  `RestrictPublicBuckets`
- there is now a shared typed bucket policy parser and public-policy classifier
- `PutBucketPolicy` rejects public `Allow` policies when
  `BlockPublicPolicy=true`
- request-time policy evaluation now covers object reads, object tagging, copy
  destination writes, `GetObjectAcl`, `GetBucketPublicAccessBlock`,
  `GetBucketPolicyStatus`, and `ListObjects` / `ListObjectsV2` via
  `s3:ListBucket`
- supported request-time condition keys now include:
  `s3:ExistingObjectTag/<key>`,
  `s3:x-amz-copy-source`,
  `s3:x-amz-metadata-directive`, and
  `s3:x-amz-acl`
- request-time `RestrictPublicBuckets` enforcement now suppresses policy-based
  `Allow` results for public bucket policies unless the requester is in the
  bucket owner's account
- request-time evaluation uses a parsed-policy cache outside
  `BucketFastPathInfo`, keyed by shared bucket policy generation, and snapshots
  the parsed policy before object-PG locking to avoid same-PG self-deadlock
- raw policy is intentionally not stored in bucket fast-path metadata

Remaining Ceph/AWS parity gaps after Phases 1-8:
- Ceph ACL-only policy-status cases are out of AWS scope because
  `GetBucketPolicyStatus` returns `NoSuchBucketPolicy` when no bucket policy is
  configured; `authenticated-read` bucket ACLs themselves remain intentionally
  `NotImplemented`
- SSE-S3 / SSE-KMS bucket-policy condition keys remain out of scope until the
  server implements the corresponding request headers and semantics
- Ceph tests that depend on RGW tenant namespace syntax are not AWS scope and
  should stay documented as exclusions rather than driving implementation

## Goals

Implement bucket policy support as a real policy subsystem, not as a set of
per-test string checks.

The design should:
- store the original JSON policy document durably
- normalize it into a typed internal representation for evaluation
- classify whether a policy is public
- evaluate policy statements consistently across object, tagging, copy, and ACL
  operations

## Target Behavior

### API Surface

Support:
- `PUT ?policy`
- `GET ?policy`
- `DELETE ?policy`

Requirements:
- owner-only policy administration unless AWS says otherwise
- stored policy returned as JSON in an AWS-compatible way; do not rely on
  whitespace or key-order round-tripping across AWS
- correct `NoSuchBucketPolicy` behavior when not configured

### Evaluation Model

Use explicit allow and deny evaluation with AWS-like precedence:
- explicit deny wins
- explicit allow grants access
- lack of allow leaves the request denied unless ownership, ACL, or public
  behavior already grants access

Policy evaluation must compose with:
- ownership checks
- ACL checks
- public access block checks

### Supported Policy Features In Completed Phases

Actions currently implemented:
- `s3:ListBucket`
- `s3:GetBucketPolicyStatus`
- `s3:GetObject`
- `s3:GetObjectTagging`
- `s3:PutObjectTagging`
- `s3:DeleteObjectTagging`
- `s3:PutObject`
- `s3:GetObjectAcl`

Principal forms currently implemented:
- `"*"`
- explicit AWS principal strings used by test callers

Condition keys currently implemented:
- `s3:ExistingObjectTag/<key>`
- `s3:x-amz-copy-source`
- `s3:x-amz-metadata-directive`
- `s3:x-amz-acl`
- `s3:x-amz-grant-*`
- `s3:RequestObjectTag/<key>`

Operator support currently implemented for the enforced condition subset:
- `StringEquals`
- `StringLike`
- `Null`
- `StringNotEquals`

Still explicitly out of scope:
- `s3:x-amz-server-side-encryption`
- `s3:x-amz-server-side-encryption-aws-kms-key-id`

This plan should also support the basic statement structure AWS policies use:
- `Version`
- `Statement`
- `Sid`
- `Effect`
- `Principal`
- `Action`
- `Resource`
- `Condition`

### Public Access Block Interaction

`BlockPublicPolicy`:
- rejects policy updates that would make the bucket policy public

`RestrictPublicBuckets`:
- once a bucket has a public policy, non-owner cross-account access must be
  restricted as AWS does even if the policy would otherwise allow it

The public-policy classifier must be shared between:
- `PutBucketPolicy`
- request-time authorization

## Design Changes

### 1. Storage

Add a durable bucket policy field or child table under bucket metadata.

Requirements:
- policy reads must be cheap for request-time evaluation
- policy updates must atomically update any cached classification state needed
  for public access block
- bucket fast-path metadata may include derived policy summary fields, but not
  the only copy of the policy

### 2. Internal Policy Representation

Introduce a typed internal AST or compiled policy representation.

Requirements:
- preserve the semantics of effect, principal, action, resource, and condition
- distinguish bucket-level and object-level resources
- classify public vs non-public statements

Do not evaluate raw `serde_json::Value` trees directly on hot paths.

### 3. Request Context Extraction

Build a shared authorization context used by policy evaluation.

It must expose, at minimum:
- requester account identity
- bucket and key
- operation/action
- object tags for existing-object-tag conditions
- copy source value
- metadata directive value
- ACL header value

Policy conditions should be derived once per request, not reparsed in multiple
call sites.

### 4. Authorization Integration

Insert policy evaluation into the coordinator authorization flow in a way that
composes with ownership and ACL checks.

Recommended ordering:
1. resolve requester and bucket
2. gather object metadata if needed for the request
3. evaluate explicit deny
4. evaluate ownership and ACL allow paths
5. evaluate policy allow paths
6. apply public access block restrictions that override public policy effects

The exact order may differ by operation, but policy evaluation must not remain a
separate ad hoc check in only one or two APIs.

### 5. HTTP And Error Mapping

Add the missing bucket policy operations to routing and HTTP handling.

Requirements:
- correct XML or empty-body responses where AWS uses them
- correct policy-related error mapping
- consistent handling of malformed or unsupported policy documents

## Implementation Phases

### Phase 1: Bucket Policy Storage And APIs

Status:
- completed

Deliver:
- schema support
- `PutBucketPolicy`
- `GetBucketPolicy`
- `DeleteBucketPolicy`
- owner-authorized administration path
- `NoSuchBucketPolicy` mapping for unset `GET`
- storage/core/http/integration coverage for the surface contract

Success criteria:
- policy documents can be stored, retrieved, and removed without affecting
  request authorization yet

Notes:
- the raw JSON policy is stored durably in `BucketInfo`
- the raw policy is not propagated into `BucketFastPathInfo`
- this phase does not parse, validate, classify, or evaluate bucket policies

### Phase 2: Typed Policy Parser And Public Classifier

Status:
- completed

Deliver:
- typed policy representation
- validation for the supported policy subset
- public-policy classification shared with public access block logic

Success criteria:
- `BlockPublicPolicy` can reject public policy writes correctly

Notes:
- the shared parser/classifier lives outside the HTTP layer so later phases can
  reuse it for request-time authorization
- the current classifier covers the public-policy cases exercised by the
  unignored public-access-block tests: wildcard public `Allow` is public, fixed
  principals are non-public, and `Deny` statements are not treated as public

### Phase 3: Core Evaluation For Public Object And Tagging Access

Status:
- completed

Deliver:
- policy evaluation for object reads and tagging APIs
- condition support for `s3:ExistingObjectTag`

Success criteria:
- the current tagging policy tests can be unignored

Notes:
- request-time evaluation now lives in the shared `auth` policy layer with
  explicit deny / explicit allow / no-match outcomes
- Phase 3 currently covers `s3:GetObject`,
  `s3:GetObject{Version,VersionTagging,Tagging}`,
  `s3:PutObject{VersionTagging,Tagging}`, and
  `s3:DeleteObject{VersionTagging,Tagging}` for object reads and tagging APIs
- `s3:ExistingObjectTag/<key>` currently supports `StringEquals`
- policies that use other condition operators or condition keys on those
  currently-enforced object/tagging actions are rejected at `PutBucketPolicy`
  time, and unsupported `Deny` conditions are still treated conservatively at
  request time
- raw bucket policy still stays out of `BucketFastPathInfo`; coordinator policy
  evaluation uses a separate parsed-policy cache keyed by a shared bucket policy
  generation so replacements invalidate correctly across coordinators
- explicit AWS root-account principals are matched in a way that keeps the same
  test policies usable against both the local test server and AWS

### Phase 4: Copy And ACL-Condition Evaluation

Status:
- completed

Deliver:
- policy evaluation for `s3:x-amz-copy-source`
- policy evaluation for `s3:x-amz-metadata-directive`
- policy evaluation for `s3:x-amz-acl`
- policy evaluation for `s3:GetObjectAcl` requests using the shared request-time
  evaluator

Success criteria:
- copy-source and ACL-conditioned policy tests can be unignored

Notes:
- Phase 4 extends request-time evaluation to `s3:PutObject` and
  `s3:GetObjectAcl` for the condition keys exercised by the current Ceph tests
- `s3:x-amz-copy-source`, `s3:x-amz-metadata-directive`, and `s3:x-amz-acl`
  currently support `StringEquals` and `StringLike`
- policy request context for copy destination writes is derived once and carried
  through direct put, streaming put, and finalize paths so large-copy and
  streamed-write authorization is consistent
- `x-amz-metadata-directive: COPY` is distinguished from an absent
  `x-amz-metadata-directive` header because AWS treats those differently for the
  conditional policy tests
- the copy-source, metadata-directive, canned-ACL, and `GetObjectAcl`
  bucket-policy tagging tests are now unignored

### Phase 5: Restrict Public Buckets

Status:
- completed

Deliver:
- request-time enforcement of `RestrictPublicBuckets`
- shared policy-publicness summary available to hot auth paths

Success criteria:
- public access block policy restriction tests can be unignored

Notes:
- bucket metadata and fast-path summaries now carry a compact
  `bucket_policy_public` flag, while parsed policy documents remain outside the
  hot bucket fast path
- `RestrictPublicBuckets` only suppresses bucket-policy `Allow` results for
  public policies; explicit `Deny` still wins, and ACL / ownership fallback
  behavior remains intact
- when a bucket policy is public and `RestrictPublicBuckets=true`, same-account
  requesters can still use policy-based access, but anonymous and cross-account
  policy-based access is blocked
- request-time bucket-policy evaluation now also covers
  `s3:GetBucketPublicAccessBlock`, which is enough for the remaining public
  access block deny test

### Phase 6: Bucket-Level Policy Evaluation

Status:
- completed

Deliver:
- request-time evaluation for `s3:ListBucket` on `ListObjects` and
  `ListObjectsV2`
- bucket-resource matching that distinguishes `arn:aws:s3:::bucket` from
  `arn:aws:s3:::bucket/*`
- deny/allow composition with bucket ACL fallback on listing APIs
- alternate-account integration coverage for bucket-level allow and deny cases

Success criteria:
- native equivalents exist for Ceph `test_bucket_policy`,
  `test_bucketv2_policy`, `test_bucket_policy_acl`,
  `test_bucketv2_policy_acl`, and the bucket-vs-object ARN multipart setup case

Notes:
- `ListObjects` and `ListObjectsV2` now evaluate `s3:ListBucket` through the
  shared bucket-policy evaluator
- explicit bucket-policy `Deny` now overrides bucket ACL list access, and
  explicit `Allow` composes with `RestrictPublicBuckets` in the same way as the
  existing object-level helpers
- native coverage now includes v1/v2 list allow, deny-overrides-ACL, and
  bucket-resource-vs-object-resource mismatch cases
- RGW tenant-addressing tests such as `test_bucket_policy_different_tenant` and
  `test_bucket_policy_tenanted_bucket` should not drive implementation because
  AWS does not expose tenant-prefixed bucket names
- cross-account principal matching remains in scope; tenant-specific resource
  syntax does not

### Phase 7: Multipart And Extended PutObject Conditions

Status:
- completed for the supported write surface
- deferred follow-up remains for SSE-S3 / SSE-KMS condition keys because the
  implementation currently supports SSE-C only

Deliver:
- bucket-policy enforcement on `CreateMultipartUpload`
- bucket-policy enforcement on `UploadPartCopy` using source-read and
  destination-write request context
- `PutObject` condition support for `s3:x-amz-grant-*`
- `PutObject` condition support for `s3:RequestObjectTag/<key>`
- tagged `PutObject` / `CreateMultipartUpload` requests require
  `s3:PutObjectTagging` authorization in addition to `s3:PutObject`, matching
  AWS
- request-time operator support needed by those tests, including `Null` and
  `StringNotEquals`

Success criteria:
- native equivalents exist for Ceph `test_bucket_policy_multipart`,
  `test_bucket_policy_upload_part_copy`, `test_bucket_policy_put_obj_grant`,
  `test_bucket_policy_put_obj_request_obj_tag`
- the SSE-S3 / SSE-KMS bucket-policy tests remain explicitly out of scope until
  the server implements the corresponding request headers and semantics

Notes:
- this phase should continue reusing the shared evaluator and request-context
  extraction rather than adding multipart-specific string checks
- encryption-policy follow-up is about policy condition evaluation on real
  request headers, not about introducing non-AWS KMS shortcuts

### Phase 8: Policy Status And Remaining Surface Parity

Status:
- completed for the currently implemented ACL and encryption surface

Deliver:
- `GetBucketPolicyStatus`
- `IsPublic` computation wired from bucket policy publicness
- end-to-end S3 coverage for rejected unsupported policy forms such as
  `NotPrincipal`
- explicit documentation of the remaining RGW-only exclusions after AWS parity
  work is complete

Success criteria:
- native equivalents exist for Ceph `test_get_bucket_policy_status`,
  `test_get_publicpolicy_acl_bucket_policy_status`,
  `test_get_nonpublicpolicy_acl_bucket_policy_status`,
  `test_get_nonpublicpolicy_principal_bucket_policy_status`, and
  `test_bucket_policy_allow_notprincipal`
- ACL-only buckets follow AWS and return `NoSuchBucketPolicy`

## Test Plan

Targeted integration tests:
- `cargo test -p s3-tests --test bucket_policy test_get_bucket_policy_status_private_bucket -- --exact --nocapture`
- `cargo test -p s3-tests --test bucket_policy test_get_bucket_policy_status_public_bucket_acl -- --exact --nocapture`
- `cargo test -p s3-tests --test bucket_policy test_get_bucket_policy_status_public_bucket_policy -- --exact --nocapture`
- `cargo test -p s3-tests --test bucket_policy test_get_bucket_policy_status_nonpublic_bucket_policy -- --exact --nocapture`
- `cargo test -p s3-tests --test bucket_policy test_get_bucket_policy_status_nonpublic_fixed_principal_policy -- --exact --nocapture`
- `cargo test -p s3-tests --test bucket_policy test_get_bucket_policy_status_cross_account_allow -- --exact --nocapture`
- `cargo test -p s3-tests --test bucket_policy test_get_bucket_policy_status_cross_account_deny_overrides_allow -- --exact --nocapture`
- `cargo test -p s3-tests --test bucket_policy test_put_bucket_policy_not_principal_rejected -- --exact --nocapture`
- `cargo test -p s3-tests --test bucket_policy test_bucket_policy_list_objects_v1 -- --exact --nocapture`
- `cargo test -p s3-tests --test bucket_policy test_bucket_policy_list_objects_v2 -- --exact --nocapture`
- `cargo test -p s3-tests --test bucket_policy test_bucket_policy_list_deny_overrides_bucket_acl -- --exact --nocapture`
- `cargo test -p s3-tests --test bucket_policy test_bucket_policy_list_requires_bucket_resource -- --exact --nocapture`
- `cargo test -p s3-tests --test public_access_block test_block_public_policy -- --exact --nocapture`
- `cargo test -p s3-tests --test public_access_block test_block_public_policy_with_principal -- --exact --nocapture`
- `cargo test -p s3-tests --test public_access_block test_block_public_restrict_public_buckets -- --exact --nocapture`
- `cargo test -p s3-tests --test public_access_block test_get_public_block_deny_bucket_policy -- --exact --nocapture`
- `cargo test -p s3-tests --test tagging test_get_tags_acl_public -- --exact --nocapture`
- `cargo test -p s3-tests --test tagging test_put_tags_acl_public -- --exact --nocapture`
- `cargo test -p s3-tests --test tagging test_delete_tags_obj_public -- --exact --nocapture`
- `cargo test -p s3-tests --test tagging test_bucket_policy_get_obj_existing_tag -- --exact --nocapture`
- `cargo test -p s3-tests --test tagging test_bucket_policy_get_obj_tagging_existing_tag -- --exact --nocapture`
- `cargo test -p s3-tests --test tagging test_bucket_policy_put_obj_tagging_existing_tag -- --exact --nocapture`
- `cargo test -p s3-tests --test tagging test_bucket_policy_put_obj_copy_source -- --exact --nocapture`
- `cargo test -p s3-tests --test tagging test_bucket_policy_put_obj_copy_source_meta -- --exact --nocapture`
- `cargo test -p s3-tests --test tagging test_bucket_policy_put_obj_acl -- --exact --nocapture`
- `cargo test -p s3-tests --test tagging test_bucket_policy_get_obj_acl_existing_tag -- --exact --nocapture`

Regression coverage:
- `cargo test -p s3-tests --test bucket_policy`
- `cargo test -p s3-tests --test public_access_block`
- `cargo test -p s3-tests --test tagging`
- `cargo test -p s3-tests --test copy_object`
- `cargo test -p s3-tests --test presigned`

AWS validation:
- compare the supported public-policy and condition-key subset against AWS
  before freezing the evaluator behavior

## Open Decisions

1. Parser strictness
- whether to reject unsupported but syntactically valid policy features up front
  or accept and store them while marking the policy unevaluable

2. Policy storage format
- whether to store both original JSON and compiled representation, or store only
  the original JSON and compile on update into derived metadata

3. Hot-path caching
- whether bucket fast-path info should carry a compact compiled policy summary,
  or whether policy fetch and evaluation should remain on the metadata path until
  profiling shows a need

## Recommended Defaults

- add full bucket-policy CRUD together with storage rather than leaving write
  support for a later pass
- build a typed internal policy representation and shared request-evaluation
  context instead of per-action JSON checks
- support the exact action and condition-key subset needed by the current tests
  in the first evaluation pass, while keeping the representation extensible
- make public-policy classification a first-class output of policy compilation so
  `BlockPublicPolicy` and `RestrictPublicBuckets` share one implementation
