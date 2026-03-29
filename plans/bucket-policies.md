# Bucket Policies

## Scope

This plan covers bucket policy support needed for the remaining ignored Ceph
tests tied to public-policy and conditional-policy behavior.

In scope:
- `PutBucketPolicy`, `GetBucketPolicy`, and `DeleteBucketPolicy`
- durable bucket policy storage
- bucket policy parsing, validation, and evaluation
- public-policy classification for public access block interaction
- request-context extraction for the condition keys exercised by current tests

Out of scope:
- IAM user and role policy APIs
- STS APIs
- account-level public access block
- a generic external policy engine dependency without prior approval

Dependencies:
- `plans/completed/ownership-and-multi-user-foundations.md`
- `plans/completed/object-and-bucket-acls.md` for the tests that involve
  `GetObjectAcl` or ACL-conditioned policy behavior

## Current State

Current implementation status:
- bucket metadata now stores raw bucket policy JSON in `buckets.bucket_policy`
- storage exposes `put/get/delete_bucket_policy`
- the router and HTTP handler support `PUT ?policy`, `GET ?policy`, and
  `DELETE ?policy`
- `PUT ?policy` requires a UTF-8 body, parses the policy into a typed internal
  representation, and stores the provided JSON string only after validation
- `GET ?policy` returns the stored policy JSON, or `NoSuchBucketPolicy` when
  no policy is configured
- `DELETE ?policy` removes the stored policy
- bucket policy administration is wired through bucket-admin authorization
- the public access block XML parser already supports `BlockPublicPolicy` and
  `RestrictPublicBuckets`
- there is now a shared typed bucket policy parser and public-policy classifier
- `PutBucketPolicy` rejects public `Allow` policies when
  `BlockPublicPolicy=true`
- there is still no request-time policy evaluation yet
- raw policy is intentionally not stored in bucket fast-path metadata

Remaining ignored tests tied directly to the unimplemented policy-evaluation
gap:
- restrict public buckets
- get public block deny bucket policy
- all current tagging tests marked `bucket policies`
- policy tests involving `s3:ExistingObjectTag`
- policy tests involving `s3:x-amz-copy-source`
- policy tests involving `s3:x-amz-metadata-directive`
- policy tests involving `s3:x-amz-acl`

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

### Supported Policy Features For This Plan

Actions needed for the currently ignored tests:
- `s3:GetObject`
- `s3:GetObjectTagging`
- `s3:PutObjectTagging`
- `s3:DeleteObjectTagging`
- `s3:PutObject`
- `s3:GetObjectAcl`

Principal forms needed for the current tests:
- `"*"`
- explicit AWS principal strings used by test callers

Condition keys needed for the current tests:
- `s3:ExistingObjectTag/<key>`
- `s3:x-amz-copy-source`
- `s3:x-amz-metadata-directive`
- `s3:x-amz-acl`

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

Deliver:
- policy evaluation for object reads and tagging APIs
- condition support for `s3:ExistingObjectTag`

Success criteria:
- the current tagging policy tests can be unignored

### Phase 4: Copy And ACL-Condition Evaluation

Deliver:
- policy evaluation for `s3:x-amz-copy-source`
- policy evaluation for `s3:x-amz-metadata-directive`
- policy evaluation for `s3:x-amz-acl`

Success criteria:
- copy-source and ACL-conditioned policy tests can be unignored

### Phase 5: Restrict Public Buckets

Deliver:
- request-time enforcement of `RestrictPublicBuckets`
- shared policy-publicness summary available to hot auth paths

Success criteria:
- public access block policy restriction tests can be unignored

## Test Plan

Targeted integration tests:
- `cargo test -p s3-tests --test public_access_block test_block_public_policy -- --ignored`
- `cargo test -p s3-tests --test public_access_block test_block_public_policy_with_principal -- --ignored`
- `cargo test -p s3-tests --test public_access_block test_block_public_restrict_public_buckets -- --ignored`
- `cargo test -p s3-tests --test public_access_block test_get_public_block_deny_bucket_policy -- --ignored`
- `cargo test -p s3-tests --test tagging test_get_tags_acl_public -- --ignored`
- `cargo test -p s3-tests --test tagging test_put_tags_acl_public -- --ignored`
- `cargo test -p s3-tests --test tagging test_delete_tags_obj_public -- --ignored`
- `cargo test -p s3-tests --test tagging test_bucket_policy_get_obj_existing_tag -- --ignored`
- `cargo test -p s3-tests --test tagging test_bucket_policy_get_obj_tagging_existing_tag -- --ignored`
- `cargo test -p s3-tests --test tagging test_bucket_policy_put_obj_tagging_existing_tag -- --ignored`
- `cargo test -p s3-tests --test tagging test_bucket_policy_put_obj_copy_source -- --ignored`
- `cargo test -p s3-tests --test tagging test_bucket_policy_put_obj_copy_source_meta -- --ignored`
- `cargo test -p s3-tests --test tagging test_bucket_policy_put_obj_acl -- --ignored`
- `cargo test -p s3-tests --test tagging test_bucket_policy_get_obj_acl_existing_tag -- --ignored`

Regression coverage:
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
