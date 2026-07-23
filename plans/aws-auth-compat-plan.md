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
- the remaining bucket ABAC surface, including the `s3-control` tag-management
  subset it depends on after enablement
- conformance tests and documentation for the above

This still does not cover:
- full IAM policy language
- full bucket policy evaluation
- temporary/session credentials, including session-token authentication,
  request-time expiry, and the STS AssumeRole API

The new work for the last item, including the IAM identity-policy foundation it
requires, is tracked in
[sts-assume-role-issuance.md](sts-assume-role-issuance.md).

Important current bucket-policy note:
- AWS-backed testing now shows that `PutBucketPolicy` acceptance is broader
  than our current runtime evaluator. We are not keeping an acceptance-only
  subset for unsupported object-condition keys because that creates a stored
  policy state the evaluator cannot use correctly.
- In particular, request context does not yet carry values for
  `aws:SourceVpc`, `aws:SourceVpce`, `aws:SourceArn`, `aws:SourceAccount`,
  `aws:SourceOwner`, `aws:PrincipalOrgID`,
  `s3:DataAccessPointAccount`, or `s3:DataAccessPointArn`.
- The Phase 3 IAM foundation now supplies AWS-pinned `aws:PrincipalArn` for
  assumed roles and validated, account-bound configured IAM users, plus
  assumed-role `aws:userid` and `aws:TokenIssueTime`. Other configured-user and
  tag-derived IAM context remains deferred.
- Those keys remain outside the current accepted/evaluable object-condition
  subset; they are future follow-up rather than partially accepted support
  today.
- `s3:ResourceTag/*` remains a separate deferred evaluator gap.

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
- Stored static-credential expiry checks and rejection of unexpected session
  tokens on static credentials; issued temporary credentials are not yet
  supported
- Constant-time signature comparison
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

Deferred outside this plan:
- Temporary/session credential support

Still open:
- Account-level Block Public Access controls
- Final compatibility/conformance documentation
- Decision on where to track and validate the remaining `s3:ResourceTag/*`
  investigation once policy-evaluator expansion resumes
- bucket-policy evaluator expansion for AWS-accepted condition keys whose
  request context is still missing locally
- bucket ABAC compatibility, where `GetBucketAbac` / `PutBucketAbac` are
  ordinary S3 bucket APIs but post-enable bucket tag mutation depends on
  `s3-control` tag-management APIs

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

Still useful follow-up inside the currently implemented bucket-policy surface:
- add request-context plumbing and evaluator support for currently unsupported
  keys such as `aws:SourceVpc`
- decide, with AWS-backed evidence, whether any future upload-time acceptance
  mismatch is worth taking on without matching request-time semantics

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
- AWS validation that `PutBucketPolicy` accepts at least some object-policy
  condition clauses that our current runtime evaluator still cannot fully model

### 4. Bucket ABAC And Minimal `s3-control` Tag Management

Still missing:
- bucket ABAC enablement support for general purpose buckets
- AWS-backed mapping of `s3:BucketTag/${TagKey}` behavior in the current
  ABAC-disabled baseline and after ABAC enablement
- the minimum bucket-tag control-plane subset required once ABAC is enabled
- a documented initial endpoint/routing shape for the narrow `s3-control`
  tag-management surface we need here

Initial target scope:
- `PutBucketAbac`
- `GetBucketAbac`
- whatever bucket-tag control-plane support AWS requires once ABAC is enabled
  (likely `TagResource` / `UntagResource`, while `GetBucketTagging` remains on
  the ordinary S3 API)

These two are ordinary S3 bucket APIs, not `s3-control`.

Explicitly out of scope:
- broad `s3-control` parity
- access points and multi-region access points
- batch operations
- Storage Lens
- broader account-scoped `s3-control` administration

Current AWS-backed baseline:
- with bucket ABAC disabled, `s3:BucketTag/${TagKey}` statements can be accepted
  at `PutBucketPolicy` time but still fail to authorize later requests
- we should not treat accepted syntax as operative behavior until the
  ABAC-enabled state is mapped too

Implementation note for the first step:
- bucket metadata should carry an explicit `bucket_abac_enabled: bool`
- the evaluator should continue to see only bucket-tag input availability
  (`Available` vs `Unavailable`), not the raw ABAC flag itself
- when bucket ABAC is disabled, authz should derive
  `BucketTags::Unavailable` from that bucket state rather than inferring
  non-operability from policy shape alone
- that ABAC bit should have real storage/coordinator accessors rather than
  living only as schema state plus raw test SQL, so later `GetBucketAbac` /
  `PutBucketAbac` wiring can reuse the same state surface

## Target Behavior

### Authentication

1. Accept and validate:
- Header SigV4
- Presigned SigV4 query auth
- POST SigV4 form auth

2. Preserve current static-credential token and expiry behavior:
- reject unexpected session-token input on a stored static credential with the
  AWS-pinned post-signature precedence
- reject an expired stored credential when an expiry is configured
- implement issued temporary credentials, mandatory token binding, and session
  expiry through
  [sts-assume-role-issuance.md](sts-assume-role-issuance.md),
  not as completed work in this follow-up

3. Enforce AWS-like header auth rules:
- strict header scope validation for configured region/service
- every header listed in `SignedHeaders` must be present
- all transmitted `x-amz-*` headers except `x-amz-content-sha256` must be
  listed in `SignedHeaders`; when omitted from that list,
  `x-amz-content-sha256` still supplies the canonical request payload hash and
  is therefore bound by the signature
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
- rerun the targeted auth/public-access suites after Phase 5 lands:
  - `public_access_block.rs`
  - `bucket_policy.rs`
  - `bucket_policy_root.rs`
  - `bucket_admin_root.rs`
  - `ownership.rs`
  - `expected_bucket_owner.rs` if Phase 5 changes interact with it
- document the actual remaining deferred auth/authz gaps in one place:
  - account-level Block Public Access, if still deferred
  - account-policy/IAM-side `CreateBucket` / `ListBuckets`
  - bucket-policy condition keys whose request context is still intentionally
    missing locally:
    - `aws:SourceVpc`
    - `aws:SourceVpce`
    - `aws:SourceArn`
    - `aws:SourceAccount`
    - `aws:SourceOwner`
    - `aws:PrincipalOrgID`
    - `s3:DataAccessPointAccount`
    - `s3:DataAccessPointArn`
  - deferred `s3:ResourceTag/*`

Notes:
- this is now a short release-closeout pass, not a broad new implementation
  phase
- the earlier placeholder about “remaining auth/header-related Ceph ports” is
  intentionally retired here unless concrete missing test cases are identified

### Phase 7: Minimal `s3-control` Support For Bucket ABAC

Status: complete.

Deliver:
- AWS-compatible `GetBucketAbac` / `PutBucketAbac` behavior
- the minimum endpoint/routing support required for that subset
- the minimum bucket-tag control-plane support required once ABAC is enabled
- AWS-backed `s3:BucketTag/${TagKey}` conformance tests in both ABAC-disabled
  and ABAC-enabled states

Completed so far:
- persisted `bucket_abac_enabled` bucket state with coordinator/storage accessors
- AWS-compatible disabled baseline:
  - fresh buckets return `GetBucketAbac = Disabled`
  - `s3:BucketTag/${TagKey}` policy clauses can be accepted but remain
    non-operative while ABAC is disabled
- ordinary S3 endpoint support for:
  - `GetBucketAbac`
  - `PutBucketAbac`
- AWS-backed same-account root/non-root admin coverage for enabling ABAC
- AWS-backed post-enable bucket-tagging behavior:
  - `GetBucketTagging` still works
  - `PutBucketTagging` returns `400 BadRequest` with:
    `This S3 general purpose bucket has attribute-based access control (ABAC) enabled. To add tags to this bucket, initiate a TagResource request. To delete tags from this bucket, initiate an UntagResource request.`
  - `DeleteBucketTagging` returns `400 BadRequest` with:
    `This S3 general purpose bucket has attribute-based access control (ABAC) enabled. To delete tags from this bucket, initiate an UntagResource request.`
- temporary endpoint/routing decision:
  - the minimal `TagResource` / `UntagResource` subset is currently accepted on
    the same endpoint as the normal S3 API
  - local Host / endpoint distinction against the real AWS `s3-control`
    surface is not enforced yet
  - this is explicitly temporary and should be revisited once the broader
    `s3-control` shape is implemented
- local minimal bucket-tag control-plane support:
  - `TagResource` merges into the stored bucket tag set
  - `UntagResource` removes only the requested keys
  - the same endpoint path `/v20180820/tags/{resourceArn}` is accepted locally
- AWS-pinned `TagResource` / `UntagResource` control-plane behavior:
  - the real AWS host shape is
    `https://{account_id}.s3-control.{region}.amazonaws.com`
  - both operations still sign with SigV4 service name `s3`
  - both operations require `x-amz-account-id`
  - the targeted AWS `bucket_admin_root` probe now runs successfully against
    the real control-plane host
  - the committed IAM test-user policy needed `s3:TagResource` /
    `s3:UntagResource` on `Resource: "*"` for those AWS control-plane calls
- AWS-pinned enabled-state `s3:BucketTag/${TagKey}` behavior:
  - with ABAC enabled and bucket tag `security=public`, the same conditional
    allow shape authorizes:
    - `GetBucketTagging`
    - `GetBucketPolicyStatus`
    - `GetBucketAcl`
    - `PutBucketAcl`
    - `GetBucketVersioning`
    - `PutBucketVersioning`
    - `ListBucketVersions`
    - `ListBucketMultipartUploads`
    - `GetBucketLocation`
    - `GetBucketCors`
    - `PutBucketCors`
    - `GetLifecycleConfiguration`
    - `PutLifecycleConfiguration`
    - `GetBucketOwnershipControls`
    - `PutBucketOwnershipControls`
    - `GetEncryptionConfiguration`
    - `PutEncryptionConfiguration`
    - `GetBucketPublicAccessBlock`
    - `PutBucketPublicAccessBlock`
    - `GetBucketObjectLockConfiguration`
    - `PutBucketObjectLockConfiguration`
    - `ListBucket`
    - `HeadBucket`
      - the enabled-state bucket-tag path is not a dedicated single action:
        `s3:GetBucketLocation` alone is insufficient, but the combined
        `s3:ListBucket` + `s3:GetBucketLocation` allow shape authorizes
        `HeadBucket` on AWS
    - `GetObject`
    - `HeadObject`
    - `GetObjectAttributes`
      - pinned with an unconditional `GetObjectAttributes` allow plus a
        bucket-tag-conditioned `GetObject` allow, which proves the implicit
        read-side check is bucket-tag-aware on AWS
    - `GetObjectTagging`
    - `PutObjectTagging`
    - `GetObjectAcl`
    - `PutObjectAcl`
    - `PutObject`
    - `DeleteObject`
    - `CreateMultipartUpload`
    - `UploadPart`
    - `CompleteMultipartUpload`
    - `CopyObject` destination writes
    - `UploadPartCopy` destination writes
    - `CopyObject` source reads
    - `UploadPartCopy` source reads
    - `GetObjectRetention`
    - `PutObjectRetention`
    - `BypassGovernanceRetention`
    - `GetObjectLegalHold`
    - `PutObjectLegalHold`
  - local authz now derives bucket-tag availability from
    `bucket_abac_enabled` on both bucket and object request paths, and the
    previously implicit `GetObject` / `PutObject` families above are now
    explicitly AWS-pinned instead of inferred
  - `DeleteBucket` is now part of this matrix:
    under ABAC-enabled buckets, `s3:BucketTag/*` is operative for
    `s3:DeleteBucket`
  - `CreateBucket` remains separate:
    AWS rejects `s3:CreateBucket` in bucket policy as `MalformedPolicy`
    (`Policy has invalid action`), so it is not part of the enabled-state
    bucket-tag matrix
  - the versioned object subresource/mutation rows are now pinned too:
    - `GetObjectVersionAcl`
    - `PutObjectVersionAcl`
    - `GetObjectVersionTagging`
    - `PutObjectVersionTagging`
    - `DeleteObjectVersion`
    - `DeleteObjectTagging`
    - `DeleteObjectVersionTagging`
  - `PutObjectVersionAcl` is not a special bucket-ABAC exception after all:
    once ACLs are enabled on the bucket, AWS evaluates
    `s3:PutObjectVersionAcl` normally under `s3:BucketTag/*`
  - the bucket-policy-management actions are now pinned too, using the
    same-account constrained user plus owner-root harness:
    - `GetBucketPolicy`
    - `PutBucketPolicy`
    - `DeleteBucketPolicy`
  - revocation after `TagResource` is now partially pinned too:
    - warm `GetObject` revocation is eventually consistent rather than
      immediate
    - in the current AWS probes, the owner sees the new private tag via
      `GetBucketTagging` quickly, but a previously successful cross-principal
      `GetObject` can continue to succeed for roughly 30 seconds before
      converging to `AccessDenied`
    - there is now an AWS-backed regression for that warm `GetObject`
      revocation path

Follow-up outside Phase 7:
- additional revocation coverage can be added in `s3-local-tests` if we want
  more local-only assertions on slow eventual-convergence paths like
  `ListBucket`, without turning the AWS compatibility suite into a long
  propagation harness
- endpoint / host-routing parity remains deferred to a separate plan:
  [endpoint-routing-compat-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/endpoint-routing-compat-plan.md)
  That broader deferred area includes:
  - explicit `s3-control` host/endpoint validation
  - virtual-hosted-style bucket addressing
  - website endpoints
  - other bucket-in-host routing surfaces

## Test Plan

Unit tests:
- `cargo test -p auth`

Targeted integration tests:
- `cargo test -p s3-tests --test headers`
- `cargo test -p s3-tests --test presigned`
- `cargo test -p s3-tests --test chunked`
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
- run targeted bucket ABAC probes against AWS while working on Phase 7,
  covering both ABAC-disabled and ABAC-enabled buckets

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

4. Endpoint-routing follow-up ownership
- keep the bucket-ABAC behavior work complete here
- track all deferred host/endpoint routing work under the separate
  endpoint-routing compatibility plan rather than keeping a partial
  `s3-control` routing note open in this auth plan

## Recommended Default Decisions

- Keep region strictness enabled
- Keep bucket-level public write as the current supported bucket-ACL surface
- Keep `AuthorizationProfile` explicit at auth boundaries rather than inferring
  broad rights from same-account identity
- Keep `owner_canonical_id` explicitly stored rather than deriving it ad hoc in XML rendering
- Keep endpoint / host-routing parity explicitly deferred to the separate
  endpoint-routing compatibility plan
