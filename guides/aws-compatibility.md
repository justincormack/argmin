# AWS Compatibility Guide

Argmin aims to match AWS S3 behavior as closely as possible on the implemented
surface.

The default expectation in this repository is:

- behavioral differences from AWS are bugs unless they are explicitly documented
- AWS-backed tests are the source of truth when docs and assumptions diverge
- known incompatibilities should be written down clearly rather than left as
  tribal knowledge

This guide is a short list of currently known gaps. It is intentionally
non-exhaustive, but anything listed here should be treated as an explicit,
temporary compatibility exception rather than a surprise.

## Bucket scope

Argmin currently supports the normal S3 API surface for standard buckets.
It does not implement AWS S3 directory buckets / S3 Express One Zone as a
bucket class.

That said, directory-bucket-only request features still need to behave like
AWS when they are sent to a standard bucket. We keep explicit compatibility
coverage for that behavior in
[crates/s3-tests/tests/directory_bucket_features.rs](../crates/s3-tests/tests/directory_bucket_features.rs),
including cases where AWS ignores a directory-bucket-specific query and cases
where AWS rejects a directory-bucket-specific header or parameter with the
corresponding error.

## Behavior notes

### STS and temporary session credentials are not implemented

Argmin currently supports static S3 access key credentials only. It does not
implement STS endpoints or temporary session credentials, and credential records
do not carry an expected session token.

For static credentials, supplied security-token inputs are still rejected with
AWS-compatible S3 errors:

- a signed `x-amz-security-token` header, presigned
  `X-Amz-Security-Token` query parameter, or POST Object
  `x-amz-security-token` form field is rejected with `InvalidToken`
- an unsigned `x-amz-security-token` header is rejected by the general SigV4
  `x-amz-*` signing rule with `HeadersNotSigned`

Tests for temporary credential authentication should not be added until Argmin
has real temporary credential support. AWS-backed compatibility tests should
instead pin how S3 rejects token inputs supplied with static credentials.

Ad hoc AWS validation on 2026-07-07 with expired `GetSessionToken`
credentials showed that S3 returns `ExpiredToken` before signature mismatch for
header SigV4, presigned SigV4, and POST Object SigV4. The temporary Phase 0 STS
oracle subsequently pinned the same ordering for an expired `AssumeRole`
session across those modes and aws-chunked streaming. It also proved that
expiry wins after the stable issuer role has been deleted: an independent
still-unexpired session first returned three consecutive `InvalidAccessKeyId`
responses through each S3 authentication mode, proving deletion convergence
before the expired-session collisions. Issuer-role liveness itself wins over
signature comparison. The local auth pipeline must preserve that order;
unexpected security-token inputs for static credentials remain a separate
post-signature check.

The Phase 0 role-policy mutation oracle also established that an already-issued
role session uses the role's current permission policy for S3 authorization.
After an inline `PutObject` allow was replaced by an explicit deny, the original
session remained valid through STS but returned the same explicit
identity-policy-deny response as sessions issued after the replacement. A bad
SigV4 signature on that original session returned `SignatureDoesNotMatch`
instead. Current role permissions must therefore be loaded at the authorization
boundary after signature verification, not sealed into the session token or
resolved during authentication.

### Delete-then-recreate bucket name reuse may require retry

`DeleteBucket` should not be treated as proof that the same bucket name is
immediately reusable.

AWS appears to have a name-reuse window here as well: a client that deletes a
bucket and immediately issues `CreateBucket` for the same name can observe a
transient `409` such as `BucketAlreadyExists`, `BucketAlreadyOwnedByYou`, or
`OperationAborted` before the namespace is fully reusable.

This is not tracked as a compatibility gap. It is a behavior note for tests and
clients:

- prefer retrying the exact `CreateBucket` operation when reusing a just-deleted
  bucket name
- do not assume that a successful `DeleteBucket` implies synchronous namespace
  release
- in `crates/s3-tests`, use the bounded recreate helper rather than a fixed
  sleep for delete-then-recreate flows

### Some AWS `500 InternalError` responses are intentionally rejected as client errors

AWS currently responds with `500 InternalError` for a small number of malformed
client requests where the input is not a server fault. Known cases:

- `CopyObject` with a percent-encoded NUL byte in `x-amz-copy-source`.
  Argmin returns `400 InvalidArgument`.
- S3 Control `UntagResource` with more than 50 `tagKeys` query parameters.
  Argmin returns `400 InvalidTag`.
- S3 Control `UntagResource` with two identical `tagKeys` query parameters.
  Argmin returns `400 InvalidTag` with an explicit duplicate-key message.
- `CompleteMultipartUpload` for a checksum-configured upload when the supplied
  valid parts are not consecutive from part 1, such as completing with part 2
  alone. Argmin returns `400 InvalidRequest`.
- Presigned `PutObject` with a signed- or unsigned-trailer streaming marker
  when the raw request body exceeds `x-amz-decoded-content-length`, including
  an aws-chunked framed body. AWS returns `500 InternalError`; Argmin returns
  `400 MalformedTrailerError`. The corresponding presigned `UploadPart`
  request already returns `400 MalformedTrailerError` on AWS.

Argmin intentionally does not match the `500 InternalError` status in these
cases. Treating malformed client input as a server fault would be the wrong
contract to preserve. The AWS-backed `s3-tests` coverage for these cases is
allowed to accept either the live AWS `500` or Argmin's `4xx` response, and this
should not be treated as a general license to ignore AWS results elsewhere.

### Streaming write prepare failures can differ at the transport level

For streaming `PutObject`, `UploadPart`, and streaming `POST Object` flows
that fail before or during body ingestion, Argmin uses a bounded unread-body
reject path rather than an unbounded drain to EOF.

The server distinguishes between:

- auth-layer failures or anonymous denies, where the server closes promptly to
  avoid letting an unauthenticated client hold request capacity by continuing
  to stream a body, and
- authenticated permission or validation failures, where the server first
  attempts to drain a bounded amount of unread body and return a normal S3
  error so SDK clients do not see a transport-level broken pipe while still
  writing the request body.

That bounded drain has both byte and wall-clock limits. If the unread body is
too large or the client keeps sending too slowly, the server closes the
connection instead of continuing to read indefinitely after the request has
already been rejected.

The compatibility target remains the same final S3 error code and body where
practical. Exact transport timing, and whether an unread-body rejection is
reported as a normal S3 error or an early connection close, can still differ
slightly from AWS in these edge cases.

## AWS documentation divergences

This section records cases where AWS's public S3 documentation and live AWS
behavior diverge. These are not Argmin compatibility exceptions. When this
happens, live AWS behavior remains the compatibility target, and the relevant
AWS-backed test should be treated as the oracle.

### Conditional-write bucket policies and `CopyObject`

AWS's conditional-write bucket policy guide says that if a bucket policy
enforces `s3:if-match` or `s3:if-none-match`, `CopyObject` requests without
those HTTP headers fail with `403 AccessDenied`, and `CopyObject` requests
with those HTTP headers fail with `501 NotImplemented`.

Live AWS behavior is narrower than that statement. With a bucket policy that
allows `s3:PutObject` only when both of the following are true:

- the raw destination `If-Match` or `If-None-Match` HTTP header is present
- `Bool { "s3:ObjectCreationOperation": "true" }` matches

AWS treats `CopyObject` as an object-creation operation and allows the copy
when the raw destination conditional header is present. Without the header,
the policy does not match and the request is denied.

The AWS-pinned coverage is in:

- `test_bucket_policy_copy_object_if_match_condition`
- `test_bucket_policy_copy_object_if_none_match_condition`

Source with the contradictory note:

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html

### Bucket-policy `s3:x-amz-copy-source` uses the raw header spelling

Live AWS evaluates `s3:x-amz-copy-source` bucket-policy conditions against the
raw `x-amz-copy-source` header value. Source resolution still parses and
percent-decodes the source path and `versionId` where applicable. The
percent-encoded key-path case and the percent-encoded `versionId` case are the
same AWS rule: policy matching sees the raw header spelling, but the copy
operation uses the decoded source.

Policy authors should not rely on canonical decoded spellings as deny
guardrails for equivalent percent-encoded header spellings. For example, a
deny that matches `{bucket}/private/*` does not match
`{bucket}/%70rivate/...`, and a deny that matches
`{bucket}/key?versionId=1` does not match
`{bucket}/key?versionId=%31`, even though AWS resolves the same source key or
source version for the copy operation.

This is a deny-list hazard, not an allow-list hazard. A policy that allows only
the canonical raw spelling does not also allow an equivalent percent-encoded
spelling, so the encoded request fails unless some other statement permits it.
For copy-source restrictions, use an allow pattern for the accepted raw header
spelling instead of a deny pattern over one canonical decoded form. AWS's
conditional-write examples follow this allow-style pattern, although the
documentation does not call out that deny policies are unsafe for equivalent
copy-source encodings.

The AWS-pinned coverage is in:

- `test_bucket_policy_copy_source_condition_percent_encoded_unreserved_bypasses_canonical_deny`
- `test_bucket_policy_copy_source_percent_encoded_versionid_bypasses_canonical_deny`
- `test_bucket_policy_upload_part_copy_percent_encoded_versionid_bypasses_canonical_deny`

### `aws:RequestTag/*` on S3 Control `UntagResource`

AWS's S3 service authorization reference lists `aws:TagKeys` for
`s3:UntagResource`, but does not list `aws:RequestTag/${TagKey}` for that
action.

Live AWS accepts bucket policies that use `aws:RequestTag/${TagKey}` on
`s3:UntagResource`. For a requested tag key, AWS evaluates the corresponding
`aws:RequestTag/${TagKey}` value as an empty string:

- `StringEquals { "aws:RequestTag/security": "allow" }` does not match
- `StringEquals { "aws:RequestTag/security": "" }` matches

The AWS-pinned coverage is in:

- `test_bucket_policy_request_tag_condition_on_untag_resource_is_accepted_but_does_not_match_value`
- `test_bucket_policy_request_tag_condition_on_untag_resource_matches_empty_value`

## Current known gaps

### 1. Single-part `ETag` is not AWS MD5

For single-part objects, Argmin does not currently return the AWS-style MD5
`ETag`.

Our current `ETag` is an opaque integrity token derived from the internal object
representation. Clients should treat `ETag` as opaque.

If a client needs payload checksumming semantics, use S3 checksum fields
instead:

- `x-amz-checksum-*` response headers
- checksum fields in XML responses where AWS provides them
- `Content-MD5` if an MD5 payload checksum is specifically wanted

Related notes:

- [plans/completed/territory-map.md](../plans/completed/territory-map.md)
- [plans/completed/sse-c-encryption-plan.md](../plans/completed/sse-c-encryption-plan.md)

### 2. `SSE-KMS` is not implemented

Argmin supports `SSE-S3` and `SSE-C`, but not real AWS-compatible `SSE-KMS`
yet.

Related plan:

- [plans/encryption-compat-plan.md](../plans/encryption-compat-plan.md)

### 3. Bucket logging is not implemented

Bucket logging and the related `LogDelivery` compatibility surface are not
implemented yet.

This also means the logging-specific ACL compatibility work remains deferred.

Related notes:

- [plans/completed/ceph-closeout-plan.md](../plans/completed/ceph-closeout-plan.md)
- [plans/completed/acl-compatibility-follow-up-plan.md](../plans/completed/acl-compatibility-follow-up-plan.md)

### 4. Bucket website support is only partial

Argmin now supports the REST object-metadata surface for
`x-amz-website-redirect-location` / `WebsiteRedirectLocation`, including
request parsing, persistence, readback, copy, multipart, POST, and the
corresponding AWS error shapes.

The remaining website-related gaps are:

- bucket website configuration APIs such as `PutBucketWebsite`,
  `GetBucketWebsite`, and `DeleteBucketWebsite`
- broader website-endpoint hosting behavior

In practice, treat bucket website hosting as unsupported. The implemented
piece here is limited to object redirect metadata on the normal S3 REST
endpoint, not the website endpoint or bucket website configuration APIs.

Related note:

- [plans/website-behavior-note.md](../plans/website-behavior-note.md)

### 5. Alternate AWS access URL forms are not supported

Argmin currently targets path-style access on the normal API endpoint.

Argmin currently exposes a single S3 API endpoint. It does not implement the
separate AWS `s3-control` endpoint family, and it does not currently inspect or
differentiate endpoint-style host headers beyond the normal S3 API surface.

Current narrow exception:

- the minimal bucket-ABAC `TagResource` / `UntagResource` subset is currently
  accepted on the same endpoint as the rest of the S3 API
- AWS itself expects those operations on the account-prefixed control-plane
  host `https://{account_id}.s3-control.{region}.amazonaws.com`
- Argmin does not yet enforce that distinct AWS `s3-control` `Host` /
  endpoint shape or AWS's documented HTTPS-only S3 Control transport for those
  operations and still accepts them on the ordinary S3 endpoint, including an
  ordinary plain-HTTP listener
- local raw-listener tests establish the security boundary for that temporary
  shared endpoint: changing accepted `Host` text, TLS SNI, or their relationship
  does not select a different parser, while malformed HTTP authority and normal
  certificate-name mismatches may be rejected before request classification
- this is a temporary compatibility compromise that will be removed when the
  dedicated typed `s3-control` routing surface exists locally

Unsupported URL/addressing forms include:

- website endpoints
- DNS / virtual-hosted-style bucket addressing
- `s3-control` endpoints and their distinct host/endpoint routing model
- other AWS endpoint variants that depend on bucket-in-host routing

Related note:

- [guides/threat_model.md](threat_model.md)

### 6. Billing surfaces are not implemented

Argmin does not implement AWS billing behavior.

This is broader than one missing header or bucket flag. Matching AWS here would
require a real billing model with charge attribution, owner-versus-requester
cost semantics, and the corresponding request / response behavior across the
affected APIs. That infrastructure is not currently planned.

#### Requester Pays

Requester Pays is therefore also not implemented.

This remains a real AWS compatibility gap, but it does not make sense as a
standalone feature without the wider billing surface behind it.

The missing wire-visible surface includes:

- bucket requester-pays configuration
- `x-amz-request-payer: requester`
- `x-amz-request-charged: requester`

### 7. MFA Delete is not implemented

Argmin supports the basic bucket versioning state (`Enabled` / `Suspended`),
but it does not implement AWS's MFA Delete surface.

That means the current gap includes both the bucket-versioning control path and
the delete path that AWS protects with the same `x-amz-mfa` header family:

- `GetBucketVersioning` does not expose the `MfaDelete` response element
- `PutBucketVersioning` does not implement `MfaDelete` request handling or the
  required `x-amz-mfa` header flow for changing MFA Delete state
- `DeleteObjects` does not implement the optional `x-amz-mfa` header behavior
  used by AWS when MFA Delete is enabled on the target bucket

AWS documents this as part of the bucket versioning configuration rather than a
separate API surface:

- `GetBucketVersioning` returns `MfaDelete` when the bucket has been configured
  with MFA Delete
- `PutBucketVersioning` requires `x-amz-mfa` plus both `Status` and
  `MfaDelete` when enabling MFA Delete

Sources:

- https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html

### 8. Lifecycle transition-default header is not implemented

Argmin does not currently implement lifecycle transition behavior, and it also
does not implement the corresponding
`x-amz-transition-default-minimum-object-size` request surface on
`PutBucketLifecycleConfiguration`.

Today:

- `PutBucketLifecycleConfiguration` ignores that modeled request header because
  transition semantics are not implemented
- `GetBucketLifecycleConfiguration` deliberately does not return the
  `x-amz-transition-default-minimum-object-size` header. AWS returns
  `all_storage_classes_128K` by default, but emitting a fixed value without
  the ability to configure it (or any transition behavior behind it) would
  advertise an unsupported setting.

Treat this header as missing until lifecycle transition behavior is
implemented end-to-end. Response-shape tests for
`GetBucketLifecycleConfiguration` accept both the AWS response (with the
header) and the Argmin response (without it).

Source:

- https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html

### 9. Transport framing is not part of the compatibility surface

AWS serves the same response sized with `Content-Length` or with
`Transfer-Encoding: chunked` depending on which frontend fleet handles the
request (observed flapping on `GetBucketOwnershipControls` in July 2026),
and includes `Transfer-Encoding: chunked` on `HeadBucket` where Hyper's
`HEAD` handling suppresses it locally.

Because the framing carries no S3 semantics, golden shape tests ignore
`content-length` and `transfer-encoding` entirely unless a test pins one
explicitly. Pinning is reserved for responses where the value is semantic,
e.g. `content-length` equals the payload size on data GET/HEAD responses.
Argmin makes no effort to mirror AWS's framing choice per operation.

### 10. SigV2 is not implemented

Argmin does not implement AWS Signature Version 2 authentication.

That is intentionally not part of the current auth surface, but it is still a
real compatibility gap because AWS continues to accept SigV2 in at least some
older regions even though newer regions require SigV4.

In practice:

- Argmin requires SigV4 everywhere
- AWS still accepts SigV2 in some regions such as `us-east-1` and `us-west-2`
- AWS rejects SigV2 in newer regions such as `eu-central-1`

Related note:

- [plans/sigv2-compat-note.md](../plans/sigv2-compat-note.md)

### 11. Account-level Block Public Access is not implemented

Argmin supports the bucket-level public access block surface needed by the
current feature set, but it does not implement AWS account-level Block Public
Access controls yet.

That means account-scoped public-access restrictions and their interaction with
bucket ACL and public-write behavior can still differ from AWS.

Related plan:

- [plans/aws-auth-compat-plan.md](../plans/aws-auth-compat-plan.md)

### 12. The `s3-control` API family is not implemented

Argmin implements the normal S3 API/data-plane endpoint and only a very narrow
bucket-ABAC subset of `s3-control`.

That means the following AWS surface is currently unsupported:

- `s3-control` endpoint routing and host-style distinctions
- broader `s3-control` surfaces such as access-point, multi-region access
  point, Storage Lens, batch operations, and other account/control-plane APIs

Current narrow exception:

- `TagResource` / `UntagResource` for the bucket-ABAC general-purpose-bucket
  flow are implemented
- AWS-pinned control-plane behavior uses the account-prefixed host
  `https://{account_id}.s3-control.{region}.amazonaws.com`
- locally they are still accepted on the ordinary S3 endpoint without distinct
  `s3-control` host or HTTPS-only transport validation
- this is intentionally temporary and will be removed by the typed
  `s3-control` routing work

In practice, any AWS behavior that depends on `s3-control` APIs, endpoint
routing, or control-plane state such as `TagResource` / `UntagResource` should
be treated as unsupported apart from this narrow bucket-ABAC tagging subset.

### 12. Bucket-policy condition support is broad but still partial

Argmin recognizes AWS-documented bucket-policy condition keys on the
implemented S3 surface, but deliberately rejects keys that are not yet
runtime-evaluable locally. This is an intentional AWS divergence: AWS may store
some policies that Argmin rejects with `MalformedPolicy`, because silently
storing a policy that cannot be enforced exactly would make users think it is
working.

The important split is between condition operators and condition keys. The AWS
IAM operator reference groups operators into string, numeric, date/time,
boolean, binary, IP address, ARN, `...IfExists`, and `Null` families. The AWS
S3 service-authorization reference separately lists the S3-specific condition
keys and their value types.

Today, two compatibility questions have to be kept separate:

- policy upload acceptance
- runtime evaluation of a stored policy against a live request

Policy upload acceptance is being aligned to AWS-backed behavior. Runtime
evaluation is still narrower.

Current runtime-evaluation limits include:

- network/account condition keys such as `aws:SourceVpc`, `aws:SourceVpce`,
  `aws:SourceArn`, `aws:SourceAccount`,
  `aws:SourceOwner`, `aws:PrincipalOrgID`, `aws:PrincipalAccount`,
  `aws:PrincipalIsAWSService`, `aws:VpcSourceIp`, `aws:ResourceAccount`,
  `s3:DataAccessPointAccount`, and `s3:DataAccessPointArn` still remain
  outside the current accepted/evaluable object-condition subset
- S3 request-context keys that require resource-owner account context, such as
  `s3:ResourceAccount`, are still deferred
- S3 object-lock condition keys such as `s3:object-lock-mode`,
  `s3:object-lock-legal-hold`, `s3:object-lock-retain-until-date`, and
  `s3:object-lock-remaining-retention-days` are not yet runtime-evaluable
- access point, access grants, storage lens, batch job, replication, annotation,
  and KMS-specific condition keys remain outside the current general-purpose
  bucket/object API subset

The current implemented evaluator is strongest on:

- `s3:ExistingObjectTag/*`
- `s3:BucketTag/*`
- `aws:ResourceTag/*`, as bucket tags on the currently supported bucket-ABAC
  surface
- `s3:RequestObjectTag/*`
- `aws:RequestTag/*`
- `s3:RequestObjectTagKeys`
- `aws:TagKeys`
- `aws:SourceIp`
- `aws:CurrentTime`
- `aws:EpochTime`
- `aws:SecureTransport`
- `aws:RequestedRegion`
- `aws:referer`
- `s3:authType`
- `s3:signatureversion`
- `s3:signatureAge` for presigned query and POST requests
- `s3:TlsVersion` when the request is served over TLS
- `s3:x-amz-content-sha256`
- `s3:x-amz-website-redirect-location`
- the supported `s3:x-amz-*` request condition keys already threaded through
  `PolicyRequest`

Current supported condition operators are:

- `Bool`, `BoolIfExists`
- `ForAllValues:Bool`, `ForAnyValue:Bool`
- `Null`
- `StringEquals`, `StringEqualsIfExists`
- `StringEqualsIgnoreCase`, `StringEqualsIgnoreCaseIfExists`
- `StringNotEquals`, `StringNotEqualsIfExists`
- `StringNotEqualsIgnoreCase`, `StringNotEqualsIgnoreCaseIfExists`
- `StringLike`, `StringLikeIfExists`
- `StringNotLike`, `StringNotLikeIfExists`
- `ForAllValues:StringEquals`, `ForAnyValue:StringEquals`
- `ForAllValues:StringEqualsIgnoreCase`,
  `ForAnyValue:StringEqualsIgnoreCase`
- `ForAllValues:StringNotEquals`, `ForAnyValue:StringNotEquals`
- `ForAllValues:StringNotEqualsIgnoreCase`,
  `ForAnyValue:StringNotEqualsIgnoreCase`
- `ForAllValues:StringLike`, `ForAnyValue:StringLike`
- `ForAllValues:StringNotLike`, `ForAnyValue:StringNotLike`
- `BinaryEquals`, `BinaryEqualsIfExists`
- `ForAllValues:BinaryEquals`, `ForAnyValue:BinaryEquals`
- `DateEquals`, `DateEqualsIfExists`
- `DateNotEquals`, `DateNotEqualsIfExists`
- `DateLessThan`, `DateLessThanIfExists`
- `DateLessThanEquals`, `DateLessThanEqualsIfExists`
- `DateGreaterThan`, `DateGreaterThanIfExists`
- `DateGreaterThanEquals`, `DateGreaterThanEqualsIfExists`
- `NumericEquals`, `NumericEqualsIfExists`
- `NumericNotEquals`, `NumericNotEqualsIfExists`
- `NumericLessThan`, `NumericLessThanIfExists`
- `NumericLessThanEquals`, `NumericLessThanEqualsIfExists`
- `NumericGreaterThan`, `NumericGreaterThanIfExists`
- `NumericGreaterThanEquals`, `NumericGreaterThanEqualsIfExists`
- `IpAddress`, `IpAddressIfExists`
- `NotIpAddress`, `NotIpAddressIfExists`
- `ForAllValues:IpAddress`, `ForAnyValue:IpAddress`
- `ForAllValues:NotIpAddress`, `ForAnyValue:NotIpAddress`

Compared with the IAM condition-operator reference, unsupported operator
forms are:

- ARN operators: `ArnEquals`, `ArnLike`, `ArnNotEquals`, `ArnNotLike`, and
  their applicable set/`IfExists` forms. These are intentionally deferred
  rather than stubbed because the interesting S3 ARN condition keys mostly
  depend on service-to-service, access point, KMS, or delivery-source context
  that Argmin does not yet authenticate or model.

AWS documents generic set-operator semantics for multivalued context keys. The
implemented set-qualified operators are the forms already needed by the current
AWS-pinned S3 bucket-policy surface; any additional set-qualified numeric,
date, or binary forms should be added only with AWS-facing coverage for the
specific S3 condition key that exposes multiple values.

`BinaryEquals` decodes the policy operand and request-context value as
standard base64 and compares the resulting bytes exactly. It does not perform
string wildcard matching.

String wildcard operators treat `*` as a multi-character wildcard and `?` as
a single-character wildcard. Policy-variable escapes such as `${*}` and `${?}`
produce literal `*` and `?` characters instead of wildcard tokens.

`aws:CurrentTime` is evaluated against a single timestamp captured for the S3
request with date comparison operators, including fractional seconds accepted
by AWS up to nanosecond precision. `aws:EpochTime` exposes the same timestamp
as Unix epoch seconds for numeric operators. AWS accepts malformed date
operands in stored bucket policies; invalid operands are therefore handled at
evaluation time rather than rejected by `PutBucketPolicy`.

`aws:SecureTransport` is derived from the actual listener transport,
`aws:RequestedRegion` is the configured S3 endpoint region, and `aws:referer`
uses the request `Referer` header with normal absent-key condition semantics.
`s3:authType`, `s3:signatureversion`, `s3:signatureAge`, `s3:TlsVersion`, and
`s3:x-amz-content-sha256` are evaluated from the authenticated request context.
AWS exposes `s3:signatureAge` for presigned query and POST authentication;
header-auth requests treat that key as absent.

`aws:PrincipalArn` is evaluated with the AWS-pinned `ArnEquals` operator. An
assumed-role request exposes the path-bearing IAM role ARN rather than its STS
session ARN. A configured principal exposes an IAM user ARN only after its
syntax and account binding have been validated. Opaque configured principals
cannot satisfy an allow and make a dependent deny fail closed.

Policy variables are implemented only for request-backed values that Argmin
can model exactly. Variables are expanded in `Resource` patterns and string
condition values, but not in numeric, date, boolean, binary, IP, or `Null`
condition values.

Assumed-role identity context now resolves `aws:userid` as the stable role ID
plus session name, `aws:PrincipalType` as `AssumedRole`, and
`aws:TokenIssueTime` from the immutable authenticated session lifetime.
Anonymous principal type/user ID behavior also remains available. IAM-user
stable IDs, `aws:username`, principal tags, session tags, and other richer IAM
variables remain unresolved; Argmin leaves those values unavailable rather
than guessing from a visible principal string. ABAC policies that depend on
those values remain a compatibility gap.

References:

- AWS IAM condition operators:
  <https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html>
- AWS IAM global condition keys:
  <https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html>
- AWS S3 condition keys:
  <https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-policy-keys>
- AWS IAM policy variables:
  <https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html>

The local condition-key inventory is a dated documentation snapshot, not a
claim that AWS will never add keys. Future AWS additions should be treated as
snapshot drift: classify the new key explicitly, then either implement exact
runtime evaluation or reject it as recognized-but-unsupported until the
necessary request/resource context is modeled.

Related plan:

- [plans/aws-auth-compat-plan.md](../plans/aws-auth-compat-plan.md)

### 13. `AccessDenied` does not yet match AWS principal-specific error text

Argmin now matches the generic XML error shape for several `AccessDenied`
cases, but it does not yet reproduce AWS's more specific denial messages that
name the requester principal and the denied action/resource.

In practice this means anonymous/private-object `AccessDenied` responses are
covered, but authenticated denials that AWS renders with caller-specific IAM
or account details may still differ in `<Message>`.

A concrete probed example: requesting governance bypass without permission
(`x-amz-bypass-governance-retention: true` while a bucket policy denies
`s3:BypassGovernanceRetention`) returns, on AWS,

```
User: arn:aws:iam::<account>:user/<name> is not authorized to perform:
s3:BypassGovernanceRetention on resource: "arn:aws:s3:::<bucket>/<key>"
with an explicit deny in a resource-based policy
```

while Argmin returns the generic `Access Denied` message with the same 403
status and `AccessDenied` code. Object-lock *protection* denials (retention
shortening or delete without bypass requested) are a distinct case and do
match AWS exactly: `Access Denied because object protected by object lock.`

This should be revisited as part of durable account and credential work,
because exact matching depends on carrying richer persistent account identity
through to error rendering.

Related plan:

- [plans/persistent-account-and-credential-management.md](../plans/persistent-account-and-credential-management.md)

### 14. Bucket-policy principal validation is format-level only

`PutBucketPolicy` rejects `AWS` principal entries whose IAM ARN qualifier is
not one AWS accepts (`root`, `user/<name>`, `role/<name>`), with the
AWS-shaped `MalformedPolicy` error (`Invalid principal in policy` plus a
`<Detail>` echoing the entry). AWS additionally rejects well-formed
principals that do not exist — probed example: a bare nonexistent account ID
gets the same error. Existence depends on the account universe, so Argmin
does not check it; a policy naming a well-formed but unknown account is
accepted locally and rejected by AWS. The golden tests pin only
format-invalid principals so they hold on both endpoints.

## How to treat new differences

If AWS-backed tests expose a difference that is not listed here:

1. treat it as a bug first
2. verify against real AWS behavior
3. either fix it or document it here as an intentional temporary gap

The goal is to keep the undocumented compatibility surface as small as
possible.
