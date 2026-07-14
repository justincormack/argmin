# AWS Authentication Oracle Coverage Plan

## Goal

Close the authentication compatibility gaps found in the July 2026 review by
pinning every relevant behavior against live AWS S3 before changing the local
implementation.

AWS documentation and SDK behavior are useful inputs, but neither is the final
compatibility oracle. Each behavioral item below must begin with a raw AWS test
that records the actual status, error code, relevant response details, and
success semantics. Only then should the matching local integration test and
implementation change be made.

STS and temporary session credentials are out of scope. They are not currently
implemented, so their AWS behavior should be tested as part of the future
temporary-credential work rather than this plan.

## Work

### 1. Validate header SigV4 alternatives — completed 2026-07-13

Live AWS returned success for all of the following, now pinned by matching local
integration tests:

- `x-amz-content-sha256` present but omitted from `SignedHeaders`, with both a
  hashed payload and `UNSIGNED-PAYLOAD`
- a transmitted unsigned `x-amz-content-sha256` value that differs from the
  signed canonical payload hash returns `SignatureDoesNotMatch` and creates no
  object
- ISO-8601 basic `Date`-only signing
- agreeing `Date` and `x-amz-date` values
- disagreeing date headers, with `x-amz-date` taking precedence
- Date-only `aws-chunked` payload and signed-trailer requests

Header verification now matches those results. One selected signing timestamp
drives skew validation, the seed signature, and chunk/trailer verification.

### 2. Pin authentication ambiguity and parser grammar — completed 2026-07-13

Live AWS established the following behavior, now pinned by matching local
integration tests:

- A request containing both an Authorization header and presigned-query auth
  is rejected before either signature or principal is selected. All validity
  and opposite-account principal combinations return `400 InvalidArgument`,
  identify `Authorization`, echo its value, and report that only one auth
  mechanism is allowed.
- The proposed independently-valid header/POST-policy matrix cannot exist:
  multipart POST Object does not accept SigV4 Authorization-header auth even
  when it is the only authentication mechanism. A present
  `x-amz-content-sha256` produces `400 InvalidArgument` with that argument name
  and no argument value; an omitted header produces the existing missing-header
  `InvalidRequest`. The mixed validity and opposite-account POST matrix reaches
  the same payload-header rejection before POST-policy auth is selected.
- Authorization attributes are strict. Unknown attributes or duplicate
  `Credential`, `SignedHeaders`, or `Signature` attributes return
  `400 AuthorizationHeaderMalformed` with the three-required-components
  message. An empty `SignedHeaders` value has AWS's component-specific message.
- Names inside header `SignedHeaders` are lowercased, sorted, and deduplicated
  before canonicalization. Duplicate, unsorted, and non-lowercase variants of
  an otherwise valid request all authenticate successfully.
- Duplicate presign auth query parameters are accepted and the first value on
  the wire is authoritative. Identical duplicates authenticate; a conflicting
  second value is ignored; a conflicting first value controls parsing, expiry,
  signed-header canonicalization, or signature verification as applicable.
- Presigned canonicalization represents a listed but absent signed header with
  an empty value and reaches signature verification. It does not fail early as
  a missing signed header.
- Presigned time-window rejection precedes the check that `X-Amz-Date` agrees
  with the credential date scope.

### 3. Pin time and error precedence

- Use comfortably inside/outside accepted and rejected margins for stable AWS
  CI coverage of header and POST signing time.
- Keep exact clock-skew-boundary probes documentary or repeated rather than
  making them single-shot CI assertions, because AWS server time, transit time,
  and timestamp precision make the exact boundary unstable.
- Probe POST requests with an unexpired policy but old or future
  `x-amz-date` values using the same stable-margin rule.
- Add a small AWS cross-product covering bad scope, unknown credentials, bad
  signatures, skew, and token inputs supplied with static credentials so error
  precedence for the implemented AWS surface is explicit.
- Test locally configured static-record expiry separately. Do not add STS,
  temporary-credential, or session-token-expiry cases to the AWS matrix.

### 4. Strengthen authorization identities used by the oracle

- Commit and document the IAM policy expected for the same-account constrained
  `SECOND` user.
- Before asserting denials, verify the expected account and principal and run at
  least one operation that the constrained user is explicitly allowed to
  perform. Treat failure of this canary as a fixture failure, not a successful
  authorization denial.
- Extend its negative AWS matrix across multipart control operations, copy
  source and destination authorization, deletion, tagging/ACL operations, and
  Object Lock governance operations.
- Keep these tests focused on preventing accidental `Standard`-principal
  elevation to owner-account-admin behavior.

### 5. Repair coverage documentation

- Remove or replace the stale `access_matrix` command in
  `guides/security-testing.md`.
- Correct `plans/aws-auth-compat-plan.md` so temporary/session credentials are
  not described as implemented.
- List `headers`, `presigned`, `post_object`, and `chunked` as the AWS-backed
  authentication oracle surface.

## Completion Criteria

- Every behavioral item begins with a raw AWS result; no expected behavior is
  inferred solely from AWS documentation, an SDK, or the current local
  implementation.
- Every resulting compatibility rule in the implemented credential surface has
  a matching local integration test.
- Header SigV4 matches the observed AWS payload-header and date-header behavior,
  including any differences from the documentation.
- Multiple-auth, malformed grammar, time-boundary, and error-precedence behavior
  is no longer inferred from local code.
- The constrained-user fixture is reproducible from committed IAM policy.
- The targeted `./scripts/aws-tests --test ...` runs for `headers`, `presigned`,
  `post_object`, `chunked`, `object_write_constrained`, and `boe_constrained`
  pass against AWS.
- `cargo nextest run`, formatting, and clippy are clean.
