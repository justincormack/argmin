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

### 1. Validate header SigV4 alternatives

- Probe AWS with `x-amz-content-sha256` present but omitted from
  `SignedHeaders`, for both a hashed payload and `UNSIGNED-PAYLOAD`.
- Probe AWS with ISO-8601 basic `Date`-only signing, both date headers agreeing,
  and both date headers disagreeing.
- Include ordinary and `aws-chunked` requests. Use one selected signing
  timestamp for skew validation, the seed signature, and every chunk and trailer
  signature; do not allow those paths to select timestamps independently.
- Record the AWS outcomes before deciding whether the apparent local rejection
  paths are conformance defects.
- Add matching local integration tests and change header verification only to
  the extent required by the observed AWS behavior.

### 2. Pin authentication ambiguity and parser grammar

- Test simultaneous header/presigned-query and header/POST-policy
  authentication with an explicit precedence matrix:
  - both mechanisms independently valid
  - header valid and the other mechanism invalid
  - header invalid and the other mechanism valid
  - both valid under different principals, with only one principal authorized
    for the requested operation
- Use the different-principal cases to make the selected mechanism observable;
  do not infer precedence from an otherwise ambiguous `200` or `403`.
- Test duplicate and unknown Authorization attributes; empty, duplicate,
  unsorted, and non-lowercase `SignedHeaders`; and duplicate presign auth query
  parameters.
- Record exact AWS status, error code, relevant error details, and effective
  principal before changing parser acceptance or precedence.

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
