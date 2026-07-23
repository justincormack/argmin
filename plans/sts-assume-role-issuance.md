# STS AssumeRole Issuance

## Status

This is the active, deliberately narrow STS plan. It replaces the rejected
in-memory STS and IAM foundations plan.

The initial shared test structure and first `AssumeRole` success cases are
already present. Those tests run unchanged against AWS and the embedded local
STS endpoint. They cover the default duration, an explicit 900-second duration,
first-value handling for duplicate scalar parameters, and the successful
response shape.

The older `sts_oracle` binary is retained temporarily as a reference for
observed AWS behavior. Its cases are not accepted implementation coverage until
they have been moved into shared tests that run against both AWS and the local
server.

## Working Rules

- Add permanent shared AWS/local tests for every behavior in this plan. Do not
  use ad hoc probes or copy an oracle expectation into a local-only test.
- Lock complete semantic response shapes, including status, headers, and XML.
- Keep protocol parsing, typed request validation, trust evaluation, and
  credential issuance as distinct boundaries.
- Build on the existing general authentication and identity-provider seams. Do
  not introduce credential-kind branches into S3 or server-core authorization.
- Keep use of issued credentials on S3 fail-closed throughout this plan. S3
  authorization with role sessions requires a later plan and its own positive
  AWS/local conformance tests.
- Complete and review one bounded slice before starting the next. If AWS
  exposes additional branches, add shared tests and update this plan before
  implementation.

## Initial Scope

### 1. RoleSessionName validation

Add shared `AssumeRole` tests for:

- missing, empty, and one-character values
- an invalid character such as `/`
- the accepted two-character minimum
- the accepted 64-character maximum
- a rejected 65-character value

Rejected cases must lock the complete AWS error response. Accepted boundaries
must use the existing credential and success-envelope assertions.

Parse the field into the existing typed `RoleSessionName` before trust
evaluation or issuance, with protocol validation failures mapped to the exact
AWS responses.

### 2. DurationSeconds parsing and bounds

Add shared tests for absent and accepted values, malformed numeric input, the
900-second minimum, values below the minimum, the target role's configured
maximum, and values above that maximum. Include signed and overflow boundaries
where AWS distinguishes validation errors from malformed input.

Represent the parsed duration with a type that cannot contain an invalid
session lifetime. Keep generic Query numeric parsing separate from validation
against the selected role's maximum session duration.

### 3. RoleArn validation and authorization denial

Add shared tests for missing, empty, structurally invalid, unknown, and valid
role ARNs. Add fixture roles as needed to distinguish an unknown role from a
known role whose trust policy denies the caller.

Lock AWS validation and authorization ordering as well as the complete denial
responses. Continue binding the authorization record to the complete live role
identity before sealing credentials.

### 4. ExternalId and trust-policy conditions

Add shared accepted and rejected `ExternalId` grammar and length boundaries,
then add roles whose trust policies require or reject specific external IDs.
The parsed request context must feed the ordinary typed trust-policy evaluator;
the HTTP handler must not implement a parallel condition evaluator.

Do not silently ignore `ExternalId` while it can affect authorization.

### 5. Planning gate for remaining AssumeRole optional parameters

This item is a planning gate, not an implementation slice. Do not implement
additional optional parameters under this item.

Before expanding the implementation surface beyond slices 1–4, inventory every
remaining `AssumeRole` parameter covered by the AWS oracle and public API
surface. At minimum, the inventory must separately cover:

- inline session policy and managed-policy ARN inputs
- session tags
- transitive tag keys
- source identity
- provided contexts
- MFA serial number and token code

Group only parameters that share parsing, validation, authorization, and
issued-session semantics. For each group, amend this plan with a bounded,
reviewed implementation slice before writing production code.

Each resulting slice must define:

- accepted values, validation boundaries, duplicate-member behavior, and
  interaction with trust evaluation
- the AWS response for unsupported security-relevant input, which must be
  rejected rather than accepted and ignored
- any required issued-credential-envelope extension through typed,
  authenticated session facts

## Initial Completion Boundary

The currently selected implementation surface is exactly slices 1–4:
`RoleSessionName`, `DurationSeconds`, `RoleArn` validation and authorization,
and `ExternalId`. Those slices are complete only when they are covered by
shared AWS/local tests and none of those accepted parameters is silently
ignored.

The initial planning scope is complete when slices 1–4 are implemented and the
item 5 inventory has been reviewed and converted into explicitly bounded plan
slices. The newly planned optional-parameter slices are not implicitly complete
at that point and may define later milestones.

Completion does not authorize role sessions to use S3. A later plan must
introduce that support through the normal authentication and authorization
architecture, one independently tested behavior at a time.

## Out of Scope

- S3 or S3 Control authorization with issued credentials
- credential-kind-specific S3 behavior
- other STS operations such as `GetCallerIdentity`
- durable IAM storage or a production IAM administration API
- federation, SAML, web identity, or Identity Center
- replacing the temporary `sts_oracle` binary before its useful cases have
  moved into shared tests
