# Stateless STS And In-Memory IAM Foundations

## Status

Proposed plan. The first implementation target is a test-enablement vertical
slice, not a production identity service.

The first public STS operation will be `AssumeRole`. It will be exposed on the
existing HTTP listener and backed by process-local, in-memory role state. Issued
session credentials will be stateless: the session token will be a sealed,
self-contained credential and authorization envelope rather than a key into an
issued-session table. The design must nevertheless preserve AWS identity,
signing, authorization, expiry, and error semantics on the implemented surface.
The in-memory and non-replicated nature of the first role backend and token key
ring is a deployment limitation, not permission to invent different API
behavior.

## Motivation

Argmin's S3 compatibility tests cannot currently exercise temporary security
credentials against the local server. The auth crate has an expiry field on a
credential record, but every supplied session token is deliberately rejected
because credential records do not carry an expected token and there is no STS
issuer.

Starting with STS also gives us a useful forcing function for the identity work
that is already missing elsewhere:

- authoritative user, role, and role-session principal data
- identity-based policies for account-scoped operations such as
  `s3:CreateBucket` and `s3:ListAllMyBuckets`
- role trust policies and `sts:AssumeRole` authorization
- request context such as `aws:PrincipalArn`, `aws:userid`, principal type,
  source identity, and principal/session tags
- principal existence checks when validating resource policies
- eventual replacement of the coarse `AuthorizationProfile` model

This plan is separate from
[persistent-account-and-credential-management.md](persistent-account-and-credential-management.md).
That plan owns durable accounts and long-lived credentials. This plan owns the
initial STS/IAM semantics and deliberately starts with volatile state. The
interfaces introduced here should allow the durable plan to replace the
backend without rewriting SigV4 or S3 authorization.

## Goals

The first usable milestone must:

1. Accept AWS Query protocol `AssumeRole` requests through the same listener as
   S3, signed with SigV4 service name `sts`.
2. Issue SDK-compatible, Argmin-namespaced access key IDs, plus secret access
   keys, session tokens, expiry timestamps, assumed-role IDs, and assumed-role
   ARNs. Argmin-generated credentials must not look like credentials issued by
   AWS.
3. Accept the issued temporary credentials on all existing S3 SigV4 paths:
   header auth, presigned query auth, POST Object, and aws-chunked streaming.
4. Require the exact session token and reject missing, wrong, unexpected, and
   expired tokens with AWS-compatible ordering and response shapes.
5. Give the role session the permissions of the configured role, restricted by
   any supported session policy, after applying AWS's same-account or
   cross-account `AssumeRole` authorization rules. Do not emulate this by
   copying the caller's `AuthorizationProfile` or granting blanket same-account
   access.
6. Seal all session authentication and session-specific authorization data into
   a versioned authenticated token, so issuance creates no per-session server
   record.
7. Share the in-memory role provider and session-token sealing key ring across
   every HTTP worker in the process so a token can be used immediately on any
   worker.
8. Keep roles and the sealing key ring process-local and non-replicated for now,
   while making that limitation explicit in configuration, status, tests, and
   documentation.
9. Add AWS-backed conformance coverage before relying on assumptions from the
   public documentation.

## Initial Non-Goals

The first vertical slice does not need:

- durable or replicated roles, users, policies, or token-sealing keys
- cross-process token validation
- migration support for an older identity schema
- the complete IAM or STS API families
- federation, SAML, web identity, Identity Center, or OIDC
- console federation
- MFA devices
- production credential rotation, audit history, quotas, or administrative UI
- a general configuration-file identity backend

These are scope boundaries, not silent compatibility exceptions. Unsupported
actions and parameters must receive an AWS-shaped error and remain listed as
open work. Supported actions must not ignore security-relevant input.

## Current Repository State

### Long-lived credential and role lookup is static and worker-local

`auth::CredentialStore` is a plain `HashMap<String, CredentialRecord>`.
`argmin-s3` builds a new copy for every `HttpFrontend` worker, and the embedded
`s3-tests` server does the same. Stateless session issuance does not require
mutating these maps, but dynamic in-memory IAM operations will still require a
shared role/user/long-lived-credential provider. Every worker must also share
the same token-sealing key ring.

### Credential records cannot represent temporary credentials

`CredentialRecord` currently contains:

- access key ID
- secret key
- account identity
- coarse authorization profile
- optional expiry
- enabled state

It has no way to obtain a temporary credential from a sealed token and no typed
distinction between a stored long-lived credential and a decoded temporary
credential. Auth currently checks an optional expiry on stored records but then
calls `validate_static_credential_has_no_token` on header, presigned, and POST
paths. An expiring stored record is therefore not a usable STS credential.

### Identity is too coarse for roles and sessions

`AccountIdentity` carries one principal string, canonical user ID, and display
name. A role session needs at least these distinct concepts:

- owning account
- IAM role ARN and stable role ID
- STS assumed-role ARN and assumed-role ID
- role session name
- source/caller identity
- principal type and `aws:userid` value
- optional session and principal tags

Overloading the existing principal string would make policy matching and error
rendering ambiguous.

### S3 has resource policies but not IAM identity policies

The bucket-policy evaluator models a useful subset of resource-policy actions,
principals, resources, conditions, and explicit deny. There is no equivalent
identity-policy attachment/evaluation path for users or roles, and there is no
trust-policy evaluator for `sts:AssumeRole`.

This means `AssumeRole` cannot be implemented correctly by merely issuing a
credential. The role trust policy and caller identity permissions must combine
according to AWS's same-account/cross-account rules, and the resulting session
needs a real permission decision. `AuthorizationProfile::OwnerAccountAdmin`
must not be used as a shortcut for either.

### HTTP dispatch assumes S3

The normal request path routes to an `S3Operation` and authenticates with
expected service name `s3`. STS uses AWS Query protocol at `/`, normally with
form-encoded parameters and service name `sts`. Service classification must
happen before the S3 router and before choosing the expected SigV4 service.

## AWS Contract To Pin

The public documentation provides the baseline:

- STS API version `2011-06-15`
- `AssumeRole` requires `RoleArn` and `RoleSessionName`
- default duration 3,600 seconds
- documented duration range 900 through 43,200 seconds, further restricted by
  the role maximum and by the one-hour role-chaining limit
- success returns `Credentials`, `AssumedRoleUser`, response metadata, and
  optional policy/source-identity fields
- role sessions use an ARN shaped like
  `arn:aws:sts::<account>:assumed-role/<role-name>/<session-name>`; the IAM
  role path is omitted even though it remains present in the IAM role ARN
- temporary S3 requests must include `X-Amz-Security-Token`

References:

- <https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html>
- <https://docs.aws.amazon.com/STS/latest/APIReference/API_Credentials.html>
- <https://docs.aws.amazon.com/STS/latest/APIReference/CommonParameters.html>
- <https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html>
- <https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html>
- <https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html>
- <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_revoke-sessions.html>

Documentation is not sufficient for exact compatibility. Before implementation,
add focused AWS probes for:

- GET query versus POST form requests and exact content types
- global versus regional signing-region behavior relevant to our local endpoint
- missing, duplicate, malformed, and unknown `Action`/`Version` parameters
- form decoding, repeated members, ordering, empty values, and body size limits
- unknown role, malformed role ARN, trust denial, and caller-policy denial
- a role with a non-root IAM path, proving that the path is present in the IAM
  role ARN but absent from `AssumedRoleUser.Arn`, the session principal ARN,
  and every path-free assumed-role identifier derived from the role name
- duration boundaries, role maximums, and role chaining
- missing, wrong, duplicated, header/query, signed/unsigned, and unexpected
  session-token inputs on every S3 signing mode
- token, expiry, signature, region, and service error precedence
- invalid signatures combined with role deletion and policy changes, separating
  issuer-credential invalidation from mutable authorization behavior; inject
  issuer-liveness and authorization-provider failures independently in the
  equivalent local tests
- exact success XML, namespaces, timestamps, headers, request IDs, error XML,
  status codes, error codes, and messages
- immediate use near issuance and the exact expiry boundary
- resource-policy matching for a role ARN versus an assumed-role session ARN
- the distinct role-session values used for resource-policy principal matching,
  `aws:PrincipalArn`, `aws:userid`, and `aws:TokenIssueTime`, including a
  path-bearing role and a deny policy that revokes sessions issued before a
  boundary

The committed tests should encode observed AWS behavior. If live AWS differs
from documentation, follow the repository's normal rule: tests and the
compatibility guide record the observed behavior.

## Ceph Reference Design

Ceph RGW provides a useful reference for stateless issuance:

- [`src/rgw/rgw_sts.cc`](https://github.com/ceph/ceph/blob/main/src/rgw/rgw_sts.cc)
  generates an access key and secret, then serializes the
  access key, secret, issued-at time, expiration, role ID, role-session name,
  inline session policy, token claims, user/account data, and principal tags
  into the returned session token.
- the serialized token is encrypted and base64 encoded; it is not inserted into
  an issued-session table
- [`src/rgw/rgw_rest_s3.cc`](https://github.com/ceph/ceph/blob/main/src/rgw/rgw_rest_s3.cc)
  decrypts the presented token, checks that its embedded
  access key matches the SigV4 credential, checks the embedded expiry before
  signature verification, uses the embedded secret to verify SigV4, and reloads
  the current role by its embedded role ID for policy evaluation

This demonstrates that server-side session storage is unnecessary. It also
means expiry has not disappeared: there is no stored expiry record, but the
expiry is authenticated as part of the token payload and enforced on each use.

Argmin should adopt the stateless envelope, not Ceph's exact cryptographic
construction. The inspected
[`src/auth/Crypto.cc`](https://github.com/ceph/ceph/blob/main/src/auth/Crypto.cc)
implementation uses AES-128-CBC with a fixed IV and no separate authentication
tag. Argmin should use a versioned AEAD envelope so ciphertext, metadata, and
payload tampering fail authentication before any decoded field is trusted.

## Proposed Architecture

### 1. Split identity data from credential material

Introduce structured public-request identity rather than continuing to add
optional fields to `AccountIdentity`.

The exact Rust types should be settled during the illegal-states review, but
the model needs to distinguish at least:

- `Account`: account ID, canonical user ID, display name
- `IamUser`: user ARN, stable user ID, user name/path, account
- `IamRole`: IAM role ARN, stable role ID, separately stored role name and path,
  and account
- `AssumedRoleSession`: role identity, path-free STS session principal ARN,
  assumed-role ID, session name, issuer/caller, issue/expiry times, source
  identity, and tags
- existing configured/root-style principals used by current tests

`Requester` and policy request context should receive this structured identity.
The model must expose distinct typed accessors rather than one ambiguous
"effective principal ARN":

- the session principal ARN is
  `arn:aws:sts::<account>:assumed-role/<role-name>/<session-name>`, omits the IAM
  role path, and is used where AWS evaluates or reports the assumed-role session
  principal
- the `aws:PrincipalArn` condition value for an assumed-role request is the IAM
  role ARN, including its path; it is not the session principal ARN
- the IAM role ARN, role name, role path, stable role ID, assumed-role ID, and
  `aws:userid` value remain separate fields or typed derivations

Authorization code must not reverse-parse an arbitrary principal string to
discover whether it represents an account, user, role, or session. Resource
policy matching must deliberately request the role principal or session
principal form required by the AWS-pinned rule; denial rendering and audit
identity must likewise select an explicit representation.

The account's canonical user ID remains stable across its users and role
sessions. Tests must independently pin session-principal matching,
role-principal matching, `aws:PrincipalArn`, and denial-message identity instead
of assuming that one ARN is interchangeable across those surfaces.

### 2. Separate stored and decoded credential kinds

Authentication should produce a typed credential along these lines:

- `LongLived(StoredCredential)`
- `Session(DecodedSessionCredential)`

A decoded session credential contains the access key ID, secret access key,
issued-at and expiry times, role/session identity, and session-specific policy
context authenticated by the sealed token. A session credential without an
expiry or role/session identity must be unrepresentable. A stored long-lived
credential must not accidentally acquire session-token semantics.

Secret access keys and session-token material must remain redacted in `Debug`,
logs, errors, traces, status, and panic diagnostics. The raw presented token
should live only in an explicitly redacted request value and be replaced by a
decoded typed credential after successful AEAD opening.

AWS S3's header-auth `SignatureDoesNotMatch` response includes the raw signed
`x-amz-security-token` inside `CanonicalRequest` and includes its hexadecimal
bytes inside `CanonicalRequestBytes`. Argmin must reproduce that client-visible
wire response for compatibility, which makes the error body sensitive even
though the requesting client already possesses the token. Server tracing,
panic diagnostics, and conformance-test failure output must therefore redact
both the literal token and its byte-encoded form before recording the body.
The presigned equivalent includes the URI-encoded token in the canonical query
string and the hexadecimal bytes of that encoded form. Test sanitization must
therefore cover literal, URI-encoded, and both byte-encoded representations.

### 3. Share identity state and token keys, not issued sessions

All frontend workers in a process should hold the same provider handle. The
first implementation can use `Arc` plus a standard-library lock around bounded
in-memory state, with these separable capabilities:

- resolve a long-lived credential for authentication
- resolve accounts/users/roles for policy and principal validation
- evaluate or retrieve attached identity/trust/session policies
- seal and open session credential envelopes using a shared versioned key ring
- inspect bounded, non-secret status

Authentication lookup must return an owned immutable record or `Arc` and
release the identity-state lock before canonicalization, token cryptography, or
HMAC work. STS issuance performs no identity-store mutation: after authorizing
the request, it generates credential material and seals the complete session
payload. No state lock may be held across HTTP work, token sealing/opening,
policy evaluation, storage calls, or response writing.

The lookup API must distinguish `unknown credential` from `provider failure`.
A poisoned or unavailable identity backend must fail closed as an internal
service failure, not be translated into `InvalidAccessKeyId`.

Future persistent providers can implement the same logical capabilities behind
a read-through cache or a different frontend-facing provider. Do not expose the
concrete in-memory lock or `HashMap` through auth APIs.

### 4. Use a versioned AEAD session-token envelope

The token should contain a small versioned envelope and an encrypted payload.
The exact encoding is an implementation decision, but it must include:

- format version and sealing-key ID
- unique random AEAD nonce
- access key ID and secret access key
- issued-at and expires-at instants in a canonical integer representation
- account, role ID, role-session name, and the identity fields needed to
  reconstruct the assumed-role principal
- inline session policy and later source identity/session tags/transitive tags
- any credential-domain binding needed to prevent a token from one Argmin
  deployment being accepted by another deployment that accidentally shares a
  key

Use an authenticated-encryption primitive already available through `ring`
unless implementation review identifies a reason to add another dependency.
The format version, key ID, and credential-domain binding must be authenticated
as associated data or included inside the authenticated ciphertext. Parsing
must reject unknown versions, unknown key IDs, nonce/tag truncation, trailing
bytes, oversized decoded tokens, invalid field encodings, and impossible time
ranges before constructing a session credential.

The initial key ring can contain one random process-start key shared by all
workers. A later configured/persistent key ring should support one active
issuance key plus overlapping validation-only keys for rotation. Status may
expose only format version and non-secret key IDs, never raw key material,
tokens, decoded secrets, or session payloads.

Because issuance stores nothing, there is no issued-session capacity, expiry
scan, or tombstone requirement. Memory remains bounded by the existing request
admission controls plus strict encoded-token and decoded-payload limits.

### 5. Add token-aware authentication once, shared by every SigV4 mode

Credential validation should have one common decision path used by header,
presigned, POST Object, and streaming authentication:

1. extract the access key ID, every mode-relevant session-token input, and the
   signature coverage of each input without logging any of them
2. apply mode-specific structural validation and deterministically select one
   effective token input; represent the presented inputs and selected input as
   distinct typed values so duplicate locations cannot be mistaken for an
   ambiguous credential
3. for presigned requests, reject a present but unsigned
   `x-amz-security-token` as `HeadersNotSigned`; otherwise select a signed
   header token when present, even when an `X-Amz-Security-Token` query value is
   also present, and fall back to the query value only when no signed header
   token is present
4. if the access key ID resolves to a stored long-lived credential, use the
   static path and preserve AWS's post-signature unexpected-token behavior
5. otherwise, require and AEAD-open the selected effective session token
6. validate the embedded access key ID against the SigV4 credential in constant
   time and check credential-domain binding
7. check embedded expiry at the AWS-pinned boundary and resolve only the
   embedded stable issuer-role ID, rejecting a deleted issuer as
   `InvalidClientTokenId`; both checks precede signature comparison, but their
   relative order remains unresolved
8. verify SigV4 with the embedded secret access key
9. return an immutable authenticated-session value containing the structured
   token identity and session-policy context

POST Object and streaming must plug their AWS-pinned selection rules into step
2 rather than adding separate credential-validation pipelines. All presented
token values, not only the selected one, remain sensitive diagnostic inputs:
canonical-error construction and test failure sanitization must account for
every location included in the request.

Authentication must not resolve mutable trust or permission-policy state before
signature verification. AWS probes do require one narrower mutable dependency:
after role deletion converges, credentials issued by that role return
`InvalidClientTokenId`, and this result wins over a bad signature. Token
opening, access-key/domain binding, expiry validation, and issuer-role existence
therefore establish credential validity before signature comparison. The
pre-signature lookup must expose only stable issuer identity/liveness, not role
authorization state.

The existing AWS observations establish separately that expiry and deleted-role
liveness each win over signature mismatch. They do not establish which wins
when an expired session's issuer has also been deleted. Keep that collision
explicitly unresolved until a probe waits through AWS's 900-second minimum
session lifetime; do not infer an order from the independently observed cases.

At the authorization boundary, resolve current trust-independent role
permission state, then combine it with the authenticated session policy and
request context. A changed permission policy or authorization-provider failure
fails closed there. No other mutable role field may move into authentication
without AWS evidence. Phase 0 probes must still pin policy-change behavior, and
the matching local tests must inject issuer-liveness and authorization-provider
failures separately.

The precise error mapping for missing, malformed, wrong-key, tampered, mismatched,
and expired tokens must come from AWS probes. Existing evidence
already says expired temporary credentials win over signature mismatch, while
unexpected token input on a static credential is checked after signature
comparison. Add committed regressions for all paths once real temporary
credentials exist.

Do not conflate the service-facing error mapping with the shared internal
credential-validation decision. The AWS STS Query endpoint maps missing,
mismatched, or invalidated session credentials to `InvalidClientTokenId`; the
S3 SigV4 header path maps the equivalent observed cases to
`InvalidAccessKeyId`. Both can consume one typed invalid-credential result while
rendering the AWS-pinned service-specific response.

A stateless verifier cannot recover expiry or identity when the token is
missing. Phase 0 must pin AWS's missing-token precedence. If Argmin needs to
recognize one of its own issued access key IDs without a token, use a
cryptographically self-identifying access-key encoding or another authenticated
stateless technique; an Argmin namespace prefix alone is only a routing hint and
must not be treated as proof that Argmin issued the key.

Presigned and POST requests need the same semantics as header auth. Streaming
requests must bind the seed request to the temporary credential and must not
drop the validated session identity when constructing streaming signing state.

### 6. Introduce a reusable IAM policy core

Do not fork bucket-policy parsing into unrelated trust and identity evaluators.
Extract or build a shared policy core that can represent the common IAM grammar
while retaining typed wrappers for:

- S3 bucket/resource policies, which contain `Principal`
- role trust policies, which contain `Principal` and authorize STS actions
- identity policies, which omit `Principal`
- inline session policies, which restrict but never expand role permissions

The first evaluator slice only needs actions/resources/conditions required by
the initial tests, but its combination rules must be real:

- explicit deny wins
- a role session's identity permissions come from the role permission policy,
  not from the caller's unrelated S3 permissions
- a session policy is an intersection with the role permission policy, not an
  additional allow source
- the trust policy and caller identity policy for `sts:AssumeRole` combine
  according to AWS's same-account direct-trust, account-delegation, and
  cross-account rules; do not require an identity-policy allow in cases where
  AWS treats a direct same-account trust-policy grant as sufficient
- resource-policy grants, identity-policy grants, ACLs, public access block,
  ownership controls, and current owner/root behavior combine according to the
  AWS-pinned S3 rules

The plan should not claim full IAM policy support until every grammar and
condition family is covered. Unknown or unevaluable security-relevant policy
features must fail closed and must not be accepted into a state that appears to
work.

### 7. Model roles and configured callers as first-class in-memory records

An initial role record should contain:

- owning account identity
- role name/path, ARN, and stable generated role ID
- create/update timestamps needed by later IAM responses
- maximum session duration
- trust policy
- zero or more inline permission policies for the first slice
- role tags if session/principal tag work is in scope for that phase

Configured long-lived callers also need an identity record with typed principal
identity and attached identity policies. The initial fixtures must include an
explicit `sts:AssumeRole` allow over the intended role resource for cases where
AWS requires the caller-side grant, especially cross-account delegation. The
same attachment path should carry the configured caller's S3 permissions during
the migration away from `AuthorizationProfile`; neither caller-side STS
permission nor role-session S3 permission may be inferred from that profile.

Seed at least one same-account caller/role pair and one genuinely cross-account
caller/role pair. Trust policies and caller identity policies must be separate
records so tests can independently remove or deny either side of the
authorization decision.

The embedded test server can receive deterministic constructor-supplied role
fixtures. The standalone UAT binary may use explicitly named UAT-only fixture
configuration, following the existing extra-credential pattern. This is test
setup, not a public account-management API and not the future config-file
backend.

The first public role-management surface comes later through IAM-compatible
Query APIs. There must be no undocumented production environment-variable
format that becomes an accidental long-term role database.

### 8. Add logical service routing before S3 routing

Add a small HTTP-layer service/endpoint classification step before
`S3Operation` routing:

- normal S3 request: existing path, expected SigV4 service `s3`
- STS Query request: Query protocol parser, expected service `sts`
- later IAM Query request: expected service `iam`

For the first slice, STS uses the same bound address, port, TLS configuration,
admission control, request IDs, and worker pool as S3. A custom AWS SDK STS
endpoint URL can therefore point at the existing S3 endpoint.

Classification must not trust only one attacker-controlled hint. Pin and define
the interaction among request path, method, content type, `Action`, `Version`,
Host, and SigV4 credential service. Ambiguous requests must receive the same
error family AWS uses rather than falling through to an unrelated S3 operation.

Do not fold STS operations into `S3Operation`. Introduce a service-level request
enum or equivalent so service-specific parsing, auth expectation, response
format, and errors stay explicit. Host-specific global/regional STS endpoint
parity can later integrate with
[endpoint-routing-compat-plan.md](endpoint-routing-compat-plan.md); the initial
same-listener mode should represent one local regional STS endpoint.

### 9. Implement a bounded AWS Query protocol parser/renderer

STS and IAM use a form/query protocol, not S3 XML request bodies. Add a shared,
bounded parser for the subset rather than ad hoc `split('&')` handling.

It must cover:

- GET query and POST `application/x-www-form-urlencoded` as AWS accepts them
- percent and plus decoding
- duplicate scalar/member behavior
- numbered list/object members
- exact `Action` and `Version` dispatch
- per-field and total request limits
- invalid UTF-8 and invalid percent encodings
- rejection of trailing or unsupported security-relevant parameters

Add a service-specific XML renderer for success and errors, with XML escaping,
AWS namespace/version, request metadata, and deterministic test injection for
request IDs/time. Parsing and rendering belong in focused modules rather than
the already large S3 HTTP dispatcher.

No new production parsing dependency should be added without the normal
dependency discussion. Prefer existing repository primitives if they can meet
the protocol and fuzzing requirements cleanly.

### 10. Generate and seal unpredictable, clearly Argmin-owned session material

Use the repository's existing cryptographic randomness facilities where
possible. Generation needs:

- SDK-compatible session access key IDs in an Argmin-owned namespace, initially
  `ARGS` followed by sufficient random uppercase alphanumeric material
- secret access keys with sufficient entropy and SDK-compatible characters
- unique AEAD nonces and opaque base64-encoded session envelopes with no
  client-visible fixed-size assumption
- stable role IDs and unique assumed-role IDs

Session access key IDs need enough entropy to make collisions negligible without
an issued-key registry. They must also be checked against stored long-lived keys
before issuance. Tests should validate shape, uniqueness sampling, access-key
binding, redaction, nonce uniqueness, and seal/open round trips, but must not
depend on literal random values. A deterministic generator may be injected only
into focused tests; production and UAT issuance must use secure randomness.

Argmin must never emit AWS-owned access-key namespaces such as `AKIA` or `ASIA`.
Secret scanners, breach-test tools, and incident responders use those prefixes
to identify possible AWS credentials; reusing them would create false alarms
and blur credential provenance. AWS SDKs and SigV4 do not require those
prefixes.

When public long-lived key creation arrives, generated long-lived and temporary
keys should use disjoint Argmin-owned namespaces: initially `ARGK` for
long-lived keys and `ARGS` for session keys. The provider must reject a
caller-supplied long-lived key in the reserved temporary namespace. This
prevents a later static-key insertion from shadowing an active stateless session
credential. Namespace and total-length constants should be centralized rather
than repeated across IAM, STS, authentication, and tests. A namespace is a
routing/collision and provenance invariant only; its prefix is not proof that a
credential was issued by Argmin.

## `AssumeRole` Semantics

### Core first slice

The first end-to-end slice should support:

- `RoleArn`
- `RoleSessionName`
- optional `DurationSeconds`
- API `Version=2011-06-15`
- authenticated caller using long-lived credentials
- same-account and cross-account trust decisions for explicitly configured
  fixture roles
- role maximum session duration
- AWS-shaped success and core error responses
- role permission policy applied to subsequent S3 requests

Temporary credentials should be allowed to call `AssumeRole` for role chaining
once the one-hour limit and source/session context propagation are implemented.
Until then, reject chaining explicitly rather than losing caller context.

### Follow-on parameter completeness

Complete the `AssumeRole` surface in AWS-backed slices:

- inline `Policy`
- `PolicyArns.member.N`
- `ExternalId`
- `SourceIdentity`
- `Tags.member.N`
- `TransitiveTagKeys.member.N`
- MFA `SerialNumber` and `TokenCode` once MFA identities exist
- `ProvidedContexts.member.N` only when its trusted-context model can be
  implemented rather than treated as opaque decoration
- packed policy calculation and `PackedPolicySize`
- `PackedPolicyTooLarge`, malformed policy, and tag-related errors

No optional policy, tag, source identity, external ID, or context parameter may
be silently ignored. Each is permission-relevant.

## IAM Roadmap After The First STS Slice

### Role management

Add IAM Query APIs needed to create and inspect the role state that
`AssumeRole` consumes:

- `CreateRole`, `GetRole`, `ListRoles`, `DeleteRole`
- `UpdateAssumeRolePolicy`
- `PutRolePolicy`, `GetRolePolicy`, `ListRolePolicies`, `DeleteRolePolicy`
- role tagging APIs when role/principal tags become evaluable

Deletion and update constraints must match AWS: for example, do not allow role
deletion while attached state that AWS requires callers to remove still exists.

### User and long-lived access-key management

Once tests need dynamic users rather than fixture credentials:

- `CreateUser`, `GetUser`, `ListUsers`, `DeleteUser`
- inline user-policy operations
- `CreateAccessKey`, `ListAccessKeys`, `UpdateAccessKey`, `DeleteAccessKey`
- caller-scoped/default user behavior where AWS supports it

Secret access keys are returned only at creation. The in-memory backend still
loses them on restart by design until the durable credential plan lands.

### Account-scoped S3 IAM decisions

Move `s3:CreateBucket`, `s3:ListAllMyBuckets`, and any pinned `DeleteBucket`
identity-policy behavior out of coarse profiles and into the shared policy
evaluator. Expand action coverage operation-by-operation with AWS tests.

The migration away from `AuthorizationProfile` should be explicit:

1. keep existing configured test credentials working while identity policies
   are introduced
2. represent their broad current behavior as explicit fixture policy, not
   inferred same-account authority
3. remove the profile once all callers use policy-backed identity

### Identity request context and policy fidelity

Use the structured identity model to add, with AWS-backed tests:

- `aws:PrincipalArn`
- `aws:userid`
- `aws:PrincipalType`
- `aws:username`
- `aws:PrincipalAccount`
- `aws:PrincipalTag/*`
- `aws:TokenIssueTime`, sourced from the authenticated issued-at instant in the
  sealed session token and exposed only for temporary credentials
- `aws:SourceIdentity`
- `sts:RoleSessionName`, `sts:ExternalId`, tag, and MFA trust-policy keys
- role ARN versus role-session ARN resource-policy semantics
- principal existence validation during `PutBucketPolicy`
- principal-specific `AccessDenied` messages

Do not derive these from string patterns once authoritative identity records
exist.

The initial role-policy condition matrix must include the date operators needed
for an `aws:TokenIssueTime` explicit deny. This is the AWS mechanism for
revoking permissions from sessions issued before a chosen instant; it must use
the authenticated token issuance time, not request time or mutable role state.

### Additional STS operations

After `AssumeRole`, likely useful operations are:

- `GetCallerIdentity`, which is small and valuable for SDK/test diagnostics
- `GetSessionToken`, which exercises temporary credentials that retain the
  caller's identity rather than assuming a role
- `GetFederationToken` only after federated-user policy semantics exist
- web identity/SAML operations only with real token/assertion validation

Adding an endpoint that merely trusts an unvalidated bearer assertion is not an
acceptable test shortcut.

## Implementation Phases

### Phase 0: AWS oracle and test fixture design

- provision/document tightly scoped same-account and cross-account AWS roles
  trusted by the intended test principals, including role permission policies
  suitable for isolated S3 tests and explicit caller identity-policy grants
  wherever AWS requires them
- give at least one oracle role a non-root IAM path and test that its IAM role
  ARN retains the path while its assumed-role ARN uses only the role name
- add the role ARNs and any account metadata required by the targeted AWS test
  wrapper without weakening the existing broad test-user policies unnecessarily
- add raw HTTP probes for Query protocol and error precedence
- probe authentication precedence independently from mutable role deletion and
  policy-change outcomes; cover injected provider failure in the matching local
  test
- pin session-principal matching separately from `aws:PrincipalArn`, and pin an
  `aws:TokenIssueTime` deny boundary
- decide the exact first supported `AssumeRole` parameter set from evidence
- record success/error golden shapes and token-auth matrices

Exit condition: the committed plan/test expectations do not rely on guessed
wire or precedence behavior.

The initial Query-protocol slice completed on 2026-07-13:

- added the explicitly invoked, read-only `./scripts/aws-sts-oracle` command;
  it signs raw Query requests with service `sts`, uses the existing primary AWS
  test identity, and makes no IAM mutations
- pinned regional AWS behavior for `GetCallerIdentity` over GET and POST,
  including form content type with and without a charset
- pinned missing, empty, unknown, and duplicate `Action`/`Version` behavior;
  duplicate scalar parameters use the first wire value, while an unknown extra
  parameter is ignored for this action
- pinned full golden response bodies and complete normalized semantic header
  sets for the missing-action/no-form-content-type redirect, successful STS XML
  namespace, AWSFault error namespace, exact core `InvalidAction` messages,
  `text/xml` response type, and request-ID agreement

The optional `AssumeRole` security-context parameters, S3 POST/streaming token
matrices, expiry-versus-issuer-deletion precedence,
trust/permission-policy mutation precedence, session-principal context, and
`aws:TokenIssueTime` behavior remain before Phase 0 can satisfy its exit
condition.

The role-fixture capability will use the ordinary primary and alternate test
users, not the owner/root credential. The shared test-user policy grants only
`CreateRole`, `GetRole`, `UpdateAssumeRolePolicy`, and `DeleteRole` for roles
under `/argmin-sts-oracle/`. Its identity-policy `sts:AssumeRole` grant covers
only `/argmin-sts-oracle/cross-account/` roles in any account. Same-account
roles use `/argmin-sts-oracle/same-account/`, so their successful assumption
is preceded by an IAM simulation of every inline, attached, and group policy on
the caller for that exact unique role ARN. The simulation must return
`implicitDeny`, proving that the subsequent success comes from direct trust
rather than an unnoticed identity-policy allow. The simulator grant is limited
to the calling user's own ARN through `${aws:username}`. AWS requires
`iam:ListRoles` to use `Resource: "*"`, so the policy grants that read-only
action separately; the cleanup command sends the `/argmin-sts-oracle/` path
prefix and applies stricter returned-path and role-name checks before deletion.
The policy does not grant role-policy attachment or mutation, `PassRole`, or
any other general IAM administration. The fixture users therefore cannot add
identity permissions to these roles; roles created solely through this grant
remain permissionless. These temporary grants should be removed with the
oracle after Phase 0.

`SimulatePrincipalPolicy` is temporary AWS-oracle fixture validation only. It
must not appear in the endpoint-neutral `s3-tests` scenario and does not add
that IAM operation to the implementation roadmap. When this oracle is removed,
the real AWS/local conformance test will call only APIs implemented by both
endpoints, beginning with `AssumeRole`; local caller policy state will be seeded
deterministically, and keeping the dedicated AWS caller free of unrelated
identity grants remains an out-of-band fixture-administration responsibility.
If that AWS fixture cannot be kept controlled, provision a dedicated caller
rather than adding an AWS-only branch to `s3-tests` or expanding the local IAM
surface merely to inspect the fixture.

The same-account role-fixture slice now:

- provides `./scripts/aws-apply-test-user-policy`, which uses the primary
  credential only to identify the target user and confines owner/root use to
  creating or versioning the customer-managed test policy and attaching it
- creates, converges, assumes, and deletes uniquely named
  `role/argmin-sts-oracle/same-account/path-shape-*`, `default-max-*`, and
  `chain-target-*` roles using the primary test user; it never adopts or
  mutates an existing role, grants none of the roles identity permissions, and
  normalizes temporary secrets before golden response comparison
- proves that the IAM role ARN retains `/argmin-sts-oracle/same-account/` while
  its returned STS ARN omits the entire IAM path and retains only the unique
  role name and `path-shape-session` session name
- pins required `RoleArn` and `RoleSessionName` validation errors to the STS
  2011 namespace, distinct from the AWSFault namespace used by dispatch errors
- pins the no-session-policy success response, including element order and the
  complete omission of `PackedPolicySize` rather than a zero value

AWS also checks `GetRole` for a nonexistent role name against a pathless role
resource, so a policy restricted to `/argmin-sts-oracle/` receives
`AccessDenied` rather than `NoSuchEntity`. The fixture asserts this result for
its unique missing role before creation, then requires `CreateRole` to succeed;
`EntityAlreadyExists` is a hard failure and is never treated as an adoptable
fixture. It never broadens the IAM resource grant merely to perform an
existence check.

`./scripts/cleanup` discovers all uniquely named same-account oracle role shapes
and deletes only those at least one hour old. The age floor prevents a
periodic cleanup run from deleting another concurrently active oracle fixture.
IAM discovery failure is reported and makes the command exit unsuccessfully
after the existing bucket cleanup has still run; it cannot disable bucket
recovery.

The cross-account role-fixture slice completed on 2026-07-13. The alternate
account's administrator applied the same test-user policy there; the
primary-account owner credential was not used as a substitute.

`./scripts/aws-sts-oracle --cross-account` creates three unique,
permissionless roles in the primary account and independently proved against
AWS that:

- caller identity policy `allowed` plus trust allow produces success
- caller identity policy `allowed` plus trust omission produces trust denial
- caller identity policy `implicitDeny` plus trust allow produces caller-policy
  denial

The success and trust-denial roles use `/argmin-sts-oracle/cross-account/`,
while the caller-denial role uses `/argmin-sts-oracle/caller-denied/`, outside
the caller's identity-policy resource grant. Every created role is registered
for cleanup only after `CreateRole` succeeds, and periodic cleanup recognizes
the new path/name pairs with the same one-hour age floor.

Each negative probe has an exact-target positive STS convergence control. The
trust-denial role initially trusts the alternate user, is successfully assumed
by that user, and only then has its trust replaced with the primary user; the
fixture waits for the alternate request to transition to `AccessDenied` before
running the golden probe. The caller-policy-denial role directly trusts both
users. IAM simulation proves that the primary user also has `implicitDeny` for
that exact role, after which a successful primary-user assumption proves the
role is visible and usable through STS solely through same-account direct
trust. The alternate user's separately simulated `implicitDeny` can therefore
be tested against an already converged target that explicitly trusts it.

Both denial paths return HTTP 403, the STS 2011 XML namespace,
`AccessDenied`, and the same exact message shape naming the alternate IAM user
and target role ARN. This means the wire response alone does not identify
whether caller identity policy or role trust caused the denial. Cross-account
success has the same exact golden XML and semantic header shape as the
same-account success: the assumed-role ARN omits the IAM role path,
`PackedPolicySize` is absent when no session policy is supplied, and only the
credential values, role unique ID, expiration, and request IDs require
normalization.

The core `AssumeRole` parameter slice completed on 2026-07-13. Its
path-bearing same-account role is created with a 43,200-second maximum session
duration, allowing one target to pin the complete API-level range without
confounding it with the role's configured maximum. The AWS-backed exact golden
matrix establishes that:

- omitted, empty, too-short, and length-valid malformed `RoleArn` values and
  omitted, empty, too-short, too-long, and pattern-invalid `RoleSessionName`
  values receive the observed STS validation shapes, while a syntactically
  valid nonexistent role produces the same HTTP 403 `AccessDenied` shape as an
  authorization failure
- `RoleArn` length is counted in decoded Unicode scalar values, not UTF-8 bytes
  or UTF-16 code units: 2,048 multibyte BMP characters pass the length check,
  while 2,049 fail; 2,048 supplementary characters receive only the pattern
  error, while 2,049 receive the pattern error followed by the maximum-length
  error
- AWS does not normalize `RoleArn` before counting: 1,025 decomposed `e` plus
  combining-acute pairs are rejected as 2,050 scalar values, rather than being
  normalized to 1,025 precomposed characters
- supplementary characters are rejected by AWS's pattern validator even
  though the pattern rendered in the error appears to include
  `U+10000`-`U+10FFFF`; the implementation must reproduce the observed
  behavior rather than interpreting the displayed pattern literally
- `RoleSessionName` accepts the two- and 64-character boundaries and rejects
  values outside that range or outside `[\w+=,.@-]*`
- absent `DurationSeconds` defaults to 3,600 seconds, 900 and 43,200 are the
  accepted API boundaries, and 899 and 43,201 receive exact `ValidationError`
  responses
- duration parsing is signed 32-bit: `+900` and `0900` are accepted as 900;
  `-1` and the in-range signed extrema reach ordinary minimum/maximum
  validation; values one past either signed 32-bit extreme and a decimal form
  return HTTP 400 `MalformedInput` with no `Message` element
- an empty duration is also HTTP 400 `MalformedInput`, but uniquely includes
  `missing value for decimal type`; alphabetic input has no `Message` element,
  like the other numeric parse failures
- each successful response's `Expiration` is either the default or requested
  duration after the HTTP response date, or one second less when credential
  issuance and response-date generation cross a whole-second boundary, in
  addition to matching the complete success XML and semantic header golden
- duplicate `RoleArn`, `RoleSessionName`, and `DurationSeconds` parameters use
  the first wire value, and an unknown extra parameter is ignored

The first implementation-facing core can therefore parse and validate
`RoleArn`, `RoleSessionName`, and optional `DurationSeconds` from the Query
request without guessed behavior. This does not authorize silently ignoring
known optional `AssumeRole` inputs: session policy/policy ARNs, external ID,
source identity, tags/transitive tags, MFA fields, and provided contexts still
need explicit AWS-backed scope decisions. The one-hour role-chaining limit also
remains separate from the API-level and configured-role duration bounds pinned
here.

The configured role-maximum slice completed on 2026-07-13. A second unique
same-account role is left at IAM's default 3,600-second maximum. The fixture
proves an exact-target `implicitDeny` from the caller's identity policies,
checks `GetRole` reports `MaxSessionDuration=3600`, and establishes successful
STS convergence before the raw probes run. An explicit 3,600-second request
receives the complete success golden, while 3,601 receives HTTP 400
`ValidationError` with exactly `The requested DurationSeconds exceeds the
MaxSessionDuration set for this role.` A 43,201-second request against that
same low-maximum role instead receives the ordinary API maximum-value
validation response. The fixed Query-schema range is therefore validated
before the target role's mutable configured maximum.

The role-chaining slice completed on 2026-07-13 without expanding the fixture
IAM policy. The permissionless path-bearing role is assumed as the source. A
unique target role has a 43,200-second configured maximum and directly trusts
the source IAM role ARN; the long-lived primary caller has an exact-target
`implicitDeny` for that target. The fixture verifies the source session's
caller ARN is the path-free
`arn:aws:sts::<account>:assumed-role/<role-name>/<session-name>` shape and
establishes a successful target assumption with those credentials before the
raw goldens run. This proves same-account direct role trust permits chaining
without an identity-policy allow on the permissionless source role.

IAM's `role-exists` waiter can complete before the new source role ARN is
accepted as a trust-policy principal. Creation of the low-maximum chaining
target therefore retries only AWS's explicit `MalformedPolicyDocument` plus
`Invalid principal in policy` response. Every other creation failure remains
immediate, and the role is registered for cleanup only after `CreateRole`
succeeds.

The raw request signs `x-amz-security-token` along with the other SigV4
headers. A 3,600-second chained assumption receives the complete success
golden. A 3,601-second request against the 43,200-second target receives HTTP
400 `ValidationError` with exactly `The requested DurationSeconds exceeds the
1 hour session limit for roles assumed by role chaining.` The authenticated
caller's session status therefore imposes the one-hour bound independently of
the target's higher configured maximum. A 43,201-second chained request instead
receives the ordinary API maximum-value validation response, establishing that
the fixed Query-schema range is validated before both contextual limits. A
second target directly trusts the same source role but retains the 3,600-second
configured maximum. After target-specific chained success convergence at that
boundary, a 3,601-second request receives the configured-role
`MaxSessionDuration` error rather than the role-chaining error. The complete
observed duration-check order is therefore fixed Query-schema range, target
role configured maximum, then the one-hour role-chaining limit.

The deleted-role authentication slice completed on 2026-07-13. A separate
permissionless role has an exact-target caller identity-policy `implicitDeny`
and direct trust of the primary user. The fixture establishes STS assumption
success and a valid `GetCallerIdentity` session before deleting the role, then
waits through the observed IAM/STS convergence window until that same session
returns `InvalidClientTokenId`. The deliberately deleted role is removed from
the normal exit cleanup set only after `DeleteRole` succeeds; periodic cleanup
also recognizes its unique prefix if the oracle exits earlier.

The fixture then recreates the identical path, role name, and IAM role ARN. IAM
assigns the new incarnation a different stable role ID; a newly issued session
works and its `AssumedRoleId` and `GetCallerIdentity` `UserId` contain that new
ID. The old session continues to return `InvalidClientTokenId`, including when
its otherwise correct token is combined with a bad signature. Because the two
incarnations have the same role ARN, this proves issuer liveness must bind the
session to the stable role ID rather than looking up the mutable ARN or name.

The raw exact-golden matrix compares this invalidated session with the still
live chaining-source session. A second session from that still-live role is
independently authenticated before supplying its token as the mismatch control.
With a live issuer role, the correct token and secret succeed; a missing token
or the other live session's mismatched token returns HTTP 403
`InvalidClientTokenId` before signature validation, with
exactly `The security token included in the request is invalid.` Supplying the
correct token with a bad secret instead reaches HTTP 403
`SignatureDoesNotMatch` and its standard STS message. Missing or mismatched
tokens still win when combined with the bad secret.

After issuer-role deletion converges and after the same role ARN is recreated,
the old session's correct token and secret return the same
`InvalidClientTokenId` golden. Correct-token/bad-signature, missing-token, and
mismatched-token combinations all produce that result too. All errors use the
STS 2011 namespace and complete semantic header/body shapes. This establishes
that token presence, token/access-key binding, and stable issuer-role liveness
are credential-validity checks before SigV4 comparison; mutable trust and
permission policies remain authorization inputs rather than authentication
inputs. Expiry-versus-issuer-deletion precedence is not established by this
matrix and remains an explicit Phase 0 question.

The S3 SigV4 header-authentication slice completed on 2026-07-13 using the
permissionless recreated-role session as its positive authentication control.
The correct access key, token, and signature reach S3 authorization and return
the complete `AccessDenied` golden for `s3:ListAllMyBuckets`, including the new
incarnation's assumed-role ARN. A correct token plus a bad signature instead
returns the complete S3 `SignatureDoesNotMatch` response. The oracle validates
the credential scope, canonical request, canonical-request hash, string to sign,
both byte encodings, and exact XML element order before comparing a sanitized
golden.

For the live session access key, a missing token or an independently valid but
mismatched session token returns HTTP 403 `InvalidAccessKeyId`, including when
combined with a bad signature. After deleting and recreating the identical role
ARN, the old session also returns that same `InvalidAccessKeyId` golden for
correct, missing, and mismatched tokens with both correct and bad signatures.
The S3 header path therefore orders token/access-key binding and stable issuer
liveness before signature comparison, but unlike STS renders those failures as
`InvalidAccessKeyId`. This result is limited to header SigV4 until the other
authentication modes are independently pinned.

The S3 SigV4 presigned-query slice completed on 2026-07-13 against the same
live and invalidated sessions. A valid `X-Amz-Security-Token` query parameter
reaches the same complete `AccessDenied` authorization response. Missing and
independently valid but mismatched query tokens return the complete HTTP 403
`InvalidAccessKeyId` golden before signature comparison, including when paired
with a bad signature. A valid query token plus a bad signature reaches the
complete `SignatureDoesNotMatch` response. The invalidated old session remains
`InvalidAccessKeyId` with its correct query token and signature, with a bad
signature, or with the token missing.

A signed `x-amz-security-token` header is also accepted on a presigned request
and takes precedence over `X-Amz-Security-Token` in the query string: a valid
signed header overrides a mismatched query token, while a mismatched signed
header overrides a valid query token. A present but unsigned
`x-amz-security-token` header is not a credential fallback. AWS rejects it with
the existing complete `HeadersNotSigned` `AccessDenied` golden before token or
signature validation, whether the header token is valid or mismatched, whether
the query token is valid or mismatched, and when the signature is bad.

The same signed-header selection remains authoritative in signature
collisions. With a bad signature, a valid signed header plus a mismatched query
token reaches `SignatureDoesNotMatch`, while a mismatched signed header plus a
valid query token returns `InvalidAccessKeyId`. An invalidated old session in a
signed header also returns `InvalidAccessKeyId` before the bad signature is
considered. Presigned location selection, selected-token binding, and stable
issuer liveness therefore precede signature comparison.

For presigned `SignatureDoesNotMatch`, the oracle validates the canonical query,
canonical-request hash and byte list, string to sign and byte list, credential
scope, and echoed signature. AWS XML-escapes the literal query separators to
`&amp;` inside the `CanonicalRequest` element, while
`CanonicalRequestBytes` and the canonical-request hash are calculated from the
unescaped `&` bytes. Exact golden diagnostics sanitize the raw token, its
URI-encoded form, and the byte encodings of both for every presented token, not
only the selected header token. The dual-location bad-signature golden exercises
both token values in one canonical request. POST Object and streaming token
matrices remain independently unpinned.

The oracle executable is a temporary Phase 0 research artifact, not a test of
Argmin and not a normal testing-guide workflow. Remove it after its observations
have been transferred into implementation-facing conformance tests and the
compatibility record.

### Phase 1: Shared identity substrate and sealed-token codec

- introduce structured principal/session identity
- introduce the minimal stable role identity/liveness record needed to validate
  a session issuer, without trust or permission-policy evaluation
- distinguish stored long-lived credentials from decoded session credentials
- replace per-worker copied identity stores with one shared provider
- keep static credential behavior unchanged
- add a versioned AEAD envelope, process-local sealing key ring, strict token
  size/codec limits, and secure generation interfaces
- add redaction, tamper rejection, and provider/key-ring failure behavior

Exit condition: all current S3 suites remain green, every frontend worker can
open a test-sealed credential using the shared key ring, and no issued-session
record exists.

### Phase 2: Temporary credential authentication plumbing

- implement mandatory session-token verification across header, presigned,
  POST, and streaming paths
- implement expiry and error precedence from Phase 0
- return a typed authenticated session after checking only stable issuer-role
  liveness; current trust and permission-policy state remain authorization
  inputs for a separate consumer
- add deterministic clock boundary tests
- add concurrency, token redaction, malformed/tampered-token, wrong-key,
  access-key-binding, and unknown-version tests
- exercise each SigV4 mode at the authentication boundary without claiming that
  a role session yet has usable S3 permissions

Exit condition: directly sealed session credentials authenticate with
AWS-pinned token/signature/expiry precedence on every SigV4 mode and produce a
typed authenticated session. Temporary role credentials remain documented as
unsupported for end-to-end S3 use until Phase 3 supplies role authorization.

### Phase 3: IAM policy and role core

- extend the minimal role identity records with role configuration, trust, and
  permission-policy state
- add configured long-lived principal records and explicit identity-policy
  attachments, including caller-side `sts:AssumeRole` resource grants for the
  cross-account fixture; do not synthesize them from `AuthorizationProfile`
- add shared policy AST/evaluation support for trust, identity, and session
  policies
- add typed session-principal versus `aws:PrincipalArn` context and
  `aws:TokenIssueTime` date-condition evaluation
- integrate role identity policy decisions into the S3 authorization paths
  required by the first conformance suite
- implement correct explicit-deny/intersection/resource-policy composition
- seed deterministic roles in embedded tests and UAT-only setup

Exit condition: a constructed role session can authenticate and use S3 with
only the AWS-equivalent permissions of its current role, sealed session, and
resource-policy combination. The configured caller's identity-policy path is
also capable of representing `sts:AssumeRole`; no broad profile shortcut is
involved.

### Phase 4: STS Query endpoint and core `AssumeRole`

- add logical service routing and bounded Query protocol parsing
- authenticate STS requests with service name `sts`
- authorize `AssumeRole` using the AWS-equivalent combination of caller identity
  permissions and the role trust policy
- validate role/session/duration inputs
- generate the session credential and seal its authentication/session-policy
  context without mutating identity state
- render AWS-shaped success/errors
- add `aws-sdk-sts` after the normal test-dependency review

Exit condition: the AWS SDK can call `AssumeRole` on the same local endpoint,
then use the returned credentials through the AWS S3 SDK with correct
permissions.

### Phase 5: End-to-end conformance and standalone UAT

- add a dedicated `s3-tests` STS/temporary-credentials binary
- run it against AWS and the embedded local server
- add standalone `argmin-s3` UAT coverage with multiple workers
- cover issuance/use races, simultaneous sessions, expiry, restart loss,
  ciphertext tampering, access-key/token mismatch, and role deletion/update
  interactions defined for this phase
- cover sealing-key mismatch and later key-overlap rotation behavior at the
  appropriate phase
- add fuzz targets for Query parsing and STS XML rendering inputs where useful

Exit condition: targeted local, AWS, and standalone-binary suites are green and
the known-gap documentation is current.

### Phase 6: Complete `AssumeRole` parameters and role chaining

- add session policies, external IDs, source identity, session tags,
  transitivity, packed policy behavior, and chaining
- add MFA only with a real MFA identity/verification model
- add every associated request context key and policy test

Exit condition: every documented `AssumeRole` parameter is either implemented
with AWS-backed coverage or remains explicitly listed as an open compatibility
gap; no parameter is ignored.

### Phase 7: Minimal IAM role APIs

- expose role/trust/inline-policy lifecycle through the IAM Query protocol
- add SDK and raw-wire conformance tests
- replace fixture-only role setup in appropriate tests with public IAM setup
  where doing so improves coverage without making every S3 test expensive

Exit condition: tests can create, authorize, assume, inspect, and delete an
in-memory role through AWS-compatible public APIs.

### Phase 8: Users, keys, broader identity policy, and durable handoff

- add user/access-key lifecycle as needed by S3 feature coverage
- finish account-scoped S3 identity-policy gaps
- add authoritative policy principal validation and denial messages
- define the provider/cache contract for config-file and persistent stores
- move durable account/key ownership to the persistent account plan
- design replication and revocation consistency before claiming multi-frontend
  or multi-host production support

## Test Strategy

### Unit and property tests

- typed credential/identity invariants
- AEAD envelope round trips and failure uniformity without exposing token values
- expiry boundary and injected clock behavior
- secure generator shapes, nonce uniqueness, access-key binding, and negligible
  collision assumptions
- unknown version/key ID, wrong key, bit flips, truncation, trailing bytes,
  oversized tokens, invalid timestamps, and cross-domain replay
- Query parser duplicates, decoding, indexing, length/overflow, and malformed
  input
- XML escaping and golden rendering
- trust/identity/session policy combination tables
- role ARN, role-session ARN, account, canonical user, and wildcard principal
  matching
- path-bearing role invariants: IAM role ARN and `aws:PrincipalArn` retain the
  path while the assumed-role session principal ARN omits it
- `aws:TokenIssueTime` availability and date-condition boundary behavior
- policy variables and condition keys introduced by each phase

### Local integration tests

- issue then immediately use temporary credentials on another worker
- header, presigned, POST Object, and streaming requests
- missing/wrong/unexpected token matrices with valid and invalid signatures
- expired session behavior with a deterministic clock
- missing or mismatched tokens and deleted issuer roles produce the AWS-pinned
  credential error before signature comparison; a live issuer plus valid token
  reaches bad-signature handling
- policy changes do not alter authentication precedence, while injected
  issuer-liveness and authorization-provider failures exercise their distinct
  fail-closed paths
- role permission allow, implicit deny, explicit deny, session-policy
  restriction, resource-policy allow/deny, and ACL interaction
- same-account and cross-account assume-role paths, with explicit caller
  identity-policy attachments where AWS requires them
- path-bearing role response, principal-matching, `aws:PrincipalArn`,
  `aws:userid`, and `aws:TokenIssueTime` matrices
- simultaneous session issuance and unique identifiers
- process restart invalidates tokens sealed by the old volatile key as documented
- role deletion and policy updates affect active sessions according to
  AWS-pinned behavior

### AWS-backed tests

Add a focused test binary rather than spreading STS setup through unrelated S3
files. It should use the same scenario against AWS and Argmin:

1. call `AssumeRole`, including a path-bearing role and a caller whose
   cross-account permission comes from an explicit identity policy
2. validate response shape without pinning random values
3. build an S3 client from access key, secret, and session token
4. exercise allowed and denied S3 actions
5. exercise raw malformed/token/expiry cases where the SDK hides wire details
6. clean up with the long-lived primary client

AWS credentials and the target role ARN must remain external test
configuration. The role trust and permission policy required by the suite
should be committed in `guides/testing.md` alongside the existing IAM fixture
documentation.

### Verification gates for implementation commits

Use the repository's normal gates, scaled during development and complete
before committing:

- focused `auth`, `server-http`, `server-core`, and new STS/IAM tests
- targeted `s3-tests` temporary-credential suite locally and against AWS
- standalone UAT targeted suite
- `cargo fmt`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo nextest run`
- `./scripts/coverage` to measure the new integration surface
- relevant security/parser fuzz targets

## Operational And Security Boundaries

The initial backend is intentionally not production quality:

- all runtime-created identity state and the token-sealing key disappear on
  restart
- existing ciphertext tokens become invalid when the process-local sealing key
  is lost
- multiple frontend processes cannot validate each other's tokens unless they
  share the same configured key ring and identity backend
- token keys and role state are not replicated through the control plane
- revocation/update visibility is process-local
- there is no durable audit history
- there is no individual-session revocation list; role deletion, current policy
  state (including `aws:TokenIssueTime` denies), expiry, or sealing-key
  retirement are the available revocation mechanisms

Startup/status output should state the identity backend mode and whether STS is
enabled. Multi-process frontend configurations must reject enabling the
process-local key ring unless requests are guaranteed to return to the same
credential domain. A configured key ring may later make stateless tokens
portable across frontends, but only when the role/account backend is equally
consistent. Silently issuing credentials that work on only some frontends is
unacceptable.

STS should be disabled by default until the role/policy fixture is explicitly
configured. Enabling it on plain HTTP is permitted only to the extent the main
S3 endpoint permits plain HTTP, but documentation must call out that AssumeRole
responses contain bearer credentials and therefore require TLS for
confidentiality. No session response or request body may appear in traces.

## Open Decisions To Resolve During Phase 0

1. What exact same-account, cross-account, and path-bearing AWS role fixtures
   and permission scopes can be added to the external test accounts without
   broadening the test users' permissions more than needed?
2. Does the first local endpoint model only configured-region STS behavior, or
   must it also recognize the legacy global endpoint signing rules immediately?
3. Which existing identity/policy types can be generalized without making S3
   bucket-policy code less explicit?
4. What is the exact AWS precedence among wrong token, missing token, bad
   signature, wrong region/service, disabled credential, and expired session on
   POST Object and streaming modes? The core header and presigned
   token/signature collisions are pinned; their remaining
   region/service/expiry collisions are not.
5. What total Query body and member limits does live STS enforce for the first
   supported parameter set?
6. Should the first standalone UAT role be injected through a dedicated
   test-only constructor/config object or through explicitly UAT-only
   environment variables?
7. Which S3 actions form the smallest meaningful identity-policy conformance
   matrix while still proving that role permissions are real rather than a
   profile shortcut?
8. What missing-token error can a stateless implementation reproduce from only
   the access key ID, and does AWS behavior require a self-authenticating issued
   access-key format?
9. Which AEAD and envelope encoding should be the initial format, and what
   credential-domain value should be authenticated to prevent cross-deployment
   token reuse?
10. What key-overlap and retirement semantics are required before configured
    token keys can be shared by multiple frontend processes?

## Definition Of The First Usable Milestone

The initial milestone is complete only when all of the following are true:

- an AWS SDK STS client can call local `AssumeRole` on the same listener
- the request is signed and validated as service `sts`
- the configured role trust policy and caller identity permissions combine
  according to the AWS-pinned same-account or cross-account rule
- the result has the AWS response shape and contains unpredictable, redacted,
  Argmin-namespaced temporary credential material and assumed-role identity
- a path-bearing IAM role produces a path-free assumed-role session ARN while
  retaining the path in its IAM role ARN and `aws:PrincipalArn` value
- the session token is a versioned AEAD-sealed, self-contained credential and
  session-policy envelope; issuance creates no per-session record
- every HTTP worker accepts the new credential immediately through the shared
  key ring and role provider
- S3 header, presigned, POST, and streaming auth require the exact session token
- expiry and malformed-token error precedence match committed AWS observations
- stable issuer-role liveness is checked at the AWS-pinned point before SigV4
  comparison, while current trust and permission-policy lookup occurs only at
  the authorization boundary
- the role session's S3 permissions come from policy evaluation, including
  explicit deny, rather than `AuthorizationProfile`
- cross-account `AssumeRole` uses an explicit caller identity-policy attachment,
  and temporary request context exposes authenticated `aws:TokenIssueTime`
- local, AWS-backed, and standalone UAT coverage passes
- restart, key rotation, per-session revocation, and non-replication limitations
  are documented
- all unsupported `AssumeRole` parameters are explicitly rejected and tracked
  for the completeness phase rather than ignored
