# Stateless STS And In-Memory IAM Foundations

## Status

Phase 0 design, AWS-oracle, and fixture work is complete for the first usable
`AssumeRole` milestone as of 2026-07-19. Phase 1 identity-provider and stateless
credential foundations and Phase 2 temporary-credential authentication
plumbing are complete as of 2026-07-20. Phase 3 role authorization is complete,
and the Phase 4 typed S3/S3 Control endpoint-routing foundation is complete as
of 2026-07-22. The bounded STS Query classifier/parser is the next
implementation slice. The first implementation target is a test-enablement
vertical slice, not a production identity service.

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

### Long-lived credentials and stable role liveness are shared

`auth::CredentialStore` is now a bootstrap collection consumed by an
`IdentityProvider`. The initial provider owns that bounded in-memory state
behind a private standard-library lock, and every `HttpFrontend` worker in both
`argmin-s3` and the embedded test servers receives a clone of the same provider
handle. Lookups return owned immutable records and distinguish absence from
backend failure, releasing the provider lock before canonicalization, HMAC,
storage, or response work. Existing configured-key authentication remains
unchanged, and an unavailable provider fails closed as an internal service
failure rather than `InvalidAccessKeyId`. The shared boundary validates access
key, stable role ID, and canonical-user ID binding on custom-backend results.
Unavailable and invalid-record failures remain typed through authentication
and use distinct fixed redacted diagnostic labels, while both retain the same
generic external `InternalError` response.

The same provider now indexes minimal immutable live-role incarnations and
their authoritative S3 account identities by stable role ID. The index rejects
account/role mismatches, duplicate stable IDs, and duplicate live role ARNs.
The shared provider validates that a backend result is bound to the requested
stable ID before returning an opaque resolved-role value. Decoded session
construction is crate-internal and accepts only that provider-resolved value,
so callers cannot substitute a fabricated/deleted role or alternate canonical
user/display-name fields. The index deliberately contains no mutable trust or
permission-policy state. This is the narrow pre-signature liveness capability;
current role authorization remains a later, separate provider capability. No
production role fixture is seeded yet.

The provider now owns one process-local session-token sealing key ring and
shares it with every clone handed to a frontend worker. The ring starts with a
random 256-bit AES-GCM key, public 16-byte key ID, four-byte nonce prefix,
atomic 64-bit issuance counter, and 16-byte credential domain. There is no
issued-session collection.

### Stored and decoded session credentials are distinct

`StoredCredential` contains:

- access key ID
- secret key
- composed account and configured-principal identity
- coarse authorization profile
- optional expiry
- enabled state

Its constructor accepts only a configured principal, so an assumed-role session
cannot be inserted into the long-lived store. `DecodedSessionCredential`
instead requires an Argmin-namespaced 24-byte temporary access key ID, a
40-byte secret access key, an account-matched assumed-role session identity
with mandatory issue/expiry time, and an explicit versioned session
authorization context. Production construction is available only after the
shared token codec authenticates its payload and the provider resolves the
stable live-role/account record. `AuthenticatedCredential` preserves the
long-lived versus session kind while exposing only their common signing
identity and secret material.
Long-lived store insertion and server configuration reject the reserved
`ARGS` namespace. The shared provider also refuses to query that namespace as
long-lived and validates access-key binding on every custom-backend result, so
a future backend cannot bypass the reservation.

The shared provider can now authenticate one already-selected temporary
credential into the session variant. That boundary strictly opens the
version-1 envelope, binds its access key in constant time, checks expiry, and
only then resolves the stable issuer incarnation. Ordinary non-streaming S3
Authorization-header authentication now selects and calls that path for the
reserved `ARGS` namespace. Presigned-query authentication now does the same
through its own query-versus-signed-header selector. POST Object now selects
ordered multipart form-token inputs and calls the same shared session
authentication path. Aws-chunked streaming now uses that same selected-token
path before constructing its seed/chunk signing context.
Active static credentials retain their post-signature unexpected-token check,
and expiring stored records remain static rather than becoming STS
credentials.

### Structured session authentication exists but role authorization does not

`AccountIdentity` remains the durable account/owner value. Authentication now
composes it with a typed configured principal or assumed-role session identity,
and exposes the IAM role ARN, STS session ARN, stable role ID, assumed-role ID,
and `aws:userid` through distinct accessors. The first ordinary header-auth
slice can now populate the assumed-role-session variant. Existing S3
authorization paths explicitly refuse to reinterpret it as a configured user;
role permission evaluation remains Phase 3. Session and principal tags remain
later versioned policy context as described below.

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
- <https://docs.aws.amazon.com/general/latest/gr/sts.html>
- <https://docs.aws.amazon.com/general/latest/gr/s3.html>
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
  issuer-credential invalidation from mutable authorization behavior; record
  the distinct issuer-liveness and authorization-provider failure regressions
  required from the equivalent Phase 2 and Phase 3 local tests
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

The Phase 0 identity decision is to retain `AccountIdentity` as the stable
account/owner value already used by S3 metadata and compose it with a separate
typed principal identity for authenticated requests. Do not add optional
user, role, or session fields to `AccountIdentity`, and do not replace its
account-level meaning with a universal principal enum. `AuthContext`,
`Requester`, and later identity records should carry an authenticated identity
whose account and principal kind are both mandatory; anonymous requests remain
a separate state. Authenticated principal kinds must distinguish at least:

- existing configured/root-style principals used by current tests
- `IamUser`: user ARN, stable user ID, and user name/path
- `AssumedRoleSession`: role identity, path-free STS session principal ARN,
  assumed-role ID, session name, issuer/caller, issue/expiry times, source
  identity, and tags

An `IamRole` is a separate stored issuer/authorization identity containing its
IAM role ARN, stable role ID, and separately stored role name and path; it must
not become a directly authenticated request-principal variant merely to reuse
the same enum.

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
POST Object's `SignatureDoesNotMatch` response echoes the complete base64 POST
policy as `StringToSign` and its hexadecimal bytes as `StringToSignBytes`; a
policy containing the token condition is bearer-sensitive even though the raw
token is not directly visible. Sanitize both complete policy representations
as well as every separately presented token.

### 3. Share identity state and token keys, not issued sessions

All frontend workers in a process should hold the same provider handle. The
first implementation can use `Arc` plus a standard-library lock around
in-memory state, with these separable capabilities:

- resolve a long-lived credential for authentication
- resolve accounts/users/roles for policy and principal validation
- evaluate or retrieve attached identity/trust/session policies
- seal and open session credential envelopes using a shared versioned key ring
- inspect non-secret status whose output is bounded by the configured key-ring
  record limit

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

The first format is now fixed rather than being left to Phase 1. The external
token is the ASCII prefix `ARGST1.` followed by unpadded URL-safe base64 of one
binary frame. Version 1's frame is, in order:

- a 16-byte public sealing-key ID
- a 12-byte AES-GCM nonce
- the ciphertext
- the 16-byte AES-GCM authentication tag appended by `ring`

Use `ring::aead::AES_256_GCM`, which is already a production dependency. The
clear key ID selects a validation key. For the process-local active key, the
nonce is a random four-byte per-key prefix followed by an atomic unsigned
64-bit big-endian issuance counter. The prefix, key, and key ID are regenerated
together at process start, and counter exhaustion fails issuance closed. This
guarantees nonce uniqueness under that key without storing issued sessions or
relying on probabilistic nonce-collision tests. The exact ASCII prefix, key ID,
and nonce are authenticated together with the credential domain as associated
data.
Construct that associated data with a fixed binary layout containing the
literal `argmin:sts:session-token:v1`, the 16-byte credential domain, the
external prefix, key ID, and nonce; do not authenticate ambiguous concatenated
strings.

The encrypted version-1 payload uses this exact fixed order:

1. the 24-byte ASCII session access key ID
2. the 40-byte ASCII secret access key
3. issued-at and expires-at as two signed 64-bit big-endian Unix seconds
4. the 12-byte ASCII account ID
5. the 24-byte ASCII stable role ID (`ARGR` plus 20 random uppercase
   alphanumeric characters)
6. role name and role-session name as separate unsigned 16-bit big-endian byte
   lengths followed by their UTF-8 bytes
7. a one-byte source-identity presence tag (`0` or `1`), followed when present
   by an unsigned 16-bit big-endian byte length and UTF-8 bytes

Every variable-length string is length-prefixed, and every fixed or variable
field is validated against its typed API bound before the authenticated session
value is constructed. Version 1 has no generic map, ignored extension area, or
trailing bytes. Inline session policies, managed policy references, session
tags, transitive-tag keys, and provided contexts require a new outer format
version when those Phase 6 features are implemented; they must not be smuggled
into an unvalidated version 1 extension field. Until then every request
containing one of those parameters is rejected before issuance.

Bound allocation before base64 decoding. Version 1 accepts at most 21,853 ASCII
bytes (`ARGST1.` plus the unpadded base64 expansion of a 16,384-byte frame) and
at most 16,384 decoded frame bytes as defensive decoder limits. Issuance has a
separate, smaller invariant. The AWS-pinned role-name, role-session-name, and
source-identity patterns are ASCII-only, with respective maxima of 64, 64, and
256 bytes. Together with their length/presence fields and the fixed payload,
these give a maximum 507-byte plaintext, 551-byte frame, and 742-byte external
token. Define that result as `MAX_ISSUED_V1_TOKEN_LEN`; derive it from the typed
field/frame constants and assert it after sealing so a future field change
cannot silently enlarge issued tokens.
Exceeding it is an internal invariant failure, never a credential returned to
the client. Focused tests must construct every version-1 field at its maximum
accepted encoded size, sign ordinary PutObject and aws-chunked requests with the
result, and assert that each complete request-header section remains within the
server's 8,192-byte limit. Before any STS issuance route is enabled, the Phase 2
request-path tests must also authenticate the same maximum token through
presigned-query and POST Object. A future envelope version may change the
issuance ceiling only after AWS parameter-length probes and matching header,
Query, POST, and streaming transport coverage establish that the resulting
credentials work in every required authentication mode.
Compact encoding or compression is a Phase 6 design decision, not permission to
issue an oversized version-1 token.

Parsing must reject an unknown prefix/version, invalid or non-canonical base64,
unknown key ID, nonce/tag truncation, authentication failure, trailing bytes,
oversized encoded or decoded tokens, duplicate/impossible payload state,
invalid UTF-8 or field encodings, and impossible time ranges before constructing
a session credential. These failures collapse to one typed invalid-session-
token result for service-specific AWS error rendering. Unknown key IDs are
invalid tokens; an unavailable key-ring provider is an internal provider
failure. Neither path may log the token, plaintext, secret, nonce, tag, or raw
key material.

The initial key ring contains one process-start 256-bit key, a random 16-byte
key ID, and a random 16-byte credential domain, shared by all workers in the
process. A configured ring later contains exactly one active issuance key and
zero or more validation-only keys with unique 16-byte IDs. Each key object owns
its nonce prefix and atomic counter for its entire in-process lifetime. Its
state transition is one-way: `Active -> ValidationOnly -> Removed`. A
validation-only or removed key ID/key-material pair can never become active
again, and a new active key always has new key material, key ID, nonce prefix,
and counter. Rotation changes issuance to that new key immediately while
retaining the old key for validation. An old validation key must remain until
the maximum possible expiry of every session it issued; removing it is
immediate bulk revocation and is never a transparent rotation step. Key-ring
APIs and configuration validation must make reactivation unrepresentable.
Removal drops the decrypting key but retains an internal key-ID tombstone and a
one-way key-material fingerprint for the lifetime of the ring, so neither the
ID nor the same AES material under another ID can return with a reset nonce
allocator. Tests must cover attempted validation-only and removed-key
promotion, duplicate live and removed key material under another ID, and
concurrent rotation/issuance. Configured multi-frontend use also requires the
same explicit credential domain and identity provider on every frontend.
The initial process-local ring has no production rotation surface, so it stays
at one record. Before configured or administrative rotation is exposed, the
key-ring configuration and transition API must impose a finite maximum on the
total of active, validation-only, and removed records and fail closed when that
history is full. Removed tombstones count toward that limit and are never
evicted or compacted within the credential domain; planned rotation beyond the
limit requires starting a new domain and intentionally invalidating all tokens
from the old domain.
No configured key may become active for issuance across process restarts or on
multiple frontends until that later implementation provides a non-repeating
nonce allocation for the key's entire lifetime; loading it as validation-only
does not have that constraint.
Duplicate key IDs, duplicate key material under another ID, no active key,
malformed key material, or inconsistent domain configuration are startup
errors. Status may expose only format version, credential-domain ID, non-secret
key IDs, and active/validation-only state, never raw key material, tokens,
decoded secrets, or session payloads.

Because issuance stores nothing, there is no issued-session capacity, expiry
scan, or per-session tombstone requirement. Per-request memory remains bounded
by the existing admission controls plus strict encoded-token and
decoded-payload limits. The separate per-key tombstones above are retained for
the ring's lifetime; their eventual production memory bound comes from the
mandatory total key-record limit, not from request admission.

### 5. Add token-aware authentication once, shared by every SigV4 mode

The STS route performs its AWS-pinned credential-scope validation before
entering the shared temporary-credential path: simultaneous region and service
errors are aggregated in that order, and either individual error precedes STS
token resolution, token/access-key binding, expiry, issuer liveness, and HMAC
comparison. This placement is STS-specific and is not evidence for any S3
mode; each S3 mode requires its own collision matrix.

The S3 header-auth route is now independently pinned to perform its own scope
validation before the probed signed-header token structure, presence, binding,
expiry, stable issuer-role liveness, and HMAC checks; wrong region wins when
region and service are both wrong. The scope matrix covers missing, empty,
malformed, independently valid mismatched, identical-duplicate, and
conflicting-duplicate signed token headers in both conflicting orders for live
and invalidated sessions.

The presigned-query route is independently pinned to parse its
`X-Amz-Credential` scope before the probed query and HTTP-header token
selection, structure, coverage, binding, issuer-liveness, and HMAC checks;
expiry is also behind the probed scope and binding checks. Wrong region wins
when region and service are both wrong. Its matrix
includes missing, empty, malformed, mismatched, identical-duplicate, and
conflicting-duplicate query tokens in both conflicting orders, plus selected
signed-header and present unsigned-header cases. Its signed-header matrix also
covers identical duplicates and conflicting duplicates in both wire orders,
with valid and bad HMACs. These results apply only to header and presigned
SigV4 respectively.

POST Object is independently pinned to parse the form
`x-amz-credential` scope before the probed form-token structure, binding,
expiry, issuer-liveness, and policy-signature checks, with wrong region winning
when region and service are both wrong. Its already-pinned HTTP-token-header
presence routing occurs earlier still: any probed `x-amz-security-token` HTTP
header produces `No AWSAccessKey was presented.` before form credential-scope
parsing.

The aws-chunked streaming route is independently pinned to validate the
Authorization-header credential scope before token structure, signature
coverage, binding, expiry, issuer liveness, seed-signature comparison, or
chunk-chain verification. Its matrix covers missing, empty, malformed, mismatched,
identical-duplicate, and conflicting-duplicate signed token headers in both
conflicting orders, plus a present unsigned token, invalidated old sessions,
bad seed signatures, and bad first-chunk signatures. Wrong region wins when
region and service are both wrong.

That aws-chunked rule is specific to Authorization-header SigV4. AWS does not
activate aws-chunked decoding merely because a presigned request carries one
of the three supported `STREAMING-*` payload markers. Exact presigned
PutObject probes repeat the matrix with configured long-lived and assumed-role
credentials, sign the complete streaming header set, and establish:

- a signed aws-chunked wire body, whether its chunk signatures are valid,
  invalid, or absent, is treated as raw bytes. For the non-trailer signed
  marker AWS returns HTTP 400 `IncompleteBody`, comparing
  `x-amz-decoded-content-length` with the raw wire byte count and rendering
  both as `NumberBytesExpected` and `NumberBytesProvided`
- an ordinary raw body whose length exactly equals
  `x-amz-decoded-content-length` is hashed as-is. Each streaming marker is
  treated as the literal client hash and produces HTTP 400
  `XAmzContentSHA256Mismatch`; this response omits `Resource`
- for PutObject, a raw trailer-marker body shorter than the declared length is
  `IncompleteBody`, an exact-length body is the same hash mismatch, and an
  overlong or aws-chunked framed body reproducibly returns generic HTTP 500
  `InternalError`. The signed framed case was repeated three consecutive times
- UploadPart was probed independently against live multipart uploads. Its
  non-trailer signed-marker behavior matches PutObject, but either trailer
  marker returns `IncompleteBody` for a short raw body and HTTP 400
  `MalformedTrailerError` for exact, overlong, or framed bodies. A signed
  framed case was repeated three times. A signed ListParts request after every
  failed probe confirmed that no part was stored. Signed HeadObject checks
  after every failed PutObject likewise confirmed that no object was stored,
  including after each generic HTTP 500 response

The initial local implementation must therefore carry authentication mode to
the body adapter. Header SigV4 constructs the production incremental decoder;
presigned SigV4 follows the separately pinned raw-body validation path. A
generic `STREAMING-*` classifier must not imply a common decoder decision.

Credential validation should have one common decision path used by STS and S3
header, presigned, POST Object, and streaming authentication:

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
4. for aws-chunked streaming, require `x-amz-security-token` to be covered by
   the seed request's signed headers; a present unsigned token returns
   `HeadersNotSigned`. Collapse repeated signed header values only when every
   value is identical. Conflicting duplicates, in either order, return
   `InvalidAccessKeyId`; an empty value is equivalent to a missing token, while
   a malformed non-empty value returns `InvalidToken`. Token shape/binding and
   issuer liveness precede both seed- and chunk-signature verification
5. for POST Object, select `x-amz-security-token` only from the multipart form;
   its matching POST-policy condition follows the existing validation rules.
   Credential extraction accepts duplicate form-token fields only when every
   value is identical, while retaining their multiplicity for later POST-policy
   condition evaluation. Conflicting duplicates return `InvalidAccessKeyId`
   without selecting either value. Treat an empty value like a missing token,
   and map a malformed non-empty token to `InvalidToken`. These form-token
   shape/binding checks precede POST policy-signature verification.
   A token in the HTTP header is not a fallback or a competing token location:
   its presence selects the non-POST authentication route, which rejects the
   request as `AccessDenied` with `No AWSAccessKey was presented.` before POST
   form-token, token decoding, issuer-liveness, or policy-signature validation
6. if the access key ID resolves to a stored long-lived credential, reject an
   inactive record after the mode's outer scope and signature-coverage/routing
   checks but before HMAC comparison or opening/binding/expiring a selected
   token. For an active static record, use the static path and preserve AWS's
   post-signature unexpected-token behavior
7. otherwise, require and AEAD-open the selected effective session token
8. validate the embedded access key ID against the SigV4 credential in constant
   time and check credential-domain binding
9. check embedded expiry at the AWS-pinned boundary and resolve only the
   embedded stable issuer-role ID, rejecting a deleted issuer as
   `InvalidClientTokenId`; expiry precedes issuer-role liveness, and both checks
   precede signature comparison
10. verify SigV4 with the embedded secret access key
11. return an immutable authenticated-session value containing the structured
   token identity and versioned session context; version 1 has no session
   policy, while later formats may add the independently validated policy state

Streaming must plug its AWS-pinned selection rules into step 2 rather than
adding a separate credential-validation pipeline. All presented token values,
not only the selected one, remain sensitive diagnostic inputs: canonical-error
construction and test failure sanitization must account for every location
included in the request.

Authentication must not resolve mutable trust or permission-policy state before
signature verification. AWS probes do require one narrower mutable dependency:
after role deletion converges, credentials issued by that role return
`InvalidClientTokenId`, and this result wins over a bad signature. Token
opening, access-key/domain binding, expiry validation, and issuer-role existence
therefore establish credential validity before signature comparison. The
pre-signature lookup must expose only stable issuer identity/liveness, not role
authorization state.

AWS observations establish that expiry and deleted-role liveness each win over
signature mismatch. The combined-state oracle additionally establishes that
expiry wins when the session has expired and its stable issuer role has been
deleted. This ordering is shared by STS and every initial S3 temporary-
credential authentication mode.

At the authorization boundary, resolve current trust-independent role
permission state, then combine it with the authenticated session policy and
request context. A changed permission policy or authorization-provider failure
fails closed there. No other mutable role field may move into authentication
without AWS evidence. The Phase 0 mutation oracle pins current role-policy
replacement at this boundary: an already-issued session adopts the replacement
policy after remaining valid through authentication, and a bad signature is
rejected before the policy deny is evaluated. Matching local tests must inject
issuer-liveness and authorization-provider failures separately.

The completed Phase 0 matrices pin the precise service-facing mappings and
precedence for missing, malformed, wrong-key, tampered, mismatched, expired,
and inactive credential/token inputs. They establish, among other cases, that
expired temporary credentials win over signature mismatch, while unexpected
token input on an active static credential is checked after signature
comparison. Transfer those observations into committed implementation
regressions once real temporary credentials exist.

Do not conflate the service-facing error mapping with the shared internal
credential-validation decision. The AWS STS Query endpoint maps missing,
mismatched, or invalidated session credentials to `InvalidClientTokenId`; the
S3 SigV4 header path maps the equivalent observed cases to
`InvalidAccessKeyId`. Both can consume one typed invalid-credential result while
rendering the AWS-pinned service-specific response.

A stateless verifier cannot recover expiry or identity when the token is
missing. The completed Phase 0 credential matrices pin AWS's missing-token
precedence, and the Argmin-owned `ARGS` namespace selects the corresponding
temporary-credential error path. The prefix remains only a routing hint and
must not be treated as proof that Argmin issued the key; authentication still
requires successfully opening and binding the token.

Presigned and POST requests need the same semantics as header auth. Streaming
requests must bind the seed request to the temporary credential and must not
drop the validated session identity when constructing streaming signing state.

### 6. Introduce a reusable IAM policy core

Do not fork bucket-policy parsing into unrelated trust and identity evaluators.
The Phase 0 policy decision is to extract the common language and matching
mechanics behind the existing `BucketPolicy` API while retaining typed policy
documents, typed decision inputs, and policy-kind-specific composition rules.
The existing S3 bucket-policy API, S3 action type, S3 resource construction,
condition-input loading, public-policy classification, and S3 authorization
composition must remain explicit rather than becoming one universal request
context full of optional IAM and S3 fields.

The shared core should own the reusable policy-language pieces:

- policy version and effect
- common JSON scalar-or-list parsing
- condition clauses, operators, variables, and wildcard matching
- action and resource pattern matching
- explicit-deny and statement-match primitives
- validated common value types and lookups for tags and other global condition
  facts

Common data does not imply ambiguous provenance. Use shared validated tag key,
tag value, and tag-collection types, but retain typed sources for principal,
role, session, request, resource, existing-object, and requested-object tags.
Those sources map deliberately to condition-key families such as
`aws:PrincipalTag`, `aws:RequestTag`, and `s3:ExistingObjectTag`. Likewise,
current time, source IP, secure transport, requested region, and structured
principal identity are shared condition facts supplied through typed
service/request adapters. An unavailable input must remain distinguishable
from an available but empty value.

Build typed validated wrappers over that core for:

- S3 bucket/resource policies, which contain `Principal`
- role trust policies, which contain `Principal` and authorize STS actions
- identity policies, which omit `Principal`
- inline session policies, which restrict but never expand role permissions

Each wrapper must enforce its own allowed and required statement shape,
action namespaces, resource applicability, condition inputs, and size limits.
A permissive parsed statement containing optional `Principal` or `Resource`
may exist only as an internal parse stage; authorization-facing types must make
the policy kind's valid shape explicit. Trust, identity, session, and S3
resource-policy evaluation should feed different typed request data into the
shared matcher rather than reuse the S3 bucket/key request structure.

Implement this incrementally: first introduce structured authenticated
identity, then extract common policy primitives behind behavior-preserving
`BucketPolicy` interfaces, then add typed trust and identity policy documents,
and finally add their explicit decision-composition layer. Do not build a
parallel IAM parser/evaluator, and do not replace the mature S3 path with a
single universal policy/request type.

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

Separate S3 identity bootstrap from normal identity-backend contents. Static
server configuration needs enough S3-side authority to establish initial
accounts, their root principals, and initial root or administrative credential
references. It must not thereby become the long-term database for users,
access keys, roles, and policies. Model the boundary as a typed identity
provider configuration containing:

- bootstrap accounts with stable account/canonical identity and initial root or
  administrative credential references
- an explicit backend kind and credential domain
- backend-specific configured records

The initial process-local backend's configured records contain the principals,
long-lived credentials, roles, trust and permission policies, and policy
attachments required by the first tests. Reapplying those records at each
process start is expected because this backend is deliberately volatile. A
future config-file provider may treat its versioned file as authoritative
static identity data. A future persistent provider instead consumes bootstrap
authority only when its identity store is genuinely uninitialized, records
that initialization atomically, and thereafter treats the durable store as
authoritative. Restart must never recreate a deleted account, credential, role,
or policy merely because an old bootstrap entry remains configured; conflicting
bootstrap identity must fail closed rather than merge or replace live state.

Keep user-facing S3 identity configuration separate from cluster topology and
placement identity. The emerging static cluster manifest deliberately excludes
S3 accounts and credentials; a versioned S3 identity section or sibling
configuration may be selected by server configuration, but its users and roles
must not enter the cluster topology digest. Production-shaped secrets remain
references rather than inline values. Test constructors may inject already
resolved fixture secrets without changing the file grammar.

The embedded test server constructs the typed process-local identity
configuration directly. The standalone UAT wrapper generates the equivalent
versioned identity configuration and starts the ordinary `argmin-s3` binary
with it. The configured callers and roles are ordinary process-local backend
records, not special UAT principal variants or a privileged fixture backdoor.
Do not add per-role, per-policy, or policy-JSON UAT environment variables; any
temporary external selector should identify the generated configuration, not
encode an alternative role database.

The first public role-management surface comes later through IAM-compatible
Query APIs. The later persistent-account plan owns durable bootstrap
transactions and lifecycle semantics; this plan establishes the provider and
typed-configuration seams without claiming production-quality persistence.

### 8. Generalize the existing S3 Control routing before S3 routing

The HTTP router now has explicit trusted endpoint and typed service
classification. `S3Only` listeners route only ordinary S3. `SharedRegional`
listeners recognize the S3 Control-style
`/v20180820/tags/<resource-arn>` path before normal bucket/key parsing and route
typed `ListTagsForResource`, `TagResource`, and `UntagResource` operations.
Those operations no longer inhabit `S3Operation`. Both services use the `s3`
credential-scope name through an exhaustive typed mapping, while remaining
distinct services because their endpoint paths and wire behavior differ. The
next extension adds STS to that same service-level boundary:

- normal S3 request: existing bucket/key path, expected SigV4 service `s3`
- S3 Control request: existing versioned resource path and account-ID header
  rules, expected SigV4 service `s3`
- STS Query request: Query protocol parser, expected SigV4 service `sts`
- later IAM Query request: expected SigV4 service `iam`

For the first slice, STS uses the same bound address, port, TLS configuration,
admission control, request IDs, and worker pool as S3. A custom AWS SDK STS
endpoint URL can therefore point at the existing S3 endpoint when that listener
uses TLS. AWS documents ordinary S3 regional endpoints as supporting HTTP and
HTTPS, but documents both S3 Control and STS endpoints as HTTPS-only. The
shared listener may continue serving ordinary S3 over plain HTTP only when the
S3 Control and STS service routes are disabled; enabling either service requires
a TLS listener. This is now enforced structurally: the plain serve entry point
constructs `S3Only`, the TLS entry point constructs `SharedRegional`, and the
latter cannot be constructed internally without a TLS acceptor. Focused network
tests prove that a valid S3 Control path remains an ordinary S3 path over HTTP.

Represent the origin of classification explicitly as a typed endpoint kind,
not as an inferred string inside an operation parser. The oracle has distinct
`AwsRegionalSts` and `AwsRegionalS3Control` kinds because those endpoint
families have different authorities. The initial local server has a
`SharedRegional` kind because one configured listener/authority serves S3, S3
Control, and STS. Endpoint kind comes from the configured listener/authority
mapping plus the parsed request target; an arbitrary `Host` value alone must
not select a more privileged parser or authentication rule.

Classification must not trust only one attacker-controlled hint. Pin and define
the interaction among the existing versioned S3 Control path, request path,
method, content type, `x-amz-account-id`, `Action`, `Version`, Host, and SigV4
credential service. Ambiguous requests must receive the same error family AWS
uses rather than falling through to an unrelated S3 operation.

Do not fold STS operations into `S3Operation`. Extend the existing
`ServiceOperation` with a typed STS payload so each service's parsing, auth
expectation, response format, and errors stay explicit. `ListTagsForResource`
now uses its own `s3:ListTagsForResource` authorization action and exact S3
Control response renderer rather than aliasing ordinary S3
`GetBucketTagging`. The bucket-tag authorization API accepts a narrow exhaustive
`ListTagsForResource`/`TagResource`/`UntagResource` enum instead of an arbitrary
policy action. The versioned path classifier selects the method and strictly
percent-decodes the resource path before authentication, then validates ARN
semantics, XML or `tagKeys`, and authorization afterward, as pinned by the
oracle. `x-amz-account-id` is deliberately not an authority input for these
operations.

The completed AWS-facing routing-boundary goldens cover:

- the bounded method set `GET`, `HEAD`, `POST`, `PUT`, `DELETE`, `OPTIONS`, and
  `PATCH` on the versioned tags path, plus representative extension methods
  `PROPFIND` and `X-ARGMIN-PROBE`
- malformed percent encoding, empty or malformed resource ARN, and path-shape
  near misses
- missing, empty, duplicate-identical, duplicate-conflicting, and wrong
  `x-amz-account-id` values
- correct, missing, and wrong SigV4 signing service with valid and bad
  signatures
- malformed XML and `tagKeys` inputs collided with authentication failures
- S3 Control path requests carrying STS `Action`/`Version` parameters or STS
  form content types, in both valid- and invalid-signature cases
- STS-shaped requests sent to the S3 Control path and S3 Control-shaped requests
  sent to the shared-listener STS classifier

The matching local end-to-end regression now locks that bounded method/path
matrix, including complete nested S3 Control error envelopes and semantic
headers, bodyless outer malformed-percent responses, ordinary S3-shaped outer
extension-method errors, and URI/ARN rejection before HMAC validation. The
ordinary S3 streaming-write pre-router explicitly defers both successful S3
Control routes and S3 Control route errors to buffered service dispatch, so a
`PUT` on the reserved path cannot be consumed as an S3 `PutObject`. A distinct
typed `S3Control` service also owns its signing-path rule: the encoded
`tags%2F` separator is canonicalized as `tags/`, matching the independently
SDK-checked request accepted by AWS, without changing ordinary S3 object-key
canonicalization.

Send the identical cross-service collision requests to both the regional AWS
STS endpoint and the account/region-specific AWS S3 Control endpoint, signing
and connecting to each endpoint normally so its real authority/Host is part of
the observation. Record both complete results even when they differ. Before
Phase 4 implementation, commit a table that maps every collision to the AWS
endpoint family deliberately emulated by `SharedRegional`; there must be no
implicit fallback based on whichever parser happens to run first. Ordinary
unambiguous versioned-tag requests should map to the S3 Control observation and
ordinary unambiguous Query requests to the STS observation, while every mixed
case was deliberately left unresolved at this design point. The completed
Phase 0 tables below resolve the named bounded cases before Phase 4
implementation.

The AWS probes do not verify the local Host/authority trust boundary. Add
separate `SharedRegional` tests that hold method, target, body, query, signing
scope, and signature constant while varying the HTTP authority among the
configured authority, an STS-looking name, an S3 Control-looking name, an
unrelated valid name, and a mismatched configured authority. Also cover missing
and duplicate `Host` values and an absolute-form request target whose authority
conflicts with `Host` where the HTTP stack admits those shapes. Each case must
prove either that the same trusted `SharedRegional` endpoint kind reaches the
same classifier result or that authority validation rejects the request before
service classification; changing only attacker-controlled authority text must
never select `AwsRegionalSts`, `AwsRegionalS3Control`, or another parser.

Keep trusted listener/TLS metadata separate from request headers in the
classifier API so illegal endpoint-kind construction is not representable. In
TLS standalone UAT, repeat the fixed-request cases with matching SNI, missing
SNI where the TLS stack permits it, and SNI/HTTP-authority mismatches against
the configured certificate/listener names. Assert the configured TLS policy's
handshake or authority rejection boundary and prove that a mismatched SNI cannot
change the endpoint kind after connection acceptance.

The local authority trust-boundary matrix completed on 2026-07-16 against the
real plain-HTTP and TLS listener paths. It holds the reserved S3 Control target,
method, query, `s3` credential scope, timestamp, payload hash, and deliberately
bad signature constant while varying only authority inputs. Configured,
STS-looking, S3-Control-looking, unrelated, and wrong-port authorities all
retain the S3 error root and authentication result. Missing and duplicate-
identical/conflicting `Host` fields either remain in that same S3 authentication
path or receive the HTTP parser's empty-body `400` before S3 request-ID headers
are allocated; the test accepts either EOF or chunked empty-body framing without
pinning which one is used. An absolute-form target with an authority conflicting
with `Host` retains the shared-listener result.

The TLS half uses the configured test CA normally for matching SNI and proves
that normal certificate verification rejects an unrelated DNS name. A test-
only verifier that still validates the certificate chain and the configured
`localhost` certificate name lets the client deliberately reach the listener
with an unrelated SNI, and an IP-address server name exercises a connection
without SNI. Both accepted connections retain the same S3 classifier shape, as
does a matching-SNI connection with an S3-Control-looking HTTP authority. The
custom verifier exists only to observe server behavior after a client bypasses
the normal name check; it is not a production trust policy. These deterministic
listener tests close the Phase 0 authority-input evidence. Phase 4 must retain
them while introducing the typed `SharedRegional` value and must repeat the
accepted/rejected cases through standalone TLS UAT to verify production
configuration wiring.

Those goldens must identify, per endpoint kind, which path/method/decoding
checks happen before authentication and which account/ARN/body/query checks
happen after it. The typed service refactor is complete only when it retains the
selected AWS-pinned ordering and exact response families for `SharedRegional`;
existing positive scenarios remain regression controls but are not sufficient
acceptance evidence. Other host-specific global/regional STS endpoint parity
can later integrate with
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

- 24-character SDK-compatible session access key IDs: `ARGS` followed by 20
  uniformly sampled uppercase ASCII letters or decimal digits (more than 103
  bits of entropy); use rejection sampling rather than biased byte modulo
- 40-character secret access keys produced as unpadded URL-safe base64 of 30
  random bytes (240 bits), so the client-visible length matches common SigV4
  tooling assumptions without using an AWS-owned shape
- unique AEAD nonces and opaque base64-encoded session envelopes with no
  client-visible fixed-size assumption
- 24-character stable role IDs: `ARGR` plus the same 20-character uniform
  alphabet; assumed-role IDs are the stable role ID, a colon, and the validated
  role-session name

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
than repeated across IAM, STS, authentication, and tests. An unresolved
`ARGS` access key is sufficient to select the temporary-credential error path
when the token is absent, but the prefix is only a routing hint: it never proves
issuance or permits authentication without successfully opening and binding the
token. No self-authenticating access-key encoding is required. A namespace is a
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
  policy-change outcomes, and record the distinct local provider-failure
  requirements for the implementation phases
- pin session-principal matching separately from `aws:PrincipalArn`, and pin an
  `aws:TokenIssueTime` deny boundary
- decide the exact first supported `AssumeRole` parameter set from evidence
- record success/error golden shapes and token-auth matrices

Exit condition for the first usable `AssumeRole` milestone: the committed
plan/test expectations needed by Phases 1 through 5 do not rely on guessed wire
or precedence behavior. Phase 0 does not block those phases on optional
`AssumeRole` parameter families assigned to Phase 6; until those parameters are
implemented, requests containing them must be rejected rather than silently
ignored.

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

The architecture decisions required by Phase 0's first-milestone exit condition
are now fixed. They include structured identity composition; the shared policy
core with typed policy wrappers and decision inputs; S3 identity bootstrap
separated from backend contents; typed process-local and standalone-UAT
configuration; the credential-precedence matrix; bounded Query-parser limits;
the temporary access-key namespace and generation shape; the session-token
envelope; credential-domain binding; and key-overlap semantics.
Session policies, tags and transitive tags, MFA, and provided contexts are
Phase 6 completeness work rather than blockers for beginning Phase 1. They
remain unsupported compatibility gaps and must never be silently ignored.

The role-fixture capability will use the ordinary primary and alternate test
users, not the owner/root credential. The shared test-user policy grants role
lifecycle and read-only inline-policy inspection/deletion under
`/argmin-sts-oracle/`. `PutRolePolicy` and `PutRolePermissionsBoundary` are
available only for uniquely named
`/argmin-sts-oracle/same-account/policy-mutation-*` roles carrying the exact
owner-managed `/argmin-s3-tests/sts-oracle-role-boundary`; that boundary limits
effective identity permissions to `s3:PutObject` in `claude-s3-*` buckets. The
user cannot remove or replace the boundary, attach managed role policies, or
use `PassRole`, so the mutation probe does not create a path to broader account
permissions. The fixture directly requires `AccessDenied` when the primary
user tries to write the policy to an unbounded oracle role or remove the
mutation role's boundary. Its identity-policy `sts:AssumeRole` grant covers
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
Every role except the dedicated bounded mutation fixture remains
permissionless. These temporary grants and the persistent boundary policy
remain only while the AWS oracle is needed. Remove them after its observations
have been transferred into implementation-facing conformance coverage in
Phase 5; completing Phase 0 alone is not the removal trigger.

The disabled-credential fixture also uses the ordinary primary test user. Its
IAM grants can create, inspect, deactivate, and delete only permissionless
users named `/argmin-sts-oracle/disabled-credential-*` and their access keys;
it has no user-policy, group, role, or policy-attachment permissions through
that surface. Every run creates a unique user, registers it for trap cleanup
before creating its one key, and deletes the key before the user. The periodic
cleanup command lists only the `/argmin-sts-oracle/` path, rechecks the exact
path/name prefix, applies the configured minimum age, deletes every key on a
stale matching user, and then deletes the permissionless user. `ListUsers`
requires the same narrowly handled `Resource: "*"` exception as `ListRoles`.

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
  creating or versioning the customer-managed test policy and bounded mutation
  role permissions boundary, and attaching only the test-user policy
- creates, converges, assumes, and deletes uniquely named
  roles under `role/argmin-sts-oracle/same-account/`, including the
  `path-shape-*`, `default-max-*`, `external-id-*`, `source-identity-*`, and
  `chain-target-*` permissionless fixture families plus the boundary-constrained
  `policy-mutation-*` family, using the primary test user; it never adopts an
  existing role and normalizes temporary secrets before golden response
  comparison
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
Before deleting a stale role, cleanup lists and removes its inline policies;
the normal oracle trap does the same for every role it successfully created.
IAM discovery failure is reported and makes the command exit unsuccessfully
after the existing bucket cleanup has still run; it cannot disable bucket
recovery.

The cross-account role-fixture slice completed on 2026-07-13. The alternate
account's administrator applied the same test-user policy there; the
primary-account owner credential was not used as a substitute.

`./scripts/aws-sts-oracle --cross-account` creates five unique,
permissionless roles in the primary account and independently proved against
AWS that:

- caller identity policy `allowed` plus trust allow produces success
- caller identity policy `allowed` plus trust omission produces trust denial
- caller identity policy `implicitDeny` plus trust allow produces caller-policy
  denial
- wildcard trust plus caller identity policy `allowed` produces cross-account
  success, while the same wildcard trust plus `implicitDeny` produces
  caller-policy denial

The success, wildcard-success, and trust-denial roles use
`/argmin-sts-oracle/cross-account/`, while both caller-denial roles use
`/argmin-sts-oracle/caller-denied/`, outside the caller's identity-policy
resource grant. Every created role is registered
for cleanup only after `CreateRole` succeeds, and periodic cleanup recognizes
the new path/name pairs with the same one-hour age floor.

The exact-principal caller-denial role directly trusts both test users. The
wildcard caller-denial role trusts `*`. IAM simulation proves that the primary
user also has `implicitDeny` for each denied role ARN, while successful primary
assumption proves the role and trust policy have reached STS before the
alternate caller's denial is asserted. This separates caller-policy denial from
target propagation for both trust-principal forms.

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
- an empty `RoleArn` always reports the same pattern and minimum-length
  validation failures, but AWS does not give their order stable precedence:
  20 identical requests to the `eu-central-1` regional endpoint on 2026-07-19
  returned pattern-first 17 times and minimum-length-first three times; the
  oracle therefore accepts exactly those two complete golden messages while
  continuing to pin the rest of the response shape, and the implementation
  must not infer precedence between those two validation failures
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
  behavior rather than interpreting the displayed pattern literally. For the
  2,049-character supplementary input, repeated live runs returned its exact
  pattern and maximum-length clauses in both orders, so the oracle accepts
  only those two complete messages and does not infer precedence between them
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
known optional `AssumeRole` inputs: session policy/policy ARNs, tags/transitive
tags, MFA fields, and provided contexts still need explicit AWS-backed scope
decisions. External ID and source identity are pinned by the following slices.
The one-hour role-chaining limit also remains separate from the API-level and
configured-role duration bounds pinned here.

The Query POST body-limit slice completed on 2026-07-16. The
[AWS Query API guide](https://docs.aws.amazon.com/IAM/latest/UserGuide/programming.html)
recommends POST for requests too large for practical GET URLs but does not
publish a total POST body ceiling. The regional live endpoint accepts an exact
10,000,000-byte `application/x-www-form-urlencoded` body and executes
`GetCallerIdentity` successfully when one ignored scalar member contains a
9,999,954-byte value. At 10,000,001 bytes it returns HTTP `413` with an empty
body and no semantic response headers, including no STS request ID or extended
request ID; transport framing and the ordinary HTTP date remain ignored. The
total is therefore decimal wire bytes, not 10 MiB.

At that same exact body ceiling, AWS also executes `GetCallerIdentity` with a
9,999,956-byte ASCII unknown key-only member name, and with 4,999,980 total
form members: the required `Action` and `Version`, 4,999,977 repeated shortest
key-only `x` members, and one final `y=` member that consumes the odd remainder.
Those constructions exhaust the body with the largest possible name, value,
and nonempty member count respectively, so there is no independent generic
member-name, member-value, or member-count ceiling below the total POST body
ceiling for these syntactically valid forms. Empty components introduced only
by repeated separators are not counted as form members.

The signed GET boundary is imposed by the HTTP request head rather than by a
fixed Query-string scalar limit. The boundary golden is deliberately restricted
to the exact fixture on which it was observed: the
`https://sts.eu-central-1.amazonaws.com` authority, `eu-central-1` signing
region, and an ordinary 20-byte IAM access key. With that fixed signing shape,
a 15,844-byte query executes successfully while 15,845 bytes returns HTTP `400`
with an empty body and no semantic response headers. Adding an empty signed
`x-test-padding` header makes the otherwise accepted 15,844-byte query return
that same `400`; a 15,800-byte query with a one-byte value for the same signed
header still succeeds. The oracle skips only these aggregate-head rows when an
advertised `--region` or `--endpoint` override, or a differently sized access
key, changes the wire-head arithmetic; the portable POST-limit rows continue
to run. Do not encode 15,844 as a universal Query-string constant: the shared
HTTP frontend must bound the complete request head and the local oracle must
reproduce these accepted/rejected fixed-fixture signed shapes before Query
decoding.

The first supported `AssumeRole` scalar members already have their individual
decoded-value boundaries pinned by the core, external-ID, and source-identity
slices. The shared Query reader must enforce the 10,000,000-byte POST wire-body
limit before allocating a decoded form. It must scan and bind incrementally so
millions of ignored or duplicate members do not require millions of owned
allocations, while per-operation binding applies the separately pinned member
constraints and first-value duplicate semantics. This does not authorize
silently ignoring known optional security-relevant `AssumeRole` members that
remain assigned to Phase 6.

The `ExternalId` validation and trust-policy slice completed on 2026-07-14.
The exact AWS-backed matrix establishes that:

- omission remains valid; present values must contain 2 through 1,224
  characters and match `[\w+=,.@:\/-]*`
- `azAZ09_+=,.@:/-` succeeds, pinning letters, digits, underscore, and every
  punctuation character admitted by the rendered pattern
- the two- and 1,224-character ASCII boundaries succeed with the ordinary
  no-session-policy response shape: `ExternalId` is not echoed and
  `PackedPolicySize` remains absent
- empty, one-character, space-containing, and 1,225-character values receive
  the exact single-error `ValidationError` shapes for the violated constraint
- when pattern and length both fail, AWS returns both exact applicable clauses.
  Repeated live runs of the one-character invalid value returned the pattern
  and minimum-length clauses in both orders, so only those two complete
  messages are accepted. Later live runs likewise returned the 1,225-character
  invalid value's pattern and maximum-length clauses in both orders; only the
  two complete messages observed for each collision shape are accepted
- length is counted in decoded Unicode scalar values, not UTF-8 bytes or UTF-16
  units: 613 `é` characters occupy 1,226 bytes but receive only the pattern
  error, and 613 supplementary characters occupy 2,452 bytes and 1,226 UTF-16
  units but likewise receive only the pattern error
- duplicate `ExternalId` parameter validation uses the first wire value: a
  valid first value followed by an invalid value succeeds, while the reversed
  order returns the invalid first value's exact pattern error
- a separate uniquely named same-account role has an exact
  `StringEquals`/`sts:ExternalId` trust condition; IAM policy simulation first
  proves that the caller has no applicable identity-policy grant, then the
  fixture requires three consecutive successful assumptions with the expected
  external ID before running the response matrix
- the expected external ID succeeds with the ordinary exact response shape;
  omission or a different shape-valid value receives the same exact
  `AccessDenied` response for the target role
- duplicate values also use the first wire value during trust evaluation: the
  expected value followed by the wrong value succeeds, while the reverse order
  receives `AccessDenied`

The unconditioned role isolates fixed-schema validation, while the conditioned
role pins the security meaning of the selected value. Local support must carry
that selected value in typed request context and evaluate it as
`sts:ExternalId` in the role trust policy; accepting and ignoring the parameter
would fail the oracle.

The `SourceIdentity` validation, trust-policy, and chaining slice completed on
2026-07-14 and its length boundary was re-observed on 2026-07-19 after the live
oracle became part of the ordinary AWS wrapper. It uses three unique
permissionless role shapes: an unconditioned source role granting both
`sts:AssumeRole` and `sts:SetSourceIdentity`, a role with an exact
`sts:SourceIdentity` trust condition, and a chaining target that trusts the
source IAM role with the same condition. The exact AWS-backed matrix
establishes that:

- omission is schema-valid; a present value must contain 2 through 256
  characters and match `[\w+=,.@-]*`
- `azAZ09_+=,.@-` and the two- and 256-character boundaries succeed, pinning
  every rendered character class and the exact success XML; the response adds
  `SourceIdentity` after `Credentials`, while `PackedPolicySize` remains absent
- empty and one-character pattern-valid inputs receive the exact minimum-length
  `ValidationError`, a space-containing value receives the exact pattern error,
  and a 257-character pattern-valid value receives the exact maximum-length
  error. Live observations of both the one-character and 257-character
  pattern-invalid probes returned their two applicable clauses in both
  pattern-first and length-first order, so each collision accepts only its two
  complete observed messages
- length is counted in decoded Unicode scalar values rather than UTF-8 bytes or
  UTF-16 units: 129 `é` characters and 129 supplementary characters receive
  only the pattern error despite occupying 258 UTF-8 bytes and, for the
  supplementary case, 258 UTF-16 units
- both `aws:reserved` and `AWS:reserved` receive the ordinary exact pattern
  error because the colon is outside the admitted pattern; AWS exposes no
  separate reserved-prefix error for these inputs
- the condition-matching value succeeds; omission denies `sts:AssumeRole`,
  while a different shape-valid value denies `sts:SetSourceIdentity`, each with
  its complete exact `AccessDenied` response
- duplicate `SourceIdentity` fields use the first wire value for validation,
  trust evaluation, and the returned source identity: expected-first succeeds
  even when followed by either a wrong or pattern-invalid value, while the
  reverse orders return the first value's authorization or validation error
- a source identity is inherited when the resulting temporary credentials
  assume another role, even when the chained request omits the parameter; the
  inherited value appears in the exact success response
- explicitly repeating the inherited value also succeeds, while trying to
  replace it returns HTTP 400 `ValidationError` with exactly `The source
  identity is already set for this assume role session`
- the target must grant `sts:SetSourceIdentity` for inheritance. The fixture
  first converges a successful assumption against the exact target, then
  removes only that action with `UpdateAssumeRolePolicy` and requires three
  consecutive denials, resetting the count on any success or other response;
  the raw request then receives the exact `AccessDenied` naming the source
  session, `sts:SetSourceIdentity`, and target role ARN

Local sessions therefore need an immutable optional source-identity field in
the sealed session context. AssumeRole must authorize `sts:SetSourceIdentity`
when setting or propagating it, evaluate `sts:SourceIdentity` in trust policy,
and later expose the same immutable value as `aws:SourceIdentity` during
resource authorization. Treating it as request-only decoration would fail both
the direct and chained oracle matrices.

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
inputs. The later expiry/deletion slice orders expiry before issuer liveness.

The session-principal context and trust-mutation slice completed on 2026-07-14.
It uses the permissionless recreated-role session and a self-cleaning bucket
policy whose statements isolate each identity representation on a separate
object key. The fixture requires three consecutive successful passes across
every positive statement before and after trust mutation. Complete S3 response
goldens establish that:

- both the IAM role ARN and the exact path-free assumed-role session ARN are
  accepted as resource-policy principals for the session
- `aws:PrincipalArn` is the path-bearing IAM role ARN, not the assumed-role
  session ARN: the role value permits `PutObject`, while the session value does
  not match and receives the ordinary authorization `AccessDenied`
- `aws:userid` is `<stable-role-id>:<role-session-name>`; that exact value
  permits the request and a distinct value does not
- `aws:TokenIssueTime` is present and is the immutable credential issuance
  instant rather than request time. Both bounds are derived from the AWS
  response's `Credentials.Expiration` minus the requested 3,600-second duration,
  avoiding any dependency on the oracle host's clock. One-second bounds around
  that server-derived issuance time let `DateGreaterThan`/`DateLessThan`
  independently prove both sides. A true `DateLessThan` explicit deny overrides
  a matching allow with the exact resource-policy-deny response; the
  corresponding false deny leaves the allow effective

A separate path-bearing IAM user with a fresh access key and no identity
policies pins the long-lived caller form of `aws:PrincipalArn`. A matching
`ArnEquals` resource-policy allow is sufficient for same-account `PutObject`;
a matching conditional deny overrides an otherwise matching resource-policy
allow, while the same deny with the primary user's distinct ARN does not
match. The fixture requires three consecutive allow/control/deny result sets
before the Rust oracle asserts the complete success and explicit-resource-deny
response shapes. The temporary user and access key are self-cleaning and are
also recognized by periodic cleanup.

After positive policy convergence, the fixture changes the recreated role's
trust policy so the primary user can no longer perform an ordinary assumption
and requires three consecutive STS `AccessDenied` results. The already-issued
session continues to pass every positive resource-policy statement for three
consecutive iterations, and the complete raw matrix runs only after that
convergence.
Current trust policy is therefore an `AssumeRole` issuance input, not an active
session authentication or resource-authorization input. Stable issuer-role
liveness remains an authentication requirement as pinned by role deletion.

The role permission-policy mutation slice completed on 2026-07-15. A unique
same-account role is created with the owner-managed permissions boundary that
limits it to `s3:PutObject` in prefixed test buckets. Its inline role policy
first allows `PutObject` only on `role-policy-mutation-*` keys. The fixture
requires three successful requests from independently issued sessions and then
three from one retained pre-mutation session before replacing that same inline
policy with an explicit deny.

After the replacement round-trips through `GetRolePolicy`, independently
issued post-mutation sessions must produce three consecutive S3 explicit
identity-policy denials. The retained post-mutation session then produces
three more denials. Only after these target-specific convergence controls does
the retained pre-mutation session prove three consecutive STS
`GetCallerIdentity` successes followed by three S3 explicit identity-policy
denials. This distinguishes a live session using current role policy from an
issuance-time policy snapshot.

Complete raw goldens establish the same explicit identity-policy-deny response
for both the pre- and post-mutation sessions. The pre-mutation session combined
with a bad signature instead returns the complete `SignatureDoesNotMatch`
response. Current role permission policy is therefore resolved at the S3
authorization boundary after signature verification; it is not sealed into
the session, consulted during authentication, or permitted to override a bad
signature.

The stateless credential-envelope design slice completed on 2026-07-15. The
Ceph reference confirms the useful property that the issued access key, secret,
expiry, issuer, and session authorization context can travel in an encrypted
token without an issued-session database. Argmin deliberately does not copy
Ceph's fixed-IV unauthenticated AES-CBC construction. Version 1 instead uses
the `ARGST1.` format and AES-256-GCM frame specified above, with strict size and
canonical payload bounds and a credential domain authenticated as associated
data.

Version 1 carries no inline or managed session policy; those unsupported
parameters remain explicit request errors until Phase 6 pins their character
limits and introduces a transport-safe later envelope version. The 16,384-byte
decoded limit is only a defensive decoder allocation bound. Issuance is capped
at the derived 742-byte external-token maximum and must prove its maximum
field shapes fit the complete 8,192-byte PutObject and aws-chunked header
sections as well as the presigned and POST transports.

Temporary access key IDs are fixed at `ARGS` plus 20 uniform uppercase
alphanumeric characters, and secret keys are fixed at 40 URL-safe characters
generated from 30 random bytes. Static providers reserve and reject the `ARGS`
namespace. This lets a missing-token request select the AWS-pinned temporary-
credential error mapping without turning the namespace into authentication:
only a successfully opened token, constant-time access-key binding, expiry and
issuer-liveness validation, and SigV4 verification authenticate the session.

The process-local ring has one random active key, key ID, and credential domain
shared by all workers. The configured-ring contract is also fixed for later
work: one active issuance key, unique validation-only key IDs, an explicit
shared domain, irreversible `Active -> ValidationOnly -> Removed` transitions,
and overlap until every session issued by a retired key has expired. Removing a
validation key is intentional bulk revocation; an old key is never promoted and
never receives a reset nonce allocator. Removed IDs and one-way key-material
fingerprints remain as internal tombstones, including after the decrypting key
has been dropped. This resolves the envelope encoding, missing-token routing,
and key-overlap questions without adding a production dependency.

The STS signing-scope slice completed on 2026-07-14. The existing configured
regional endpoint success is its positive control. Complete STS response
goldens establish that:

- signing that endpoint for another valid region returns HTTP 403
  `SignatureDoesNotMatch` with exactly `Credential should be scoped to a valid
  region. `, including its trailing space
- signing with service `s3` returns the same code with exactly `Credential
  should be scoped to correct service: 'sts'. `; when both region and service
  are wrong, AWS concatenates the two messages in region-then-service order
- the global `https://sts.amazonaws.com` endpoint accepts a request scoped to
  `us-east-1`, while a `us-west-2` scope receives the same wrong-region golden
- wrong-region and wrong-service scope errors each precede session-token
  presence and binding, stable issuer-role liveness, and HMAC comparison. A
  live session with a valid, missing, or independently valid mismatched token,
  and a deleted-role session with those same token shapes, all receive the
  scope error; a valid token combined with a bad secret does too

On the STS route, credential-scope parsing must therefore occur before
temporary-credential resolution. Only a correctly region- and service-scoped
STS request proceeds to token opening, token/access-key binding, expiry and
issuer-liveness checks, and signature comparison. No ordering relative to
scope validation is inferred here for any S3 authentication mode; the header
ordering described next comes only from its separate S3 matrix.

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

The S3 header signing-scope slice completed on 2026-07-14 against the fixture's
existing bucket-specific endpoint. A correctly scoped live session first
reaches authorization and receives the complete `s3:ListBucket` `AccessDenied`
golden, so the scope failures cannot pass merely because the target is absent
or unroutable. Complete response goldens establish that:

- a wrong region returns HTTP 400 `AuthorizationHeaderMalformed`, the exact
  wrong/expected-region message, the expected `Region` XML element, and the
  `x-amz-bucket-region` response header
- service `sts` returns HTTP 400 `AuthorizationHeaderMalformed` with exactly
  `The authorization header is malformed; incorrect service "sts". This
  endpoint belongs to "s3".`, omits the `Region` XML element, and retains the
  `x-amz-bucket-region` header
- when region and service are both wrong, only the wrong-region response is
  rendered; this differs from STS, which aggregates both scope messages
- each scope error precedes signed session-token structure, presence and
  binding, stable issuer-role liveness, and HMAC comparison. Live and
  invalidated sessions with valid, missing, empty, malformed, independently
  valid mismatched, identical-duplicate, or conflicting-duplicate tokens in
  both conflicting orders receive the scope error, as does a valid token
  combined with a bad secret

The S3 header route must therefore validate its credential scope before the
probed signed token-header structural checks, token/access-key binding, issuer
liveness, and HMAC checks. This does not establish the placement or collision
behavior for presigned, POST Object, or streaming authentication; each mode's
separate matrix below establishes its own behavior.

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

Duplicate signed token headers are canonicalized as one comma-joined value in
wire order. Two identical live values are accepted as one effective token:
the correct HMAC reaches the ordinary authorization `AccessDenied`, while a bad
HMAC reaches `SignatureDoesNotMatch`. Two different independently live token
values return `InvalidAccessKeyId` in both wire orders, with correct and bad
HMACs, so conflicting-value rejection precedes signature comparison rather
than selecting the first or last value. Signed-header precedence over the query
location still applies to duplicates: two identical live header values override
an independently live mismatched query token, while conflicting header values
return `InvalidAccessKeyId` and are not rescued by the matching query token.
The oracle asserts the duplicate header wire order before sending every case.

For presigned `SignatureDoesNotMatch`, the oracle validates the canonical query,
canonical-request hash and byte list, string to sign and byte list, credential
scope, and echoed signature. AWS XML-escapes the literal query separators to
`&amp;` inside the `CanonicalRequest` element, while
`CanonicalRequestBytes` and the canonical-request hash are calculated from the
unescaped `&` bytes. Exact golden diagnostics sanitize the raw token, its
URI-encoded form, and the byte encodings of both for every presented token, not
only the selected header token. The dual-location bad-signature golden exercises
both token values in one canonical request. The POST Object matrix described
next and the streaming matrix were not part of that presigned slice.

The S3 SigV4 presigned-query signing-scope slice completed on 2026-07-14 against
the existing bucket-specific endpoint. A correctly scoped live session first
reaches authorization and receives the complete `s3:ListBucket` `AccessDenied`
golden. Complete response goldens then establish that:

- a wrong region returns HTTP 400 `AuthorizationQueryParametersError`, the
  exact `X-Amz-Credential` wrong/expected-region message, the expected `Region`
  XML element, and the `x-amz-bucket-region` response header
- service `sts` returns HTTP 400 `AuthorizationQueryParametersError` with
  exactly `Error parsing the X-Amz-Credential parameter; incorrect service
  "sts". This endpoint belongs to "s3".`, omits the `Region` XML element, and
  retains the `x-amz-bucket-region` header
- when region and service are both wrong, only the wrong-region response is
  rendered
- each scope error precedes the probed query-token presence and structural
  cases, signed-header selection, unsigned-header coverage validation,
  token/access-key binding, stable issuer-role liveness, and HMAC comparison.
  The query cases include missing, empty, malformed, independently valid but
  mismatched, identical duplicate, and conflicting duplicate tokens in both
  conflicting wire orders. Signed-header cases include valid, empty, malformed,
  and mismatched-selected values, plus identical duplicates and conflicting
  duplicates in both wire orders with valid and bad HMACs. The unsigned-header
  case combines a valid query token with a present valid but uncovered header.
  The invalidated old session and a live token signed with a bad secret also
  receive the scope error. The presigner sorts a separate canonical query for
  SigV4 while retaining the supplied base-query order in the emitted URI; the
  oracle parses each URI to assert that order and asserts that the two
  conflicting-order URIs differ. It also asserts the supplied signed-header
  order before every request.

This pins presigned-query scope ordering only for those cases. The matrix does
not establish scope placement for POST Object or aws-chunked streaming.

The S3 POST Object session-authentication slice completed on 2026-07-14. The
`--assume-role` fixture creates one unique `claude-s3-` bucket with the primary
test user, exports only its name, and independently empties and deletes it on
exit; it does not use the owner credential. The permissionless recreated role
then provides the positive authentication control without writing an object: a
correct form token and policy signature reach authorization and return the
complete `s3:PutObject` `AccessDenied` golden containing the assumed-role ARN
and exact object resource.

For POST-policy authentication, `x-amz-security-token` is selected from the
multipart form and is covered by a matching policy condition. A missing, empty,
or independently valid but mismatched form token returns the complete HTTP 403
`InvalidAccessKeyId` golden before policy-signature comparison, including with
a bad signature. Two conflicting form-token fields return `InvalidAccessKeyId`
rather than selecting the first or last value: valid-then-mismatched and
mismatched-then-valid orders produce the same result with correct and bad
signatures. Two identical valid values are accepted as one effective token for
credential and signature verification, but are not erased from POST-policy
evaluation. With a correct signature, the equality condition fails with the
complete HTTP 403 `AccessDenied` message `Invalid according to Policy: Policy
Condition failed: ["eq", "$x-amz-security-token", "<token>"]`. With a bad
signature, `SignatureDoesNotMatch` wins over that policy-condition failure. A
malformed non-empty token returns the complete HTTP 400 `InvalidToken` golden,
echoes the rejected value in `Token-0`, and also wins over a bad signature.

A correct form token plus a bad signature reaches the complete
`SignatureDoesNotMatch` golden. Its `StringToSign` is exactly the base64 POST
policy, `StringToSignBytes` is the hexadecimal encoding of that policy, and the
provided signature and access key are echoed without canonical-request fields.
The golden sanitizes the complete policy and its byte encoding in addition to
all presented token forms.

The invalidated old role session returns `InvalidAccessKeyId` with its correct
or mismatched form token, with a missing token, and with correct or bad
signatures. Form-token presence, token/access-key binding, and stable issuer
liveness therefore precede POST policy-signature verification just as they do
on the header and presigned paths.

An HTTP `x-amz-security-token` header does not supply or override the POST form
token. Its mere presence selects a different authentication route, which
returns the complete HTTP 403 `AccessDenied` golden with exactly
`No AWSAccessKey was presented.` This response wins for a header-only token, a
valid form plus mismatched header, a mismatched form plus valid header, and a
bad POST policy signature. A malformed header token and an invalidated old
session header token produce the same response when combined with a valid form
token and bad policy signature. Header-presence routing therefore precedes
header-token decoding, issuer-liveness validation, form-token selection, and
POST policy-signature comparison. With the empty, malformed, and duplicate form
matrix pinned, aws-chunked streaming was the remaining independently unpinned
S3 temporary credential mode.

The S3 POST Object signing-scope slice completed on 2026-07-14 using the same
bucket and permissionless live-session authorization control. Complete response
goldens establish that, when no HTTP session-token header is present:

- a wrong form `x-amz-credential` region returns HTTP 400 `InvalidArgument`
  with the exact wrong/expected-region message, `ArgumentName` equal to
  `X-Amz-Credential`, the complete credential in `ArgumentValue`, and the
  expected `Region` element; unlike the header and presigned responses it has
  no `x-amz-bucket-region` header
- service `sts` returns HTTP 400 `InvalidArgument` with exactly `incorrect
  service "sts". This endpoint belongs to "s3".`, the same argument name and
  complete credential value, no `Region` element, and no bucket-region header
- when region and service are both wrong, only the wrong-region response is
  rendered
- both scope errors precede missing, empty, malformed, independently valid but
  mismatched, identical-duplicate, and conflicting-duplicate form tokens in
  both conflicting orders. They also precede stable issuer-role liveness and
  policy-signature comparison: an invalidated old session and a live session
  signed with a bad secret receive the scope error

An HTTP `x-amz-security-token` header retains the earlier routing precedence
over these new scope checks. Valid, malformed, and invalidated-old-session
header values, each combined with an otherwise valid form credential and token,
all return the exact HTTP 403 `No AWSAccessKey was presented.` golden for both
wrong region and wrong service. POST implementations must therefore perform
header-presence routing before form scope parsing, and scope parsing before the
probed form credential-validity and signature checks.

The S3 aws-chunked session-authentication slice completed on 2026-07-14. The
primary user applies a self-cleaning bucket policy granting only the recreated
role `s3:PutObject` on the fixture bucket's `streaming-*` keys, and the fixture
proves that grant has converged with the temporary credential before running
the raw oracle. A live role session with a signed token, valid seed signature,
and valid chunk chain completes the upload. The exact HTTP 200 golden includes
the request and host IDs, SSE-S3, ETag, CRC64NVME checksum, and full-object
checksum-type headers. A bad first chunk signature reaches the complete
`SignatureDoesNotMatch` golden: AWS echoes the chunk string to sign and
provided chunk signature while retaining the seed request's canonical request
and byte encodings.

A missing or empty token returns `InvalidAccessKeyId`; a malformed non-empty
token returns HTTP 400 `InvalidToken` and echoes the rejected value in
`Token-0`; and an independently valid but mismatched token returns
`InvalidAccessKeyId`. Each token error wins over both a bad seed signature and
a bad first-chunk signature. A present but unsigned token returns
`HeadersNotSigned` before credential, seed-signature, or chunk-signature
validation: the same result wins over a bad seed signature, a bad first-chunk
signature, and invalidated old-session credentials, including when invalidation
and a bad seed collide. Two identical signed token headers collapse to one
effective credential and therefore complete the upload or reach
`SignatureDoesNotMatch` at the deliberately corrupted seed or first chunk.
Conflicting signed token headers return `InvalidAccessKeyId` in both value
orders, with valid or bad seed signatures, and also win over a bad first-chunk
signature.

After deletion and same-name role recreation, the old role session returns
`InvalidAccessKeyId` with its correct token, a missing token, or a mismatched
live token. Stable issuer invalidation also wins over bad seed and bad chunk
signatures. Streaming token selection, token/access-key binding, and stable
issuer liveness therefore feed the shared credential-validity pipeline before
the seed signature is accepted and before a streaming signing context is
constructed. The streaming slice completes the independently pinned S3
temporary-credential token-selection modes; scope and expiry collisions are
tracked by separate Phase 0 slices.

The S3 aws-chunked signing-scope slice completed on 2026-07-14. Dedicated exact
goldens establish that a wrong region or service returns HTTP 400
`AuthorizationHeaderMalformed` with the same response body as ordinary header
authentication, but without the `x-amz-bucket-region` response header. The
wrong-region body includes the expected `Region` element; the wrong-service
body does not. Wrong region wins when region and service are both wrong.

Both scope errors precede missing, empty, malformed, independently valid but
mismatched, identical-duplicate, and conflicting-duplicate token headers in
both conflicting orders. Scope also precedes the streaming-specific
`HeadersNotSigned` check for a present unsigned token, stable issuer-role
liveness for an invalidated old session, seed-signature comparison, and first
chunk-signature comparison. The invalidated-session cases collide liveness
with correct and bad seed signatures and correct and bad first-chunk
signatures. Together with the earlier header, presigned-query, and POST Object
matrices, region/service scope placement is now pinned independently for every
initial S3 temporary-credential authentication mode. Expiry collisions were
handled by the separate slice described next.

The expiry-versus-issuer-deletion slice completed on 2026-07-15. A dedicated
permissionless same-account role issues both AWS's minimum 900-second session
and a separate 3,600-second liveness control. While the issuer still exists,
the fixture polls the short session until AWS itself returns three consecutive
`ExpiredToken` responses, avoiding any host-clock assumption. It then deletes
the issuer and requires three consecutive `InvalidClientTokenId` responses
from the still-unexpired control through STS. Before testing the expired
credential through S3, the oracle independently requires three consecutive
`InvalidAccessKeyId` responses from that same control through header,
presigned-query, POST Object, and aws-chunked streaming authentication. These
mode-specific gates prove that each S3 issuer-liveness view has converged after
the deletion rather than allowing `ExpiredToken` to mask a stale live-role
view.
The long-running fixture is explicitly selected with
`./scripts/aws-sts-oracle --expiry`; ordinary `--assume-role` runs do not wait
through the minimum session lifetime. The specialized
`--disabled-credential` option also waits for and consumes a real expired-token
fixture for its inactive-key collisions, but does not run the ordinary expiry
suite.

Complete goldens establish that the expired-and-deleted session returns STS
HTTP 403 `ExpiredToken` with exactly `The security token included in the
request is expired`, for both correct and bad signatures. S3 header,
presigned-query, POST Object, and aws-chunked streaming authentication all
return HTTP 400 `ExpiredToken` with exactly `The provided token has expired.`
and echo the expired credential in `Token-0`; assertions sanitize that value
before comparing or reporting failures. The S3 result wins over a bad request
or POST-policy signature, and streaming expiry wins over both a bad seed
signature and a bad first-chunk signature. Expiry therefore precedes stable
issuer-role liveness, which in turn precedes signature verification.

The STS expiry-input collision extension completed on 2026-07-16 using the
same already-expired credential and an independently live session token. With
the correct STS scope, omitting the expired credential's token or substituting
the live token returns the exact HTTP 403 `InvalidClientTokenId` golden with
`The security token included in the request is invalid.`, for both correct and
bad signatures. AWS therefore cannot reach the embedded expiry check until a
token has been selected and bound to the access key. Conversely, wrong-region
and wrong-service scope errors win over the expired credential with its
correct, missing, or mismatched token; the correct expired token combined with
each scope error and a bad signature receives the scope error too. This closes
the STS expiry collision ordering.

The equivalent S3 expiry-input collision extension completed on 2026-07-16
for header, presigned-query, POST Object, and aws-chunked streaming
authentication. Each mode uses the already-expired, deleted-issuer credential
and the independently live session token from the same fixture. With correct
scope, omitting the expired credential's token, supplying an empty token, or
substituting the live token returns that mode's exact HTTP 403
`InvalidAccessKeyId` golden for both correct and bad signatures. A malformed
non-empty token instead returns the exact HTTP 400 `InvalidToken` golden before
signature comparison. Two identical expired-token inputs are accepted through
structural validation and reach `ExpiredToken`; that error preserves both
presented values as `Token-0` and `Token-1`. Conflicting expired and live
tokens return `InvalidAccessKeyId` in both wire orders. These cases cover the
signed HTTP header for header authentication, query member for presigning,
multipart form field for POST Object, and signed HTTP header for aws-chunked.
Mode-specific structural validation and a successful token open/access-key
binding therefore gate the embedded expiry check in every initial S3 mode.
Conversely, each mode's wrong-region and wrong-service response wins over the
expired credential with its correct, missing, or mismatched token, and also
wins when the correct expired token is combined with a bad header/presigned
request, POST-policy, or streaming seed signature. Header and presigned scope
probes use the same virtual-hosted bucket endpoint as their established scope
goldens; their correct-scope authentication collisions retain the regional S3
endpoint, so endpoint style is not an uncontrolled response-shape variable.
Together with the prior expiry-versus-signature and expiry-versus-liveness
probes, this pins, for each mode's tested primary token location, the ordering
as scope, then mode-specific token structure/selection/opening/binding, then
expiry, then issuer liveness, then signature verification.

The presigned alternate-header expiry extension completed on 2026-07-19 using
the same expired/deleted credential and independently live token. For a signed
`x-amz-security-token` header, a single expired token returns the exact
`ExpiredToken` golden with correct and bad HMACs. Empty and independently live
mismatched headers return `InvalidAccessKeyId`, a malformed header returns
`InvalidToken`, two identical expired headers reach `ExpiredToken` and preserve
both values as `Token-0` and `Token-1`, and conflicting expired/live headers
return `InvalidAccessKeyId` in both wire orders. Every case has correct- and
bad-HMAC variants and asserts the duplicate header wire order before sending.

The same extension pins signed-header selection against the query location.
An expired signed header overrides an independently live query token and
returns `ExpiredToken`; an independently live signed header overrides the
expired query token and returns `InvalidAccessKeyId` because it cannot bind to
the access key. Empty and malformed signed headers likewise override the
otherwise matching expired query token. Identical expired signed headers
override a live query token and reach duplicate-token `ExpiredToken`, while
conflicting signed headers are not rescued by a matching expired query token
in either header order. These results close the presigned signed-header path to
the same ordering already established for the primary query location: scope,
signed-header structure and authoritative location selection, token
opening/access-key binding, expiry, issuer liveness, then HMAC comparison.
The exact wrong-region and wrong-service goldens also win over an expired
signed-header token with both correct and bad HMACs, directly establishing the
scope-before-expiry edge for this alternate location.

A present but unsigned `x-amz-security-token` remains a coverage error rather
than an alternate authenticated token location. It returns the exact
`HeadersNotSigned` golden before expiry, header-token decoding/binding, and HMAC
comparison when the expired query token is present and the uncovered header is
expired, independently live, or malformed, and when the expired uncovered
header is present without any query token. Each case is pinned with correct and
bad HMACs. The separately established scope matrix places presigned scope
validation before this coverage error.

The disabled long-lived credential slice completed on 2026-07-19. The fixture
creates a unique permissionless IAM user and one access key rather than
deactivating either persistent test credential. Before changing its status, it
requires three consecutive positive authentication results independently
through STS, S3 header auth, presigned query auth, POST Object, and aws-chunked
streaming, then compares a following control response to that mode's complete
success or authorization-denial golden. After `UpdateAccessKey` changes only
the new key to `Inactive`, each of those five modes must converge to three
consecutive credential errors before collision assertions begin. This
prevents IAM or service-specific propagation delay from making a negative
probe pass for the wrong reason. Header and presigned additionally require
their active authorization-denial and inactive `InvalidAccessKeyId` controls
to converge on the exact virtual-hosted bucket endpoint used by their
wrong-scope requests, followed in each state by a complete exact golden on
that endpoint. The virtual-hosted inactive golden includes
`x-amz-bucket-region`; the regional-root inactive golden does not.

With correct scope, STS returns the exact HTTP 403 `InvalidClientTokenId`
golden for the inactive key with no session token, an independently live role
session token, or a genuinely expired role session token. Each result is
unchanged with a deliberately bad HMAC. S3 header, presigned-query, POST
Object, and aws-chunked authentication instead return their exact HTTP 403
`InvalidAccessKeyId` golden for the same missing/live/expired token and HMAC
matrix. An inactive stored credential is therefore rejected before HMAC
comparison and before an otherwise selected token is opened, bound to the
access key, or checked for expiry. This does not change the separately pinned
post-signature unexpected-token behavior of an active static credential.

Every mode's established wrong-region and wrong-service error wins over the
inactive status for all three token cases with both valid and bad HMACs. The
virtual-hosted active/inactive controls establish that this is scope-versus-
status precedence on the same header and presigned request path, not a
credential propagation difference between regional-root and bucket endpoints.
The outer mode-specific checks retain their earlier precedence too: a present
but unsigned token header on presigned and streaming requests returns
`HeadersNotSigned`, while any HTTP token header on POST Object selects the
non-POST route and returns `No AWSAccessKey was presented.` These are coverage
or route-selection failures, not successful selection of a session credential.
The resulting implementation order is mode-specific outer routing/coverage
and scope validation, inactive stored-credential rejection, HMAC comparison,
then the active-static unexpected-token rule where applicable; temporary
credentials continue through their separately pinned token/open/expiry/
liveness pipeline.

During this slice, one newly created role produced one successful STS
assumption followed immediately by `AccessDenied` for the same request. The
fixture now requires three consecutive successful assumptions, resetting the
count on any failure, before treating a positive target as converged. A single
successful response is not sufficient evidence that distributed STS caches
have converged.

The first cross-service routing-boundary slice completed on 2026-07-15. The
read-only default oracle now sends the same method, request target, body,
content type, and account-ID header to the regional STS and account-specific
regional S3 Control endpoint families, changing only the authority and normal
SigV4 service scope required by each endpoint. It uses a unique nonexistent
`claude-s3-*` resource ARN, so the probes cannot mutate bucket tags. Complete
response goldens establish this initial `SharedRegional` table:

| Request shape | `AwsRegionalSts` | `AwsRegionalS3Control` | Local `SharedRegional` selection |
| --- | --- | --- | --- |
| `POST /`, Query-form `GetCallerIdentity`, with `x-amz-account-id` | `200 GetCallerIdentity` success; the extra account header is ignored | `400 InvalidURI`, with `/` in the nested S3 Control error | STS |
| `POST /v20180820/tags/<arn>`, Query-form `GetCallerIdentity` in the body | `403 SignatureDoesNotMatch` in the STS error namespace | `403 SignatureDoesNotMatch` in the nested S3 Control error shape | S3 Control |
| `POST /v20180820/tags/<arn>?Action=GetCallerIdentity&Version=2011-06-15`, valid TagResource XML body | `403 SignatureDoesNotMatch` in the STS error namespace | valid-signature `404 NoSuchResource`; the STS query does not divert the request | S3 Control |

For the S3 Control form-body collision, AWS reports a canonical request whose
canonical-query line contains the form `Action` and `Version` even though the
wire URI has no query. The exact golden independently reconstructs and checks
that canonical request, its hash and byte encoding, the string to sign and its
byte encoding, and sanitizes the access key before any full-shape comparison.
This pins the surprising signature failure rather than treating it as an
ordinary bad-signature response. On the STS endpoint, signing either non-root
tags request target normally also produces `SignatureDoesNotMatch`; the exact
STS error shape is distinct from S3 Control's nested error envelope.

The local table deliberately gives the reserved versioned tags path precedence
over form content type and STS `Action`/`Version`, while an unambiguous root
Query request selects STS. This is only the first bounded collision subset.

The bounded HTTP-method slice then sent the same validly signed versioned tags
request to both endpoint families for `GET`, `HEAD`, `POST`, `PUT`, `DELETE`,
`OPTIONS`, `PATCH`, `PROPFIND`, and `X-ARGMIN-PROBE`. `POST` carried valid
TagResource XML, `DELETE` carried one `tagKeys` query member, and every request
used the unique nonexistent resource ARN and correct account-ID header. That
nonexistent-resource GET pins routing and error precedence only; it cannot by
itself establish which S3 Control operation was selected. Exact status,
complete normalized semantic headers, and complete bodies establish:

| Methods | `AwsRegionalSts` | `AwsRegionalS3Control` | Local `SharedRegional` selection |
| --- | --- | --- | --- |
| `GET`, `POST`, `DELETE` against the nonexistent resource | `404` `<UnknownOperationException/>` | valid-signature `404 NoSuchResource` | S3 Control |
| `HEAD` | `404`, the `UnknownOperationException` body suppressed with its representation length pinned as 29 | `405`, empty body, `Allow: DELETE, POST, GET` | S3 Control |
| `PUT`, `PATCH` | `404` `<UnknownOperationException/>` | `405 MethodNotAllowed`, with the exact method and `BUCKET_TAGS` resource type, plus `Allow: DELETE, POST, GET` | S3 Control |
| `OPTIONS` without an `Origin` header | `404` `<UnknownOperationException/>` | `400 BadRequest`: `Insufficient information. Origin request header needed.` | S3 Control |
| `OPTIONS` with origin `https://example.com` and requested method `POST` | `200` empty CORS response allowing origin `*` and method `POST`, with the exact exposed-header list and 172,800-second maximum age | `403 AccessForbidden`: `CORSResponse: Bucket not found`, with method `POST` and resource type `BUCKET` | S3 Control |
| `PROPFIND`, `X-ARGMIN-PROBE` | outer HTTP `400`, empty body, ordinary S3-shaped request ID rather than STS request IDs | outer HTTP `400` standard S3 `BadRequest`: `An error occurred when parsing the HTTP request.` | S3 Control outer-error shape for this reserved path |

The mutating `--assume-role` fixture separately creates and tags a unique
bucket, then requires three consecutive successful
`ListTagsForResource` calls before the raw golden runs. An explicit bucket-
policy deny for `s3:GetBucketTagging` against the primary test user is required
to converge to three consecutive `GetBucketTagging` `AccessDenied` responses;
`ListTagsForResource` must then continue to succeed three consecutive times.
This proves that AWS authorizes the S3 Control GET with the distinct
`s3:ListTagsForResource` action rather than aliasing it to ordinary S3
`GetBucketTagging`.

The exact existing-resource wire probe sends the identical GET to both endpoint
families. Regional STS returns its `404 <UnknownOperationException/>`, while S3
Control returns HTTP 200 with only the two normal AWS request-ID headers and no
`Content-Type`. Its complete body is an XML declaration followed by
`ListTagsForResourceResult` in the
`http://awss3control.amazonaws.com/doc/2018-08-20/` namespace, containing
`Tags/Tag/Key` and `Value` in that order. This pins a real typed
`ListTagsForResource` success and its distinct response renderer; the Phase 4
refactor must not implement it as an alias for the normal S3 tagging operation.
The AWS oracle also removes the fixture's only tag, captures the empty success,
restores and verifies the fixture tag before asserting the captured response,
and pins the empty collection as the self-closing `<Tags/>` member. The local
serializer and network regression use that exact representation.

The endpoint-transport review completed on 2026-07-15. AWS's endpoint tables
list ordinary S3 regional endpoints as HTTP and HTTPS, but list S3 Control and
STS as HTTPS-only. A safe unsigned live check found that the regional STS HTTP
request timed out from the probe host, while an account-prefixed S3 Control
HTTP request reached an AWS frontend and returned its normal unauthenticated
`AccessDenied` envelope. No live credential was sent over HTTP, so the latter
does not establish support for authenticated S3 Control operations and does
not override the documented endpoint contract. The temporary AWS oracle now
parses and requires HTTPS URLs for both endpoint families before loading
credentials into its request signer; the shell wrapper independently rejects
plaintext overrides before exporting credentials.

The extension-method probes also observed that this outer AWS frontend can
emit an unpadded uppercase hexadecimal `x-amz-request-id` shorter than the
ordinary fixed-width S3 shape (15 and 16 characters were observed). Those two
outer response assertions therefore use a dedicated nonempty, at-most-16-
character uppercase-hex validator without weakening the standard response-ID
invariant used by every normal S3, S3 Control, and STS response.

The extension methods use an outer endpoint HTTP-parser response rather than
either normal STS XML or the nested S3 Control error envelope. The different
AWS endpoint families render that outer response differently. The local choice
of the S3 Control outer-error shape follows the reserved-path precedence rule;
it is not evidence that AWS's S3 Control operation router saw the extension
method, and this valid-signature slice does not yet order the outer parser
against authentication. The raw test client now accepts arbitrary valid HTTP
method tokens so these named extension methods are sent on the wire rather than
approximated with a recognized method.

The path, percent-decoding, and resource-ARN near-miss slice completed on
2026-07-15. It sends the same read-only, correctly signed GET and account-ID
header to both endpoint families for 19 request targets. Complete normalized
headers and bodies establish:

| Request-target group | `AwsRegionalSts` | `AwsRegionalS3Control` | Local `SharedRegional` selection |
| --- | --- | --- | --- |
| Encoded path separator `tags%2F<encoded-arn>` and a valid ARN sent with literal colons | `404 <UnknownOperationException/>` | `404 NoSuchResource` | S3 Control |
| Bare, truncated, or non-hex percent triplet (`%`, `%2`, `%GG`) | outer HTTP `400`, empty body, no STS request IDs | outer HTTP `400`, empty body, no S3 request IDs | S3 Control outer-error shape |
| Missing or empty resource, extra path segment, singular `tag`, `tagsx`, wrong or case-mismatched version, doubled leading slash, percent-decoded invalid UTF-8, malformed/empty/wrong-service/object ARN, and double-encoded ARN | `404 <UnknownOperationException/>` | `400 InvalidURI` with the exact target-dependent `<URI>` value | S3 Control |

The valid ARN spellings require a deliberate distinction between the wire
target and SigV4 canonical target. AWS treats `%2F` between `tags` and the ARN
as a path separator, and percent-encodes literal ARN colons when it constructs
the canonical path. Signing the normalized canonical path while preserving the
original wire spelling succeeds and reaches `NoSuchResource`; signing the raw
spelling instead produces only a signer-induced `SignatureDoesNotMatch` and is
not routing evidence.

The `InvalidURI` detail also pins the decode boundary. For the exact
`/v20180820/` prefix, AWS removes that version prefix and percent-decodes one
layer in `<URI>`; a double-encoded ARN therefore remains singly encoded.
Wrong-version, case-mismatched-version, doubled-leading-slash, and invalid
UTF-8 targets retain their full encoded path instead. Malformed percent
triplets do not reach either service's XML renderer at all.

For the named bounded rows, `SharedRegional` deliberately selects the S3
Control observation instead of falling through to STS or ordinary S3. These
are valid-signature probes. They pin routing and response shapes but do not yet
order path parsing or percent validation against authentication; those
collisions remain part of the signing-service/signature slice.

The account-ID-header slice completed on 2026-07-15. It sends correct, missing,
empty, wrong 12-digit, malformed short, malformed alphabetic,
duplicate-identical, and duplicate-conflicting `x-amz-account-id` headers in
both conflicting wire orders. The raw signer now canonicalizes repeated signed
headers correctly:
the signed-header name appears once and its values are comma-joined in wire
order. The duplicate results are therefore valid-signature observations, not
artifacts of the earlier test helper.

The default read-only matrix uses a nonexistent bucket ARN. Every header form
returns the same STS `404 <UnknownOperationException/>` and S3 Control `404
NoSuchResource` shapes as the correct-header control. Because resource absence
could mask later validation, the mutating fixture repeats every header form
against its existing tagged bucket for all three initial typed operations:

| Operation | `AwsRegionalSts` | `AwsRegionalS3Control` | Local `SharedRegional` selection |
| --- | --- | --- | --- |
| `GET` / `ListTagsForResource` | `404 <UnknownOperationException/>` | `200` exact tag-list success | S3 Control; ignore this header |
| idempotent `POST` / `TagResource` of the existing tag | `404 <UnknownOperationException/>` | `204` with only the normal request-ID headers | S3 Control; ignore this header |
| idempotent `DELETE` / `UntagResource` of an absent probe key | `404 <UnknownOperationException/>` | `204` with only the normal request-ID headers | S3 Control; ignore this header |

The account-specific AWS S3 Control authority accepts every tested header form,
including absence and conflicting duplicates. The initial shared listener must
therefore not introduce a header validation error that these AWS operations do
not have. Its configured `SharedRegional` endpoint kind and reserved path
select S3 Control; authenticated identity and the resource determine account
authority, while `x-amz-account-id` is ignored for these three operations. An
arbitrary header value must never select another account or endpoint kind.

The signing-service and bad-HMAC routing slice completed on 2026-07-15. On the
valid reserved tags path it crosses the endpoint's correct, empty, and other
service scope with both the real secret and a deliberately wrong secret. Exact
goldens establish:

| Request | `AwsRegionalSts` | `AwsRegionalS3Control` | Local `SharedRegional` selection |
| --- | --- | --- | --- |
| valid path, correct service, valid HMAC | `404 <UnknownOperationException/>` | `404 NoSuchResource` | S3 Control |
| valid path, correct service, bad HMAC | `404 <UnknownOperationException/>` | `403 SignatureDoesNotMatch` with complete signing diagnostics | S3 Control |
| valid path, empty service, either HMAC | `404 <UnknownOperationException/>` | `400 AuthorizationHeaderMalformed`: incorrect service `""`, endpoint `"s3"` | S3 Control |
| valid path, other service, either HMAC | `404 <UnknownOperationException/>` | `400 AuthorizationHeaderMalformed`: incorrect service `"sts"`, endpoint `"s3"` | S3 Control |

Regional STS therefore classifies a non-root reserved path as unknown before
checking the SigV4 service component or HMAC. S3 Control classifies the valid
reserved path first, validates the service component before the HMAC, then
validates the HMAC before resource lookup. Empty and wrong service results are
identical for valid and bad HMACs, so they are not signature-verification
artifacts.

The same slice repeats the earlier mixed routing requests with a bad HMAC:

| Bad-HMAC collision | `AwsRegionalSts` | `AwsRegionalS3Control` | Local `SharedRegional` selection |
| --- | --- | --- | --- |
| root Query-form `GetCallerIdentity` | `403 SignatureDoesNotMatch` in the STS namespace | `400 InvalidURI` for `/` | STS |
| Query-form `GetCallerIdentity` body on the reserved tags path | `403 SignatureDoesNotMatch` in the STS namespace | `403 SignatureDoesNotMatch` in the S3 Control envelope | S3 Control |
| STS `Action`/`Version` query on the reserved tags path with TagResource XML | `403 SignatureDoesNotMatch` in the STS namespace | `403 SignatureDoesNotMatch` in the S3 Control envelope | S3 Control |
| malformed percent triplet on the reserved path | outer HTTP `400`, empty body | outer HTTP `400`, empty body | S3 Control outer-error shape |
| malformed ARN on the reserved path | `404 <UnknownOperationException/>` | `400 InvalidURI` | S3 Control |

Thus root Query classification reaches STS authentication before the bad HMAC,
whereas the S3 Control endpoint's root URI rejection wins first. On the
reserved path, malformed percent decoding and ARN validation win before HMAC
verification; a syntactically valid path reaches HMAC verification before
resource lookup. Adding STS parameters never diverts that reserved path.

The S3 Control signature-mismatch assertion now reconstructs the complete
canonical request generically for method, path, canonical query, payload, and
signed headers. It checks the canonical-request and string-to-sign hashes and
byte encodings, then sanitizes the echoed access key before the complete
response-shape comparison.

The operation-body routing slice completed on 2026-07-15 against the existing
tagged-bucket fixture, so resource absence cannot mask XML, query-member, or
authentication ordering. It establishes:

- on the S3 Control endpoint, an empty `TagResource` body is
  `MissingRequestBodyError`, truncated XML is `MalformedXML`, and a wrong root,
  missing `Tags`, or empty `Tags` is `InvalidTag` with `At least one tag is
  required.` Despite the published shape permitting a zero-member tag array,
  the live bucket operation requires at least one tag
- correct-service bad HMAC wins before every tested `TagResource` XML error;
  wrong service wins before the representative truncated-XML error, regardless
  of whether the HMAC is valid
- complete absence of `UntagResource`'s required `tagKeys` member is
  `InvalidTag` before service-scope and HMAC validation. Once the member is
  present, empty, invalid-character, and 129-character keys reach HMAC first;
  a valid HMAC then produces the exact common `InvalidTag` validation error
- a single absent tag key and two distinct repeated absent keys succeed with
  `204`. Two identical repeated absent keys produce the generic
  `500 InternalError`; three consecutive requests reproduced that result. A bad
  HMAC still wins before this backend outcome
- the regional STS endpoint returns its non-root `UnknownOperation` shape for
  every body/query-member case with valid or bad HMAC under the correct `sts`
  signing scope. The alternate `s3` scope is pinned for the representative
  truncated-XML and missing-`tagKeys` collisions, with both valid and bad HMAC.
  The local `SharedRegional` mapping remains S3 Control for the reserved
  versioned path

Every valid-result assertion is an exact response golden. Every
signature-mismatch assertion reconstructs and validates the complete canonical
request without printing live credentials. Together with the completed local
Host/authority/SNI trust-boundary matrix, the routing-boundary evidence required
before the Phase 4 service refactor is complete.

The matching local body/query regression now implements and locks those
validation boundaries. A completely absent `tagKeys` member is rejected before
service-scope or HMAC validation; present values and TagResource XML are
validated only after authentication. Empty bodies, malformed XML, structurally
empty tag requests, invalid characters, and length limits use the exact typed
S3 Control error codes and messages above. Permanent AWS-facing TagResource
member probes lock empty and overlong keys, overlong values, and invalid
characters to the common `InvalidTag` message; duplicate member keys use AWS's
distinct `There are duplicate tag keys in your request` message. The shared
incremental parser carries an explicit S3 Control schema so these errors cannot
leak ordinary S3 tagging diagnostics. A separate key/value character matrix
proves that `!` and an `Alphabetic` nonspacing combining mark are rejected in
either member, while the Unicode `L`, `Z`, and `N` general-category boundaries
(`Zs`/`Zl`/`Zp` and `Nd`/`Nl`/`No`) and every `+-=._:/@` punctuation character
are accepted in both keys and values. Production uses
`unicode-general-category` to implement that exact AWS-confirmed category rule
rather than Rust's broader derived `Alphabetic` property. A further AWS-facing
regression locks the tag-state branch that the tagged-fixture oracle did not
exercise:
invalid-character and 129-character removal keys are successful `204` no-ops
when the resource has no tags, but become the common `InvalidTag` response once
any resource tag exists. Local validation therefore performs the authorized
tag lookup before applying character and length checks. Malformed-short and
malformed-alpha account-ID values are included in the existing-resource
GET/POST/DELETE matrix, confirming that they remain non-authoritative.

There are two deliberate bounded incompatibilities where AWS repeatedly returns
`500 InternalError` for deterministic client input. Identical duplicate
`tagKeys` become nested local `400 InvalidTag` with `Duplicate tag keys are not
supported.`; 51 distinct `tagKeys` become the exact common nested local `400
InvalidTag` validation response. Exact local goldens lock both conversions.
The duplicate regression distinguishes identical values from two distinct
repeated keys, which retain AWS's `204` behavior, and both local conversions
confirm that a bad HMAC still wins before value/count validation.

The typed Phase 4 endpoint refactor has removed the former transport gap.
Embedded tests default to HTTPS and enable the shared S3/S3 Control endpoint;
explicit plain-HTTP tests get S3-only routing. Exact local success tests pin the
absence of a content type on `ListTagsForResource`, the ID-header-only `204`
responses for `TagResource` and `UntagResource`, and every missing, empty,
correct, wrong, and duplicate `x-amz-account-id` form across all three
operations. Separate policy tests prove the list operation uses only its
distinct action.

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

Completed on 2026-07-20. Structured account, configured-principal, and
assumed-role-session identities exist with surface-specific ARN accessors;
stored credentials accept only configured principals; and one cloneable
identity-provider handle now supplies owned long-lived credential/account
lookups to every frontend worker. The provider also supplies a separate minimal
stable role/account liveness lookup whose opaque result is required by the
crate-internal decoded-session construction seam. Decoded session credentials
are a distinct mandatory-lifetime credential kind that cannot enter the stored
long-lived collection, and neither a custom backend nor bootstrap
configuration can expose an `ARGS` key as long-lived. Unknown identities
remain distinct from provider failure, and invalid provider records remain
diagnostically distinct from provider outages across authentication and
rendering paths. The shared sealing key ring and versioned token codec complete
the Phase 1 slice. The provider now owns that ring, and every frontend
clone shares its credential domain and keys. Version 1 uses `ARGST1.`,
canonical unpadded URL-safe base64, AES-256-GCM, fixed authenticated associated
data, a random per-key nonce prefix plus atomic counter, strict defensive decode
bounds, strict typed payload reconstruction, and a derived 742-byte issuance
ceiling. The codec rejects unknown keys/versions, non-canonical encoding,
truncation, tampering, invalid fields, impossible lifetimes, and trailing data
without exposing bearer material. Stable role incarnation and authoritative
account resolution are deliberately outside the cryptographic codec.

Temporary access/secret generation uses the Argmin-owned `ARGS` namespace,
unbiased rejection sampling for the 20-character suffix, and 30 random bytes
encoded as a 40-character URL-safe secret. The production provider sealing
boundary consumes that opaque generated-material type; arbitrary raw access and
secret values can reach sealing only from test-only constructors. Focused tests
cover entropy failure, shape and uniqueness sampling, maximum-field issuance,
tampering and domain/key mismatch, every plaintext truncation, nonce
exhaustion, concurrent issuance and rotation, rejected validation-only and
removed-ID reactivation, duplicate live and removed AES material under another
ID, irreversible validation-key removal, key-ring poisoning, redaction, issuer
deletion, cross-provider rejection, and actual opening through two frontend
workers. The maximum token is also checked in complete ordinary and aws-chunked
write-header shapes against the server's 8,192-byte aggregate limit.
Presigned-query authentication now accepts the same maximum issued token
within its complete bounded query. POST authentication of that maximum token
is covered by its Phase 2 request-path test.

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
- add injected provider-failure regressions for long-lived credential lookup
  and stable issuer-role liveness, distinguishing each failure from an unknown
  credential or deleted issuer and requiring the internal fail-closed response
- exercise each SigV4 mode at the authentication boundary without claiming that
  a role session yet has usable S3 permissions

Progress as of 2026-07-20: the shared provider authentication decision accepts
only an access key, one mode-selected optional token, and an injected current
time. Missing/empty tokens, malformed tokens, access-key mismatch, exact
expiry, deleted stable issuers, key-ring failure, and identity-provider failure
remain typed and contain no bearer input. The implemented order is token open,
constant-time access-key binding, expiry, then authoritative stable-issuer
liveness. Deterministic tests pin the exact expiration instant, binding before
expiry and liveness-provider failure, expiry before deletion and provider
failure, token opening before binding, authentication-path key-ring failure,
cross-provider key/domain rejection, and redacted diagnostics. A live-role
record whose immutable account or role name disagrees with its authenticated
sealed payload is a typed invalid provider record, not a client credential
error.

The ordinary non-streaming S3 Authorization-header adapter is also complete.
After its already-pinned region and service scope checks, it requires a signed
session-token header, collapses identical duplicate values, rejects conflicting
duplicates as `InvalidAccessKeyId`, and maps missing/empty, malformed,
mismatched, expired, deleted-issuer, provider-failure, and key-ring-failure
decisions at the S3 boundary before HMAC comparison. A correct credential
returns a typed assumed-role session; a bad HMAC then reaches
`SignatureDoesNotMatch`. The header wrong-service path now has its pinned
`AuthorizationHeaderMalformed` message and response shape rather than the
previous generic malformed-header response. Expired sessions retain every
presented identical token header and render the exact HTTP 400 `ExpiredToken`
body with ordered `Token-N` elements. Bucket-scoped `InvalidAccessKeyId` and
wrong-service responses on an existing bucket receive
`x-amz-bucket-region`, as do the previously pinned presigned wrong-region and
wrong-service responses; object-scoped requests, missing buckets, and POST
Object retain their pinned exclusions. Active and inactive static credential
ordering is unchanged.

The presigned-query adapter is now complete as a separate mode-specific
selector. It retains every `X-Amz-Security-Token` query member in wire order,
collapses identical values, and rejects conflicting values in either order.
When `x-amz-security-token` is declared in `X-Amz-SignedHeaders`, all values
from that signed header location are selected instead and the query location
cannot rescue a missing, empty, malformed, mismatched, expired, or conflicting
header selection. A present but unsigned token header still produces
`HeadersNotSigned` before the query token is resolved. Scope validation remains
before both token locations, issuer liveness, and HMAC comparison. Selected
token opening, binding, expiry, deleted-issuer, and provider-failure decisions
remain before HMAC comparison, while a live correctly bound token with a bad
HMAC reaches `SignatureDoesNotMatch`. Expired identical duplicates retain
their complete ordered presentation for the shared exact `ExpiredToken`
response. Focused tests cover correct and bad signatures, both conflicting
orders, signed-header authority, structural-token collisions with expiry,
scope collisions at both token locations, deletion and provider failure at
both locations, typed assumed-role identity, and successful authentication of
the derived 742-byte maximum issued token within the bounded presigned query.
The static-credential path retains its prior lookup, expiry, signature, and
post-signature unexpected-token ordering.

The POST Object adapter is now complete as a separate mode-specific selector.
It collects every `x-amz-security-token` multipart form field in wire order,
uses the common session selection/authentication pipeline for `ARGS`
credentials, and returns the typed assumed-role identity with the standard
authorization profile. Scope remains ahead of form-token selection. A missing
or empty token and an independently valid but mismatched token return
`InvalidAccessKeyId`; malformed non-empty input returns `InvalidToken`;
conflicting duplicates in either order return `InvalidAccessKeyId`; and
identical duplicates authenticate as one effective token while retaining their
ordered multiplicity for expiry rendering and POST-policy evaluation. Token
opening, access-key binding, expiry, stable issuer liveness, provider failure,
and key-ring failure remain ahead of policy-signature comparison. Static
credentials preserve their prior lookup, expiry, HMAC, and post-signature
unexpected-token behavior.

The adapter also preserves POST's independent HTTP-header routing rule. Any
present `x-amz-security-token` request header, including empty, malformed, or
duplicated values, returns the exact HTTP 403 `AccessDenied` response with `No
AWSAccessKey was presented.` before form scope, token, or policy-signature
processing. Identical form-token duplicates with a valid HMAC continue into
policy evaluation and produce AWS's exact condition-expression denial,
whether the policy writes the exact token condition in the AWS-oracle object
form or the equivalent array-form `eq` representation. The response includes
the token only on the protocol surface; diagnostics and `Debug` remain
redacted. A bad HMAC still wins before that policy-condition failure.
Focused authentication and server-adapter tests cover the structural and
binding matrix, both conflicting orders, identical duplicates with correct and
bad HMACs, expired duplicate preservation, deletion and liveness-provider
failure, scope precedence, typed assumed-role identity, exact response shapes,
and successful authentication of the derived 742-byte maximum issued token
through the server's parsed POST adapter path.

The aws-chunked streaming adapter is now implemented as a separate
mode-specific consumer of the shared Authorization-header selector. Region and
service scope remain ahead of token coverage and structure. A present token
must be covered by `SignedHeaders`; identical signed duplicates collapse for
credential selection while retaining their presentation for expiry rendering,
and conflicting duplicates in either order fail before the seed signature.
Missing, empty, malformed, mismatched, expired, deleted-issuer,
liveness-provider-failure, and key-ring decisions use the shared mappings and
all occur before seed-signature comparison. Successful authentication returns
the typed assumed-role identity plus a streaming signing context for
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` and
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`; the adjacent
`STREAMING-UNSIGNED-PAYLOAD-TRAILER` mode returns the same identity without
chunk-signing state. First-chunk or signed-trailer verification occurs only
after that boundary. Adapter tests prove a valid maximum-size issued credential
reaches bad first-chunk and bad trailer `SignatureDoesNotMatch`, while token
failures stop before body decoding. The complete signed aws-chunked
request-header shape containing the 742-byte maximum issued token remains below
the 8,192-byte transport ceiling. Because the seed canonical request
necessarily contains the bearer token for AWS's client-visible chunk-signature
error body, its diagnostic `Debug` form is explicitly redacted.

Presigned streaming-marker handling is also complete as a distinct body
adapter. Authentication still validates the presigned seed request and returns
no chunk-signing context, matching AWS: the handler counts and hashes the raw
body instead of decoding aws-chunked framing. Non-trailer length mismatches,
exact-length PutObject hash mismatches, and UploadPart trailer errors use the
exact AWS XML shapes. PutObject's client-triggerable trailer overrun is the one
intentional exception: AWS returns `500 InternalError`, while Argmin follows
the repository's fail-on-500 safety rule and returns the same deterministic
`400 MalformedTrailerError` used by AWS UploadPart. This incompatibility is
recorded in the compatibility guide. PutObject never starts a write session on
this path, and UploadPart aborts its prepared session before returning the
error. End-to-end tests cover both operations and prove no object or part is
committed.

Signed aws-chunked decoder construction now requires a streaming signing
context and treats its absence as an internal authentication/adapter invariant
failure, preserving the repository's fail-on-500 diagnostics; it cannot
silently downgrade to unsigned chunk parsing. Tests exercise the same
production incremental decoder used by the network handlers through a thin
feed/finish adapter. The former duplicate test-only batch decoder has been
removed.

The AWS oracle separately confirms temporary credentials for signed payload
trailers and unsigned payload trailers: success, missing and malformed tokens,
and bad seed signatures for both, plus the exact signed-trailer
signature-failure golden and token-before-bad-trailer ordering.

Exit condition: directly sealed session credentials authenticate with
AWS-pinned token/signature/expiry precedence on every SigV4 mode and produce a
typed authenticated session; injected credential and issuer-liveness provider
failures remain distinct from invalid credentials and deleted issuers.
Phase 2 did not by itself make a role session usable through an S3
authorization path; the first such path is supplied by the Phase 3 PutObject
composition slice below.

### Phase 3: IAM policy and role core

Phase 3 began by extracting the policy version, effect, normalized condition
clause, statement-core, evaluation-result, and wildcard/action matching
primitives from the S3 bucket-policy implementation into a shared internal
policy-language module. `BucketPolicy` remains the typed public S3 resource-
policy wrapper and retains its existing parser, request adapter, validation,
and composition behavior. Subsequent Phase 3 slices will build typed trust,
identity, and session-policy wrappers over these shared primitives; they must
not introduce a second permissive IAM parser or a universal request context.

The next Phase 3 foundation adds typed, programmatically constructed identity,
session, and initial AWS-principal role-trust policy documents; named inline
policy attachments; validated role timestamps and 3,600--43,200 second maximum
session duration; and distinct role/configured-principal authorization
records. Identity/session evaluation takes a service-specific request enum
whose S3 and `sts:AssumeRole` variants supply different typed data. This slice
does not yet accept JSON policy documents or conditions: the shared parser and
condition-context work remains mandatory before configuration can attach those
features, so no unsupported policy member is silently ignored.

Mutable authorization records are exposed through provider capabilities that
are separate from stable issuer-liveness lookup. The role authorization
boundary validates the complete immutable role identity and its full S3
`AccountIdentity` (including canonical-user and display identity), while
configured-principal lookup validates both its account/principal key and the
same full account record. Local tests require a
session to authenticate without reading an injected failing authorization
provider, after which the explicit authorization lookup returns the typed
provider failure. Role and principal tags remain deferred until the shared
validated tag representation and their condition-key inputs are added.

The first explicit decision-composition slice now treats every named inline
identity-policy attachment on a role or configured principal as one union with
document-independent explicit-deny precedence. Role-session evaluation takes a
typed absent-or-present session-policy restriction: absence leaves the role
decision unchanged, while a present session policy intersects with current
role permissions and cannot add an allow. A complete three-by-three decision
matrix pins deny precedence and both implicit-deny cases. Trust-policy/caller
composition and S3 resource-policy composition remain separate later Phase 3
slices so neither is hidden inside this identity/session boundary.

The configured-caller `AssumeRole` composition slice now evaluates role trust
and caller identity policy as distinct sources. An exact same-account IAM user
principal in role trust is a direct grant and does not require a redundant
identity-policy allow. An AWS oracle role with `Principal: {"AWS": "*"}` and an
IAM simulation result of `implicitDeny` proves that wildcard trust is likewise
a direct grant for a same-account IAM user. Account-ID, account-root, and every
cross-account trust match—including wildcard—are delegation and require the
caller-side `sts:AssumeRole` allow. Dedicated cross-account wildcard roles pin
both success with caller allow and denial with caller `implicitDeny`. An
explicit deny on either side wins, and trust omission cannot be repaired by
caller permission. The boundary accepts only an account-bound IAM user ARN for
this first long-lived-caller path; legacy opaque configured principals, root,
role ARNs, malformed ARNs, and account-mismatched ARNs fail closed.
Assumed-role callers remain a separate typed role-chaining path rather than
being disguised as configured long-lived credentials. A complete
trust/identity composition matrix plus same-account, account-delegation,
wildcard, cross-account, omission, and invalid-caller tests pin the decision.

The assumed-role S3 resource-policy context slice now constructs its requester
facts directly from one `AuthenticatedIdentity`; callers cannot independently
mix the role ARN, session ARN, assumed-role ID, canonical user, or issuance
time. Bucket-policy `Principal` matching accepts both the path-bearing IAM role
ARN and the exact path-free assumed-role session ARN, as established by the AWS
oracle. The condition adapter separately exposes the role ARN as
`aws:PrincipalArn`, `<stable-role-id>:<session-name>` as `aws:userid`, and the
sealed session issue instant as `aws:TokenIssueTime`. `ArnEquals` and the
shared exact matcher reproduce the oracle's principal-ARN condition, while its
distinct operator kind prevents unpinned ARN comparisons from leaking onto
tags, list parameters, or other string/numeric keys. Token issue time uses the
existing date evaluator. Both object- and bucket-resource request adapters
carry this typed context, and the public request API cannot independently
supply principal and canonical-user fields. Focused policy and server-core
tests reproduce the AWS role/session principal and condition matrix on both
resource scopes. A configured principal is exposed as `aws:PrincipalArn` only
when it is a syntactically valid IAM user ARN bound to the authenticated
account; opaque or account-mismatched configured principals remain lossless but
make a dependent deny fail closed rather than bypassing it. Identity/session
decisions and the resulting resource-policy decision were still separate at
the end of that context-only slice.

The first S3 authorization-boundary slice now carries the token-versioned
session authorization context as part of the assumed-role identity so it
cannot be discarded between authentication and authorization. Only after
signature verification has produced an authenticated identity does the HTTP
adapter resolve the current mutable authorization record for that exact role
incarnation. A provider failure is retained as a typed result and is consumed
at the policy boundary, where it fails closed without being relabeled as an
authentication failure. Missing or identity-mismatched role authorization
state is an invalid provider record rather than an implicit-deny substitute.

The first operation slice is intentionally limited to an ordinary, untagged
PutObject against a same-account BucketOwnerEnforced bucket. It composes the
current role identity policy, the version-1 absent session-policy restriction,
and the S3 bucket resource-policy decision. The Phase 0 same-account AWS matrix
pins their union semantics and explicit-deny precedence: an allow on either
side is sufficient and a deny on either side wins. RestrictPublicBuckets
continues to filter resource-policy allows before composition. Cross-account
role sessions fail closed even when both policy sides allow; exact role/session
principal behavior remains deliberately unresolved until its AWS matrix is
added. Within this new BucketOwnerEnforced identity-policy composition path,
role-based PutObjectTagging, CreateMultipartUpload, UploadPart, UploadPartCopy,
CompleteMultipartUpload, CopyObject, explicit PutObject ACLs, PutObject
retention, PutObject legal holds, and `If-Match` conditional overwrites also
remain closed until their operation-specific AWS probes establish the
additional `s3:PutObjectAcl`, `s3:PutObjectRetention`,
`s3:PutObjectLegalHold`, and conditional `s3:GetObject` composition. Configured-
principal authorization behavior is unchanged by this slice. Focused decision
tests cover the complete
same-account allow/deny composition and every deliberately closed adjacent
branch, while end-to-end HTTP tests prove both successful temporary-credential
PutObject and authentication-before-current-role-provider-failure ordering.
ACL-enabled object authorization and all other S3 action families remain
subsequent Phase 3 integration slices rather than inheriting untested generic
composition.

The same-account `GetObject` AWS matrix completed on 2026-07-21 as the evidence
prerequisite for the next operation slice. Two unique path-bearing roles use
the owner-managed permissions boundary, now limited to `s3:GetObject` and
`s3:PutObject` on objects in prefixed test buckets. One role has identity
allows on two exact object ARNs and the other has an explicit identity deny on
one exact object ARN. A third, permissionless recreated role supplies the
resource-policy-only case. Every role and inline policy is round-tripped
through IAM, and the fixture requires three consecutive target-endpoint
results for each decision before the raw oracle runs.

On a same-account bucket, either an identity-policy allow with no matching
resource statement or an exact role-principal resource-policy allow with no
identity allow is sufficient for `GetObject`. An explicit identity deny wins
over a matching resource allow, and an explicit resource deny wins over a
matching identity allow. When neither source allows, AWS reports that no
identity-based policy permits `s3:GetObject`. Complete raw goldens pin the
successful empty-object response, including its semantic zero content length,
and the principal-, action-, resource-, and policy-source-specific denial
messages. Local `GetObject` role-policy composition remains deliberately
closed at this evidence checkpoint rather than inheriting generic policy
composition before the implementation and focused coverage below. Repeated AWS
probes of simultaneous identity- and resource-policy explicit denies have
produced both the identity-policy and resource-policy source suffixes, while
the corresponding single-source controls remain stable. One 64-request sample
against the same object and unchanged policies returned 26 identity-policy and
38 resource-policy suffixes.

The corresponding local slice now enables only an ordinary full-payload
`GetObject` for an existing current-version object on a same-account
BucketOwnerEnforced bucket. It uses the same current-role/session and resource-
policy composition boundary as the AWS matrix: either allow is sufficient,
either explicit deny wins, and no allow denies. Authentication still completes
before mutable role-policy lookup, and provider corruption or outage retains
its typed internal classification.

The read request carries a typed role-read surface in addition to its policy
action so a successful `s3:GetObject` decision cannot silently authorize HEAD,
ranged GET, `partNumber`, versioned reads, or GetObjectAttributes. Optional
retention, legal-hold, and tag-count response fields also remain hidden from
role sessions until their additional action composition is pinned. Cross-
account role reads, ACL-enabled buckets, and missing-object discovery remain
closed. The role-read surface fence runs before BucketOwnerEnforced versus ACL-
enabled dispatch, so a matching ACL-bucket resource policy cannot bypass those
limits.

An allowed decision remains separate from four typed AWS-pinned denial kinds:
no identity-policy allow, explicit identity-policy deny, explicit resource-
policy deny, and simultaneous explicit denies in both sources. The denial
retains the path-free assumed-role session ARN, typed action, and exact object
ARN through HTTP rendering. Full routed HTTP tests pin the complete XML body,
including principal, action, resource, policy-source suffix, request ID, and
host ID. AWS has returned both source suffixes for the simultaneous-deny state;
the local renderer uses the observed resource-policy spelling without erasing
the dual-source state from the typed error. Focused decision tests also cover
the complete same-account matrix,
cross-account and ACL-enabled closure, adjacent actions, identity binding, and
provider failure; end-to-end tests prove current-object success,
authentication-before-provider-failure ordering, and HEAD/range/missing-key
closure.

Testing this slice exposed that AWS accepts some correctly signed STS requests
carrying `x-amz-content-sha256` and rejects others with
`SignatureDoesNotMatch`. This is recorded as observable AWS behavior rather
than a transient harness failure. The underlying oracle defect was that the
hand-built signer had applied the S3 signing profile to STS: STS hashes the
payload in the canonical request but its standard signing profile does not emit
that header. Removing the 109-byte signed header also moves the observed STS
GET request-head boundary from a 15,844-byte query succeeding and 15,845 bytes
failing to 15,953 bytes succeeding and 15,954 bytes failing. The target API is
now a typed `S3`, `S3Control`, or `Sts` value, separate from the raw credential-
scope service used by malformed-scope probes, and both profile mappings are
exhaustive. `S3Control` remains distinct from `S3` because it targets a distinct
endpoint even though both currently use the S3 credential-scope name and
payload-header rule. The signer also preserves serialized malformed-percent
paths rather than repairing them before signing. Ordinary hand-signed oracle
requests are independently compared with the AWS SDK SigV4 signer at the same
timestamp before transmission. POST, malformed paths, wrong credential scopes,
and all three target-service profiles have focused differential coverage; a
dedicated stress mode sends both one fixed signature and freshly generated
signatures 256 times each.

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
- inject current-role authorization-provider failure independently from stable
  issuer liveness and require authorization to fail closed after successful
  authentication
- seed deterministic roles in embedded tests and UAT-only setup

Exit condition: a constructed role session can authenticate and use S3 with
only the AWS-equivalent permissions of its current role, sealed session, and
resource-policy combination. The configured caller's identity-policy path is
also capable of representing `sts:AssumeRole`; no broad profile shortcut is
involved. An injected current-role authorization-provider failure fails closed
without being misreported as an authentication failure.

### Phase 4: STS Query endpoint and core `AssumeRole`

The typed endpoint-routing foundation is complete as of 2026-07-22. Trusted
listener configuration selects `S3Only` or TLS-only `SharedRegional` without
consulting Host, authority, or SNI; the service operation and expected signing
service mappings are exhaustive. S3 Control tag operations are typed outside
`S3Operation`, ignore the non-authoritative account-ID header, and have distinct
authorization and wire coverage. The bounded S3 Control route/error follow-up
is also complete: service context survives route, authentication, and dispatch
errors; ARN validation precedes authentication; the full method/path matrix is
locally locked; empty tag-list XML is AWS-oracle-backed; and the body/query
validation and authentication-precedence matrix is locally locked with the
explicit duplicate-`tagKeys` non-500 incompatibility documented above. The next
slice is the bounded STS Query classifier/parser and typed STS operation
payload.

- generalize the existing S3 Control endpoint-family routing into typed service
  and endpoint-kind routing only after the dual-endpoint AWS method/path/
  account-ID/signing/body/query collision goldens and the committed
  `SharedRegional` mapping above pin its pre-authentication and post-
  authentication boundaries, and after local Host/authority/SNI tests prove
  attacker-controlled authority cannot change endpoint kind; require the
  configured listener to use TLS before enabling S3 Control or STS, while
  preserving plain-HTTP support for an S3-only listener; then add bounded Query
  protocol parsing
- introduce typed S3 Control `ListTagsForResource`, `TagResource`, and
  `UntagResource` operations during that refactor, preserving their distinct
  authorization actions and wire renderers
- authenticate STS requests with service name `sts`
- authorize `AssumeRole` using the AWS-equivalent combination of caller identity
  permissions and the role trust policy
- validate role/session/duration inputs
- generate the session credential and seal its version-1 authentication/session
  context without mutating identity state; reject every Phase 6 policy/tag/
  context parameter explicitly
- render AWS-shaped success/errors
- add `aws-sdk-sts` after the normal test-dependency review

Exit condition: the AWS SDK can call `AssumeRole` on the same local endpoint,
then use the returned credentials through the AWS S3 SDK with correct
permissions.

### Phase 5: End-to-end conformance and standalone UAT

- extend the dedicated `sts-tests` crate from its Phase 0 AWS oracle into the
  endpoint-neutral STS/temporary-credentials conformance suite
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

- implement the already pinned external-ID, source-identity, and role-chaining
  behavior
- probe and implement session policies, session tags, transitivity, provided
  contexts, and packed-policy behavior
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
- secure generator shapes, concurrent nonce-counter uniqueness and exhaustion,
  access-key binding, and negligible random identifier collision assumptions
- unknown version/key ID, wrong key, bit flips, truncation, trailing bytes,
  oversized tokens, invalid timestamps, and cross-domain replay
- irreversible active/validation-only/removed key transitions, attempted key
  reactivation, duplicate key material under a new ID, and concurrent issuance
  during rotation
- maximum version-1 token issuance through complete header, streaming,
  presigned-query, and POST Object transport limits
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
- reject configuration that enables S3 Control or STS on a plain-HTTP listener;
  run their endpoint-neutral conformance scenarios over the local TLS listener,
  while retaining separate ordinary-S3 HTTP coverage
- hold routed requests constant while varying `Host`/authority and prove
  `SharedRegional` is unchanged or the authority is rejected before service
  classification; cover configured-authority mismatch and TLS SNI/authority
  mismatch in standalone UAT
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
- targeted `sts-tests` temporary-credential suite locally and against AWS
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
configured. Both STS and S3 Control require a TLS listener; startup must reject
enabling either service on plain HTTP. An S3-only listener may still use plain
HTTP because ordinary AWS S3 regional endpoints document both HTTP and HTTPS.
`AssumeRole` responses contain bearer credentials, so this is a hard transport
invariant rather than a documentation warning. No session response or request
body may appear in traces.

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
  session-context envelope; issuance creates no per-session record and rejects
  unsupported session-policy parameters
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
