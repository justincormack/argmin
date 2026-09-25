<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Threat model

This document describes current security boundaries and assumptions, not a
certification that they are free of defects. Argmin is pre-release and not yet
suited for production use. Implemented distributed operation does not change
that status. Configuration requirements live in the
[configuration guide](configuration.md); format and upgrade policy lives in
the [versioning guide](versioning.md).

## 1. Deployment and assets

Argmin exposes an S3-compatible API through `server-http`, authenticates
requests in `auth`, evaluates S3 authorization in `server-core`, and owns
physical storage, internal protocols, and durable formats in `storage`.

There are two deployment shapes:

- **Embedded standalone:** one `argmin-s3` process contains the frontend,
  storage engine, and single metadata/control authority. Environment-only and
  standalone-manifest operation use one storage node and EC 1+0; neither
  provides Argmin-managed disk or node redundancy. The current embedded
  runtime does not activate separate internal RPC endpoints.
- **Replicated manifest deployment:** separate frontend, storage-node, and
  control-plane processes communicate over authenticated Unix sockets or
  TLS/TCP. The manifest defines failure domains, erasure coding, Raft voters,
  endpoint identities, credentials, and resource limits. Processes may share
  hosts where permitted by that topology; separate processes alone do not
  create independent host or disk failure domains.

Assets include object payloads and versions; names, listings, tags, ACLs,
policies, retention and legal-hold state; S3 and internal credentials; TLS and
encryption keys; routing and authority-clock state; and the availability and
integrity of metadata, journals, snapshots, and shard files.

Keys, control-plane state, and metadata are durable data dependencies, not
disposable caches. Replication and erasure coding are not backups and do not
protect against every administrative mistake or correlated failure.

## 2. Trust boundaries and assumptions

| Boundary | Untrusted input and required protection |
|---|---|
| Public API → frontend | HTTP framing, headers, queries, XML, policies, forms, bodies, and signatures are attacker-controlled. Parsing, admission, authentication, and authorization must not assume a well-behaved client. |
| S3 principal → another principal's resources | A valid credential does not authorize every operation. Ownership, policies, ACLs, public-access settings, version identity, and Object Lock rules still apply. Anonymous access is an explicit authorization outcome, not an authentication fallback. |
| Coordinator → storage | Storage owns placement, physical identifiers, durable representations, replay, and cleanup. Logical capabilities and owner-side validation bind operations to subjects and routing authority. Data must not be assumed well formed merely because it came from disk or another crate. |
| Internal network → cluster listener | TLS establishes server identity and confidentiality on TCP; signed messages authenticate and authorize callers. Protocol kind, cluster/principal identity, request/response binding, framing, and admission limits are separate checks. Unix transport does not replace protocol authentication in replicated deployments. |
| Operator → cluster administration | Provisioning, initialization, topology administration, and authority-clock recovery are privileged. Public S3 credentials are not cluster-admin credentials. Recovery endpoints must remain authenticated even when ordinary serving is fenced. |
| Process → host/filesystem | The OS, service account, mounts, and administrators are trusted. Permission, lock, path, and durable-identity checks prevent unauthorized or accidental reuse; they do not defend against root or a compromised service account. |

### Trusted cluster components, not Byzantine participants

Frontends, storage nodes, authorities, their hosts, and provisioning are trusted
runtime components. A compromised frontend can act with its internal
capabilities; a compromised storage node can lie about its state; a compromised
authority can undermine routing and admission. Raft and metadata proofs are
not Byzantine consensus or remote attestation.

Internal authentication uses symmetric HMAC credentials. A process holding a
verification secret can also forge messages authenticated with that secret.
Separate scoped credentials do not make a compromised verifier trustworthy.
Distribute only the signing and verification material required by each process,
as described in the configuration guide.

Clocks are security-relevant: S3 expiry, credential windows, leases, and
retention depend on their respective time sources. Authority-clock continuity
and leadership checks can fence serving and require explicit operator recovery.
They do not defend against a hostile host controlling the process and clocks.

## 3. Public API and credential security

### Parsing and resource admission

The public listener is Hyper-based HTTP/1. Connection and in-flight request
limits, TLS/header timeouts, body idle timeouts, pre-authentication body
deadlines, bounded buffered control bodies, and operation-specific parser and
size limits constrain resource use. They do not guarantee immunity to slow
clients, denial of service, or disk exhaustion.

Bucket names, object keys, headers, queries, XML, and POST policies have
surface-specific validation. Object keys may contain control characters
permitted by the API; output paths must encode them safely rather than assume
all accepted text is printable. User object keys are not filesystem paths.
Testing must cover malformed inputs and collisions between validation,
authentication, and authorization failures, including AWS error precedence.

### Authentication, authorization, and unfinished STS support

Header-signed, presigned, POST-policy, and aws-chunked requests have different
signing rules. Verification includes scope and time validation and
constant-time cryptographic verification. Payload modes must follow the
selected service's AWS behavior: a valid signature is not always a claim that
every body byte was signed. TLS remains necessary for confidentiality and for
protecting legitimately unsigned payloads in transit.

S3 authorization covers configured principals and anonymous requests,
including cross-account and foreign-owned-object behavior. It is not merely
owner-versus-public access. However, the normal process configuration provisions
a single S3 account/access-key identity; it is not a complete IAM provisioning
service or a claim of general multi-tenant operational isolation.

**STS is not currently functional or supported.** Partial issuance, token, and
identity-provider code and reference tests are unfinished implementation, not
an available deployment capability. Assumed-role sessions remain blocked at the
S3 authorization boundary through `auth::ConfiguredOrAnonymousAuth`; this
includes S3 Control and upload adapters. Neither this code nor an AWS-only
oracle establishes usable STS or IAM support. Future support requires its own
AWS/local tests and an updated threat model; see the
[STS issuance plan](../plans/sts-assume-role-issuance.md).

Policies, ACLs, Block Public Access, ownership controls, retention, and legal
hold must apply to the actual operation and resource version, including
concurrent writes and cleanup. Public access is intentional when permitted;
the security property is faithful authorization, not blanket denial of every
anonymous request. The [AWS compatibility guide](aws-compatibility.md)
describes API limitations separately from deployment trust assumptions.

### TLS, proxies, and encryption

The standalone listener defaults to loopback; public HTTPS is optional and
must be configured explicitly. Plain HTTP does not provide confidentiality for
objects, signatures, or presigned URLs. Presigned URLs must be handled as
secrets within their validity and permission scope.

SSE-C requires TLS on the connection accepted by Argmin itself. A proxy that
terminates TLS and forwards plaintext HTTP does **not** satisfy this check;
forwarded headers do not establish secure transport. Such deployments need TLS
on the proxy-to-Argmin hop as well. Source-IP and transport-dependent policy
context reflects the server's connection, not arbitrary client-supplied
forwarding headers.

SSE-S3 and SSE-C encrypt object payloads; they are not full-database encryption
and do not promise to hide names, policies, tags, or all metadata from the
storage operator. The SSE-S3 wrapping key and SSE-C validator secret must be
preserved for existing data. Losing required material can make data
inaccessible. A compromised live frontend can observe plaintext and request
key material. AWS-compatible SSE-KMS is not currently implemented.

### CORS and browser-facing behavior

CORS controls browser access to responses; it does not grant S3 authorization.
Broad CORS rules matter alongside public permissions or exposed presigned URLs.
Buckets may be distinguishable through AWS-compatible errors and unauthenticated
OPTIONS behavior, including account-regional names. Bucket existence is not a
blanket confidentiality guarantee. Evaluate a disclosure against the operation's
AWS behavior and the sensitivity of the actual data disclosed.

Stored objects are attacker-controlled and may be active browser content.
Deployments serving such content should isolate its origin from trusted web
applications. Escaping generated XML and diagnostics does not sanitize an object
intentionally returned byte-for-byte.

## 4. Cluster transport, storage, and recovery

### Authenticated internal protocols

Replicated manifests require credentials for every activated internal role.
Internal TCP has no plaintext mode: it uses explicit trust bundles, server
names, and TLS identities. Client identity is established by signed envelopes,
not client TLS certificates. Frontend, storage-node, Raft-peer, admin, and
maintenance principals have distinct protocol permissions.

Frame-size ceilings, pre-authentication byte budgets, worker admission, and I/O
deadlines bound exposure before and during protocol handling. Authentication and
protocol bindings reject invalid identities and crossed responses. Freshness
checks are not a general exactly-once execution guarantee: durable command
identity, deduplication, and operation-specific confirmation are separate
correctness requirements.

After a mutating request may have been sent, response loss or an unverifiable
response can leave its outcome unconfirmed. Callers and operators must not
interpret that as proof the command was not applied or blindly retry it.
Authority-clock re-establishment and other administration are included in this
rule. Outbound connections to configured cluster endpoints are real attack
surfaces: object URLs are not arbitrary fetch instructions, but endpoint
redirection must not be dismissed as categorically irrelevant to security.

### Durable state and concurrency

SQLite and shard-file access are storage-owned. Values use parameterized SQL,
physical paths derive from storage identities, and durable readers validate
format and logical invariants. These controls reduce injection, traversal, and
corruption risks; they do not make arbitrary locally modified data trustworthy.

[The durable-storage guide](durable-storage.md) inventories journals, databases,
shard files, restart artifacts, and transition markers and defines their
fsync/publication ordering. Replication, reservations, route admission, payload
leases, and subject-bound handles protect concurrent writes, reads, recovery,
and deletion. Storage-owned reclaim, repair, backfill, scavenging, and
abandoned-session cleanup must respect those lifetimes. S3-visible lifecycle and
Object Lock decisions remain part of the API semantics, not permission for physical
maintenance to delete any old-looking payload.

Checksums and metadata proofs detect accidental corruption and inconsistent
state; CRCs are not cryptographic authentication against malicious rewriting.
Erasure coding recovers only within configured, actually independent failure
domains. ETags are opaque API tokens, not a cryptographic security boundary or
a promise of AWS MD5 representation.

Startup binds state to configured identity and topology, checks ownership and
locking, and rejects incompatible formats. Recovery of permitted torn or
corrupt state follows format-specific policy; an unsupported version is not
permission to reinterpret or overwrite it. There is currently no supported
upgrade, downgrade, or mixed-format-version cluster. Old-format fixtures are
rejection evidence, not compatibility readers. Do not reinitialize a lost,
established Raft voter under its old identity.

Persistence assumes the filesystem and devices honor required durability
operations. Mount validation cannot establish power-loss safety; tmpfs tests
are not durability evidence. Protect manifests, keys, state, and backups
consistently rather than restore unrelated pieces and assume their identities
or checkpoints still agree.

## 5. Diagnostics, operational limits, and evidence

Logging uses redacted secret carriers, escaped attacker-controlled text, and
bounded diagnostic categories at selected boundaries. This is not a claim that
all logs are public-safe or every internal error type is opaque. Request
identifiers, object names, topology context, traces, and crash dumps can remain
sensitive. Do not log raw credentials, session tokens from unfinished STS
paths, SSE-C keys, signed URLs, or protocol bodies. Protect diagnostic files and
operator access.

Production artifacts must not enable test-only facilities. Use the default
production build or documented OpenSSL alternative, not `--all-features`.
Test hooks, fault injection, fixtures, and fuzzing are development surfaces,
not supported runtime endpoints. See the [README](../README.md) and
[testing guide](testing.md).

Current limits and exclusions include:

- no general per-tenant storage/CPU quotas or hostile-tenant fairness guarantee;
- no Byzantine-node protection or defense against a compromised host/service account;
- no functional STS, full IAM/KMS service, or assumed-role S3 authorization;
- no supported format migration or rolling mixed-version upgrade; and
- no assertion that test coverage alone proves production durability or security.

The [security testing guide](security-testing.md),
[finding inventory](security-findings-inventory.md), and
[format evidence ledger](storage-format-ledger.md) identify focused tests,
known dispositions, and format guarantees. AWS-facing tests, deterministic
race/fault tests, model/property tests, and fuzzing provide complementary
evidence. Historical inventory entries and incomplete mappings must not be read
as proof of a completed independent security audit.

## 6. Severity calibration

Assess reachability, prerequisites, scope, and recoverability rather than only
the subsystem or error code:

- **Critical:** unauthenticated code execution, broad authentication bypass, or
  exposure of signing/wrapping secrets enabling broad compromise.
- **High:** cross-principal access to protected data, retention bypass, unsafe
  acknowledged mutations, or corruption causing unrecoverable data loss.
- **Medium:** bounded service denial or disclosure of sensitive operational
  metadata without protected object content; impact may rise with persistence,
  scale, or exploitability.
- **Low:** interoperability or diagnostic defects with no demonstrated
  confidentiality, integrity, or significant availability impact.

An incorrect error classification can be high impact if it enables an unsafe
retry; a canonicalization difference can be critical if it bypasses
authentication. Conversely, intentional AWS-compatible public/discovery
behavior is not automatically a security defect.
