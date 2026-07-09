# Internal Control-Plane Identity and Authentication Plan

This plan defines the shared identity and authentication boundary for Argmin's
internal control-plane traffic. It is intentionally broader than the Phase 12
OpenRaft peer transport: Raft peer RPCs are the first enforcement target, but
frontend/control-plane RPCs, storage-node heartbeats/refreshes, admin control
operations, and future runtime-map/read-index paths need the same identity
model.

The goal is not public S3 authentication. Public request authentication remains
the AWS-compatible SigV4/S3 policy surface. This plan covers internal process
identity for trusted cluster components.

## Why This Is Separate From Raft

Phase 12.3 added a Raft peer frame identity envelope that binds
cluster/source/target fields to the configured peer map. That is necessary but
not sufficient: any process with access to the peer socket can claim those
fields. The same shape exists elsewhere in the control plane:

- a storage node reports heartbeat, endpoint, incarnation, history floor, and
  peering observations;
- a frontend asks for runtime maps and submits control-plane/admin operations;
- a control-plane peer exchanges Raft vote/append/snapshot/transfer messages;
- future read-index and lease-read flows will expose freshness proofs that
  callers must not be able to mint themselves.

A Raft-only credential format would either duplicate later work or bake in
assumptions that do not fit storage-node and frontend callers. The shared model
should be designed once, then enforced path-by-path.

## Security Properties

Internal control-plane authentication must provide:

1. **Cluster binding.** Credentials and frames are scoped to one configured
   control-plane cluster identity. A valid credential for another test cluster,
   deployment, or restored artifact must fail closed.
2. **Principal identity.** The receiver can distinguish Raft peer node,
   storage node, frontend, admin, and local maintenance principals.
3. **Role binding.** A credential issued for one internal role cannot be reused
   as another role. For example, a storage-node heartbeat credential cannot
   submit Raft vote frames.
4. **Source and target binding.** Authenticated material covers the claimed
   source, target, frame kind, and payload bytes. It must not be possible to
   replay a valid payload as a different caller or to a different recipient.
5. **Freshness or replay bounds.** The design must either include an explicit
   nonce/sequence/timestamp policy or document why the enclosed operation is
   already safely idempotent and term/index/fence-protected.
6. **Fail-closed parsing.** Missing, malformed, wrong-cluster, wrong-role,
   wrong-source, wrong-target, stale, or bad-MAC credentials fail before any
   state mutation or OpenRaft dispatch.
7. **Credential rotation path.** Pre-release does not require compatibility
   with old artifacts, but the format should support credential id/version so
   rotation can be staged before production cutover.
8. **Observability.** Auth failures are counted and exposed by role/reason
   without exposing raw secrets or full payloads.

## Principal Model

The first version should define explicit internal principals:

- `RaftPeer { node_id }`: participates in OpenRaft peer RPCs.
- `StorageNode { node_id, incarnation }`: reports node heartbeat, endpoint,
  history floor, peering observations, and storage-node control-plane refresh
  state.
- `Frontend { instance_id }`: reads runtime maps and submits frontend-originated
  control-plane operations.
- `Admin { instance_id }`: invokes internal admin/control-plane operations.
- `LocalMaintenance { process_id }`: internal process-owned maintenance loops
  such as lease expiry scans, checkpoint compaction, and debug/status reads.

The principal set should be encoded as a versioned enum, not as free-form
strings. Unknown roles fail closed.

## Credential Shape

The concrete cryptographic primitive is deliberately a later implementation
choice. Prefer existing repository dependencies if suitable; adding a new
production crypto dependency requires the normal dependency review.

The envelope should be versioned and structured roughly as:

- magic/version for internal-auth envelope format;
- cluster identity;
- credential id/version;
- source principal;
- target principal or target service;
- frame/operation kind;
- optional issued-at / expires-at / nonce / sequence fields, depending on the
  replay policy chosen for that path;
- MAC/signature over the canonical envelope header and canonical payload bytes.

The auth envelope must wrap or cover the existing versioned/CRC-protected
protocol frames rather than replacing their structural validation. CRC remains
for accidental corruption detection; auth covers identity and tampering by a
process that can reach the transport.

## Key and Configuration Model

Initial implementation should use symmetric credentials/MACs for internal
control-plane auth. That matches the existing S3 frontend signing model, fits
the symmetric-secret distribution work already needed for internal encryption
keys, keeps the first implementation small and fail-closed, and avoids pulling
in a certificate or public-key infrastructure before the multi-host control
plane needs it. The envelope is still described in terms of credential
id/version and MAC/signature coverage so a later asymmetric or mTLS-backed
credential can replace the primitive without changing the identity model.

The usable signing/verifying material should be scoped by principal and role.
This is a credential-capability boundary, not a requirement to split roles into
separate OS processes: one Argmin process may legitimately host multiple
internal roles and may therefore hold multiple scoped credentials. The
important property is that each call path receives and uses the credential for
the role it is performing, rather than a generic helper deriving arbitrary roles
from an ambient cluster-wide root secret.

A cluster root secret may exist as provisioning material or as process-local
configuration from which the process derives only the scoped credentials it is
configured to use. The root should not be passed into generic frame-signing
helpers that can mint any role on demand.

Required constraints:

- secrets are configured explicitly for experimental multi-process mode;
- absence of a configured secret fails closed for paths that require auth;
- the secret is never stored in durable restart artifacts or status output;
- status exposes only credential id/version and auth failure counters;
- scoped credentials are bound to cluster identity, principal role, and
  principal id before they are used to sign frames;
- a function handling `StorageNode { node_id }` traffic should receive only the
  storage-node credential for that node, not a key handle that can also mint
  `RaftPeer`, `Frontend`, or `Admin` frames;
- a multi-role process may hold several scoped credentials, but compromise of
  that process grants only the configured roles present in that process, not
  every possible cluster role unless the deployment deliberately configured it
  that way;
- if an implementation deliberately deploys one shared root secret to every
  internal process and exposes it to generic auth helpers, that mode must be
  documented and treated as cluster-wide authority compromise on any single
  process compromise. It must not be the production-shaped target for this
  plan.

Future production work may replace this with per-node credentials or mTLS, but
the frame contract should not assume the credential is symmetric.

## Transport Policy, Config Files, and Test Modes

The mandatory-auth decision should be tied to the transport exposure boundary,
not applied uniformly to every control-plane path before the transport work is
ready.

- **TCP / non-local control-plane transports:** auth is mandatory from the
  first TCP slice. A TCP Raft peer, storage-node heartbeat, frontend runtime-map
  read, or admin control-plane RPC listener must reject startup if the required
  scoped credentials are absent. There should be no unauthenticated TCP mode.
- **Unix socket transport:** auth remains supported in both enabled and
  disabled configurations while Unix sockets serve as the main local/test
  transport. Filesystem permissions and socket placement are part of the Unix
  trust boundary, but standard multi-node/UAT coverage should move toward
  authenticated Unix sockets so the auth paths do not become optional side
  branches.
- **Test-only unauthenticated Unix mode:** keep this as an explicit local/test
  compatibility mode for focused correctness tests that are not exercising
  identity. It must be named and configured as an opt-out, not left as an
  accidental fallback.
- **Production Unix mode:** expected to use authenticated control-plane
  credentials once the config path is ergonomic, but the final mandatory policy
  can be decided during production cutover.

Env vars are adequate for the first experimental slices and focused tests, but
the full TCP plus secret-distribution/rotation shape will outgrow flat env
configuration. The TCP/configuration slice should introduce an optional
configuration file, and production-shaped TCP mode may require one. That file
should be able to describe:

- cluster identity and transport listeners;
- Raft peer, storage-node, frontend, admin, and local-maintenance principals;
- scoped credential ids/versions and secret references, not raw secret dumps
  in diagnostics;
- rotation windows with overlapping accepted credentials;
- whether each transport path requires auth, including the rule that TCP cannot
  disable auth;
- peer endpoint identity and restart-artifact identity checks already required
  by the multihost/Raft plan.

The existing env vars can remain as lightweight overrides/test fixtures, but
they should not be the only production configuration surface once TCP and
credential rotation are in scope.

To make authenticated tests practical, add shared test helpers before broad
test conversion. A helper such as `TestControlPlaneAuth::new(cluster_id)`
should generate deterministic per-test scoped credentials and expose:

- in-process verifier/client builders for storage-node, frontend, admin, and
  Raft peer roles;
- process-test env/config injection helpers for spawned control-plane nodes and
  clients;
- assertions for missing-auth, wrong-role, wrong-cluster, stale-version, and
  redacted-diagnostics cases.

The sequencing target is:

1. land reusable auth test helpers;
2. convert representative standard Unix multi-node/UAT paths to authenticated
   mode;
3. record unauthenticated Unix as a local/test opt-out;
4. add TCP transport in the multihost plan with mandatory auth and config-file
   support;
5. finalize production Unix and secret-distribution policy during cutover.

## Implementation Sequence

The implementation should land as reviewable slices that establish the shared
contract before wiring it into any one transport. Raft peer RPCs remain the
first enforcement target, but the code shape should be reusable by storage-node,
frontend, admin, and runtime-map paths.

1. **Auth model types.** Add the shared internal principal enum, role/operation
   kind, credential id/version, cluster binding, auth decision, and auth
   rejection reason types. This slice should be pure data plus validation
   helpers and should not depend on OpenRaft or Unix transport code.
2. **Canonical envelope codec.** Add a versioned auth envelope codec that wraps
   or covers existing payload bytes without replacing the existing CRC and
   structural frame validation. Tests should cover round-trip, unknown version,
   unknown role/operation tags, truncation, trailing bytes, and malformed
   principal encodings.
3. **Scoped symmetric credential provider.** Add signing and verification
   helpers for explicitly scoped credentials. Callers should receive credentials
   for the role/principal they are performing, not a generic cluster-root helper
   that can mint arbitrary roles. This slice also defines experimental config
   names and the no-secret-in-status/artifacts/logs boundary.
4. **Raft peer transport enforcement.** Wrap or cover append, vote, pre-vote,
   snapshot, and transfer-leader peer payloads with the shared envelope. Verify
   auth after structural frame decode and identity-frame validation, but before
   OpenRaft dispatch or any checkpoint mutation. Responses should carry the
   reverse authenticated identity.
5. **Raft replay/freshness rules.** Document and test the per-RPC replay
   policy. Append/vote/pre-vote/snapshot can rely primarily on OpenRaft
   term/log fences plus credential epoch binding. Transfer-leader needs an
   explicit short freshness bound, nonce/sequence rule, or strict
   current-leader/current-term/current-membership fence before it is counted as
   replay-hardened.
6. **Observability.** Expose auth requirement mode and accepted/rejected
   counters by role, operation class, and rejection reason. Status/debug output
   may include credential id/version and compact counters, but never raw
   secrets, MACs, or full payloads.
7. **Process-level Raft tests.** Add tests proving missing auth, wrong cluster,
   wrong source, wrong target, wrong role, bad MAC, payload bitflip, stale or
   unknown credential id, and transfer-leader replay/fence failures are rejected
   before OpenRaft dispatch and before checkpoint mutation.
8. **Shared authenticated test helpers.** Add common helpers for in-process and
   process-level tests so authenticated Unix sockets become the default shape
   for representative multi-node/UAT coverage without copying credential
   boilerplate into every test.
9. **Transport/config policy bridge.** Record the Unix/TCP mandatory-auth split
   and the transition from env-only experimental configuration toward optional
   or required config files once TCP, secret distribution, and rotation are in
   scope.
10. **Closeout and deferred paths.** Close the Phase 12.4 auth slice only once
   every internal control-plane path that can cross the Unix/process boundary
   has an explicit auth decision: Raft peer RPCs, storage-node heartbeat
   refresh, frontend runtime-map reads, admin RPCs, and authenticated
   runtime-map/admin responses. Any remaining unauthenticated mode must be an
   explicit opt-out for local tests or pre-production deployments, not an
   accidental fallback.

The first implementation slice should start with items 1 and 2 if they remain
small enough to review together. Raft transport wiring should wait until the
standalone envelope and fail-closed codec tests are in place.

Status:

- 2026-07-07: Started items 1 and 2 with `storage::control_plane_auth`: shared
  principal/service/operation, auth decision, and rejection-reason types plus a
  versioned canonical envelope codec covering payload bytes and authenticator
  bytes. The header exposes canonical covered bytes for the future MAC input.
  The slice is intentionally crypto-free; scoped symmetric
  signing/verification and Raft transport enforcement remain the next work.
- The initial codec tests cover round-trip for every principal role and
  fail-closed behavior for bad magic/version, unknown tags, truncation,
  trailing bytes, invalid optional fields, invalid principal fields, empty
  credential fields, empty authenticators, and payload-size limits before
  allocation.
- 2026-07-07: Started item 3 with scoped symmetric credential primitives in
  `storage::control_plane_auth`. The new helper signs canonical envelope bytes
  with HMAC-SHA256 using only the configured scoped principal credential, and
  verification requires the caller to supply the expected cluster, source,
  target, and operation before checking the MAC. Tests cover accepted frames,
  wrong cluster/source/target/role, unknown and stale credential versions,
  issued/expiry time failures, payload tampering, wrong secrets, duplicate
  scoped credentials, and redacted secret debug output.
- 2026-07-07: Added experimental Raft peer-auth process configuration via
  `ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS`
  (`node_id=credential_id:version:secret,...`). Configuration requires an
  explicit Raft cluster name, validates exact local/peer-node coverage against
  the configured peer map, rejects malformed or duplicate entries, and exposes
  only a no-secret configured credential count in process startup output.
  Transport enforcement starts in the following slice.
- 2026-07-07: Started item 4 by adding optional authenticated Raft peer
  transport envelopes. When scoped credentials are configured, outbound
  append/vote/pre-vote/snapshot/transfer-leader peer frames are signed as the
  local `RaftPeer` principal over the existing versioned+CRC peer frame bytes,
  and inbound requests/responses are verified against the configured cluster,
  source node, target node, and operation before OpenRaft dispatch or response
  decode. Existing unauthenticated peer paths remain available when no
  credentials are configured.
- 2026-07-07: Extended item 7 with process peer-listener negative coverage for
  authenticated Raft requests. The `argmin-s3` peer RPC worker now has tests
  proving missing auth, wrong cluster, wrong source, wrong target, wrong role,
  bad MAC, payload bitflip, stale credential, and unknown credential frames fail
  before OpenRaft dispatch, do not mutate the local Raft vote/term, and do not
  write a peer response.
- 2026-07-07: Added the first item 5 replay rule for transfer-leader Raft peer
  RPCs. Authenticated transfer-leader envelopes now carry a short issued/expires
  freshness window; verification requires the complete window, rejects future or
  expired envelopes, rejects overlong windows, and process-level coverage proves
  expired transfer-leader auth fails before OpenRaft dispatch or vote/term
  mutation. Remaining replay work is broader observability and any later
  non-Raft transport freshness policy.
- 2026-07-07: Started item 6 by adding compact Raft peer auth policy counters
  for accepted/rejected verification decisions keyed by operation and rejection
  reason, plus a no-operation bucket for malformed frames that fail before an
  operation can be decoded. The first regressions pin transfer-leader freshness
  failures as rejected `RaftTransferLeader` observations and prove process-level
  pre-dispatch auth failures are counted. A later process/debug-status slice
  should expose these counters without leaking secrets, MACs, nonces, or
  payloads.
- 2026-07-07: Extended item 6 with a redacted Raft peer-auth status snapshot on
  the transport policy and an `argmin-s3` diagnostic formatter. The surface
  reports auth-required mode, local principal, credential id/version, and
  accepted/rejected counters by operation/reason, while tests assert configured
  secrets, MAC/authenticator bytes, nonces, and payload contents are not
  exposed. Storage-node, frontend, admin, and runtime-map auth diagnostics
  remain deferred with their respective enforcement slices.
- 2026-07-07: Started Slice B storage-node enforcement. The Unix control-plane
  heartbeat refresh path now accepts node-scoped storage credentials configured
  by `ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS` and derives the
  incarnation-specific `StorageNode { node_id, incarnation }` credential only
  after the storage-node incarnation is known. Control-plane listeners can
  require authenticated heartbeat refresh frames for both single-authority and
  experimental Raft-backed process modes; missing auth, wrong node,
  wrong incarnation, and missing/overlong freshness windows fail before
  heartbeat mutation. Frontend/admin/runtime-map auth remains out of this
  slice.
- 2026-07-07: Tightened item 4 response coverage for Raft peer RPCs. Unix peer
  network tests now prove an authenticated vote request receives and verifies a
  reverse-identity authenticated response, and that an unauthenticated response
  fails closed when peer auth is required.
- 2026-07-07: Extended response-side item 4 coverage at the reusable policy
  boundary. Tests now sign and verify append, vote, pre-vote, snapshot, and
  transfer-leader response frames with reverse peer identity, and reject a
  forged envelope whose operation does not match the embedded response kind.
- 2026-07-07: Tightened item 4 process enforcement. Experimental multi-node
  Unix-peer control-plane startup now requires
  `ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS` covering the configured peer
  map; unauthenticated peer transport remains only for low-level unit tests and
  single-node local peer-socket mode.
- 2026-07-07: Closed Slice A for Phase 12.4. Raft peer RPCs now have the shared
  envelope foundation, scoped symmetric peer credentials, authenticated
  request/response payload coverage, transfer-leader freshness bounds,
  process-level fail-closed tests, redacted diagnostics, and mandatory
  credentials for multi-node process peer mode. This closes security finding
  `security/codex-e41688b` via commits `09fa6d0c`, `d5f7b19b`, `a260b300`,
  `c03e0c85`, `2f4090ae`, `fa8a5fa4`, and `d08d5d25`. Storage-node,
  frontend, admin, and runtime-map response auth were then pulled into this
  plan as follow-on slices so the shared auth foundation covers the broader
  control-plane boundary rather than only Raft.
- Raft peer process/env config now accepts overlapping credential identities for
  a node as long as the credential id/version pair is unique. Peer verifiers
  accept every configured overlapping credential, while the local Raft peer
  signs outbound frames with the highest configured credential version for its
  node so staged rotation works the same way as Unix control-plane auth.
- 2026-07-08: Started Slice C frontend read enforcement at the reusable Unix
  control-plane boundary. The storage-layer verifier can now carry scoped
  frontend credentials, require authenticated runtime-map read envelopes when
  any frontend credential is configured, enforce short issued/expires replay
  windows, reject missing/wrong-role frontend reads before authority access,
  and report frontend credentials through the redacted Unix auth diagnostics.
  Process-level frontend credential configuration and admin mutation auth
  remain follow-on Slice C work.

## Path-Specific Enforcement Order

### Slice A: Raft Peer RPCs

Raft peer RPCs are the first enforcement target because they already have a
bounded/versioned frame boundary and are explicitly in Phase 12.4.

Required:

- bind auth to existing Raft peer frame identity: cluster, source node, target
  node, and frame kind;
- cover append/vote/pre-vote/snapshot/transfer-leader payload bytes;
- fail before OpenRaft dispatch on missing or bad auth;
- expose peer-auth failure counts/reasons through replicated authority status
  or process debug status;
- test missing auth, wrong cluster, wrong source, wrong target, wrong role,
  modified payload, and stale/unknown credential id.

### Slice B: Storage-Node Control-Plane Reports

Storage-node heartbeats and refresh reports mutate serving state and runtime-map
history. They need the same cluster/node/incarnation binding.

Required:

- bind heartbeat and refresh reports to `StorageNode { node_id, incarnation }`;
- reject reports for another node id or stale incarnation before mutation;
- cover endpoint, requested lease duration/deadline, history floor, and peering
  observation payloads;
- preserve deterministic apply: auth is checked before constructing or
  accepting the durable command, not during replay.

Progress:

- The Unix control-plane heartbeat refresh path now has an opt-in authenticated
  client/verifier boundary that wraps the serialized heartbeat payload in the
  shared auth envelope. The verifier derives the expected `StorageNode`
  principal from the decoded heartbeat payload and rejects missing auth or a
  mismatched node/incarnation before calling the authority.
- Process wiring now uses node-scoped storage credentials from
  `ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS` and derives the scoped
  `StorageNode { node_id, incarnation }` credential only after the dynamic
  storage-node incarnation is known at startup.
- The storage-node Unix verifier exposes compact accepted/rejected auth
  counters by operation and rejection reason for heartbeat refresh auth, without
  exposing secrets, MACs, nonces, or payload bytes. Process/debug-status
  exposure reuses this surface at control-plane process startup for both
  single-authority and experimental Raft-backed process modes. Raw
  unauthenticated heartbeat payloads are counted as `Missing`; auth-looking but
  structurally invalid envelopes are counted as `Malformed`.
- Storage-node heartbeat auth is enforced only when storage-node credentials are
  configured. Enabling frontend runtime-map auth or admin command auth alone
  does not implicitly require storage-node heartbeat envelopes, so each
  internal control-plane credential class can be rolled out independently.
- Unix control-plane auth status and process diagnostics now expose explicit
  per-path requirement flags for storage-node heartbeat refreshes, frontend
  runtime-map reads, and admin control-plane commands. This avoids treating the
  presence of one credential class as evidence that every Unix control-plane
  path is enforcing auth.
- Unix control-plane verifiers and process/env config parsers now accept
  overlapping credential identities for one storage node, frontend instance, or
  admin instance as long as the credential id/version pair is unique. This
  gives storage-node heartbeat, frontend runtime-map read, and admin command
  auth a staged rotation window; signed responses are emitted with the same
  credential id/version that verified the request so old and new clients can
  coexist during the overlap.
- Local storage-node, frontend, and admin Unix control-plane clients now select
  the highest configured credential version for their own node/instance when
  overlapping credentials are present. Servers still accept every configured
  overlapping credential, but new outbound traffic moves forward to the staged
  active version without depending on env entry order.
- Unix auth diagnostics now have regression coverage for overlapping
  storage-node, frontend, and admin credentials. The exposed status includes
  credential ids/versions and counts for each auth class while continuing to
  redact all configured secret material and raw rejected payload bytes.

### Slice C: Frontend and Admin Control-Plane RPCs

Frontend/admin operations need caller identity primarily for internal trust and
audit. Public S3 auth still determines user authorization.

Required:

- distinguish frontend read/runtime-map requests from admin mutation requests;
- bind retry confirmation and read-index freshness requests to authenticated
  internal callers;
- expose auth failure reasons without leaking request payloads;
- keep command replay independent of auth metadata unless the command itself
  intentionally records caller identity.

Progress:

- The Unix control-plane RPC boundary now classifies every current RPC kind
  into an explicit auth operation: frontend runtime-map reads,
  storage-node heartbeat refresh, or admin control-plane command. This pins the
  operation mapping that future request-auth enforcement must use and forces
  new RPC kinds to choose their auth class instead of inheriting an implicit
  default.
- Frontend runtime-map read auth is now implemented as an opt-in Unix
  control-plane verifier mode. When frontend credentials are configured, plain
  read requests fail closed as missing auth, signed reads must use a
  `Frontend { instance_id }` principal with the `FrontendRuntimeMapRead`
  operation, a concrete Unix RPC kind bound inside the signed payload, and a
  bounded freshness window. Storage-node/admin credentials cannot be reused for
  frontend reads, and a captured signed snapshot read cannot be replayed as a
  status read within the freshness window. Existing unauthenticated runtime-map
  reads remain accepted until process-level frontend credential configuration
  enables this verifier mode.
- Process configuration now enables that verifier mode with
  `ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS` and lets frontend processes
  select their local signing credential with
  `ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID`. Frontend startup runtime-map
  fetches and refresh-loop reads use the authenticated Unix control-plane
  client when configured; control-plane listeners verify the configured
  frontend principal set and expose the credential count through the existing
  redacted Unix auth diagnostics. The lightweight
  `control-plane-runtime-map-ready` and
  `control-plane-runtime-map-diagnostics` probes also load this auth-only
  frontend runtime-map config, so UAT/admin readiness checks can query an
  auth-enforcing control plane without requiring unrelated server config.
- Admin control-plane command auth is now implemented as an opt-in Unix
  verifier mode. `ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS` configures the
  accepted admin principals, and when present the server requires
  `Admin { instance_id }` envelopes with the `AdminControlPlaneCommand`
  operation, concrete Unix RPC kind binding, and the same short freshness
  window before any admin payload is parsed or applied. Missing auth and
  wrong-role credentials fail before mutation and are counted through the
  redacted Unix auth diagnostics. Admin clients now load an auth-only
  `ARGMIN_CONTROL_PLANE_ADMIN_AUTH_*` config and sign live PG acting-set
  updates, metadata-transfer fence/install flows, and Raft leadership/snapshot/
  election triggers when credentials are present. Admin-only command clients do
  not perform unauthenticated runtime-map confirmation reads. PG acting-set
  updates and metadata-transfer installs now use dedicated admin-signed PG
  runtime-map confirmation reads after ambiguous response loss. Raft
  leadership/snapshot/election trigger clients now fail closed as
  `RpcUnconfirmed` after ambiguous response loss. This is a client-side retry
  decision, not a Raft liveness state: if the trigger reached OpenRaft, the
  election, transfer, or snapshot/purge continues under normal Raft behavior;
  if it did not, the cluster remains in its previous Raft state and normal
  timers still apply. Automatic admin retry remains deferred until those
  triggers have operation-specific confirmation predicates or an authoritative
  Raft status/read surface.

### Slice D: Runtime-Map and Read Freshness Proof Consumers

Runtime-map freshness proofs must not be confused with authentication. Auth
proves who sent the response; the freshness proof proves what committed/applied
state the response reflects.

Required:

- authenticated response identity covers the map bytes and freshness proof;
- clients reject maps whose auth target/source is wrong even if the freshness
  proof decodes;
- read-index proof semantics remain tied to the state machine's applied log id.

Progress:

- 2026-07-08: Started Slice D for Unix runtime-map reads. When frontend read
  auth is required, successful runtime-map snapshot/status responses are now
  signed as the `RuntimeMap` service using the configured bilateral frontend
  secret and are targeted back to the verified `Frontend { instance_id }`
  principal. Authenticated frontend clients verify the `RuntimeMapResponse`
  envelope, concrete RPC kind binding, freshness window, source service, target
  frontend principal, and MAC before decoding the runtime map or status bytes.
  Plain runtime-map responses remain accepted only on unauthenticated clients.
- 2026-07-08: Tightened Slice D so authenticated runtime-map responses cover
  the encoded response status/body, not only successful map bytes. Authenticated
  frontend clients now verify the signed runtime-map response envelope before
  decoding either a success body or a remote error.
- 2026-07-08: Extended the signed `RuntimeMapResponse` envelope to
  authenticated storage-node heartbeat refresh responses. The control plane now
  derives a narrow runtime-map service response credential from the verified
  `StorageNode { node_id, incarnation }` credential and signs the encoded
  heartbeat response status/body before returning lease/runtime-map state.
  Authenticated storage-node clients reject unsigned or wrong-target heartbeat
  responses before decoding them.
- 2026-07-08: Added authenticated admin mutation responses as a distinct
  `AdminControlPlaneResponse` operation signed by the `Admin` service and
  targeted back to the verified `Admin { instance_id }` principal. Authenticated
  admin clients now reject unsigned responses and verify signed success/error
  response status bodies before decoding them. Response verification samples
  receive time separately from request signing time, matching the runtime-map
  and heartbeat response freshness shape.
- 2026-07-08: Storage-node, frontend, admin, runtime-map response, and admin
  response auth now all support staged credential rotation. Process/env config
  accepts overlapping credentials for the same principal when the credential
  id/version pair is unique, verifiers retain all configured versions, and local
  clients sign with the highest configured version for their principal.
- 2026-07-09: Started the shared authenticated test-helper slice. Storage
  control-plane auth tests now build storage-node, frontend, admin, scoped
  credentials, and Unix verifiers through a single deterministic
  `TestControlPlaneAuth` helper while preserving the older wrapper names for
  low-churn migration. The experimental Raft process tests now use a
  `ProcessTestControlPlaneAuth` helper for both `ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS`
  env generation and direct authenticated peer-frame signing, so process-test
  credentials have one source of truth before broader authenticated Unix/UAT
  conversion.
- 2026-07-09: Extended the process auth helper to frontend runtime-map
  credentials and converted the first experimental two-node Raft process smoke
  to require frontend runtime-map auth on the control-plane Unix sockets. The
  `control-plane-runtime-map-ready` helper now runs with matching signed
  frontend credentials in that smoke, giving the standard process bootstrap
  path real authenticated Unix coverage while preserving unauthenticated helper
  wrappers for the remaining local/test-only paths.
- 2026-07-09: Extended the process auth helper to admin credentials and
  converted the experimental Raft leadership-transfer process smoke to require
  admin auth on every control-plane process. The
  `control-plane-transfer-raft-leadership` and follow-up
  `control-plane-set-pg-acting-set-live` helpers now run with a signed admin
  credential in that smoke, covering real process-level admin command paths
  alongside the existing unauthenticated wrappers used by tests that have not
  opted into admin auth. The authenticated admin response verifier also now
  tolerates only a tiny same-host response issue-time skew while keeping request
  freshness strict.
- 2026-07-09: Added argmin-s3 storage-node config wiring coverage for
  authenticated heartbeat/runtime-map refresh. A storage-node client built from
  `ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS`-shaped config now sends a
  signed heartbeat to an auth-enforcing Unix control-plane server and verifies
  the signed runtime-map response, including accepted-operation metrics. This
  closes the main process/config gap between storage-level heartbeat auth tests
  and the storage-node refresh loop client construction path.
- Next planned slice: add a full-auth composition smoke before broader UAT
  conversion. The smoke should run a representative process setup with storage
  heartbeat auth, frontend runtime-map auth, admin command auth, and Raft peer
  auth enabled together; wait for an authenticated storage-node heartbeat to
  make the node visible/serving; run `control-plane-runtime-map-ready` with
  frontend credentials; run one admin helper with admin credentials; and assert
  auth diagnostics show accepted storage/frontend/admin/Raft operations with no
  unexpected rejects. This is intentionally separate from TCP/config-file work.

## Replay Policy

Replay handling should be selected per operation class:

- Raft append, vote, pre-vote, and snapshot RPCs carry terms/log ids and
  OpenRaft enforces stale-term and log-boundary behavior, but auth must still
  bind them to a bounded credential epoch or key id so old-cluster frames cannot
  be replayed after credential rotation. Snapshot requests/responses also need
  payload coverage of the requested/returned snapshot metadata and bytes.
- Raft transfer-leader is not fully protected by term/log-id idempotence alone.
  A captured valid transfer request can remain semantically meaningful within
  a credential lifetime. The first Raft peer implementation uses a short
  issued/expires freshness bound for transfer-leader frames; a stricter
  current-leader/current-term/current-membership fence or nonce/sequence cache
  can still be added later if operational review wants replay protection beyond
  the bounded window.
- Heartbeat/lease commands are time-sensitive and must include committed
  timestamp/deadline semantics. Auth replay protection must not make apply
  depend on wall clock.
- Admin mutations should prefer explicit command ids or confirmation
  predicates over blind retry.
- Read-only runtime-map requests may use short-lived request/response freshness
  bounds once the monotonic-clock design is finalized.

Any replay cache must be bounded and must not become a new durability
requirement unless the command semantics need it.

## Artifact and Restart Interaction

Auth configuration is process configuration, not replicated state-machine
content in the first version. Durable artifacts should record only enough
non-secret identity to prevent misconfiguration:

- cluster identity;
- local Raft node id / process role where applicable;
- configured peer set identity already required by Phase 12;
- credential id/version if needed to detect a process starting with stale or
  wrong auth config.

Secrets themselves must never be written to restart artifacts, WAL frames,
flight-recorder logs, or debug endpoints.

## Testing Plan

Minimum coverage before closing the shared-auth foundation:

- codec round-trip and fail-closed tests for every envelope version and role;
- wrong-cluster/source/target/role tests;
- bad-MAC and payload-bitflip tests proving auth covers payload bytes;
- unknown credential id/version tests;
- replay/stale credential tests for paths with freshness fields;
- process-level Raft peer RPC tests proving unauthenticated frames fail before
  OpenRaft dispatch and before checkpoint mutation;
- storage-node heartbeat/report tests proving wrong node/incarnation fails
  before command construction or state mutation;
- status/debug tests proving auth failures are observable without secret leaks.

Coverage audit as of 2026-07-08:

| Boundary | Covered regression families | Remaining / deferred |
| --- | --- | --- |
| Canonical auth envelope | Principal/role round trips; malformed frames; unknown tags; payload-size limits before allocation; wrong cluster/source/target/role; unknown and stale credential versions; issued/expiry failures; tampered payloads; wrong secrets; duplicate scoped credentials; redacted debug output. | None for the shared symmetric envelope. A future asymmetric or mTLS credential backend must reuse the same source/target/operation/freshness tests. |
| Raft peer RPC | Missing auth, malformed frames, wrong cluster/source/target/role, bad MAC/payload bitflip, stale/unknown credentials, transfer-leader freshness/replay bounds, response reverse identity, and no-dispatch/no-checkpoint process behavior before OpenRaft mutation. Redacted peer-auth diagnostics expose required mode, local principal, credential id/version, and counters. | Broader per-RPC replay caches remain deferred unless operational review requires protection beyond transfer-leader freshness and Raft's own term/log fences. |
| Storage-node heartbeat refresh | Missing and malformed auth, wrong node/incarnation, wrong source, missing/overlong/expired freshness windows, overlapping credential rotation, authenticated signed responses, unsigned response rejection, and pre-mutation state unchanged on rejection. | Production rollout still needs secret distribution and a decision on when heartbeat auth is mandatory rather than opt-in by configured credentials. |
| Frontend runtime-map reads | Missing auth, wrong-role storage credentials, RPC-kind replay between snapshot/status, overlapping credential rotation, per-attempt re-signing during read retries, authenticated response identity, wrong-target response rejection, signed error responses, and unsigned response rejection. | Admin confirmation reads intentionally do not borrow frontend credentials; PG acting-set confirmation now has a dedicated admin-signed PG runtime-map read path. |
| Admin control-plane commands | Missing auth, wrong-role frontend credentials, overlapping credential rotation, metadata-transfer admin command signing, Raft leadership/snapshot/election admin signing, authenticated response identity, signed error responses, unsigned response rejection, and receive-time response freshness. Authenticated PG acting-set updates now use admin-signed PG runtime-map confirmation reads after ambiguous response loss; metadata-transfer install uses the same admin-signed confirmation-read helper shape with response-loss regression coverage. Raft leadership/snapshot/election trigger clients fail closed as `RpcUnconfirmed` after ambiguous response loss without stopping Raft protocol progress. | Automatic retry for Raft admin triggers remains deferred until operation-specific confirmation predicates or an authoritative Raft status/read surface exist. |
| Process/env configuration and diagnostics | Storage-node, frontend, admin, and Raft peer env parsers accept staged rotation by unique principal + credential id/version; local clients sign with the highest configured version; diagnostics expose counts and credential ids/versions without secrets, MACs, nonces, or payloads. | Add shared auth test helpers, move representative Unix multi-node/UAT paths to authenticated mode, and introduce a config-file surface with the TCP/secret-distribution slice instead of growing env vars indefinitely. Production cutover must decide final Unix mandatory-auth policy and external secret rotation/distribution mechanics. |

## Observability

Expose counters and compact diagnostics for:

- auth accepted/rejected by role and operation class;
- rejection reason: missing, malformed, unsupported version, wrong cluster,
  wrong source, wrong target, wrong role, unknown credential, stale credential,
  MAC mismatch, replay/freshness failure;
- last credential id/version currently accepted by the process;
- auth requirement mode for each internal transport path.

Do not expose secrets, MACs, raw credential material, or full internal payloads.

## Phase Relationship

- **Phase 12.4:** design and implement the shared foundation; enforce it first
  on Raft peer RPCs, then extend the same envelope/credential model to
  storage-node heartbeat refresh, frontend runtime-map reads, admin
  control-plane RPCs, and authenticated control-plane responses.
- **Later Phase 12 / production cutover:** decide when each auth mode becomes
  mandatory in production configuration, add any required external secret
  distribution/rotation automation, introduce TCP/non-local transport only with
  mandatory auth, move complex control-plane auth/transport config into an
  optional or required config-file surface, and revisit asymmetric or
  mTLS-backed credential primitives if symmetric cluster credentials become too
  broad. Admin API formalization can remain a later production-readiness slice
  unless TCP/config-file work exposes ambiguity in the admin operation surface.
- **Out of scope for this plan:** public S3 authentication/authorization,
  external tenant identity, data-plane storage RPC authorization beyond the
  control-plane identity needed to issue routing/fencing decisions, and
  compatibility with pre-release artifacts that lack auth configuration.
