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

### Slice D: Runtime-Map and Read Freshness Proof Consumers

Runtime-map freshness proofs must not be confused with authentication. Auth
proves who sent the response; the freshness proof proves what committed/applied
state the response reflects.

Required:

- authenticated response identity covers the map bytes and freshness proof;
- clients reject maps whose auth target/source is wrong even if the freshness
  proof decodes;
- read-index proof semantics remain tied to the state machine's applied log id.

## Replay Policy

Replay handling should be selected per operation class:

- Raft append, vote, pre-vote, and snapshot RPCs carry terms/log ids and
  OpenRaft enforces stale-term and log-boundary behavior, but auth must still
  bind them to a bounded credential epoch or key id so old-cluster frames cannot
  be replayed after credential rotation. Snapshot requests/responses also need
  payload coverage of the requested/returned snapshot metadata and bytes.
- Raft transfer-leader is not fully protected by term/log-id idempotence alone.
  A captured valid transfer request can remain semantically meaningful within
  a credential lifetime. The first implementation should either add a short
  freshness bound or nonce/sequence for transfer-leader frames, or document and
  test a stricter fence rule such as "accepted only from the current serving
  leader for the current term and only while the requested transferee still
  matches current membership." Until one of those is implemented, transfer-
  leader must not be counted as replay-hardened.
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
  on Raft peer RPCs.
- **Later Phase 12 / production cutover:** extend enforcement to storage-node
  heartbeat/refresh, frontend runtime-map reads, and admin control-plane RPCs
  before those paths are used across a real multi-host boundary.
- **Out of scope for this plan:** public S3 authentication/authorization,
  external tenant identity, data-plane storage RPC authorization beyond the
  control-plane identity needed to issue routing/fencing decisions, and
  compatibility with pre-release artifacts that lack auth configuration.
