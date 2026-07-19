# Static Cluster Configuration Plan

Status: implementation in progress

Related plans:

- [multihost-transition-plan.md](multihost-transition-plan.md)
- [control-plane-auth-identity-plan.md](control-plane-auth-identity-plan.md)

## Purpose

Define the first production-shaped configuration-file contract for standalone
and replicated Argmin deployments. This contract is the foundation for
authenticated TCP control-plane transport and the first cross-host reference
workload. It does not introduce dynamic topology, online configuration reload,
Raft membership changes, or storage expansion.

The first implementation uses one versioned TOML cluster manifest. Every
process in a cluster receives identical manifest contents and selects one local
process record by id. Machine-local secrets are represented only by references;
raw secret material is not part of the manifest.

This shape is deliberate:

- every process validates the same cluster identity, voters, storage topology,
  endpoint identities, and authentication policy;
- a per-node file cannot silently disagree with its peers about membership or
  failure-domain guarantees;
- local state paths and bind addresses remain explicit without becoming
  replicated control-plane state;
- secret distribution can change independently of the topology schema; and
- one canonical topology digest can bind restart artifacts, peer policy, and
  the initial committed topology.

## Scope

The version 1 manifest covers:

- deployment mode and declared failure-domain guarantee;
- cluster identity and static topology generation;
- EC shape and PG count;
- hosts, disks, authority voters, storage nodes, and process topology;
- Unix and TCP internal listeners and advertised endpoints;
- transport limits needed by the existing bounded RPC policies;
- Raft peer, storage-node, frontend, admin, and maintenance principals;
- credential ids, versions, signing/verification state, rotation windows, and
  secret references;
- TLS identities and trust bundles for TCP confidentiality and server
  authentication;
- state/data paths and restart-identity binding; and
- deterministic topology, durable process-identity, and full-config
  fingerprints.

The version 1 manifest does not cover:

- S3 user/account credentials, bucket policy, or other user-facing data;
- raw symmetric keys, TLS private keys, or certificate contents;
- dynamic topology prepare/acknowledge/activation state;
- online config reload or automatic credential rotation;
- dynamic Raft membership or storage-node expansion;
- placement overrides for individual PGs;
- service discovery; or
- a general-purpose configuration override/merge language.

## File Selection And Precedence

The initial process interface is:

```text
ARGMIN_CLUSTER_CONFIG_PATH=/etc/argmin/cluster.toml
ARGMIN_PROCESS_ID=control-1
```

Both variables are required together. `ARGMIN_CLUSTER_CONFIG_PATH` must be an
absolute path. `ARGMIN_PROCESS_ID` selects exactly one `[[processes]]` record.

Configuration-file mode and legacy cluster env mode are separate input modes;
they are not merged. When `ARGMIN_CLUSTER_CONFIG_PATH` is present, startup
rejects cluster topology, internal transport, control-plane auth, Raft peer, and
storage-node identity env vars instead of applying ambiguous precedence. The
existing env-only path remains for standalone compatibility and focused tests
until its planned cleanup.

User-facing S3 credentials and narrowly local process settings that are outside
this schema may remain env-configured during the first slice. Test helpers may
construct manifests directly or write temporary files; there is no production
"accept arbitrary env overrides" escape hatch.

The file is read once at startup. Reload requires a process restart until an
explicit dynamic configuration protocol exists.

The schema/parser slice also exposes an offline structural validation command:

```text
argmin-s3 validate-cluster-config /etc/argmin/cluster.toml control-1
```

It performs the same bounded parse, reference, role, endpoint, authentication,
and deployment-policy validation as startup will use, and emits only redacted
cluster/process identity diagnostics. It does not resolve secret bytes, open
mutable state, or start listeners.

## Version 1 TOML Shape

The following example is a three-host replicated deployment with EC 2+1 and
three control-plane voters. Values are illustrative. Repeated host/disk/process,
authority, endpoint, and credential records are abbreviated; the parser test
fixture will contain the complete valid manifest.

```toml
schema_version = 1

[cluster]
id = "example-prod"
topology_generation = 1
region = "us-east-1"

[deployment]
mode = "replicated"
failure_domain = "host"
failure_tolerance = 1
internal_auth = "required"

[storage]
pg_count = 116
ec_data_shards = 2
ec_parity_shards = 1
initial_cluster_epoch = 1

[raft]
max_append_entries = 64
max_append_bytes = 8388608
max_snapshot_bytes = 15728640

[[transport_profiles]]
id = "control-plane"
max_frame_bytes = 8388608
max_connections = 256
connect_timeout_ms = 1000
io_timeout_ms = 5000

[[transport_profiles]]
id = "raft"
max_frame_bytes = 16777216
max_connections = 256
connect_timeout_ms = 1000
io_timeout_ms = 5000

[[transport_profiles]]
id = "storage"
max_frame_bytes = 67108864
max_connections = 1024
connect_timeout_ms = 1000
io_timeout_ms = 5000

[[hosts]]
id = "host-1"
zone = "zone-a"
rack = "rack-1"

[[hosts]]
id = "host-2"
zone = "zone-a"
rack = "rack-2"

[[hosts]]
id = "host-3"
zone = "zone-b"
rack = "rack-3"

[[disks]]
id = "host-1-control"
host_id = "host-1"
mount_path = "/srv/argmin/control"

[[disks]]
id = "host-1-data"
host_id = "host-1"
mount_path = "/srv/argmin/data"

# Equivalent control/data disk records exist for host-2 and host-3.

[[processes]]
id = "control-1"
host_id = "host-1"
kind = "control-plane"
admin_instance_id = "control-1-manager"

[[processes]]
id = "storage-1"
host_id = "host-1"
kind = "storage-node"

[[processes]]
id = "frontend-1"
host_id = "host-1"
kind = "frontend"
frontend_instance_id = "frontend-1"
admin_instance_id = "frontend-1-admin"

[[authorities]]
id = "authority-101"
kind = "raft-voter"
raft_node_id = 101
process_id = "control-1"
disk_id = "host-1-control"
state_path = "/srv/argmin/control/control-plane.state"

# Equivalent authority records exist for node ids 102 and 103.

[[storage_nodes]]
node_id = 1
process_id = "storage-1"
disk_id = "host-1-data"
data_dir = "/srv/argmin/data/node-1"

# Equivalent storage-node records exist for node ids 2 and 3.

[[endpoints]]
id = "raft-101"
owner_process_id = "control-1"
protocol = "raft-peer"
priority = 10
listen = "tcp://0.0.0.0:7401"
advertise = "tcp://control-1.internal:7401"
transport_profile_id = "raft"
tls_identity_id = "host-1-internal"
tls_trust_bundle_id = "cluster-internal"
tls_server_name = "control-1.internal"

[[endpoints]]
id = "control-101"
owner_process_id = "control-1"
protocol = "control-plane"
priority = 10
listen = "tcp://0.0.0.0:7501"
advertise = "tcp://control-1.internal:7501"
transport_profile_id = "control-plane"
tls_identity_id = "host-1-internal"
tls_trust_bundle_id = "cluster-internal"
tls_server_name = "control-1.internal"

[[endpoints]]
id = "clock-101"
owner_process_id = "control-1"
protocol = "authority-clock-recovery"
priority = 10
listen = "tcp://0.0.0.0:7601"
advertise = "tcp://control-1.internal:7601"
transport_profile_id = "control-plane"
tls_identity_id = "host-1-internal"
tls_trust_bundle_id = "cluster-internal"
tls_server_name = "control-1.internal"

[[endpoints]]
id = "storage-1"
owner_process_id = "storage-1"
protocol = "storage-rpc"
priority = 10
listen = "tcp://0.0.0.0:7701"
advertise = "tcp://storage-1.internal:7701"
transport_profile_id = "storage"
tls_identity_id = "host-1-internal"
tls_trust_bundle_id = "cluster-internal"
tls_server_name = "storage-1.internal"

[[tls_identities]]
id = "host-1-internal"
certificate_ref = "file:/run/argmin-secrets/host-1.crt"
private_key_ref = "file:/run/argmin-secrets/host-1.key"

[[tls_trust_bundles]]
id = "cluster-internal"
ca_bundle_ref = "file:/run/argmin-secrets/internal-ca.crt"

[[auth_credentials]]
principal = "raft-peer"
node_id = 101
credential_id = "raft-101"
credential_version = 1
use_for_signing = true
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/raft-101-v1.key"

[[auth_credentials]]
principal = "storage-node"
node_id = 1
credential_id = "storage-1"
credential_version = 1
use_for_signing = true
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/storage-1-v1.key"

[[auth_credentials]]
principal = "frontend"
instance_id = "frontend-1"
credential_id = "frontend-1"
credential_version = 1
use_for_signing = true
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/frontend-1-v1.key"

[[auth_credentials]]
principal = "admin"
instance_id = "control-1-manager"
credential_id = "control-1-manager"
credential_version = 1
use_for_signing = true
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/control-1-manager-v1.key"
```

Arrays are unordered input collections. Parsing canonicalizes them by their
stable identity fields; input order never changes a digest or runtime policy.
Unknown keys, duplicate keys, and unknown enum values fail closed.

## Closed Types

`deployment.mode` is:

- `standalone`; or
- `replicated`.

`deployment.failure_domain` is:

- `none`, valid only for standalone;
- `disk`; or
- `host`.

`deployment.internal_auth` is:

- `required`; or
- `disabled`, valid only for standalone when every internal endpoint is Unix.

`processes.kind` is:

- `all-in-one`;
- `frontend`;
- `storage-node`;
- `combined`; or
- `control-plane`.

This is process topology, not the deployment guarantee.

The role matrix is exact:

- `all-in-one` hosts the standalone frontend, storage node, and single
  authority;
- `frontend` hosts one frontend identity;
- `storage-node` hosts one storage-node identity;
- `combined` hosts one frontend and one storage-node identity; and
- `control-plane` hosts one single authority or Raft voter.

`frontend_instance_id`, `admin_instance_id`, and
`maintenance_instance_id` are present only when the process performs the
corresponding authenticated role. Replicated control-plane processes require an
admin instance for internal management operations.

`authorities.kind` is:

- `single`, valid only in standalone mode and omitting `raft_node_id` and
  Raft peer endpoints; or
- `raft-voter`, valid only in replicated mode and requiring a nonzero
  `raft_node_id` plus at least one Raft peer endpoint owned by its process.

Every authority has a stable string `id`. A Raft voter additionally has a
nonzero numeric `raft_node_id`. An embedded standalone authority still has an
authority record so its durable identity, state path, and optional separate
client/recovery endpoints are explicit.

`endpoints.protocol` is:

- `raft-peer`;
- `control-plane`;
- `authority-clock-recovery`; or
- `storage-rpc`.

Public S3 listeners remain outside the first internal cluster manifest.

Endpoint URIs are:

- `unix:///absolute/path`; or
- `tcp://host:port`, with bracketed IPv6 when applicable.

Unix endpoint paths must be absolute and normalize to unique paths. TCP
advertised hosts must not be wildcard or unspecified addresses. Bind endpoints
may use wildcard addresses. URI userinfo, query strings, and fragments are
rejected.

`auth_credentials.principal` is:

- `raft-peer`, requiring `node_id` that identifies an authority;
- `storage-node`, requiring `node_id` that identifies a storage node;
- `frontend`, requiring `instance_id`;
- `admin`, requiring `instance_id`; or
- `maintenance`, requiring `instance_id`.

Exactly one of `node_id` and `instance_id` is present according to the role.

The first secret-reference provider is `file:` followed by an absolute path.
Inline secret values are not valid. Test helpers may inject already-resolved
secrets without changing the file grammar. New providers require a schema
extension and an explicit resolver implementation; arbitrary URI schemes are
not silently accepted.

## Authentication And Rotation

Replicated mode derives `internal_auth = required`; specifying `disabled` is an
error. Whenever internal auth is required, every authority, storage node,
frontend, admin, and maintenance principal referenced by a local process must
have an accepted credential. Standalone local Unix may explicitly disable
internal auth. Every TCP endpoint requires authentication regardless of
deployment mode.

Credential identity is the tuple:

```text
(principal role, principal id, credential_id, credential_version)
```

That tuple is globally unique. Versions are nonzero. Credential ids and
principal ids are nonempty printable non-space ASCII with conservative length
bounds.

`accept_from_ms` and optional `accept_until_ms` are authority wall-clock
timestamps. `accept_until_ms` must be greater than `accept_from_ms`.
Verification accepts overlapping windows. `use_for_signing = true` marks a
credential eligible for local signing, but each principal must have exactly one
signing credential active at a given instant. A verify-only old credential may
overlap a new signing credential during rotation.

Version 1 is startup-static: a process evaluates the configured rotation state
when it starts and does not watch the file. Online rotation/reload remains a
later slice. Existing request-specific freshness and replay validation still
applies inside a credential's acceptance window.

Secret resolution occurs only after structural, identity, topology, and
reference validation succeeds. A process resolves only credentials and TLS
private material required by its roles plus verification credentials required
by its listeners. Errors and debug output contain the reference identity and
path only where operationally necessary; they never contain resolved bytes,
MACs, private keys, or payloads.

The file resolver must bound secret-file size before allocation, require a
regular file, reject empty content, and use platform-appropriate ownership and
permission checks. Exact deployment and rotation mechanics remain part of the
auth production-rollout slice.

## TLS Contract

TCP transport provides confidentiality and server authentication through
TLS. Symmetric control-plane/storage authentication remains the protocol-level
client/principal identity and authorization mechanism; possession of a TLS
connection alone grants no RPC authority.

The normal self-managed trust model uses an Argmin-cluster private CA.
Operators may create one offline root for the cluster and use it directly for
small deployments, or keep that root offline and issue through a private
intermediate CA. Public-PKI certificates are also valid: an operator may
configure a trust-bundle file containing the selected public roots and use a
publicly issued listener certificate. The schema and verification path are the
same in both cases.

CA signing private keys are provisioning material and must not be distributed
to Argmin processes. A process receives only:

- the private key and certificate chain for each TCP listener identity it
  serves; and
- an explicit trust bundle containing the CA certificates accepted for
  outbound internal connections.

The private-CA option makes internal certificate issuance independent of public
DNS and public certificate authorities. Both options keep accepted trust roots
explicit. Version 1 must not silently add operating-system root stores to the
configured trust bundle. A deployment that wants the host or distribution's
public roots references its CA-bundle file explicitly, so two hosts cannot
silently derive different trust policy from ambient defaults. Normal test
helpers issue from a private test CA.

Every TCP endpoint requires:

- a listener `tls_identity_id`;
- a client `tls_trust_bundle_id`; and
- a nonempty `tls_server_name` matching the advertised identity.

The listener certificate must contain the configured server name in its SAN,
must permit TLS server authentication, and must chain to the client's explicit
cluster trust bundle. TLS client certificates are not required in version 1:
the signed symmetric protocol envelope authenticates the calling Argmin
principal and authorizes its concrete operation. Moving that identity into
mTLS later must preserve the same cluster, principal, role, source/target,
operation, and freshness checks.

Unix endpoints must omit all TLS fields. TLS certificate, private-key, and CA
contents are referenced, not embedded. The first TCP implementation uses the
existing rustls stack and a deliberately closed protocol/version/cipher policy.
There is no plaintext or unauthenticated TCP mode.

Private-CA rotation uses an overlap: distribute a trust bundle containing old
and new CA certificates, roll listener certificates to the new issuer, then
remove the old CA after every configured endpoint has converged. Public-PKI
renewal follows the same leaf-certificate replacement path; public root changes
are adopted by deliberately updating the referenced bundle. Leaf-key and
certificate rotation does not change the topology digest. Online reload remains
out of scope for version 1, so each rotation stage requires controlled process
restarts.

## Topology Validation

All ids are unique within their namespace and all references resolve exactly.
Canonical ids have explicit length and ASCII bounds. Numeric ids and
generations are nonzero.

Every process references one host. Every authority and storage node references
one process and one disk on that process's host. State/data paths are absolute
and must be lexically contained by the referenced disk's normalized mount path.
Path uniqueness is scoped by `(host_id, normalized_path)`: identical Unix,
mount, state, or data paths on different hosts are valid. Every process parses
and lexically validates every path, but only the selected process's host is
checked against the local filesystem, canonical mount targets, ownership,
permissions, and device identity. Remote-host paths must not be required to
exist locally. One process hosts at most one authority and at most one storage
node in version 1. Its `kind` must permit those roles.

Version 1 has no `storage_nodes.pg_ids` or other static PG-ownership field.
Every configured storage node is eligible for deterministic initial placement,
subject to EC and failure-domain constraints. Actual PG acting sets, peering,
and later placement changes are replicated control-plane state rather than
machine-local configuration.

If later deployments need storage classes or constrained eligibility, a future
schema must define canonical pool/placement-policy inputs that participate in
the topology digest and committed topology transition protocol. It must not
introduce independently edited per-node PG lists that duplicate the
control-plane acting-set authority.

Every endpoint has one owner process. Configured authority
peer/client/recovery endpoints must be owned by that authority's process and
have the exact required protocol. A Raft voter always requires all three. An
embedded single authority may omit client/recovery endpoints; a separate
standalone control-plane process requires them. A storage endpoint must be
owned by that storage node's process and use `storage-rpc`.

An owner may expose multiple endpoints for one protocol, for example an
authenticated Unix endpoint for same-host clients and an authenticated TCP
endpoint for remote clients. `priority` is nonzero and unique within
`(owner_process_id, protocol)`; lower values are preferred. A client filters
out unusable transports first, then tries candidates by `(priority, endpoint
id)`. Endpoint-list input order has no semantic effect.

The canonical Raft membership peer map resolves one endpoint per voter that is
reachable from every configured voter before applying priority. If any voter is
on another host, that target's Unix candidates are ineligible and the preferred
TCP candidate is selected even when a Unix candidate has a lower priority
number. Production compatibility and encoded-membership capacity validation use
this resolved map; the validated model retains it for startup and later digest
construction.

For a Unix endpoint, every configured client process must share the endpoint
owner's host. Unix candidates are simply ineligible to clients on another host;
the owner must have a TCP candidate for every required cross-host relationship.
Any internal connection crossing a host boundary must use TCP. TCP listener and
advertised addresses must use the same port unless an explicit future proxy
model is added.

Transport profiles are uniquely named and bounded by hard-coded protocol
minimum compatibility requirements and allocation ceilings before allocation.
Version 1 accepts only limits supported by the corresponding existing protocol
implementation; a config value cannot silently raise a compile-time allocation
ceiling. The first implementation requires the live production replication
values of 64 append entries, 8 MiB encoded append payload, and a 16 MiB peer
frame, and invokes the storage authority's compatibility validator against the
resolved peer map for every Raft endpoint profile. A command accepted into the
leader log must fit the configured single-entry share, and the peer frame must
carry the complete configured batch. Every Raft peer frame limit must also carry
`max_snapshot_bytes` plus bounded snapshot metadata, voter endpoints,
cluster/topology identity, and authentication-envelope overhead. Equality
between the raw snapshot and frame limits is therefore invalid.

Standalone mode requires:

- exactly one host and one storage node;
- `failure_domain = none` and `failure_tolerance = 0`;
- EC 1+0;
- exactly one `single` authority and no `raft-voter` authority; and
- only local Unix internal endpoints unless authenticated TCP is explicitly
  configured.

Replicated mode requires:

- no `all-in-one` process; that topology is standalone-only;
- `failure_domain` of `disk` or `host`;
- `failure_tolerance >= 1`;
- `ec_parity_shards >= failure_tolerance`;
- at least `ec_data_shards + ec_parity_shards` eligible storage domains;
- every PG's deterministic initial acting set to occupy distinct selected
  failure domains;
- at least `2 * failure_tolerance + 1` `raft-voter` authorities and no
  `single` authority;
- voter durable state on distinct selected authority failure domains;
- mandatory authentication for every internal endpoint; and
- a complete credential set for all configured principals.

For `failure_domain = disk`, storage-node and authority disk ids establish the
selected domains. For `failure_domain = host`, their host ids establish the
selected domains. Operator labels do not prove physical independence by
themselves; deployment tooling and the dedicated-device release profile must
also attest the intended disk/host mapping.

The parser computes the deterministic initial placement using the same
production placement implementation and validates every PG before any listener,
state file, or data directory is opened. The validated model retains the
resulting acting sets so startup cannot substitute a different unverified
placement.

`cluster.id` is the stable cluster namespace, but it is not sufficient by
itself to authorize peer traffic. It replaces separate config-file notions of
an auth cluster id and a Raft cluster name. Scoped credentials and durable
identities derive their domain-separated cluster namespace from it.

Every authenticated Raft peer request and response additionally carries the
canonical topology generation and topology digest in its frame identity. Those
fields are covered by the existing symmetric MAC along with source, target,
operation, direction, and payload. A peer rejects a frame before OpenRaft
dispatch when either value differs from its resolved manifest, even when both
peers have fresh empty state paths. Endpoint selection does not downgrade this
check.

The initial bootstrap command and its resulting snapshot membership identity
carry the same topology generation/digest and exact canonical `raft-voter` set.
Bootstrap preflight and deterministic apply reject a mismatched digest,
generation, or voter set before publishing membership. Version 1 does not
configure learners. Later topology activation must replace this initial
binding through the separately planned committed topology-generation protocol;
editing a manifest is never topology activation.

## Canonical Digests And Durable Binding

Parsing produces two durable SHA-256 identity digests and one diagnostics-only
configuration fingerprint using versioned canonical binary encodings, not TOML
bytes or JSON serialization.

The **cluster topology digest** covers:

- schema version;
- cluster id and topology generation;
- deployment guarantee;
- EC/PG shape and initial cluster epoch;
- canonical host/disk failure-domain identities;
- process ids, kinds, and role/principal identities;
- authority and storage-node identities;
- advertised endpoint protocol and identity;
- Raft transport policy; and
- required principal identities and auth requirement mode.

It excludes:

- raw or resolved secrets;
- credential ids, versions, signing state, and rotation windows;
- secret-reference paths;
- TLS private-key and certificate-reference paths;
- local bind addresses;
- local state/data paths; and
- operational timeout values that do not change protocol compatibility.

The topology digest seeds the initial committed topology identity and binds
Raft restart artifacts, peer policy, snapshots, and control-plane runtime-map
topology generation. A reused state path with a different cluster id,
generation, or topology digest fails before constructing Raft or serving.
Fresh peers with no durable artifacts still prove the same digest through the
authenticated frame identity and bootstrap command; matching `cluster.id`
alone is not compatibility.

The **durable process-identity digest** covers the topology digest plus the
selected process id, host id, process kind, hosted authority/storage identities,
and their declared disk failure-domain ids. It binds local sidecars and detects
accidental reuse of one process's state/data directory by a different process
or role. It deliberately excludes paths, bind addresses, credentials, TLS
material, and operational limits so safe credential/certificate rotation,
timeout tuning, or state relocation does not invalidate durable state.

Path exclusion does not permit implicit reinitialization. Normal server startup
requires each configured durable authority/storage path to contain its complete
identity-bound state set and durable existence sentinel. An empty or partially
copied destination is rejected; the server never interprets it as a new
instance merely because the manifest still names the same node id.

Durable state has three explicit lifecycle operations:

1. **Initialize:** a one-shot initialization command creates the identity
   sentinel and initial state with exclusive creation and parent-directory
   durability. Ordinary replicated server startup cannot initialize it.
2. **Relocate:** stop the process and copy the complete state set, including
   sentinel, artifact/checkpoint, WAL/journal, clock evidence, and any
   generation sidecars. Startup at the new path accepts it only after existing
   restart, process-identity, topology-digest, and journal-continuity validation
   succeeds. A copied sentinel without its matching state is insufficient.
3. **Replace:** loss of the durable state set is node replacement, not path
   relocation. Replicated mode requires the committed membership/topology
   replacement ceremony and a fresh node identity; it must not recreate empty
   state under the lost Raft node id.

Standalone initialization may be exposed through the same command or an
explicit standalone-only first-start option, but it must write the same durable
sentinel. Once that sentinel has existed, missing state is a lost-state error,
not a fresh start. Exact command UX and copy tooling are implementation-slice
details; these fail-closed state transitions are normative.

The **full configuration fingerprint** is diagnostics-only. It covers the
complete unresolved canonical manifest, including local paths, bind addresses,
transport limits, credential metadata and reference identities, TLS reference
identities, and rotation windows, while still excluding resolved secret bytes.
It supports audit/comparison and change detection but is not persisted as a
condition for reopening authority or storage state.

Canonical encoding has explicit tags and lengths, sorted collections, no
platform-native integer representation, and test vectors. Any field that can
change topology, placement, protocol compatibility, principal identity, or a
durable ownership decision must be in the corresponding durable digest. Fields
that intentionally change through credential/TLS rotation or local operational
tuning remain in the full fingerprint instead.

## File And Parser Safety

Before allocation, startup atomically opens the manifest with no symlink
following and checks that the opened object is a regular file no larger than 4
MiB. UTF-8 is required. TOML unknown fields and duplicate keys are errors.
Parser diagnostics expose only a redacted category and byte location, never the
raw parser message, unknown field name, enum value, or quoted source excerpt.
The parser rejects:

- unsupported schema versions;
- missing required sections;
- duplicate ids or credential identities;
- dangling or role-incompatible references;
- relative paths;
- noncanonical/invalid endpoint URIs;
- invalid transport bounds;
- inline secret material;
- auth/TLS policy contradictions;
- a selected process absent from the manifest;
- a process whose local role requirements are incomplete; and
- any deployment/failure-domain invariant violation.

Validation completes before opening listeners or mutable state. Secret
resolution and filesystem canonicalization follow structural validation, then
durable identity checks run before authority construction.

Because the project is pre-release, version 1 does not include compatibility
migration. An unsupported version is rejected rather than guessed or partially
loaded.

## Implementation Slices

Progress as of 2026-07-19:

- Slice 1 is implemented: the manifest has strict version-1 TOML input types,
  a 4 MiB bounded atomic no-follow regular-file loader, closed enums and
  unknown-field rejection, canonical collection and endpoint ordering, an
  immutable validated model, parser-message and debug redaction,
  topology/reference/role/global-host-path/endpoint/auth/deployment validation,
  per-PG production-placer validation retained in the model, snapshot transport
  capacity validation including metadata/auth overhead, production Raft
  compatibility validation plus append identity/auth overhead, standalone and
  complete three-host replicated fixtures, and the offline validation command.
- Slice 2 is implemented: dedicated versioned canonical binary encoders produce
  a cluster topology digest, selected-process durable identity digest, and full
  unresolved-config fingerprint. Collections are identity-sorted, fields and
  collection elements are explicitly tagged and length-delimited, fixed digest
  vectors pin the encoding, and mutation tests distinguish topology/process
  identity from credential, TLS, path, bind-address, region, and timeout
  changes. The validation command emits the three redacted SHA-256 identities.
- Selected-host filesystem/device checks remain with secret/filesystem
  resolution rather than the structural parser. Slices 3 onward remain open:
  no startup `ServerConfig` mapping, env-mode exclusion, durable identity
  binding, secret resolution, or TCP transport is introduced by Slices 1-2.

1. **Schema types and parser**
   - add closed Rust input types with unknown-field rejection;
   - parse bounded TOML into an untrusted input model;
   - validate and resolve it into an immutable canonical model;
   - implement redacted `Debug`; and
   - add malformed, duplicate, unknown-field, bound, and reference tests.
2. **Canonical identity**
   - implement topology/process-identity canonical encoders and SHA-256
     digests plus the full-config fingerprint;
   - add order-independent test vectors and mutation sensitivity tests; and
   - expose redacted startup diagnostics.
3. **Legacy mapping**
   - map the resolved selected process into existing `ServerConfig`;
   - reject mixed file/env cluster configuration;
   - retain env-only standalone/test mode; and
   - run existing Unix process tests from shared manifest helpers.
4. **Durable binding**
   - bind cluster topology digest to Raft artifacts/peer policy and standalone
     state identity;
   - bind topology generation/digest to authenticated peer frames and the
     initial bootstrap command/membership;
   - bind durable process-identity digest to local state/data sidecars;
   - implement explicit initialization/relocation/replacement state lifecycle;
     and
   - add wrong-cluster, wrong-process, wrong-generation, changed-endpoint,
     empty-relocation, and incomplete-relocation restart tests.
5. **Secret/TLS resolution**
   - resolve bounded file references after validation;
   - construct existing scoped auth credentials and rustls identities;
   - enforce replicated and TCP mandatory-auth policy; and
   - prove diagnostics and errors remain redacted.
6. **Authenticated TCP control-plane transport**
   - add TCP Raft peer and control-plane listeners/clients using the same
     protocol/auth dispatch boundaries as Unix;
   - enforce endpoint source/target/cluster/topology identity before dispatch;
   - reject fresh-manifest peers whose topology generation or digest differs;
     and
   - run the first three-host authority workload.
7. **Storage RPC auth and TCP**
   - implement the transport-independent storage authorization slice over Unix;
   - reuse it unchanged over TCP; and
   - promote the cross-host workload to a complete data-plane release gate.

## Required Tests

The schema/parser release gate includes:

- minimal standalone and three-host replicated manifests;
- field-order independence and stable digest vectors;
- every unknown enum and unknown field;
- duplicate ids, paths, endpoints, credential identities, and references;
- path uniqueness scoped by host, repeated remote-host paths, and selected-host
  filesystem validation without remote filesystem probing;
- authority state and storage data path collisions across roles on one host;
- selected process missing or incompatible with its hosted role;
- mixed file/env mode rejection;
- oversized, non-UTF-8, truncated, and malformed TOML;
- secret, unknown-field-name, unknown-enum-value, parser-message, and debug
  redaction;
- relative/escaping state and data paths;
- symlinked manifest rejection at the open boundary;
- noncanonical Unix and TCP endpoint URI rejection;
- lower-priority-number local Unix Raft candidate plus globally reachable TCP
  fallback resolves to TCP in a multihost peer map;
- Unix endpoint referenced across hosts;
- TCP without auth or TLS;
- replicated auth disabled;
- standalone EC other than 1+0;
- replicated `m < f`, insufficient storage domains, and insufficient voters;
- replicated all-in-one process rejection;
- voter/storage placement sharing a prohibited failure domain;
- deterministic production placement of every configured PG;
- command/frame policy incompatibility;
- snapshot/frame incompatibility after metadata and authentication overhead;
- topology digest mutation for every topology/protocol/principal-identity
  field;
- no topology or process-identity digest mutation for credential rotation,
  secret/TLS-reference paths, bind-only changes, timeout tuning, or state-path
  relocation;
- full configuration fingerprint mutation for those operational changes;
- process-identity digest mutation for process, hosted role/node, host, or disk
  identity changes;
- wrong cluster/process/generation/digest restart rejection;
- matching cluster id with mismatched fresh-peer topology digest/generation in
  request, response, and bootstrap frames, rejected before OpenRaft dispatch;
- unsupported `pg_ids` or other static per-node PG ownership rejected as an
  unknown field;
- complete offline state relocation accepted, but empty destination,
  sentinel-only destination, incomplete artifact/WAL copy, and lost-state
  same-node-id restart rejected; and
- identical manifests selecting different processes while retaining one
  cluster topology digest and distinct durable process-identity digests.

No TCP endpoint becomes supported until the parser, canonical digest, mandatory
auth/TLS validation, durable binding, and negative tests for that endpoint
family are in place.
