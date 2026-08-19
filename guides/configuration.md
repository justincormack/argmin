<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Configuration Guide

Argmin supports two configuration modes:

- environment-only configuration for embedded standalone deployments; and
- a versioned TOML cluster manifest for production-shaped standalone and
  replicated deployments.

The modes are deliberately separate. Cluster topology is never assembled by
merging a manifest with topology-related environment overrides.

Argmin is pre-release software and is not yet certified for production use.
The manifest describes the intended production configuration boundary, but it
does not change that release status.

## Environment-Only Configuration

Environment-only mode is selected when neither `ARGMIN_CLUSTER_CONFIG_PATH`
nor `ARGMIN_PROCESS_ID` is set. It is the shortest way to run an all-in-one
standalone server.

A standalone server is one `argmin-s3` operating-system process that accepts
S3 requests and embeds both the storage engine and the metadata/control
authority. It uses one local data directory, starts no separate cluster
processes, and provides no Argmin-managed node or disk redundancy.

### Standalone server settings

These settings configure that single standalone server. The account,
credential, and SSE-S3 wrapping-key settings are always required.

| Variable | Default | Description |
|---|---|---|
| `ARGMIN_ACCOUNT_ID` | *(required)* | 12-digit bucket-owner account ID used for ownership and `x-amz-expected-bucket-owner` checks |
| `ARGMIN_ACCESS_KEY_ID` | *(required)* | S3 access key |
| `ARGMIN_SECRET_ACCESS_KEY` | *(required)* | S3 secret key |
| `ARGMIN_SSE_S3_WRAPPING_KEY` | *(required)* | Base64-encoded 32-byte SSE-S3 wrapping key; new buckets default to `AES256` |
| `ARGMIN_SSE_C_VALIDATOR_KEY` | *(unset)* | Base64-encoded 32-byte SSE-C validator secret; SSE-C is rejected when unset |
| `ARGMIN_HOST_ID` | *(random at startup)* | S3 `HostId` / `x-amz-id-2`; set it to keep the value stable across restarts |
| `ARGMIN_REGION` | `us-east-1` | AWS signing region |
| `ARGMIN_DATA_DIR` | `./data` | Standalone data directory |
| `ARGMIN_LISTEN_ADDR` | `127.0.0.1:9000` | Public S3 listen address |
| `ARGMIN_TLS_CERT_PATH` | *(unset)* | PEM certificate chain for direct HTTPS |
| `ARGMIN_TLS_KEY_PATH` | *(unset)* | PEM private key for direct HTTPS |
| `ARGMIN_WORKERS` | `4` | Server worker count |
| `ARGMIN_MAX_CONNECTIONS` | `512` | Maximum concurrent public TCP connections |
| `ARGMIN_MAX_INFLIGHT_REQUESTS` | `32` | Maximum concurrent in-flight S3 requests |
| `ARGMIN_STREAM_READ_CHUNK_SIZE` | `8388608` | HTTP streaming read chunk size in bytes |
| `ARGMIN_TRACE` | `false` | Enable structured trace output |
| `ARGMIN_TRACE_FILTER` | *(unset)* | Comma-separated trace targets |
| `ARGMIN_TRACE_FILE` | *(unset; stderr)* | Trace output file |
| `ARGMIN_TRACE_SYNC` | `false` | Flush trace output synchronously |

`ARGMIN_TLS_CERT_PATH` and `ARGMIN_TLS_KEY_PATH` must either both be set or
both be unset. They configure the public S3 listener only. Internal TCP TLS is
configured through the cluster manifest.

Generate a wrapping or validator key with:

```bash
openssl rand -base64 32
```

The wrapping and validator keys are durable data dependencies, not disposable
startup tokens. Keep `ARGMIN_SSE_S3_WRAPPING_KEY` stable for existing SSE-S3
objects and `ARGMIN_SSE_C_VALIDATOR_KEY` stable for existing SSE-C objects.

Environment-only mode always runs one embedded storage node with 16 placement
groups, EC `1+0`, and the initial cluster epoch. It has no storage RPC or
control-plane endpoints. Topology and internal process settings are available
only through a cluster manifest.

### Standalone example

```bash
ARGMIN_ACCOUNT_ID=111122223333 \
ARGMIN_ACCESS_KEY_ID=admin \
ARGMIN_SECRET_ACCESS_KEY=useasecuresecretkey \
ARGMIN_SSE_S3_WRAPPING_KEY='<base64-encoded-32-byte-secret>' \
  ./target/release/argmin-s3
```

For direct HTTPS, add both public TLS paths:

```bash
ARGMIN_TLS_CERT_PATH=/path/to/cert.pem \
ARGMIN_TLS_KEY_PATH=/path/to/key.pem
```

## Static Cluster Manifest

Manifest mode is selected by setting both:

```text
ARGMIN_CLUSTER_CONFIG_PATH=/etc/argmin/cluster.toml
ARGMIN_PROCESS_ID=control-1
```

The manifest path must be absolute. Every cluster process receives identical
manifest contents, while `ARGMIN_PROCESS_ID` selects exactly one
`[[processes]]` record. The file is read once at startup; changing it requires
a process restart. The manifest must be a regular file, is opened without
following a final symlink, and is limited to 4 MiB.

The manifest is the complete process configuration. After the two environment
variables above select the file and process, no other environment variable
changes a manifest process. S3 credentials and encryption material use the
same permission-checked `file:` references as internal credentials and TLS
keys.

### Validation and initialization

Validate the complete selected-process configuration before startup:

```bash
argmin-s3 validate /etc/argmin/cluster.toml control-1
```

This performs structural, topology, selected-host filesystem, and material
validation in one operation. It opens every S3 and SSE secret, internal
credential, TLS identity, and trust bundle required by the selected process;
checks permissions and current credential windows; and verifies certificate
relationships. Success means the selected configuration has passed the same
configuration and material checks used by startup.

Initialize a selected process's durable state before its first startup:

```bash
ARGMIN_CLUSTER_CONFIG_PATH=/etc/argmin/cluster.toml \
ARGMIN_PROCESS_ID=control-1 \
  argmin-s3 initialize
```

For replicated storage processes this initializes the bound PG state. For a
replicated control-plane process it initializes the unestablished Raft identity
sidecar. In the current standalone manifest mapping it initializes only the
selected storage identity and PG state; it does not create the declared
single-authority `state_path`.

Run the selected process with:

```bash
ARGMIN_CLUSTER_CONFIG_PATH=/etc/argmin/cluster.toml \
ARGMIN_PROCESS_ID=control-1 \
  ./target/release/argmin-s3
```

Run validation and initialization separately for each stateful process id.
Initialization is identity-bound and idempotent only for the same complete
state. It is not a replacement ceremony: an established Raft voter whose
state has been lost must not be reinitialized under its old node id.

### Version 1 schema

The top-level manifest is closed and versioned:

| Field | Purpose |
|---|---|
| `schema_version` | Must be `1` |
| `[cluster]` | Stable cluster id, topology generation, and region |
| `[s3]` | Public S3 identity, secret references, and frontend resource policy |
| `[deployment]` | `standalone` or `replicated` and its failure-domain policy |
| `[storage]` | PG count, EC shape, and initial cluster epoch |
| `[raft]` | Append batching and snapshot limits |
| `[[transport_profiles]]` | Frame, connection, connect-timeout, and I/O-timeout limits |
| `[[hosts]]` | Stable host identities used by host failure-domain placement |
| `[[disks]]` | Host-owned mount roots |
| `[[processes]]` | Process placement and authenticated frontend/admin/maintenance identities |
| `[[authorities]]` | Single-authority or Raft-voter durable state |
| `[[storage_nodes]]` | Storage-node ids and durable data roots |
| `[[endpoints]]` | Prioritized Unix or TLS/TCP internal endpoints |
| `[[tls_identities]]` | Listener certificate and private-key references |
| `[[tls_trust_bundles]]` | Explicit accepted CA bundle references |
| `[[auth_credentials]]` | Protocol principal credentials and rotation windows |

Unknown keys, duplicate identities, unresolved references, invalid enum values,
and unsupported limits fail validation. Input arrays are unordered; Argmin
canonicalizes them by stable identity before deriving topology and process
digests.

Closed enum values are:

- `deployment.mode`: `standalone`, `replicated`
- `deployment.failure_domain`: `none`, `disk`, `host`
- `processes.kind`: `all-in-one`, `frontend`, `storage-node`, `control-plane`;
  `combined` is a reserved compatibility value and is not supported
- `authorities.kind`: `single`, `raft-voter`
- `endpoints.protocol`: `raft-peer`, `control-plane`,
  `authority-clock-recovery`, `storage-rpc`
- `auth_credentials.principal`: `raft-peer`, `storage-node`, `frontend`,
  `admin`, `maintenance`

Scalar sections use these fields:

| Section | Fields |
|---|---|
| `[cluster]` | `id` (string), `topology_generation` (nonzero integer), `region` (string) |
| `[s3]` | `account_id`, `access_key_id`, `secret_access_key_ref`, and `sse_s3_wrapping_key_ref`; optional `sse_c_validator_key_ref`, `workers`, `max_connections`, `max_inflight_requests`, and `stream_read_chunk_size` |
| `[deployment]` | `mode`, `failure_domain`, `failure_tolerance` (integer) |
| `[storage]` | `pg_count` (nonzero integer), `ec_data_shards`, `ec_parity_shards`, `initial_cluster_epoch` (nonzero integer) |
| `[raft]` | `max_append_entries`, `max_append_bytes`, `max_snapshot_bytes` |

Array records use these fields:

| Record | Required fields | Conditional fields |
|---|---|---|
| `[[transport_profiles]]` | `id`, `max_frame_bytes`, `max_connections`, `connect_timeout_ms`, `io_timeout_ms` | none |
| `[[hosts]]` | `id` | none |
| `[[disks]]` | `id`, `host_id`, `mount_path` | none |
| `[[processes]]` | `id`, `host_id`, `kind` | `s3_listen_addr` for frontend-capable processes; optional `s3_tls_identity_id`; `frontend_instance_id`, `admin_instance_id`, and `maintenance_instance_id` when the process performs those authenticated roles |
| `[[authorities]]` | `id`, `kind`, `process_id`, `disk_id`, `state_path` | `raft_node_id` for `raft-voter`; omitted for `single` |
| `[[storage_nodes]]` | `node_id`, `process_id`, `disk_id`, `data_dir` | none |
| `[[endpoints]]` | `id`, `owner_process_id`, `protocol`, `priority`, `listen`, `advertise`, `transport_profile_id` | `tls_identity_id`, `tls_trust_bundle_id`, and `tls_server_name` together for TCP; all omitted for Unix |
| `[[tls_identities]]` | `id`, `certificate_ref`, `private_key_ref` | none |
| `[[tls_trust_bundles]]` | `id`, `ca_bundle_ref` | none |
| `[[auth_credentials]]` | `principal`, `credential_id`, `credential_version`, `use_for_signing`, `accept_from_ms`, `secret_ref` | `node_id` for Raft/storage nodes or `instance_id` for frontend/admin/maintenance; optional `accept_until_ms` |

Within a complete manifest, numeric Raft and storage node IDs are opaque
unsigned values. Zero is valid, and IDs need not be one-based, contiguous, or
shared between the separate Raft and storage namespaces.

Every process may also set `trace_enabled`, `trace_filter`, `trace_file`, and
`trace_sync`. `trace_file`, when present, must be absolute. Advanced process
tuning fields are `storage_node_rpc_admission_limit`,
`storage_node_rpc_admission_wait_ms`,
`storage_node_rpc_control_admission_wait_ms`, `control_plane_lease_scan_ms`,
`control_plane_frontend_refresh_ms`, and
`control_plane_heartbeat_lease_ms`. Omitted settings use the compiled defaults.

The production `[s3]` schema defaults to 4 workers, 512 connections, 32
in-flight requests, and an 8 MiB stream-read chunk.

The process role matrix is exact:

- `all-in-one` contains the standalone frontend, storage node, and single
  authority;
- `frontend` contains one frontend;
- `storage-node` contains one storage node;
- `control-plane` contains one single authority or Raft voter.

Replicated frontend and storage roles may run on the same host, but use
separate processes. A replicated `combined` process is not a supported
version-1 topology; this preserves distinct credentials, capabilities,
admission limits, and restart boundaries. `all-in-one` remains exclusive to
standalone mode.

Standalone mode requires one host, one storage node, one `single` authority,
EC 1+0, `failure_domain = "none"`, and `failure_tolerance = 0`. Its local Unix
endpoint records describe the closed topology but are not activated by the
current embedded all-in-one runtime. It therefore has no internal RPC
authentication setting.

Replicated mode requires `failure_domain = "disk"` or `"host"`, a failure
tolerance of at least one, enough parity shards and distinct storage failure
domains for that tolerance, and at least `2 * failure_tolerance + 1` Raft
voters on distinct authority failure domains. It rejects `all-in-one`, requires
complete credentials for every configured internal role, and authenticates
every activated Unix or TLS/TCP internal endpoint. Authentication is derived
from replicated mode and cannot be disabled. Initial PG placement is calculated
and checked during validation.

Storage-node and Raft-voter counts are independent. For example, a four-host
EC 2+1 deployment with `failure_tolerance = 1` may run storage on all four
hosts while placing Raft authorities on only three. Four Raft voters are also
valid, but their quorum is three, so they still tolerate only one unavailable
voter; three voters need a quorum of two and provide the same stated failure
tolerance with less coordination. Five voters are required to tolerate two
unavailable voters.

#### Failure domains

`failure_domain` defines which declared topology unit must be independent for
placement and quorum validation:

| Value | Placement unit | Guarantee |
|---|---|---|
| `none` | No redundant domain | Standalone only; no disk or host failure is tolerated |
| `disk` | `[[disks]].id` | Each shard in a PG acting set is placed on a different declared disk, and Raft voter state uses distinct declared disks |
| `host` | `[[hosts]].id` | Each shard in a PG acting set is placed on a different declared host, and Raft voters run with durable state on distinct declared hosts |

With `failure_domain = "disk"`, multiple selected disks may belong to the same
host. The configured number of individual disk failures can be tolerated, but
loss of that host may remove several shards or voters at once and is not covered
by the declared guarantee. This mode is intended for a single-host multidisk
appliance or another deployment whose required boundary is the disk.

With `failure_domain = "host"`, shards and voter state are separated across
hosts. This is the multihost setting and covers loss of up to
`failure_tolerance` selected hosts, subject to the configured EC and Raft
limits.

For either replicated setting, each PG has `ec_data_shards + ec_parity_shards`
shards in distinct selected domains. `ec_parity_shards` must be at least
`failure_tolerance`, and the authority set must contain at least
`2 * failure_tolerance + 1` voters in distinct selected domains. Disk and host
IDs are the manifest's policy identities; operators must map them to the
intended independent physical disks and machines.

Version 1 does not declare rack or zone topology. `failure_domain = "host"`
therefore claims host-loss tolerance only, not tolerance of a rack or zone
failure containing multiple configured hosts.

Version 1 has no per-node PG list. The manifest defines eligible storage nodes
and deterministic initial placement. Current acting sets and later placement
changes are control-plane state.

In version 1, `failure_domain` is applied during manifest validation and
certified initial placement only. It is not yet stored as an active
control-plane policy or rechecked for later acting-set changes; persistent
enforcement is part of the planned dynamic-topology work.

### Endpoint and transport rules

Endpoint URIs use one of these forms:

```text
unix:///absolute/path
tcp://host:port
tcp://[ipv6-address]:port
```

`listen` is the local bind address and `advertise` is the address clients use.
TCP listen hosts must be literal IP addresses and may be wildcard addresses;
advertised addresses must not be wildcard addresses and may use DNS names.
Unix paths must be absolute.

`priority` is nonzero and unique for an owner process and protocol. Lower
values are preferred. Multiple candidates can provide a local Unix route and a
TLS/TCP fallback; cross-host clients use eligible TCP candidates.

`raft-peer`, `control-plane`, and `authority-clock-recovery` endpoints are
server listeners owned only by processes that host an authority. They are not
per-process client declarations. Frontend and storage processes automatically
derive their control-plane client routes from the authorities' advertised
endpoints, so a storage-only host must not declare its own control-plane or
Raft endpoints. A `storage-rpc` endpoint is instead owned by each process that
hosts a storage node.

Every TCP endpoint requires `tls_identity_id`, `tls_trust_bundle_id`, and
`tls_server_name`. TLS provides confidentiality and server authentication.
Signed protocol envelopes still authenticate and authorize the calling Raft,
storage, frontend, admin, or maintenance principal. Version 1 does not use
client TLS certificates and has no plaintext TCP mode.

Transport limits must satisfy the protocol's compiled compatibility bounds.
They cannot raise hard allocation ceilings or describe a frame too small for
the configured maximum request, Raft append batch, membership, or snapshot.

### Secret and TLS references

Version 1 accepts only absolute file references:

```toml
secret_ref = "file:/run/argmin-secrets/storage-1-v1.key"
secret_access_key_ref = "file:/run/argmin-secrets/s3-secret-access-key"
sse_s3_wrapping_key_ref = "file:/run/argmin-secrets/sse-s3-wrapping-key"
certificate_ref = "file:/run/argmin-secrets/host-1.crt"
private_key_ref = "file:/run/argmin-secrets/host-1.key"
ca_bundle_ref = "file:/run/argmin-secrets/cluster-ca.crt"
```

Raw secrets and private keys do not belong in the manifest. Files are opened
without following symlinks and are size-bounded before allocation. Every
referenced material file, including certificates and CA bundles, must be owned
by the effective service user. Private credential and key files must also be
inaccessible to group and other users. Certificate and CA files may be group-
or other-readable but must not be group- or other-writable.

Internal credential files are limited to 64 bytes, other secret files to 4
KiB, private keys to 64 KiB, and certificate chains and trust bundles to 1 MiB
each. One selected process may resolve at most 256 material files and 16 MiB in
total.

#### Internal authentication credentials

`[[auth_credentials]]` entries are symmetric HMAC-SHA256 credentials used to
authenticate internal Raft, control-plane, and storage RPC messages. They are
not public S3 access credentials, the SSE-S3 wrapping key, or the SSE-C
validator key. Generate independent secret material for each internal
credential; do not reuse any of those other keys.

The `principal` and its `node_id` or `instance_id` identify the internal caller.
`credential_id` and the nonzero `credential_version` identify a particular
secret during verification and rotation. An active credential is accepted for
verification; `use_for_signing = true` also allows its principal to sign new
messages. `accept_from_ms` is inclusive and `accept_until_ms`, when present, is
exclusive. Both are Unix timestamps in milliseconds. Each required local
principal must have exactly one signing credential active when its process
starts; overlapping verify-only credentials allow a controlled rotation.

Like the SSE-S3 wrapping and SSE-C validator key files, the referenced
credential file contains a standard base64 encoding of exactly 32 random
bytes. On a protected provisioning machine, generate one file per credential
with:

```bash
umask 077
openssl rand -base64 32 > raft-1.key
```

Repeat this with a distinct output file for every credential in the manifest.
Install the same file, owned by the Argmin service user and with mode `0600`,
on the process that signs as that principal and on every process that must
verify it. `validate` checks that the selected process can read and decode all
credential files it needs.

Certificate files may contain only certificates; private-key files must
contain exactly one supported private key. Trust anchors must be CA
certificates with signing usage. Argmin uses only the configured trust bundle,
not ambient operating-system roots.

An internal private CA is the normal convenient deployment model, but a
deliberately selected public-PKI root bundle is also supported. CA private keys
are provisioning material and must not be installed on Argmin hosts.

#### Private CA and host certificate example

The following OpenSSL 3 commands create a private P-256 root CA and one
server certificate per host for the replicated example below. Run them on a
protected provisioning machine, not an Argmin host:

```bash
mkdir -m 0700 argmin-internal-pki
cd argmin-internal-pki
umask 077

openssl genpkey \
  -algorithm EC \
  -aes-256-cbc \
  -pkeyopt ec_paramgen_curve:P-256 \
  -out cluster-ca.key

openssl req -new -x509 \
  -key cluster-ca.key \
  -sha256 \
  -days 3650 \
  -subj '/CN=Argmin Internal Root CA' \
  -addext 'basicConstraints=critical,CA:TRUE,pathlen:0' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' \
  -addext 'subjectKeyIdentifier=hash' \
  -out cluster-ca.crt
```

`openssl genpkey` prompts for a passphrase. Use a strong passphrase stored
separately from the encrypted CA key and its backups. Keep the CA key offline
except while issuing certificates. A managed HSM or an existing offline PKI is
preferred where available; filesystem mode and directory ownership alone are
not sufficient protection for a long-lived root key.

Issue the three host certificates. Each certificate includes both internal DNS
names advertised by that host's control-plane and storage endpoints:

```bash
for number in 1 2 3; do
  openssl genpkey \
    -algorithm EC \
    -pkeyopt ec_paramgen_curve:P-256 \
    -out "host-${number}.key"

  openssl req -new \
    -key "host-${number}.key" \
    -subj "/CN=control-${number}.internal" \
    -addext 'basicConstraints=critical,CA:FALSE' \
    -addext 'keyUsage=critical,digitalSignature' \
    -addext 'extendedKeyUsage=serverAuth' \
    -addext "subjectAltName=DNS:control-${number}.internal,DNS:storage-${number}.internal" \
    -out "host-${number}.csr"

  openssl x509 -req \
    -in "host-${number}.csr" \
    -CA cluster-ca.crt \
    -CAkey cluster-ca.key \
    -set_serial "0x$(openssl rand -hex 16)" \
    -sha256 \
    -days 397 \
    -copy_extensions copy \
    -out "host-${number}.crt"

  rm "host-${number}.csr"
done
```

The SAN entries, rather than the certificate common name, establish endpoint
identity. Every TCP endpoint's advertised host and `tls_server_name` must match
one SAN in its selected certificate. For an advertised IP address, use an
`IP:192.0.2.10` SAN instead of `DNS:...`.

Verify the issued files before deployment:

```bash
for number in 1 2 3; do
  openssl verify \
    -CAfile cluster-ca.crt \
    -purpose sslserver \
    -verify_hostname "control-${number}.internal" \
    "host-${number}.crt"
  openssl verify \
    -CAfile cluster-ca.crt \
    -purpose sslserver \
    -verify_hostname "storage-${number}.internal" \
    "host-${number}.crt"
  openssl x509 \
    -in "host-${number}.crt" \
    -noout \
    -subject \
    -issuer \
    -dates \
    -ext subjectAltName
done
```

For an IP endpoint, perform the corresponding identity check with
`-verify_ip 192.0.2.10` instead of `-verify_hostname`.

Install the public root and the applicable host identity as files owned by the
service user. Replace `argmin:argmin` with the actual service account:

```bash
sudo install -d -o argmin -g argmin -m 0700 /etc/argmin/tls
sudo install -o argmin -g argmin -m 0644 \
  cluster-ca.crt /etc/argmin/tls/cluster-ca.crt
sudo install -o argmin -g argmin -m 0644 \
  host-1.crt /etc/argmin/tls/host-1.crt
sudo install -o argmin -g argmin -m 0600 \
  host-1.key /etc/argmin/tls/host-1.key
```

Install `host-2.crt` and `host-2.key` only on host 2, and the corresponding
host 3 files only on host 3. Never deploy `cluster-ca.key`. If an existing PKI
issues the certificates, require the same SANs, `CA:FALSE`, digital-signature
key usage, and TLS server-auth extended key usage. Every certificate placed in
an Argmin trust bundle must have a critical `basicConstraints` extension with
`CA:TRUE` and a critical `keyUsage` extension containing `keyCertSign`; Argmin
rejects a bundle that relies only on ordinary chain validation. A certificate
file may contain the leaf followed by intermediate CA certificates; the
configured trust bundle contains the trusted CA certificates. Version 1 reads
material at startup, so certificate renewal requires a controlled process
restart.

Finally, run validation for every process identity on its selected host. This
catches incompatible keys, expired or not-yet-valid certificates, untrusted
chains, invalid CA constraints, and SAN mismatches before startup:

```bash
argmin-s3 validate \
  /etc/argmin/cluster.toml control-1
argmin-s3 validate \
  /etc/argmin/cluster.toml storage-1
argmin-s3 validate \
  /etc/argmin/cluster.toml frontend-1
```

### Filesystem and durable identity

Authority state paths and storage data directories must be absolute,
non-overlapping, and contained by the referenced disk mount on that host.
Filesystem existence, ownership, permissions, and mount boundaries are checked
only for the selected process's host; remote paths are validated lexically.

A `[[disks]].mount_path` accepts either:

- `/`, when the deployment intentionally stores data on the host's root
  filesystem; or
- the exact root of a separately mounted filesystem, such as
  `/mnt/argmin-disk-1` when a device is mounted at that path.

An ordinary directory is not a valid non-root `mount_path`. For example, if
`/var/lib/argmin` is merely a directory on the root filesystem, declare
`mount_path = "/"` and place the storage node beneath it with a path such as
`data_dir = "/var/lib/argmin/storage-1"`. If a filesystem is mounted at
`/mnt/argmin-disk-1`, declare that exact path as `mount_path` and use a child
such as `/mnt/argmin-disk-1/storage-1` as `data_dir`.

An authority's `state_path` and a storage node's `data_dir` must be on the same
filesystem as their referenced `mount_path`; they cannot pass through a nested
mount onto another filesystem. Disk layouts may differ between hosts, so each
host's disk records should use that host's actual mount points rather than a
path copied unchanged from another machine.

On Linux, check a proposed non-root mount path before validation with:

```bash
findmnt --mountpoint /mnt/argmin-disk-1
```

This exact-mount check prevents a missing disk mount from silently redirecting
durable writes into an unmounted leftover directory on its parent filesystem.
Directories on one filesystem must not be declared as separate disks or used
to claim independent `disk` failure domains.

The manifest does not infer whether a filesystem is persistent. Deployment
and release policy must make that guarantee explicitly; tmpfs runs are useful
for functional tests but are not durability evidence.

State and PG databases are bound to cluster id, topology generation, topology
digest, selected process id, and process-identity digest. Relocating state
requires the complete matching state and identity evidence. Pointing an
established process id at an empty destination fails closed.

### Replicated manifest example

This complete example describes three hosts, EC 2+1, three Raft voters, three
storage nodes, and one frontend. It uses arrays of inline TOML tables to keep
the repeated records compact; these are equivalent to the `[[record]]` form.
Replace the DNS names, paths, and material files for the deployment. Each host
certificate must cover both advertised names for that host.

```toml
schema_version = 1

transport_profiles = [
  { id = "raft", max_frame_bytes = 16777216, max_connections = 256, connect_timeout_ms = 1000, io_timeout_ms = 5000 },
  { id = "control", max_frame_bytes = 8388648, max_connections = 256, connect_timeout_ms = 1000, io_timeout_ms = 15000 },
  { id = "storage", max_frame_bytes = 67108864, max_connections = 256, connect_timeout_ms = 1000, io_timeout_ms = 5000 },
]

hosts = [
  { id = "host-1" },
  { id = "host-2" },
  { id = "host-3" },
]

disks = [
  { id = "disk-1", host_id = "host-1", mount_path = "/srv/argmin" },
  { id = "disk-2", host_id = "host-2", mount_path = "/srv/argmin" },
  { id = "disk-3", host_id = "host-3", mount_path = "/srv/argmin" },
]

processes = [
  { id = "control-1", host_id = "host-1", kind = "control-plane", admin_instance_id = "control-1-admin" },
  { id = "control-2", host_id = "host-2", kind = "control-plane", admin_instance_id = "control-2-admin" },
  { id = "control-3", host_id = "host-3", kind = "control-plane", admin_instance_id = "control-3-admin" },
  { id = "storage-1", host_id = "host-1", kind = "storage-node" },
  { id = "storage-2", host_id = "host-2", kind = "storage-node" },
  { id = "storage-3", host_id = "host-3", kind = "storage-node" },
  { id = "frontend-1", host_id = "host-1", kind = "frontend", frontend_instance_id = "frontend-1", admin_instance_id = "frontend-1-admin", maintenance_instance_id = "frontend-1-maintenance", s3_listen_addr = "0.0.0.0:9000" },
]

authorities = [
  { id = "authority-1", kind = "raft-voter", raft_node_id = 101, process_id = "control-1", disk_id = "disk-1", state_path = "/srv/argmin/control/control.state" },
  { id = "authority-2", kind = "raft-voter", raft_node_id = 102, process_id = "control-2", disk_id = "disk-2", state_path = "/srv/argmin/control/control.state" },
  { id = "authority-3", kind = "raft-voter", raft_node_id = 103, process_id = "control-3", disk_id = "disk-3", state_path = "/srv/argmin/control/control.state" },
]

storage_nodes = [
  { node_id = 1, process_id = "storage-1", disk_id = "disk-1", data_dir = "/srv/argmin/data" },
  { node_id = 2, process_id = "storage-2", disk_id = "disk-2", data_dir = "/srv/argmin/data" },
  { node_id = 3, process_id = "storage-3", disk_id = "disk-3", data_dir = "/srv/argmin/data" },
]

endpoints = [
  { id = "raft-1", owner_process_id = "control-1", protocol = "raft-peer", priority = 10, listen = "tcp://0.0.0.0:7401", advertise = "tcp://control-1.internal:7401", transport_profile_id = "raft", tls_identity_id = "host-1-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "control-1.internal" },
  { id = "raft-2", owner_process_id = "control-2", protocol = "raft-peer", priority = 10, listen = "tcp://0.0.0.0:7401", advertise = "tcp://control-2.internal:7401", transport_profile_id = "raft", tls_identity_id = "host-2-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "control-2.internal" },
  { id = "raft-3", owner_process_id = "control-3", protocol = "raft-peer", priority = 10, listen = "tcp://0.0.0.0:7401", advertise = "tcp://control-3.internal:7401", transport_profile_id = "raft", tls_identity_id = "host-3-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "control-3.internal" },
  { id = "control-1", owner_process_id = "control-1", protocol = "control-plane", priority = 10, listen = "tcp://0.0.0.0:7501", advertise = "tcp://control-1.internal:7501", transport_profile_id = "control", tls_identity_id = "host-1-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "control-1.internal" },
  { id = "control-2", owner_process_id = "control-2", protocol = "control-plane", priority = 10, listen = "tcp://0.0.0.0:7501", advertise = "tcp://control-2.internal:7501", transport_profile_id = "control", tls_identity_id = "host-2-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "control-2.internal" },
  { id = "control-3", owner_process_id = "control-3", protocol = "control-plane", priority = 10, listen = "tcp://0.0.0.0:7501", advertise = "tcp://control-3.internal:7501", transport_profile_id = "control", tls_identity_id = "host-3-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "control-3.internal" },
  { id = "clock-1", owner_process_id = "control-1", protocol = "authority-clock-recovery", priority = 10, listen = "tcp://0.0.0.0:7601", advertise = "tcp://control-1.internal:7601", transport_profile_id = "control", tls_identity_id = "host-1-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "control-1.internal" },
  { id = "clock-2", owner_process_id = "control-2", protocol = "authority-clock-recovery", priority = 10, listen = "tcp://0.0.0.0:7601", advertise = "tcp://control-2.internal:7601", transport_profile_id = "control", tls_identity_id = "host-2-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "control-2.internal" },
  { id = "clock-3", owner_process_id = "control-3", protocol = "authority-clock-recovery", priority = 10, listen = "tcp://0.0.0.0:7601", advertise = "tcp://control-3.internal:7601", transport_profile_id = "control", tls_identity_id = "host-3-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "control-3.internal" },
  { id = "storage-1", owner_process_id = "storage-1", protocol = "storage-rpc", priority = 10, listen = "tcp://0.0.0.0:7701", advertise = "tcp://storage-1.internal:7701", transport_profile_id = "storage", tls_identity_id = "host-1-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "storage-1.internal" },
  { id = "storage-2", owner_process_id = "storage-2", protocol = "storage-rpc", priority = 10, listen = "tcp://0.0.0.0:7701", advertise = "tcp://storage-2.internal:7701", transport_profile_id = "storage", tls_identity_id = "host-2-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "storage-2.internal" },
  { id = "storage-3", owner_process_id = "storage-3", protocol = "storage-rpc", priority = 10, listen = "tcp://0.0.0.0:7701", advertise = "tcp://storage-3.internal:7701", transport_profile_id = "storage", tls_identity_id = "host-3-internal", tls_trust_bundle_id = "cluster-ca", tls_server_name = "storage-3.internal" },
]

tls_identities = [
  { id = "host-1-internal", certificate_ref = "file:/etc/argmin/tls/host-1.crt", private_key_ref = "file:/etc/argmin/tls/host-1.key" },
  { id = "host-2-internal", certificate_ref = "file:/etc/argmin/tls/host-2.crt", private_key_ref = "file:/etc/argmin/tls/host-2.key" },
  { id = "host-3-internal", certificate_ref = "file:/etc/argmin/tls/host-3.crt", private_key_ref = "file:/etc/argmin/tls/host-3.key" },
]

tls_trust_bundles = [
  { id = "cluster-ca", ca_bundle_ref = "file:/etc/argmin/tls/cluster-ca.crt" },
]

auth_credentials = [
  { principal = "raft-peer", node_id = 101, credential_id = "raft-1", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/raft-1.key" },
  { principal = "raft-peer", node_id = 102, credential_id = "raft-2", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/raft-2.key" },
  { principal = "raft-peer", node_id = 103, credential_id = "raft-3", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/raft-3.key" },
  { principal = "storage-node", node_id = 1, credential_id = "storage-1", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/storage-1.key" },
  { principal = "storage-node", node_id = 2, credential_id = "storage-2", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/storage-2.key" },
  { principal = "storage-node", node_id = 3, credential_id = "storage-3", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/storage-3.key" },
  { principal = "frontend", instance_id = "frontend-1", credential_id = "frontend-1", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/frontend-1.key" },
  { principal = "admin", instance_id = "frontend-1-admin", credential_id = "frontend-1-admin", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/frontend-1-admin.key" },
  { principal = "maintenance", instance_id = "frontend-1-maintenance", credential_id = "frontend-1-maintenance", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/frontend-1-maintenance.key" },
  { principal = "admin", instance_id = "control-1-admin", credential_id = "control-1-admin", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/control-1-admin.key" },
  { principal = "admin", instance_id = "control-2-admin", credential_id = "control-2-admin", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/control-2-admin.key" },
  { principal = "admin", instance_id = "control-3-admin", credential_id = "control-3-admin", credential_version = 1, use_for_signing = true, accept_from_ms = 0, secret_ref = "file:/etc/argmin/auth/control-3-admin.key" },
]

[s3]
account_id = "111122223333"
access_key_id = "admin"
secret_access_key_ref = "file:/etc/argmin/s3/secret-access-key"
sse_s3_wrapping_key_ref = "file:/etc/argmin/s3/sse-s3-wrapping-key"

[cluster]
id = "example-replicated"
topology_generation = 1
region = "us-east-1"

[deployment]
mode = "replicated"
failure_domain = "host"
failure_tolerance = 1

[storage]
pg_count = 116
ec_data_shards = 2
ec_parity_shards = 1
initial_cluster_epoch = 1

[raft]
max_append_entries = 64
max_append_bytes = 8388608
max_snapshot_bytes = 15728640
```

Install the same manifest and trust bundle on every participating host. Install
only the TLS private key for that host. Each process also needs its own signing
secret and the verification secrets for principals accepted by its listeners;
unrelated signing secrets should not be distributed to it. All material files
must satisfy the ownership and permission rules above.

Before first startup, validate and initialize every stateful process on its
selected host. For host 1, for example:

```bash
./target/release/argmin-s3 validate \
  /etc/argmin/cluster.toml control-1
ARGMIN_CLUSTER_CONFIG_PATH=/etc/argmin/cluster.toml \
ARGMIN_PROCESS_ID=control-1 \
  ./target/release/argmin-s3 initialize

./target/release/argmin-s3 validate \
  /etc/argmin/cluster.toml storage-1
ARGMIN_CLUSTER_CONFIG_PATH=/etc/argmin/cluster.toml \
ARGMIN_PROCESS_ID=storage-1 \
  ./target/release/argmin-s3 initialize

./target/release/argmin-s3 validate \
  /etc/argmin/cluster.toml frontend-1
```

Run every process with `ARGMIN_CLUSTER_CONFIG_PATH` and its own
`ARGMIN_PROCESS_ID`. Install the S3 secret, wrapping-key, and optional SSE-C
validator files only on frontend hosts; control-plane and storage-only
processes do not resolve them.
Start all three voters and storage nodes. Initial Raft establishment and a
storage node waiting on retryable control-plane availability do not impose an
aggregate deadline, so hosts may be brought up at different times. Permanent
configuration, authentication, and protocol errors still terminate startup.
