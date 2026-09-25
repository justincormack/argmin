<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Durable storage and crash consistency

Status: normative for new persistence work and current as an inventory. An
entry marked as a known gap is not an approved durability pattern and must not
be copied.

This guide answers four questions for every filesystem artifact owned by
Argmin:

1. Where is it stored?
2. Which state is authoritative after a crash?
3. At what point may an operation acknowledge durable success or publish a
   reference to it?
4. Which intermediate states may restart recovery observe, and what must it do
   with them?

The exact encodings, versions, and immutable evidence are maintained in the
[internal storage format ledger](storage-format-ledger.md). The rules for
changing those encodings are in the [versioning guide](versioning.md). This
guide is about physical persistence, ordering, acknowledgement, and recovery;
it does not duplicate the byte-level format ledger.

## Scope and assumptions

The inventory covers files and databases written by the server. It does not
turn operator-supplied configuration, TLS keys, executable files, logs, or
external backup products into server-owned durable state. Unix sockets and
exclusive lock files are process-lifetime coordination even when their
directory entries remain after a crash.

The operating system, filesystem, mount configuration, and storage device are
trusted to implement successful `fsync`, `fdatasync`, atomic rename, and SQLite
durability correctly. A successful write or close without the required sync is
not evidence of power-loss durability. Tests on tmpfs establish logical crash
recovery but are not evidence that a real filesystem honors these operations.

There is currently no supported upgrade, downgrade, or mixed-format cluster.
Unsupported durable formats fail closed. Recovery must not reinterpret an
unsupported format as an empty store.

## Terminology

**Written** means bytes have been passed to the operating system. They may
still exist only in volatile caches.

**Durable** means the file contents and every directory entry needed to reach
them have crossed their required persistence barriers.

**Published** means another durable or externally visible state is permitted
to reference the artifact. Publication may occur only after the referenced
artifact is durable.

**Authoritative state** is required to reconstruct the logical state after a
crash. Losing it is data or availability loss, not ordinary cleanup.

**Durable staging state** is not yet S3-visible, but is authoritative for
restart convergence or cleanup. It must remain until ownership is atomically
transferred or retired.

**Recoverable residue** may safely survive or disappear across a crash because
another durable owner determines whether it is live. Recovery or scavenging
must handle it idempotently.

**Ephemeral state** is never restart authority. Its presence after a crash
does not make it valid.

An operation's **durable acknowledgement boundary** is the last persistence
barrier that must succeed before the operation may report durable success or
allow a dependent publication.

## Global rules

### Ownership and inventory

- Every persistent artifact has one owning crate and one authoritative writer.
- Adding a file, database, journal, marker, durable directory layout, or
  durable sidecar requires updating this inventory and the format ledger in
  the same change.
- A new artifact must be classified as authoritative state, durable staging,
  recoverable residue, or ephemeral state. “Temporary” is not a durability
  classification.
- Raw paths, bytes, schemas, and recovery mechanics remain private to the
  owning crate. Other crates use typed values and capabilities.

### File creation and replacement

Unless a stronger owner-specific protocol is recorded below, publishing a new
or replacement file requires:

1. create a regular staging file without following symlinks; its name must be
   unique, or exclusively owned and restart-classified by the surrounding
   protocol;
2. write the complete, bounded, integrity-protected current format;
3. call `sync_all` or `sync_data` as required by the format;
4. atomically publish the final name, normally by rename or, for
   create-if-absent publication, by a same-filesystem hard link;
5. sync the final file's parent directory; and
6. sync each newly created ancestor directory before relying on a descendant.

The file sync makes content durable. The directory sync makes the name and
rename durable. One is not a substitute for the other. A temporary file in a
different filesystem is invalid because rename would not be atomic.

When publication moves or links a file between directories, the destination
directory sync establishes the final name. If removal of the source name must
also survive power loss, the source directory must be synced separately. A
protocol may instead classify a resurrected source name as recoverable residue,
provided restart removes it and it can never become publication authority.

Replacing an established identity, checkpoint, or manifest must preserve a
restart-safe interpretation at every crash point. If removing the old file is
part of a security or authority transition, its directory removal must become
durable before a replacement can authorize serving.

### Deletion

An acknowledged deletion that must survive power loss requires unlinking the
file and syncing its parent directory. It may be safe not to sync an unlink
only when all of the following are explicit:

- the file is recoverable residue rather than authority;
- durable metadata already makes the resurrected file unreachable;
- restart scanning safely recognizes and removes the residue; and
- no name reuse can make the old bytes satisfy a new logical identity.

That exception is an owner-specific recovery protocol, not a general
performance optimization.

### SQLite

- Production SQLite databases use a documented journal mode and synchronous
  level. Connection-local pragmas must be set and verified by the owner.
- A transaction commit is the durability boundary only under those configured
  pragmas. Code must not infer durability from statement success before
  commit.
- Schema creation must make both the database and containing directory durable
  before publishing the store as initialized.
- The complete physical catalogue is versioned. Any table, column, index,
  trigger, constraint, view, or foreign-key change advances the schema version
  and appends immutable format evidence.
- SQLite WAL, SHM, rollback-journal, and temporary files are owned companions
  of the database. They are not independent backup units.

### Ordering across stores

There is no atomic transaction spanning SQLite, loose shard files, another PG,
or a control-plane store. Cross-store workflows therefore require a durable
state machine:

- make payload durable before publishing metadata that references it;
- persist a cleanup or recovery owner before relinquishing the live owner;
- never clear the old durable owner before the new owner is irrevocable;
- treat a timeout or transport failure after a possible effect as ambiguous;
  reconcile exact identity rather than deleting or retrying blindly; and
- retain all routes, proofs, reservations, and identities needed for restart
  convergence and cleanup.

Partial success may leave recoverable residue, but must not leave visible
metadata referring to non-durable bytes. Replica apply must remain
deterministic and must not depend on live reads from another persistence
domain.

### Acknowledgement and failure

- Public success, internal `WriteAck`, and command-apply success must each have
  a documented durability meaning.
- Failure before the first irreversible effect is definitive. Failure after a
  write, sync, rename, transaction commit, or remote dispatch may require a
  typed “may have applied” result.
- A persistence failure cannot be downgraded to best-effort success merely
  because later recovery might repair it.
- Once durable authority is uncertain, fail closed until exact on-disk or
  quorum evidence resolves it.

### Capacity exhaustion and recovery liveness

Disk byte exhaustion, inode exhaustion, quotas, and filesystem-imposed file
limits are correctness events, not merely performance degradation. A full
durability domain must reject new capacity-consuming work before it consumes
space required to converge already-authorized work.

Each persistence domain must define:

- a conservative admission watermark that includes worst-case WAL, journal,
  temporary-file, metadata-command, staging, repair, and checkpoint overhead;
- bounded accounting for already admitted concurrent work, so a `statvfs`
  sample followed by many writers cannot spend the same remaining bytes;
- which operations may consume the reserved recovery headroom;
- how delete/reclaim can durably advance when SQLite or a journal needs another
  write before physical blocks can be released;
- how interrupted checkpoint, compaction, metadata transfer, repair, and
  backfill recover without assuming spare space appears;
- whether adding capacity or changing membership requires a write to the full
  domain, and the out-of-band recovery procedure if it does;
- byte and inode metrics, admission state, reserved-headroom use, and alerts;
  and
- the exact external error mapping for a capacity rejection versus a transient
  contention or unavailable replica.

Existing readable data should remain readable where its integrity and quorum
permit. A capacity error must not be retried in a loop that amplifies WAL,
journal, checkpoint, or temporary-file use. Cleanup must not discard its
durable owner merely because a later space-releasing step failed. The recovery
reserve must be mechanically unavailable to ordinary work; an alert threshold
alone is not a reserve.

### Security and integrity

Durable paths are derived from validated storage identities, not request text.
Owners reject symlinks and unexpected file types, use private directory/file
permissions, bound reads before allocation, validate checksums or authenticated
digests, and compare persisted deployment identity before mutation. A CRC
detects accidental corruption; it does not authenticate malicious local
rewriting.

### Required testing

Every new durability protocol needs owner-local tests for:

- failure before and after every write, sync, rename, transaction commit, and
  directory sync;
- restart from every reachable intermediate state;
- torn, truncated, corrupt, oversized, wrong-version, symlink, and wrong-type
  artifacts;
- idempotent exact replay and rejection of crossed identities;
- acknowledgement only after the final required barrier;
- no publication of metadata referencing non-durable payload;
- bounded orphan/residue recovery; and
- the intended behavior when deletion is durable versus merely logically
  authoritative.

Logical crash tests may use tmpfs. Claims about power-loss durability require
the production sync path and should also have targeted tests or UATs on a real
supported filesystem with injectable barriers.

## Current persistence inventory

### Storage-node data directory

The storage-node data directory is private to one configured node identity.
Startup validates its ownership and topology before opening PG state.

| Artifact | Classification | Current durability and recovery contract |
| --- | --- | --- |
| storage-node data-directory path and ancestors to its filesystem anchor | Authoritative namespace | Managed startup traverses components using directory descriptors with no-follow semantics, creates missing components privately, and syncs the complete chain from the data directory through its anchor on every preparation. Retrying after any failed barrier therefore reconstructs the complete durability proof before publishing child state. Static and standalone establishment additionally apply their identity/transition protocols. |
| `pg-NNNN/metadata.db` and SQLite companions | Authoritative | Per-PG metadata, command log, reservations, sessions, reclaim/repair/backfill state, shard catalogue, durable PG identity, and checkpoints. WAL mode and `synchronous=FULL`; transaction commit is the metadata boundary. The initialized database and PG directory are explicitly synced before publication. |
| `pg-NNNN/shards/<prefix>/<shard-key>` | Authoritative only through matching durable metadata; otherwise recoverable residue | Both writers first create and `sync_data` a same-filesystem temporary file. The overwrite-capable path publishes by rename; the write-if-absent path publishes by hard link and then removes the temporary name. Both sync the destination prefix directory before returning `WriteAck`. A shard may exist before catalogue/object publication. Reads require durable metadata and validate size/checksum. Unindexed files are scavenger input, not visible payload. Creation of a previously absent prefix has the ancestor-directory gap described below. |
| `pg-NNNN/tmp/` | Ephemeral/recoverable residue | Holds unpublished shard files and may retain or resurrect the source name after either shard publication protocol because the source-directory removal is not its durability boundary. No entry here is S3-visible or authoritative once the final shard name is published. Startup removes abandoned entries under bounded rules. |
| PG durable identity row | Authoritative identity | Stored inside `metadata.db`; binds the PG number and deployment identity before the PG may be reused or served. It is not a separate replaceable file. |
| `control-plane-node-incarnation` | Authoritative process-incarnation counter | Advanced on storage-node startup through a synced temporary file, rename, and data-directory sync. Prevents a restarted node process from reusing its prior control-plane incarnation. |
| `control-plane-runtime-config-v1` | Authoritative restart configuration | Canonical, versioned text binding installed routes and runtime authority. Replacement writes and syncs a private staging file, atomically renames it, then syncs the storage-node data directory before acknowledging publication. |
| `.argmin-storage-node.lock` | Ephemeral coordination | Exclusive storage-node data-directory lock. Its contents and continued directory presence are not restart authority. |
| `.argmin-static-storage.identity` | Authoritative static-deployment identity | Binds the storage directory to the cluster, topology, process, and storage-node identity supplied by the static manifest. Published only after initialization state is durable. |
| `.argmin-static-storage.initializing` and `.next` companions | Durable transition markers | Prevent partial static storage initialization from being mistaken for either an empty directory or an established node. Creation, replacement, and removal are file/directory synced. |
| `.argmin-static-storage.lock` | Ephemeral coordination | Exclusive initialization/runtime lock for the static storage directory. It is not restart authority. |
| `.argmin-standalone-route.identity` | Authoritative standalone deployment identity | Integrity-bound identity published through synced file/directory operations. It binds a standalone data directory to the static route authority. |
| `.argmin-standalone-route.identity.next` and `.argmin-standalone-route.initializing` | Durable transition markers | Establish gap-free standalone identity creation/replacement. Restart classifies interrupted transitions before opening stored state. |
| `.argmin-standalone-route.lock` | Ephemeral coordination | Exclusive process lock. It is not identity or restart authority even if the directory entry remains. |

The central payload ordering invariant is:

```text
durable shard files
    -> durable shard acknowledgements/catalogue
    -> replicated object/part metadata publication
    -> S3-visible success
```

Raw files may precede publication. Publication may never precede their
durability. Reclaim, repair, and backfill use durable metadata plus
deletion-exclusion leases so a file cannot be removed while a permitted reader
or writer still depends on it.

### Metadata-transfer staging store

The staging store lives under the storage-node data directory and is a
separate durable subsystem:

| Artifact | Classification | Current durability and recovery contract |
| --- | --- | --- |
| `metadata-transfer-staging.established` | Authoritative establishment marker | Published only after the staging root is initialized and synced. Prevents a missing or partial root from being silently recreated as a new store. |
| `metadata-transfer-staging/manifest` | Authoritative store identity/format | Integrity-bound manifest naming the store format and identity. Written through temp-file sync, rename, and directory sync. |
| `metadata-transfer-staging/initialized` | Authoritative initialization marker | Distinguishes a fully initialized catalogue from interrupted construction. |
| `metadata-transfer-staging/catalogue.db` and companions | Authoritative staging metadata | SQLite WAL with `synchronous=FULL`. Tracks intents, evidence, receipts, publication, tombstones, closure, and capacity. |
| `metadata-transfer-staging/artifacts/<intent-derived-name>` | Durable staging payload | Bounded, digested metadata-transfer artifacts. File durability precedes catalogue publication; tombstoning and physical removal preserve exact replay evidence and restart convergence. |
| `metadata-transfer-staging/quarantine/` entries | Recoverable diagnostic residue | Bounded and owner-controlled. Quarantine is not publication authority and cannot be interpreted as a valid artifact. |

The catalogue and artifact are not independently replaceable backup units.
Recovery validates their exact relationship and fails closed on missing,
crossed, corrupt, or unexplained established state.

### Single-authority control plane

For a configured base state path `STATE`, the single-authority store owns:

| Artifact | Classification | Current durability and recovery contract |
| --- | --- | --- |
| `STATE` | Authoritative checkpoint | Canonical complete control-plane snapshot. Checkpoint replacement is file-synced, atomically renamed, and directory-synced before becoming the replay base. |
| `STATE.journal` | Authoritative command journal | Checksummed/hash-chained durable journal. Appends are synced before acknowledgement; torn tail recovery is bounded and explicit. Compaction preserves a replayable checkpoint/journal pair. |
| `STATE.identity` | Authoritative durable identity | Random single-authority identity bound into the initialization marker and clock checkpoint. It must not be copied independently. |
| `STATE.initialized` | Authoritative initialization marker | Proves the identity, checkpoint, and journal belong to an established store. Missing or inconsistent established state fails closed. |
| `STATE.clock` | Authoritative local clock-lineage evidence | Checksummed restart checkpoint bound to the exact durable authority identity and timestamp high-water. It is local evidence and must not be copied between authorities. |
| `STATE.lock` | Ephemeral coordination | Sibling process lock held by the control-plane manager. The file may remain after exit, but only the live kernel lock excludes another manager; its bytes and directory entry are not restart authority. |
| temporary companions | Durable-transition staging or recoverable residue | Exact owner protocols determine whether restart finishes, rejects, or removes an interrupted replacement. They are never independently authoritative. |

The checkpoint, journal, identity, initialization marker, and clock checkpoint
form one recovery set. Restoring an arbitrary subset is unsupported.

### Raft control plane

For a configured Raft restart-artifact path `STATE`, each voter owns local
restart state:

| Artifact | Classification | Current durability and recovery contract |
| --- | --- | --- |
| `STATE` | Authoritative restart checkpoint | Contains the local OpenRaft log-store/state-machine restart image and snapshot state. Written through a synced temporary file, atomic rename, and parent sync. |
| `STATE.wal` | Authoritative post-checkpoint WAL | Shared durable-journal framing. Appends are file-synced before durable completion; replay starts from the checkpoint's bound logical offset. |
| `STATE.sentinel` | Authoritative prior-state evidence | Prevents a missing restart artifact from being treated as a never-initialized voter. A lost established voter must not be silently reinitialized under the same identity. |
| `STATE.clock` | Authoritative local clock-lineage evidence | Bound to cluster identity and local Raft node ID. It is deliberately not replicated and must not move between voters. |
| `STATE.static-identity` and prepared companion | Authoritative deployment identity | Binds the state set to the cluster, topology, process, and Raft-node identity. Its established flag is published only after the certified topology, restart artifact, and sentinel are durable. Pending and established forms are restart-validated. |
| `STATE.lock` | Ephemeral coordination | Sibling process lock held by the control-plane manager. Its continued presence is not evidence of ownership; exclusion comes from the live kernel lock. |
| temporary companions | Durable-transition staging or recoverable residue | Restart validates sentinel, artifact, WAL, clock checkpoint, and outer identity together before constructing serving authority. |

Raft replication does not remove the need for local persistence ordering. A
client-visible replicated success and an OpenRaft durability callback must not
run ahead of the local WAL/checkpoint barrier required by the storage
implementation.

### Operator-owned persistent inputs

Static cluster manifests, service configuration, TLS private keys and
certificates, and configured credential inputs are persistent operational
dependencies, but the server does not currently create their authoritative
copies. Their deployment, backup, rotation, permissions, and atomic
replacement belong to the operator/configuration contract. If the server later
begins writing any of them, that output becomes a new inventory entry.

Account and credential persistence is proposed but not yet an implemented
server-owned store. Its eventual design must define a persistence owner and
protocol here before being used for authentication authority.

## Implementation ownership map

This map identifies the production owners to inspect when changing a row in
the inventory. It is deliberately a source map, not a second format ledger:

| Persistence domain | Production owner |
| --- | --- |
| Private directory creation and ancestor durability | [`data_dir.rs`](../crates/storage/src/data_dir.rs) |
| Per-PG SQLite creation, identity, and directory initialization | [`pg_store.rs`](../crates/storage/src/pg_store.rs) and [`pg_store/schema.rs`](../crates/storage/src/pg_store/schema.rs) |
| Shard file create/read/delete protocol | [`pg_store/shards.rs`](../crates/storage/src/pg_store/shards.rs) |
| Storage-node incarnation and persisted runtime configuration | [`storage_node_server/session.rs`](../crates/storage/src/storage_node_server/session.rs) and [`storage_node_server/config.rs`](../crates/storage/src/storage_node_server/config.rs) |
| Static storage-directory identity and transition markers | [`static_cluster_state.rs`](../crates/argmin-s3/src/static_cluster_state.rs) |
| Standalone route identity and transition markers | [`standalone.rs`](../crates/storage/src/standalone.rs) |
| Metadata-transfer staging catalogue and artifacts | [`pg_store/metadata_transfer_staging.rs`](../crates/storage/src/pg_store/metadata_transfer_staging.rs) |
| Single-authority state set | [`control_plane/single_authority.rs`](../crates/storage/src/control_plane/single_authority.rs) and [`durable_journal.rs`](../crates/storage/src/durable_journal.rs) |
| Raft restart state, WAL, sentinel, and clock checkpoint | [`control_plane_raft.rs`](../crates/storage/src/control_plane_raft.rs) and [`durable_journal.rs`](../crates/storage/src/durable_journal.rs) |
| Outer static control-plane identity and process locks | [`static_cluster_state.rs`](../crates/argmin-s3/src/static_cluster_state.rs) and [`main.rs`](../crates/argmin-s3/src/main.rs) |

Moving persistence code does not move ownership automatically. Update this
map, the inventory, format evidence, and the relevant recovery tests together.

## Current acknowledgement boundaries

| Operation | Durable success requires |
| --- | --- |
| shard write | complete bytes, file `sync_data`, atomic destination publication by rename or create-if-absent hard link, and destination parent-directory sync; returned `WriteAck` describes those exact bytes, while any surviving temporary source name is recoverable residue |
| PG metadata mutation | successful SQLite transaction commit under the configured WAL/`synchronous=FULL` connection |
| object or part publication | every referenced shard durably acknowledged before the replicated metadata command becomes visible |
| single-authority command | replayable journal frame and required file/directory sync before durable command acknowledgement |
| Raft log mutation | OpenRaft durability completion only after the corresponding WAL durability operation |
| checkpoint replacement | complete temporary file sync, atomic rename, and parent-directory sync, with its journal/WAL offset relationship preserved |
| storage-node runtime-config replacement | complete staging-file `sync_all`, atomic rename, and storage-node data-directory sync before the installed route state becomes visible |
| staged transfer publication | durable artifact plus durable catalogue transition, in the owner-defined order, before returning publication evidence |
| authority-clock recovery | replacement clock checkpoint and directory entry durable before restored serving authority is acknowledged |

## Known gaps and required follow-up

### DUR-2: newly created shard-prefix directory durability

Shard creation uses `create_dir_all` for `shards/<prefix>`, renames the synced
file into that directory, and syncs the prefix directory. If the prefix was
created by that operation, its directory entry in `shards/` is not explicitly
synced. The common protocol requires syncing every newly created ancestor, not
only the directory containing the final file.

The fix should distinguish existing and newly created prefixes or use a helper
that durably creates the directory chain, then add a failure/restart regression
for the first shard written under a prefix. Metadata publication must not
proceed unless the complete reachable path is durable.

### DUR-3: shard unlink durability classification

Shard deletion currently marks/removes its SQLite row and unlinks the file
without syncing the shard directory. This can be correct only under the
recoverable-residue exception: after power loss an unlinked file may reappear,
but absent authoritative metadata must keep it invisible and scavenging must
remove it safely. That contract should be pinned with restart/scavenger tests
and recorded next to the deletion implementation. If deletion acknowledgement
is intended to promise immediate physical reclamation across power loss, the
parent directory must instead be synced.

### DUR-4: mechanical inventory enforcement

The format ledger checks representation changes, but there is no corresponding
fail-closed inventory for newly introduced persistence roots or raw sync/rename
protocols. Until such a check exists, reviewers must treat any new production
use of file creation, rename, unlink, SQLite database creation, or durable
journal construction as requiring an inventory update.

### DUR-5: capacity-exhaustion safety and recovery liveness

The codebase has extensive logical recovery and persistence-failure coverage,
but it does not yet have one complete, mechanically enforced model for
`ENOSPC`, inode exhaustion, quotas, or the headroom required by recovery.
Several important workflows can require a durable metadata, journal, or Raft
write before they release space. Control-plane membership or topology changes
also require durable control-plane progress, so “add storage through Raft” is
not by itself an escape path when that durability domain is full.

This requires a cross-domain audit and implementation plan. At minimum it must
inventory the maximum temporary/WAL/checkpoint amplification of each operation,
include already admitted concurrent work, define separate admission and
emergency-recovery reserves, and prove that:

- ordinary writes stop before consuming recovery headroom;
- reclaim and deletion either make bounded progress inside that reserve or
  retain exact durable retry state;
- restart, replay, checkpoint, compaction, repair, backfill, and metadata
  transfer cannot deadlock solely because they need unreserved new space;
- a full data domain cannot prevent an out-of-band capacity-expansion path,
  and a full control-plane domain has a documented operator recovery path;
- repeated capacity failures do not create further durable amplification; and
- reads and safe physical cleanup remain available wherever their invariants
  do not require a new durable allocation.

Deterministic fault injection should cover every allocation, write, sync,
rename/link, SQLite commit, journal append, WAL append, and checkpoint boundary.
Real-filesystem UATs should separately fill bytes and inodes, exercise restart
at the reserve boundary, and verify both the wire error and eventual recovery.

## Resolved inventory findings

### DUR-1: storage-node runtime configuration replacement

`control-plane-runtime-config-v1` now uses the standard durable replacement
protocol. The writer completes and `sync_all`s the private staging file before
returning a staged configuration. Publication atomically renames that complete
file and syncs the storage-node data directory before the in-memory route state
is changed or success is returned. A pre-rename failure therefore preserves
the previous complete restart configuration. Failure of the post-rename
directory sync is a may-have-applied durability failure: the live namespace
contains the completely written rename target, but the code makes no claim
about which directory state survives power loss because the required barrier
was not confirmed. The install is not acknowledged, and retry repeats the
complete protocol.

Owner-local regressions inject failures at both barriers, verify that the
post-write handoff is unreachable after a staging-file sync failure, verify
that an unpublished staging file cannot replace the prior restart state, and
verify that the live namespace after an ambiguous post-rename directory-sync
failure contains the complete rename target and can safely repeat the full
replacement protocol. This injected syscall failure is not a power-loss test
and establishes no stronger crash-recovery guarantee.

### Managed storage-node root creation

The initial inventory found that managed startup recursively created its data
directory but synced only the new directory after writing child state. A power
loss could therefore lose the directory entry from its first existing parent.
`prepare_private_data_dir` now traverses and creates each component relative to
held directory descriptors using `O_DIRECTORY | O_NOFOLLOW`, then syncs the
complete descriptor chain from the data directory through the filesystem
anchor on every call. A retry after a failed barrier replays the entire proof;
it cannot infer durability merely because the directories now exist. Any sync
failure still propagates before bootstrap can report success.

## Planned persistence changes

Plans do not change the current contract until implemented. They must update
this guide as part of their implementation:

- [Shard write group commit](../plans/shard-write-group-commit.md) may introduce
  a durability batcher and, in a later design, append-only shard logs. An append
  log would be a new authoritative format with offsets, tail recovery,
  compaction, and garbage-collection rules.
- [Unified staged write publication](../plans/unified-staged-write-publication-plan.md)
  proposes durable multipart cleanup roots and staging leases. These belong in
  per-PG SQLite, but add new cross-store ownership and acknowledgement
  boundaries with shard files.
- [Versioned physical shard files](../plans/versioned-physical-shard-files-option.md)
  would replace the current one-logical-key/one-path assumption with a durable
  logical-to-physical generation mapping.
- [Persistent accounts and credentials](../plans/persistent-account-and-credential-management.md)
  would introduce a new authentication-authority store.

## Review checklist for a new persistent artifact

Before approving a new persistence surface, answer all of the following:

- What is its owner, base directory, canonical path, and identity?
- Is it authoritative, durable staging, recoverable residue, or ephemeral?
- What exact event makes its bytes and directory entry durable?
- What may reference it, and how is durable-before-visible ordering enforced?
- What is the durable owner before, during, and after ownership transfer?
- Which failures are definitive and which may have applied?
- What does restart do at every interruption point?
- How are deletion, name reuse, and resurrected directory entries handled?
- What format/schema version and immutable evidence cover it?
- Which integrity, bounds, permissions, and no-symlink checks apply?
- Which routes, proofs, keys, or companion artifacts must be backed up and
  restored with it?
- Which metrics expose sync latency, failures, backlog, and recovery work?
- Which failure-injection, restart, corruption, and real-filesystem tests prove
  the contract?

If any answer is “best effort” or “recovery will probably handle it,” the
durability design is incomplete.
