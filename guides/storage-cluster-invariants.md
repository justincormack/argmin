<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Storage Cluster Invariants

This guide pins down the storage-cluster boundary during the multihost
transition. Phase 6.1 is moving metadata reads and writes from the
metadata-primary bridge to cluster-owned PG-primary routing; later Phase 6 work
will turn those routed operations into replicated commands.

The physical persistence surfaces beneath these routing rules, their sync and
publication ordering, and their crash-recovery classifications are inventoried
in the [durable-storage guide](durable-storage.md).

## Operation Classes

Every public `StorageCluster` operation must fit one of these classes:

| Class | Stale-handle behavior |
|---|---|
| Construction | Creates a handle for the current local map epoch, except explicit test hooks that can create stale handles. |
| Read-only topology/config | May read local map or static topology/config without an epoch fence. It must not read object metadata, payload bytes, or worker queues. |
| Epoch-fenced routed metadata read | Routes through the active PG primary or, while Peering, through the exact replica named by the control-plane metadata-read certificate. Stale handles and uncertified routes fail with typed route errors before metadata is read. |
| Epoch-fenced routed metadata mutation | Must route through the Active PG primary selected by the cluster map. Stale handles, inactive routes, and non-primary nodes fail with typed route errors before metadata is mutated. |
| Epoch-fenced metadata bridge test hook | Test-only bridge helpers must fail with `StoreError::StaleMetadataPrimaryBridge` when the handle epoch is stale. Production metadata paths must not use this class. |
| Payload placement/read/write/delete | Must use `StorageCluster` placed payload APIs. Stale placement becomes `StalePayloadOperation`; stale shard IO becomes `StaleShardOperation` or `StaleShardLocation`. |
| Best-effort cleanup/worker queue | May suppress stale-handle and cleanup failures only where the API is explicitly best effort. Suppressed payload cleanup failures must emit typed trace context when tracing is active. |
| Active token release | Releasing already-acquired state is not new work. Release must go to the node where the token was acquired even if the creating cluster handle is stale by release time. |
| Test hook | Must be `#[cfg(any(test, feature = "test-hooks"))]` or `#[cfg(feature = "test-hooks")]`. Test hooks must either be read-only topology helpers, current-handle bridge helpers, command-consistent acting-set seed helpers, or explicitly named contention/failure hooks. |

## Invariants

- Single-PG metadata mutations must route through the cluster map's Active PG
  primary. They must not fall back to the process-wide metadata-primary node or
  to a read-certified replica.
- Single-PG metadata reads route through the Active primary in the ordinary
  case. While a PG is `Peering`, they may instead use the exact node and
  metadata proof in the current control-plane `PgMetadataReadRoute`. The
  authority may issue that certificate only when the replica is healthy,
  reports `Peering`, has no pending metadata command, and its complete metadata
  proof exactly equals the committed Peering floor or the exact imported
  transfer proof. The broader provenance-aware progression rules used by
  Peering recovery do not grant read authority. A replica ahead of the
  committed floor remains unavailable for metadata reads until the authority
  durably commits or otherwise certifies that progress.
- Storage-node handlers and embedded read routes bind each read authorization
  to its exact PG, then recheck the current route, certificate, exact metadata
  proof, and empty pending slot while holding that PG's lock used for the
  SQLite read. A certificate cannot be substituted between PGs with equal
  proofs, and a runtime-map or local metadata change cannot race certificate
  validation and read publication.
  The capability exposes bucket/object point reads, read-only listings,
  multipart classification and ListParts only; it cannot construct a metadata
  mutation route.
- `PgState::Active` remains the only state that accepts new mutations.
  `Peering` without a valid metadata-read certificate, and all metadata reads
  in `Degraded`, `Backfilling`, or `Inconsistent`, fail closed with typed route
  errors. Payload reads may separately use their retained-route EC
  reconstruction capability when at least `k` valid shards are reachable.
- An exact current-epoch pending-command observation from the current Active
  primary is recoverable without changing the PG state or global epoch. The
  Active runtime route carries that exact recovery certificate, and later
  mutations remain blocked by the PG pending slot until recovery converges or
  a clean primary scan withdraws the certificate. Older-epoch pending evidence
  still requires a retained Active historical-primary route and a current
  Peering fence.
- Phase 6 writes are strict. A successful write must apply to every required
  metadata acting-set replica and write every required payload shard. The local
  implementation must not acknowledge quorum writes, degraded writes, or
  missing-shard writes.
- The remaining metadata-primary bridge is temporary, epoch-fenced, and allowed
  only for current-handle test hooks. Production metadata paths must route by
  PG primary or use explicit local-cluster runtime state for in-memory
  coordination.
- Payload bytes are always accessed through placed IO. Production request paths
  must not call node or PG shard read/write/delete APIs directly.
- Every current payload-bearing object, multipart upload, stream session, part,
  and segment has concrete SSE-S3 or SSE-C state. Payloadless delete markers
  have their own row shape. There is no plaintext placed-IO profile, and former
  tag-0 carriers are rejected before shard IO rather than routed to a fallback
  reader.
- Payload shard ack rows are metadata rows in the data PG and must route
  through that PG's primary. They hold shard checksums/sizes while payload
  files are placed through the cluster map.
- Local metadata commands are serialized per `(PG, bucket)` command stream.
  Bucket metadata commands use the bucket PG; object metadata commands use the
  object PG for the target object. A pending command for a stream must block
  later metadata mutations on that same stream until the pending command
  converges or the bucket incarnation is finalized away.
- Direct mutation of command-owned metadata is not a production boundary.
  Legacy `PgMetadataStore` and `SharedStorageNode` mutators for digest-covered
  bucket, object, upload, stream, reclaim, and allocator rows must be test-only,
  explicit test hooks, or private `PgStore` helpers used during metadata command
  apply. `PgMetadataStore::delete_finalized_bucket` is the explicit bucket
  finalization exception and may only be called by the cluster acting-set fanout
  after `MarkBucketDeleting`, write drain, visible-data checks, and reclaim
  checks have completed. The PgStore implementation must fail closed if the
  local row still exists in any state other than `Deleting`.
- Bucket write-drain authority is durable and bucket-PG-primary owned.
  Production write admission uses `bucket_write_reservations`; DeleteBucket
  begin uses `bucket_write_drains`. The legacy bucket-row write-drain counters
  have been removed and are not production fence authority. The Phase 9.4 model
  and publisher audit live in [bucket-write-drain.md](bucket-write-drain.md).
- Record-based bucket reservation release is an atomic exact compare-and-delete:
  it matches every stored identity field, the optional target, and the observed
  lease deadline. A stale observation must not erase a renewed reservation.
  Command-proof release has a different contract: it matches the complete stable
  identity under its existing transaction, deliberately ignoring the renewable
  deadline, and treats confirmed absence as idempotent success. Neither release
  mode may ignore operation kind, creation time or target context.
- Best-effort cleanup may suppress cleanup errors, but typed route/control-plane
  errors must not collapse into `NotFound` or generic IO before the suppression
  point.
- Concrete storage implementation errors (`StoreError`, `MetadataError`,
  `ObjectPgActionError`, and `ShardIoError`) are crate-private. Public storage
  operations return operation-specific opaque failures or bounded semantic
  classifications; coordinator and HTTP code cannot match, render, or retain
  the implementation errors. Rust visibility is the boundary. Owner-local
  tests, rather than a cross-crate source-symbol inventory, pin classification,
  redaction, error sources, and protocol mapping.
- Physical shard absence, CRC corruption, and length corruption are recoverable
  only inside the EC read paths that explicitly opt into reconstruction.
- Token release is not new work. Acquiring a token requires a current handle;
  releasing an acquired token must still decrement local-cluster runtime state
  after an epoch transition.
- Crash-durable orphan discovery is a later scavenger/reclaim responsibility.
  Injected best-effort cleanup failures may leave only explicitly documented
  orphan payload or ack state for that later process.
- Opening a `PgStore` is intended to become a recovery boundary, not just a
  schema bootstrap, so that a reopened store reconciles or rejects crash
  leftovers before it serves. This is a target invariant: the implementation
  is being introduced by the Phase 11 stabilisation slices and is not yet
  enforced at open/bind. See [PG Store Recovery
  Boundary](#pg-store-recovery-boundary) below.

## PG Store Recovery Boundary

This section states the current recovery boundary for local PG metadata stores.
`PgStore::open` remains a raw schema/open primitive, but production and local
cluster startup paths must run recovery before serving PG state.

### Current behavior

`PgStore::open` bootstraps the schema, digest triggers, and replica-state row.
It does not reconcile crash leftovers by itself because recovery needs the
owning `NodeId`. `StorageNodeServer::bind`, `StorageNodeBootstrap`, and the
local-cluster builder all run raw-node metadata-command recovery before serving
PG state. The bootstrap path holds the storage-node data-dir lock before
opening and recovering the node, then consumes itself to produce a one-shot
prepared server that couples the exact persisted runtime configuration to that
guard until bind. Bind also rejects any process configuration that differs from
an existing persisted control-plane runtime configuration. Raw `PgStore` and
shared-node construction are private storage-engine details.

The cleanup-ownership work adds no per-PG directory lock or separate writer
admission. The experimental exclusive-opening wrapper and associated fixture
migration were withdrawn; the existing storage-node root guard and per-PG
operation serialization remain unchanged.

The candidate complete-command settled-prefix reader retains its validation
cache and checks for local writes, external SQLite writes, schema changes and
the captured topology/allocation lifetime. It does not assume exclusive
database ownership or that startup validation alone stays valid indefinitely.
An unavailable allocation lifetime discards the optional apply cache and uses
the cold apply path; it still prevents new allocation and confirmation. Cache
removal and a broader connection/serialization redesign are deferred, not
requirements of the current cleanup-ownership integration.

The existing entry is preserved through closed complete pending installation,
publication trial/marker, exact eligible slot retirement, and confirmed embedded
stream cursor allocation, in addition to complete apply. Preservation checks the
captured state/stamp before each effect block and freezes its successor stamp
before commit. Failed/unconfirmed finish,
unwind or intervening writes leave validation unavailable; successful retries
cannot warm a cold entry. Only the fixed operation owners can construct and
finish the preservation observer. No generic cache-validation API, additional
transaction/fsync, or per-confirmation startup recovery is introduced. This is
local prefix readiness, not physical cleanup authority.

Candidate embedded window confirmation now uses that readiness under the
admitted current-primary serialization. It compares the exact active future
window and its retained subject/resource obligations in one read snapshot, then
requires the designated publication witness (lowest non-primary node ID) to
match the epoch/index/log hash/state digest. Another agreeing replica is not a
substitute; only a genuine singleton route omits the witness. Lookups are bounded
to the owner, last window, bounded participants, creation resource and indexed
reference/generation checks, not the upload's complete history. Captured route,
request and immutable window deadlines bound lock waits and are rechecked after
witness inspection and snapshot teardown. Historical recovery sections cannot
serve this confirmation. The result is a scalar observation, not transferable
physical authority. An embedded primary can also inspect its designated witness
over authenticated Unix/TLS. That bounded candidate exchange binds node, PG,
epoch, canonical topology and the once-captured absolute work cutoff; the receiver
intersects it with its own captured Active route deadline before serialization
and PG-lock waits. It returns a settled prefix, never raw-state fallback or a
grant. Pending/cold/disagreeing/unavailable peers fail closed; no other replica or
local shadow store substitutes for the designated witness.

A candidate Unix/TLS primary can perform the same observation using its existing
connection-held current-primary metadata section. The sender exclusively borrows
that session throughout witness inspection and the final primary read. In this
variant the witness is inspected **first**, then the primary opens its coherent
snapshot and validates the exact immutable window. Primary serialization stays
held across both observations; no SQLite snapshot or server-side inspection
registry is retained between RPCs. The final request must arrive on that same
held section, not a reconnected or historical session. Its bounded descriptor
is descriptive, not an installable command or effect capability. Replies bind
the request, descriptor SHA-256 and designated witness, and the sender compares
the complete settled prefix and rechecks authority. A lost reply or a successful
reply declaring the section closed cannot confirm. UploadPart creation uses the
candidate release/retirement handoff described below, before active-window
inspection.

A consuming candidate handoff now keeps that fresh observation together with the
original all-member terminal custody, after confirmed pending-slot retirement.
It derives the window from the retained context and accepts only the original
current admission's embedded section or registered remote primary/session. The
original publication, route and window cutoffs remain bounds; a narrower attempt
is captured before inspection and checked again before returning. Failure returns
the same terminal holder and histories, while success has no reverse conversion,
renewal or raw descriptor extraction. Its validity check only checks captured
time/admission, not a new prefix observation. This holder is **not** a physical
grant: local write/cleanup exclusion and mandatory live selection remain
activation work. Neither confirmation nor Drop mutates
durable state or performs cleanup.

The candidate stream allocator consumes that holder under the original
embedded or Unix/TLS primary section and PG lock. Its existing cursor-update transaction rechecks the
captured settled prefix, exact window/session/generation, original allocation
lifetime and immutable cutoffs before consuming the single slot. The deadline
is also checked before commit. Success retains custody with the allocated record;
failure retains custody without an allocation grant, including lost commit replies.
Neither result permits resetting or retrying allocation. A second independently
confirmed holder cannot consume the same cursor. This preserves the existing
prefix entry only across the fixed noncanonical cursor update, never warms a cold
entry, and adds no transaction/fsync. Unix/TLS sends once on the same held
connection, with the original conservative absolute cutoff and bounded window
descriptor, captured prefix and expected session. The receiver intersects its
captured route fence, validates Active admission/domain and current-primary
section, and rechecks authority at UPDATE/COMMIT. The reply binds the exact
request SHA-256 and allocated segment. A disconnect, invalid reply or closed
section returns custody without a grant; it does not reconnect, renew or reset
the cursor.

Direct PUT does not allocate another cursor: its reservation already owns its
sole physical VID. The candidate embedded file writer consumes confirmed
custody after rechecking the exact active window, generation reservation,
settled prefix and original issuer under the admitted primary's read snapshot.
It retains custody on failure and through file attempts; Drop does not release
the generation. File keys, placement and participant selection come from that
window and the same admitted map, not supplied PG/node/VID inputs. Bounded codec
geometry is checked before IO. The captured intersection of route, work,
confirmation and window cutoffs is checked inside existing file/delete
exclusion; the guard lasts through directory sync and file acknowledgement.
Exact retries use write-if-absent. Matching CRC64/length alone is not exactness:
the existing file bytes must match too, including for compensated CRC collisions.
This adds no allocation registry or metadata write/fsync. It does not yet bind
the frontend encrypted payload, persist acknowledgement rows, implement remote
file admission, or enable owner-driven deletion.

Candidate embedded stream file admission now consumes the successful allocation
holder for streamed PUT and UploadPart. That holder retains the exact allocated
segment and captured session authority; callers cannot supply a replacement VID,
manifest or logical size. Before binding, the primary read snapshot rechecks the
original settled prefix, active window, issuer, session/reservation authority and
the consumed cursor. Binding failure retains custody without reset or Drop
cleanup. File attempts use the same placement validation, immutable-cutoff
intersection and write/delete exclusion as Direct PUT. This adds no cursor
update, metadata write or fsync.

Both candidate file writers privately retain only acknowledgements returned by
successful file attempts. Direct PUT pins its logical size before the first
dispatch, including an uncertain result; subsequent attempts cannot substitute
another size with the same padded shard length. Embedded acknowledgement
publication requires the complete ordered shard set and derives its keys, data
PG and acknowledgement primary from retained custody. No caller batch or renewed
deadline is accepted. A borrowed opaque batch and a node-owned exact-store
authority bind the receiver's runtime domain and captured Active primary. The
existing PG mutex and single exact-registration transaction exclude cleanup and
check the immutable effect fence beside every INSERT or exact retry. Already
admitted work may finish committing after cutoff; deletion waits for it. There
is no extra acknowledgement lock or transaction. Errors and Drop leave owner,
generation and file receipts intact, so an exact retry cannot silently become
fresh authority. Success is not yet terminal publication authority.

The candidate `SWRT`/`SWOK` file transport is descriptive, not write authority.
It carries a bounded retained-window/stream-inspection description, receiver
route, original absolute cutoff, shard index and bytes, never a raw physical key.
The reply binds the complete request SHA-256 and expected shard length/CRC64.
Decoding alone cannot authorize effects or register a receipt in an owning
writer. Owner-test-selected installed Unix/TLS dispatch now connects this format
to the receiver's independent confirmation and existing physical exclusion.
It captures route authority before decoding, checks the receiver identity and
topology, and rejects connections holding metadata sections before peer IO.
A shorter outer stream cutoff is preserved through confirmation and file effect;
the longer nested allocation cutoff cannot replace it. The receiver acknowledges
file publication only, not acknowledgement-row persistence. The candidate Direct
PUT sender now borrows the released-primary owning writer, derives placement
from its captured map and requires that map's exact remote client instance. It
pins the complete-request SHA-256 before sending, retains uncertainty after any
failure, and permits only identical explicit retries. Confirmed retries reuse
their recorded file receipt without sending again. The absolute cutoff is
captured once during handoff; errors, expiry, Drop and unwind cannot release
custody or reset an attempted shard. The candidate stream sender applies the
same contract to streamed PUT and UploadPart, consuming successful allocation
custody and retaining its exact segment/session inspection bytes. Callers cannot
supply a replacement segment, session or logical size. Remote acknowledgement-row
effects remain gated. Distinct kind
`StagingShardWrite` (178) selects the candidate grammar; live dispatch rejects it
as unsupported. Legacy `ShardWrite` (3) always uses its original decoder, even
when its epoch bytes begin `SWRT`. Payload magic is never a dispatch selector.

Candidate acknowledgement transport now has its own kind
`StagingShardAckRecord` (179), with bounded ordered receipts and the same
direct-window/allocated-stream provenance. Its success reply binds the complete
request SHA-256. Normal dispatch still returns `UnsupportedOperation`. Candidate
dispatch independently confirms the retained window and Direct PUT reservation
or exact allocated stream segment with the primary/witness. It validates the
complete receipt set and installed placement, and acknowledges primary-section
release before taking the derived acknowledgement PG mutex. The existing exact
batch transaction checks the intersection of client, window and captured server
route cutoffs beside each SQL effect; exact retries do not rewrite rows. Decoding
and row descriptions alone are not mutation authority. Raw keys or a caller's
claimed data PG are not part of this carrier. Candidate released-primary writers
now derive remote acknowledgement attempts exclusively from their complete
private file-receipt set. A missing or uncertain file receipt cannot authorize
row persistence. The sender binds the exact admitted acknowledgement-primary
client and retains the complete immutable request before transport. Lost/crossed
replies and later rejection retain uncertainty; exact success records confirmation
without releasing custody. Confirmed retries send nothing and cannot regress
confirmation on later expiry. There is no raw receipt injection, renewed cutoff,
reset or cleanup-on-Drop API. A consuming candidate handoff now closes the
physical-writing phase only after exact acknowledgement persistence is confirmed.
Separate opaque holders retain the complete Direct PUT payload or one allocated
stream segment, including original custody, file attempts and cutoff. Failed
handoffs return the identical writer, preserving uncertain acknowledgement history.
Successful holders expose no file-write, receipt-extraction, reverse-conversion
or renewal API. This no-effect transfer preserves evidence after expiry; it is
not new mutation authority. Stream-segment readiness is not upload finalization.
Candidate embedded stream append preparation now consumes that segment holder,
derives its integrity witness from the confirmed receipts, and revalidates the
exact current owner/session/allocator under primary serialization and a read
snapshot. The resulting append command, successor-window context and original
physical custody remain together in an opaque prepared holder. No installation
or physical effect occurs during preparation. Consuming embedded append
installation now retains that entire prepared holder and its non-regressing
installation history. An opaque borrowed attempt binds the exact complete append
to the original admitted section; the existing slot transaction validates the
complete inventory and both admission/work cutoffs immediately before mutation.
Definitive rejection preserves prior history, uncertain finish/unwind retains
uncertainty, and confirmed retries cannot reinstall or erase confirmation.
Dropping the holder does not delete files, acknowledgements or generation custody.
A consuming candidate publication holder now accepts only confirmed append
installation, retaining the complete sender and original physical custody together.
It preserves the original monotonic and absolute wire cutoffs, rejects foreign
sections, and uses the shared witness-before-primary ordering for embedded apply.
Publication/apply uncertainty and confirmation remain inside the holder; a failed
handoff returns the identical submission. No sender, route, command or custody
extraction is exposed. Exact all-member append confirmation now permits a consuming
transfer into the existing terminal holder, preserving the original route, complete
command and all installation/publication/apply history. Only then can resolved
frontend file-attempt bookkeeping be dropped; no durable resource or payload is
deleted. Existing exact slot retirement and current-primary/witness confirmation
produce the successor window under the inherited cutoffs, without accumulating a
recursive history of prior file attempts. Unconfirmed retirement blocks that
successor handoff, including after a lost successful reply. If interrupted
retirement invalidates local prefix validation, exact retirement retry does not
restore it: explicit complete-state recovery is required before confirmation.
Remote append preparation/installation and candidate Direct PUT terminal paths
are described below. Live frontend selection remains gated.

Candidate Direct PUT post-write snapshot preparation consumes acknowledged
physical custody. A private permit binds the original admitted primary domain,
subject and cutoff. Under primary serialization and one object-PG read snapshot,
it revalidates the confirmed prefix, exact retained owner/window and generation
reservation, then loads the current object and displacement facts. It rechecks
authority before returning a holder that retains both snapshot and physical
custody; only logical precondition facts are exposed. Failure returns unchanged
custody and Drop performs no cleanup. This step cannot allocate a terminal
command or install a pending slot. The later command builder must revalidate the
captured facts atomically; this snapshot is not standalone mutation authority.

The candidate Unix/TLS sender retains that same physical custody and opens one
bounded post-write primary section (the pre-write section was released before
physical dispatch). Its private session identity is fixed on successful opening;
snapshot retries cannot substitute a reconnected section or renew the original
cutoff. The sender binds each response to the complete request digest and checks
current/displacement subjects, precondition consistency and active-owner state.
Malformed or lost replies return the unchanged owning holder, not a detached
snapshot. Confirmed file/acknowledgement evidence and cleanup responsibility remain
with that holder; Drop has no physical or reservation effects. This is read-only
candidate preparation, not remote terminal construction or live activation.

That snapshot holder can consume itself into a receipt-bound Direct PUT manifest.
Placement, subject and codec come from the original window; logical length and
the complete ordered shard acknowledgements come from the file journal. The
supplied segment CRC must match the systematic receipts with canonical padding
removed. The resulting integrity witness cannot be extracted from the holder,
which still retains snapshot and physical custody. Failure returns unchanged
prepared custody, without cleanup or cutoff renewal. This handoff adds no
storage effect and does not yet construct or authorize a terminal command.

The witnessed payload can now be consumed into candidate embedded terminal
preparation. Under the original admitted primary section and one read snapshot,
it revalidates the saved facts and uses shared apply preflight for exact ownership
and displacement. Physical command fields and the reservation proof come only
from retained custody. File binding also mints and retains the exact provisional
encryption subject before any write. Logical commit input remains separately
supplied, but its encryption must match both that subject's committed successor
and opaque evidence from the exact provisional-to-committed binding transition.
This pins the full canonical subject and provisional material as well as the
committed state; a codec/binding tag or instance comparison alone is insufficient.
The returned holder keeps the complete command and witnessed payload
together, with no independent extraction API. Failure returns unchanged
witnessed custody; Drop performs no cleanup. Preparation does not durably allocate
an ID or mutate storage. Its consuming candidate embedded installation handoff
keeps the complete command, physical custody and installation history together.
The existing slot transaction rechecks the retained snapshot, owner/window and
reservation plus shared terminal-apply preflight before insertion, then checks
the original fence and work deadline immediately beside the effect. This adds no
transaction or fsync. An uncertain finish retains uncertainty across retry;
confirmed installation never regresses or instructs a retry to repair the slot.
It does not prove terminal application or authorize cleanup.

Confirmed installation can consume into candidate embedded Direct PUT publication.
The holder retains witnessed payload custody and the complete sender together,
including the original introduction history and absolute/monotonic cutoffs. It
exposes only admitted marker publication, cohort-ordered apply and observations:
there is no detached sender, reset or cleanup API. The shared marker trial
validates durable scope/indexes and terminal effects without committing object
or ownership changes. Only confirmed marker publication permits witness apply;
only confirmed witness apply permits primary apply, followed by remaining replicas.
Lost replies and unwind preserve per-member uncertainty. Each successful apply
atomically binds the exact object references and retained release obligations;
exact retry does not repeat mutations. Physical payload remains untouched. This
reuses the existing publication/apply transactions, without new format or fsync.
Confirmed all-member application can consume into a Direct PUT terminal holder.
The same route, complete command and journals remain owned; only resolved frontend
file bookkeeping is discarded because exact durable object references now exist
on every member. Incomplete or expired handoff returns the original publication.
The holder exposes exact command-proof release with the original cutoff and
non-regressing uncertainty/confirmation. It does not expose the generic terminal,
successor allocation or physical cleanup. After confirmed proof release, Direct
PUTs expose admitted embedded slot retirement using
the original cutoff. The removal transaction authenticates the complete Applied
command and exact slot/indexes, then checks its durable owner, selected window,
reference identity, generation high-water fence and release responsibility. The
same custody validation applies to absence retries; later Object-to-Reclaim and
reclaim-completion transitions may change destinations but not retained physical
identity or resource responsibility. Confirmed retirement never regresses or
redispatches. Release and slot removal leave retained owners, obligations, payload
files and acknowledgements untouched. Overwrite retirement also checks the exact
displaced root and its retained standard/multipart owners in that transaction.
A missing root requires post-reclaim owners whose retained windows cover the
complete command-carried manifest; absence alone is not successful cleanup.
Missing owners, surviving reclaim references without a root, crossed roots and
uncovered parts/segments fail closed. Remote-primary terminal publication and
frontend activation remain gated.

The candidate remote terminal-preparation receiver now shares snapshot inspection's
Active-domain, current-primary and exact held-section admission. One read snapshot
revalidates the retained owner/reservation, the earlier canonical snapshot digest,
receipt-derived geometry and encryption-material lineage before complete apply and
inventory preflight. The reply is encoded before snapshot teardown and a final
immutable cutoff/lifetime check. It neither installs nor applies the command and
adds no fsync or cross-PG reads. Material lineage is not proof of which AEAD subject
sealed a DEK: the still-gated consuming sender must require its exact opaque
committed-encryption evidence and validate the returned command against retained
physical custody before any installation.

Remote append preparation has a candidate-only admitted receiver on distinct
RPC kind 180. Bounded stream inspection and ordered receipt claims are bound to
the original primary, request digest and cutoff. Active admission from the same
server domain and a held current-primary section are required. One object-PG
snapshot validates the exact session, generation, consumed allocation, issuer,
settled prefix and installed placement, then captures the witness-bearing append
and successor descriptor. It rechecks authority after capture before returning
borrowed, request-bound complete-context bytes. No command index, pending slot,
acknowledgement or payload is mutated by preparation, and no data-PG lookup occurs
under that object-PG lock. Receipt claims alone do not prove physical custody:
the consuming sender retains successful acknowledgement custody and validates
the returned complete payload, witness, next position and successor subject,
geometry, placement and cutoff before accepting it. The candidate sender retains
the new preparation section's identity alongside physical custody; failed
preparation returns that same acknowledged holder without renewing authority or
deleting anything. Live selection remains gated, and embedded installation
rejects remotely prepared inputs. Raw decoding cannot mint installation authority.

The candidate installation receiver now uses distinct RPC kind 181. It requires
the same Active-domain/current-primary admission as preparation, additionally
binding mutation authority to the complete installation request. Fresh insertion
rechecks the settled prefix, durable session/allocation and successor geometry
under the held primary section and object-PG mutex. The supplied complete command
must match the preparation's payload and independently reconstructed witness;
its successor must retain the exact subject, adjacent window and installed
placement within the original cutoff. Existing slots use exact command/scope/index
validation, not recapture or index repair. The rebound client fence is captured
once and intersected with the server route fence at the SQL mutation boundary.
Only a full-request SHA-256-bound success confirms installation; errors and lost
replies do not establish absence. This receiver remains test-selector gated.
The candidate owning sender now connects the prepared holder to this receiver.
It borrows the exact captured session under the original deadline, rejecting
wrong clients and same-client reopened sections before dispatch. A private
attempt borrows both the complete context and its mutable installation journal;
callers cannot supply command bytes, replacement deadlines or previous outcomes.
The journal becomes Unconfirmed before transport/unwind. A local NotSent restores
only its previous observation; remote errors, malformed confirmations and lost
replies retain uncertainty. Exact confirmation records Installed even when the
peer closes afterward. Subsequent valid-authority calls do not redispatch an
Installed command; later errors cannot erase that historical evidence. All paths
retain original physical custody, and Drop performs no payload cleanup.

Confirmed remote append installation can now consume into a candidate publication
holder retaining that exact preparation section identity, physical custody and
all observations. Publication borrows the same session under the original local
deadline, validates identity and retained authority, and keeps the mutex through
the typed publication dispatch. Its portable cutoff cannot exceed the original
physical/preparation cutoff; the consuming handoff and retries cannot renew it.
NotSent restores only previous publication history; malformed/error/lost replies
retain uncertainty, and valid confirmation remains historical success even if
the peer closes the section. It does not establish application or retirement.
Remote holders reject a substituted embedded primary section, and generic terminal
conversion returns the whole holder rather than discarding session identity. Drop never
deletes files, acknowledgements or reservations. The live frontend selector
remains unchanged.

Candidate all-remote append application now requires the original primary
client/section for every target, as well as the target's exact admitted client.
It holds that primary transport mutex through replica RPCs and applies in
witness, primary, remaining-member order. Primary application uses the held
session directly, not another connection. Publication must be confirmed first;
an uncertain witness/primary response cannot authorize the next stage. Each
member's journal remains inside the physical-custody holder and reuses the
publication's immutable cutoff. Exact replies confirm only their own member;
NotSent preserves prior history, malformed/error/lost replies retain uncertainty,
and later failures cannot erase Applied. Losing the primary section cannot be
repaired by reopening it in this foreground holder; retain custody for recovery.
Successful replica retry may use its ordinary short RPC connection while the
original primary section remains held. Neither partial nor complete application
performs terminal retirement or payload cleanup during application.

The same holder can apply to an embedded replica selected from its captured
cohort while retaining the original remote primary section. The embedded target
owns a deadline-bounded borrow of that exact session through PG admission,
transaction commit and confirmation; a same-client reopened session cannot
substitute. The witness/primary ordering, immutable cutoff and per-member
observations are shared with remote application. No caller-supplied runtime,
command, deadline or apply callback is accepted.

An exactly completed remote append consumes into a distinct opaque terminal
holder retaining its preparation-session identity, complete command, original
cohort/cutoffs and application observations. It exposes only slot retirement and
consuming next-window confirmation, without raw terminal extraction, reconnect,
renewal or physical cleanup. The immutable identity is compared before invoking
the existing bounded, locked operation on that same session reference; concurrent
closure fails live admission and cannot substitute a connection. Failed handoff
returns the original owner and observations. Ambiguous retirement remains
Unconfirmed; exact retry on the original usable section may confirm Retired.
Only confirmed retirement permits successor capture, whose read capability
retains the same session identity for allocation and file binding. Frontend
file-attempt bookkeeping is discarded only after exact all-member append apply;
the durable staged references and owner windows remain. Live activation remains
gated; the Direct PUT candidate publication-start path is described below.

The candidate consuming Direct PUT terminal sender retains the acknowledged
payload, integrity manifest, captured snapshot and original primary-session
identity. Before transport it requires the opaque committed-encryption evidence
to match the storage-minted subject and exact input encryption. It retains the
original provisional material for the wire request; retries reuse the immutable
physical cutoff. Before minting a prepared terminal holder it checks reply
correlation, ordinary context, PG/epoch and a command index after introduction,
then compares the whole command against assembly from its retained manifest,
snapshot, logical input and reservation. The primary chooses the positive write
sequence and timestamp; the sender does not independently read those allocator
facts. All other command fields, including displaced payload and its timestamp
consistency, must match. Preparation failures return the original witnessed
custody. Lost responses or closed/reopened sections cannot create installation
confirmation, release reservations or delete files/acknowledgements.

Remote terminal installation now has separate bounded DTIQ/DTIC framing carrying
the original preparation and exact complete command, with request-SHA-256-bound
confirmation. Candidate dispatch now admits installation only through an Active,
same-domain, held current-primary section. A private request-bound check rederives
the original snapshot digest, exact owner/window/reservation, next sequence and
complete terminal command inside the pending-slot transaction, including exact
retries. Existing slots must also retain their exact scope and every side index;
retry is not repair. The final effect checks the intersection of original client
and server route fences plus the work deadline. Lost replies leave the complete
slot and custody intact. Live dispatch remains gated; framing/error responses
alone confer no custody or absence proof.

Fresh remote Direct PUT installation must also match the receiver-minted
preparation identity retained by that held PG section: SHA-256 of the preparation
request and of the complete command, including server-chosen Last-Modified and
displaced reclaim timestamps. There is one fixed-size identity per section;
another preparation replaces it, and releasing the section discards it. Without
that identity, only an exact existing pending command in the insertion transaction
can establish retry origin; all scope/index/state checks still apply. Rebuilding
a command using its supplied timestamp alone is not proof of preparation. No
wire change, durable preparation record or additional fsync is required.

The candidate owning installation sender now retains that exact preparation
encoding beside the complete terminal submission. Its borrowed dispatch attempt
binds command SHA-256, preparation SHA-256, physical custody, original primary
session and immutable cutoff. It marks history Unconfirmed before dispatch (and
therefore before unwind); only a locally classified NotSent restores the prior
observation. Remote errors, transport ambiguity and malformed confirmations do
not establish absence. An exact response confirms Installed even if the peer
closes afterward; later retries validate authority but never redispatch or erase
that historical confirmation. Neither errors nor Drop delete payload or release
reservations. Consuming transfer to publication requires Installed.

Candidate fresh Direct PUT introduction can now be built from an admitted
embedded current-primary section. The node owner selects the actual PG and
privately binds construction authority to that store. One settled-prefix read
snapshot derives generation, command position, issuer, placement and the initial
descriptor; the fixed section deadline bounds capture alongside route authority.
Expiry/lifetime checks run again after snapshot teardown. The non-Clone result
retains complete submission custody and its original section, not a detached
command or descriptor. Capture/Drop do not mutate or release anything, and the
result is not physical-write authority. Its classified embedded installation
borrows only the original section/store and carries the original work deadline
and finite staging cutoff to SQLite's final pre-insert check. The same holder
retains complete bytes and acceptance history through errors and unwind; later
definitive rejection cannot erase uncertainty, and confirmed installation never
regresses. Confirmation is historical, not proof of current slot validity or
replicated acceptance. Confirmed installation can now move, without effects,
into a section-bound introduction publication owner. Failed handoff returns the
original holder and history. The sender retains the exact complete submission
and both deadline forms captured during construction; neither handoff nor retry
resamples them. It marks only the original primary through the existing trial-
apply/marker transaction. Lost confirmation and unwind remain Unconfirmed;
expiry cannot erase MarkerConfirmed. Marker success does not apply the owner,
grant physical writes, release reservations or retire the slot. The same holder
can now apply to embedded members of the cohort captured from its original
admission: designated witness first, primary second, then remaining replicas.
Shared fanout checks enforce ordering; each member's exact confirmation remains
inside the holder across lost replies, unwind and retries. All-member evidence
borrows that owner and rechecks its authority, and is not a terminal/physical
grant. Mixed Unix/TLS members now use that same owner and the exact clients
captured in its cohort; the original embedded primary section stays borrowed
through each remote attempt. Foreign clients and premature/expired attempts
fail before dispatch. Lost or malformed authenticated confirmations retain
Unconfirmed even when the remote apply committed; exact retries establish
confirmation without rewriting state. A consuming introduction terminal holder
now retains that entire section-bound owner after all-member confirmation;
failed handoff returns the same publication owner and uncertainty. Handoff and
Drop are effect-free and neither resamples deadlines. The holder now retires its
exact slot through the original embedded section, using a private borrowed
capability and the existing admitted transaction. Direct introduction retirement
also requires its exact retained owner, initial window/participants, live
creation proof and generation high-water fence—even on absent-slot confirmation.
Lost replies/unwind retain uncertainty; confirmed retries cannot remove another
slot or regress their history. Retirement preserves the durable owner and
resources and grants no window/physical authority. Confirmed retirement can now
consume into a section-bound window inspection, reusing the primary snapshot and
designated-witness prefix comparison. Failure returns the same terminal owner;
success retains it with the captured read and exposes only scalar observations.
Original authority is rechecked after inspection and cannot be renewed by retry.
Neither handoff nor Drop performs cleanup. The confirmed introduction can now
consume into file preparation under that original section, retaining the same
command history, window and cutoffs. Preparation exposes no writes. Remote
dispatch consumes the exact original section and synchronously releases its PG
guard before returning; a foreign or reopened same-PG section is not equivalent.
Failed release retains both preparation and the supplied section. The admitted
route remains pinned, and later physical effects independently recheck expiry.
The remote fresh builder now has a read-only receiver admission prerequisite
sharing that same storage-owned capture algorithm. It requires the server's
Active current-primary route, exact session/node/lock domain and actual store,
and intersects client and server cutoffs before capture. The prepared result
retains the original request/session/admission. A consuming response encoder now
uses a private borrowed capture scope; it exposes bounded descriptive complete
bytes, never an installable submission or independent descriptor. Logical V1
framing binds the exact request/command digests and conservative absolute cutoff.
The actual held session retains the prepared response identity and original
local deadline; release/reopen loses it. Receiver-local installation now consumes
that identity into one opaque holder. Its session remains claimed after errors
or Drop: duplicate claims and outstanding preparations cannot reset uncertain
history. Slot/index mutation reuses the complete pending transaction under the
original fence/deadline. Confirmed history never regresses; it does not certify
current slot health or authorize release. The holder can now consume into its
actual held session guard, moving the same submission/history while the original
request frame and admission borrow end. Later calls require fresh Active
admission, exact request/response identity and the original guard acquisition;
reopening the same scalar section is not equivalent. The original cutoffs remain
unchanged and state stays in the guard through errors/unwind. Parking after
expiry retains custody but grants no effect authority; release/Drop performs no
cleanup. The candidate preparation RPC now parks this same receiver submission
before replying. Its admitted Unix/TLS sender retains an opaque exact-session
preparation, verifies the complete reply's logical subject/proof, and cannot
reconnect or renew its cutoff. Preparation has no durable effects and exports no
holder after a lost or malformed reply. Ordinary dispatch remains unsupported.
Post-success response validation keeps the exact transport borrow armed: errors
or unwind mark the session unusable and shut it down, preventing inaccessible
parked custody from remaining on a reusable connection. This is invalidation,
not synchronous remote-release confirmation.
The distinct candidate installation RPC continues only that connection's parked
submission, binding the exact preparation request and response. It cannot
reconstruct custody after release/reopen. Exact retries keep installation history
and do not rewrite an installed slot. Its success confirmation hashes the entire
continuation request; lost replies leave durable pending state, never absence
evidence. Ordinary dispatch is still unsupported. The candidate frontend now
consumes preparation into a non-cloneable installation sender retaining the
original context, exchange, admitted route, client, session and cutoffs. Its
argument-free install operation cannot select a replacement session or deadline.
Each attempt records Unconfirmed before transport; only proven NotSent restores
the prior observation, and only the exact authenticated confirmation records
Installed. Remote errors, lost/malformed replies and unwind cannot clear earlier
uncertainty. Confirmed installation never regresses or redispatches, although
expired/closed authority still rejects further use. Drop performs no cleanup or
retry. Confirmed installation now consumes into an exact-session
publication-start sender, moving the same complete submission and installation
history rather than reconstructing confirmation from command bytes. Rejected
handoffs return the original owner. The transfer retains both original cutoffs,
even after expiry; it does not renew effect authority. Publication takes no
replacement session or deadline, preserves uncertainty on lost/malformed replies,
and cannot regress a confirmed marker. Publication-start changes only the primary
pending marker. The same owner now applies the introduction to the original
cohort, witness first, primary second, then trailing replicas. Remote targets
must be the admitted client instances; embedded targets come from the retained
route. Both paths keep the original primary transport borrow through commit and
confirmation, with no replacement session or deadline. Lost/error confirmations
remain Unconfirmed and block dependent ordering; exact replay confirms without
repeating effects. All-member completion is borrowed evidence only, invalidated
by expiry or primary closure. The publication owner can now consume into an
opaque terminal owner only after all original members are confirmed and the
original session/authority is revalidated. Failed transfer returns that same
publication history. Successful transfer performs no storage or network effects,
retains the original cutoffs, and exposes no reverse conversion or detached
sender. Historical confirmations remain observable after expiry/closure without
granting new authority; Drop does not clean or release anything. The terminal
owner now retires its exact introduction slot through the same original held
primary session. It supplies no replacement request, session or deadline, and
keeps the transport borrow through confirmation. The receiver validates exact
applied evidence and retained cleanup custody before removing only the slot and
its indexes; absence retries still validate custody. NotSent preserves the
previous observation, uncertain replies remain Unconfirmed, and Retired cannot
regress or redispatch. Retirement neither releases resources nor grants physical
write authority. Confirmed retirement now consumes into a window-confirmation
holder on the same original session, with no supplied deadline or replacement
client. Inspection keeps the primary borrow across witness and primary reads;
the handoff rechecks session/authority after the positive observation. Failure
returns the same terminal custody and retirement history. The successful holder
retains the complete history and private window observation together, exposes
no detached descriptor, and has no physical effects on Drop. Its consuming
physical-writer handoff now retains the original submission and exact admitted
route, validates provisional encryption, and reuses the shared same-session file
binding. The original primary section must acknowledge guard removal before the
writer is returned; timeout, malformed acknowledgement or lost session retains
opaque custody without permitting reconnect or cleanup on Drop. Binding does not
allocate another VID or renew either cutoff. Live frontend custody and publisher
selection remain gated.
The candidate embedded introduction now binds to the real Direct PUT request
scope before installation. It derives the exact proof and subjects from that
scope and requires its original admission/PG/primary. The owning wrapper borrows
request custody, blocks duplicate/legacy entry, and arms uncertainty before any
installation effect. Definitive first-attempt rejection permits normal release;
uncertain or installed history prevents request-return/unwind proof release.
Only confirmed installation permits transfer to the existing publication owner.
The state setters are private to that concrete introduction module. The equivalent
candidate Unix/TLS request holder derives the same exact scope subject before
transport and retains the original session, cutoff and classified sender history.
Lost or malformed installation replies cannot release the frontend proof;
confirmed installation alone permits the consuming publication handoff. Neither
holder exposes an independently installable preparation while frontend release
remains possible. Complete frontend workflow selection and autonomous cleanup
remain separate gates; the ordinary live path is not switched by these adapters.
The released-primary candidate writer can consume into a size-bound encryption
preparation. Its borrowed subjects come from the retained window, not mutable
request metadata. Actual SSE-C/SSE-S3 terminal sealing returns process-local
committed-subject evidence; existing callers may consume it into durable state.
Candidate ciphertext binding keeps that evidence, the exact codec-sized owned
buffer and original writer custody together, without file/ack effects or Drop
cleanup. The encrypted owner now dispatches through the existing shared EC
encoder and file/acknowledgement gates without allowing replacement bytes or
deadlines. It validates all transport choices before the first write, retains
partial/uncertain attempts on failure, and keeps encryption evidence and CRC
attached after acknowledgement persistence. A consuming acknowledged/inspected
wrapper carries those exact inputs through the existing snapshot and terminal
builders; reinspection discards only old metadata facts, not custody or the
original deadline/session binding. Terminal preparation rejects caller encryption
that differs from the retained committed evidence and takes no replacement CRC.
A single-attempt decision entry validates the original section/session and
deadline before invoking the request's logical condition callback. Request
errors remain distinct from storage errors, and accepted decisions still require
terminal snapshot/authority validation. It performs no internal contender drain
or retry. Request conditional-loop orchestration and live selection remain gated.
Candidate request-chain canaries now carry real encrypted bytes through terminal
installation, mixed-cohort publication, reservation release and slot retirement.
They require same-owner confirmation after an uncertain installation, refuse
terminal transfer after partial apply, and decrypt reread shard bytes with each
replica's persisted encryption metadata. These tests connect existing handles;
they do not yet supply the production budgeted owner loop.
The encrypted acknowledged-payload handoff now captures the existing Direct PUT
terminal-work time budget once. It narrows both deadline representations and the
effect fence retained by the owner. Reinspection and installation retries reuse
those exact bounds; the SQLite pre-effect check receives the shortened fence.
The same opaque work budget now travels through encrypted inspection,
preparation and installation owners. Fresh inspection and installation each
charge an attempt; decisions/preparation check time without resetting the
counter. Reinspection and uncertain outcomes retain it. Confirmed installation
now carries that same budget through closed publication, per-replica apply,
reservation-release and slot-retirement methods, each charging an effect attempt
before delegation. Partial apply and failed terminal handoffs retain the counter
with custody; no unbudgeted owner is extractable. These are effect-attempt units,
not outer-loop iterations or a new fixed attempt maximum. Full request-loop
orchestration and backoff remain unconnected.
The encrypted publication owner also offers bounded advancement: each call
confirms only the marker or one cohort member, selecting the witness before the
primary and then remaining members deterministically. Unconfirmed work remains
selected on retry. Remote connections cannot select a different next node.
Once complete, readiness is a deadline/section-checked observation with no effect;
the existing consuming handoff still proves terminal authority. There is no
internal retry, contender drain, proof release or slot retirement in this step.
The acknowledged encrypted owner can capture one exact contender under the
admitted embedded primary section, retaining it with the original payload and
budget. Only a clear-slot observation returns to snapshot inspection; a present
contender cannot be detached or discarded through that transition. Historical
slots fail closed rather than appearing clear. Capture itself is read-only.
A quiescent contender can then consume into a bounded embedded drain: one marker
or witness-first replica application per call, followed by all-member handoff
and a separate exact-slot retirement. The request's payload, deadline and budget
remain attached throughout; only confirmed retirement returns that payload for
reinspection. Non-quiescent contenders fail closed before publication. The drain
never selects a second command or changes the remote/live request-loop gates.
After a drain, snapshot inspection must reconfirm the same durable window at
the new primary/witness prefix. Only that observation changes; the original
physical descriptor, issuer, deadline and effect fence do not. The no-contender
path does not perform this additional read-only confirmation.
Cold embedded prefixes use the existing bounded read-only validator before that
confirmation, not startup replay or durable repair.
Encrypted terminal preparation now retains committed-encryption evidence through
installation. A consuming reinspection handoff may discard the prepared command
and snapshot only when no acceptance was observed; it retains acknowledged
payload custody and the original session/deadlines. Unconfirmed or Installed
history cannot take this path. Fresh inspection and request evaluation are
required before preparing again. Confirmed installation alone permits transfer
into the existing publication owner.
Admitted Direct PUT reinspection explicitly revalidates a cold prefix in a
read-only snapshot, including after definitive installation rollback. It shares
startup's history/state/index checks but performs no tail linking, cache repair
or durable mutation. Corruption and unresolved pending custody still fail
closed; the original admission/deadline is rechecked before returning facts.
Cold validation additionally shares a finite entry/row/byte work allowance
across its phases and checks the captured work/route cutoff within the scans.
Oversized suffixes fail without seeding the cache; opportunistic checkpoint
cadence is not a foreground-work bound. This path does not run maintenance to
make an oversized history fit.
This does not enable the live retry loop or change the deferred cache design.
Storage validates subject/geometry correspondence, not cryptographic
payload authenticity without the frontend's key.
Preparation success is not installation confirmation, and a
publication marker alone is not applied introduction state.

Candidate remote Direct PUT publication-start now borrows the original primary
session through the classified marker RPC. The complete command, witnessed
payload, introduction history and original absolute/monotonic cutoffs remain
in the same owning holder. Wrong clients, reconnected sessions and expired
authority fail before dispatch. NotSent restores only the previous observation;
lost replies, invalid confirmations and unwind retain Unconfirmed, while later
errors cannot erase MarkerConfirmed. Trial apply/rollback validates terminal
effects but publication-start commits only the marker, not object references.
Remote-prepared custody cannot switch to embedded publication or the generic
terminal handoff that would discard its session identity.

Candidate remote Direct PUT application retains that same primary-session borrow
through each exact replica confirmation. Targets must be the admitted cohort's
exact clients; marker, witness and primary confirmation gate the subsequent
roles. The primary uses its held connection, while other remote members use the
cohort-bound application transport. Unconfirmed responses block dependent roles;
exact replay does not repeat object/reference mutations, and later errors cannot
erase confirmed application. Files and acknowledgements remain retained; apply
only transfers durable object-PG reference/resource custody. Local retention of
the primary borrow excludes concurrent local transport/shutdown, not independent
remote failure.

Candidate Direct PUT apply also supports embedded replicas under that original
remote primary section. The closed bridge selects an actual embedded member
from the captured cohort and retains the session borrow through PG admission,
transaction commit and confirmation. The same role ordering, immutable cutoffs
and per-member history apply; no caller-supplied runtime, command or mutation
callback can substitute for the retained authority. Exact local replay is
non-mutating and failed confirmation does not detach physical custody.
After every cohort member confirms application, a consuming session-bound Direct
PUT terminal handoff retains that original session identity, all confirmation
history and both cutoffs. Exact bucket-proof release holds the original live
object-primary session borrow through the separate retained bucket-route effect.
Pending-slot retirement requires confirmed release; ambiguous replies retain
Unconfirmed custody until exact retry confirms. Neither handoff nor Drop deletes
payload files or acknowledgements. Reconnection, deadline renewal and generic
terminal extraction remain unavailable. Live publisher selection remains gated;
no new wire format, transaction or fsync is introduced.

The candidate remote primary also supports a distinct, read-only Direct PUT
file-binding inspection. On the already-held current-primary connection it
validates the exact retained window, live original issuer and fixed generation
reservation in the same read snapshot as the settled prefix. The response is
bound to its purpose, route/cutoff, descriptor digest and designated witness;
ordinary window-inspection success is not interchangeable with it.

The candidate consuming remote Direct PUT binding now invokes that inspection
on the exact original primary client/session. Confirmation retains an opaque
process-local session identity, distinct from client/endpoint/node/PG/epoch.
Binding compares it under the held transport mutex before sending: closing the
original section and reopening through the same client cannot substitute a new
session. The identity's allocation remains retained after close, preventing
address reuse from matching an old confirmation. Session-mutex acquisition,
inspection and release acknowledgement use the retained deadline; the encoded
cutoff is the exact effective value sent during window confirmation, including
route intersection. The confirmation operation returns that value and its local
deadline to the holder; independently projecting the same `Instant` can differ
even by a millisecond and is not equivalent. Neither binding nor stream
allocation recomputes it. The returned prefix must
equal that captured observation. Before returning a writer, binding consumes the
held borrow through acknowledged remote release and revalidates the captured
fences. Success retains the complete custody in the file writer; failure retains
it in an opaque failure holder with no reset or reverse conversion. Neither Drop
path releases generation ownership. Binding itself adds no allocation or durable
mutation. The resulting remote physical-effect API remains candidate-only;
live dispatch still rejects the candidate grammar.

Remote stream allocation uses the same retained-session check for streamed PUT
and UploadPart. The opaque allocation attempt validates the confirmation's
session identity under the transport mutex before request-ID allocation or
network IO. A new session through the same client cannot consume the retained
window's cursor. Rejection keeps the original custody/history and leaves the
allocator and retained owner unchanged, including after dropping the failure.

The candidate remote stream-file receiver prerequisite is also read-only. Its
distinct `SFPI`/`SFOK` exchange binds the original allocation description and
returned segment. Under the admitted current-primary section it checks one
settled-prefix/window read snapshot, the original issuer, exact session and
generation authority, and the consumed one-slot cursor for streamed PUT and
UploadPart. It neither allocates again nor grants physical authority. Live RPC
dispatch rejects this candidate grammar. Consuming remote stream file binding
now retains original allocation custody and session identity. It derives the
inspection from its private allocated segment and captured session, borrows the
original transport section with the original deadline, and checks the captured
session identity before sending. It then confirms release of that exact primary
section under the original deadline and revalidates authority before consuming
the holder into a dispatch-ready file writer; any failure retains it without reset,
retry, reconnect or cleanup on Drop. Exact reply-digest validation binds the request's original cutoff and
complete allocation context. The observational reply cannot independently mint
a writer, and the writer still exposes no remote physical-write API.

Dispatch-ready Direct PUT and stream writers are distinct opaque types from
ordinary embedded writers. Remote-primary binding requires acknowledged release;
embedded-primary binding consumes its actual admitted section, validates while
held, drops the guard synchronously and revalidates the captured fences before
returning dispatch-ready custody. A caller cannot detach the underlying writer,
convert an ordinary embedded writer, or substitute a borrowed/foreign section.
Failure retains the original owner/allocation without reset or cleanup on Drop.
These types still expose no remote physical effects; ordinary embedded file
effects are unchanged.

The candidate Direct PUT receiver now derives its exact file key/location and
encrypted geometry from the retained descriptor under Active admission. It
independently confirms the live issuer/reservation and settled window through
primary/witness IO, acknowledges release of primary serialization, then uses the
existing write-if-absent primitive with the original intersected route/request/
window fence checked inside file exclusion. It returns a file acknowledgement,
not an acknowledgement-row commit. Streamed PUT and UploadPart now share that
candidate file path, with an additional read-only primary inspection of the
exact allocated segment, captured session/generation, consumed cursor and live
issuer under the same held section. The receiver binds that inspection to its
window, topology, prefix and original cutoff; it cannot substitute a second
description or use inspection to allocate again. Peer file inspection uses the
read-only metadata-state kind with a file-only grammar; the frontend allocation
kind remains frontend-only. No live shard RPC selects these
candidates yet; remote acknowledgement persistence and live frontend dispatch
remain gated.

Receiver-side confirmation now has candidate process-local peer routing, bound
to the exact installed runtime configuration and a storage-node RPC credential
accepted by its configured verifier (exact identity lookup and local HMAC
verification, not just node/cluster/topology equality). A complete-map refresh
publishes config and directory together; a
config-only install clears the directory. Selection rejects stale captures,
foreign/non-Active admission, expiry and Peering routes, and derives the primary
and lowest-ID witness from the receiver's map, never from request endpoints.
This selection is not confirmation or effect authority. The candidate independent
receiver lookup now opens its own bounded primary section, observes the designated
witness and exact primary window, compares the complete prefixes, and closes the
section through an authenticated, bounded release acknowledgement before returning
a scalar observation. Local socket shutdown alone is not release evidence; a
missing, rejected, malformed or late acknowledgement cannot confirm the lookup.
It rechecks captured route, window
and work cutoffs after each exchange and teardown; it never reconnects, retries or
chooses another witness. The original absolute wire cutoff is only shortened.
Local IO waiting limits remain separate from portable authority: exporting a
one-second IO timeout with a one-second skew deduction would expire immediately.
Candidate Direct PUT physical dispatch now releases the frontend primary section
before sending and retains complete custody across uncertain outcomes. The
candidate streamed PUT/UploadPart sender does the same after successful allocation
and acknowledged primary-section release. The candidate installed receiver
performs confirmation before data/file
locks and rechecks original cutoffs inside physical exclusion; no new confirmation
files, cache, signing protocol or persistence barriers are added.

Live remote file/acknowledgement selection, frontend encrypted-payload/witness
integration and owner-driven cleanup remain separate gates; live selection is
still disabled.

The coordinated cleanup-ownership implementation also has an explicitly
test-selected **complete-command startup** path. It opens each PG under the
exclusive storage-node root guard and validates/repairs its complete prefix
before returning that store. Bounded startup workers must all succeed before
node construction and listener binding; failure joins the workers and closes
the opened stores. Repairs are atomic per PG, not across the whole node.
Complete startup preserves exact pending custody (including terminal and
historical slots) and reads each PG's stored epoch independently of the process
configuration. It neither falls back to the current command decoder nor grants
publication/cleanup authority. The candidate managed-bootstrap path carries the
same private startup selection through heartbeat inspection and preparation to
ordinary prepared-server bind, including its complete log handlers. Bootstrap
heartbeats reuse the grammar-neutral pending identity/route indexes; they do not
decode a command into a second, detached custody holder. Exact pending custody
and required route history survive this handoff even when the process no longer
belongs to the pending PG's current acting set. The exclusive root guard stays
held throughout; the bootstrap node closes before bind reopens its stores.
Existing incarnation advancement and runtime-config persistence are unchanged.
All production startup entry points, including managed bootstrap and
local-cluster startup, still select the current grammar until the coordinated
protocol switch; complete selection is owner-test-only.

Candidate complete pending inspection shares one request-bound owner adapter
between embedded reads and installed authenticated Unix reads. It validates the
actual slot, exact scope/indexes and local terminal relationship in one SQLite
snapshot, even when the requested epoch differs from that slot. The returned
opaque observation retains the complete context and publication-start flag;
production access exposes only scalar diagnostics, not a raw envelope or a fresh
submission. A response for another PG/epoch, malformed flags, bad integrity or
current-only command bytes fails closed. An absent response is only absence for
that inspected epoch, not global absence, convergence or permission to release
custody. The existing current handler remains selected in production.

The candidate applied-peer probe borrows that complete pending observation and
checks its topology before transport. Both embedded and installed Unix reads
compare one exact complete log entry or compacted receipt in a pinned snapshot,
including bytes/SHA-256 and the command-bound hash link. An abandoned/different
record conflicts; absence means only that this peer retains no exact applied
evidence. The result remains tied to the source observation and is neither a
full-prefix proof nor recovery/cleanup authority. Response validation binds
conflict identity and hash linkage; malformed replies are not retried. Candidate
request capacity is admitted at framing, but production command decoding remains
current-only until coordinated activation.

The candidate abandoned-peer probe borrows the same observation and performs an
exact single-entry/receipt comparison. It returns true only for the matching
complete-command tombstone (including its original-command SHA-256), false for
an exact applied record or missing retained evidence, and an error for divergent
or corrupt evidence. False does not prove nonacceptance. Both paths validate the
terminal hash link within a pinned snapshot; neither is a full-prefix proof or
permission to abandon, release custody or delete payloads. The Unix response
validator rejects malformed booleans and crossed conflict subjects without
retrying. The complete handler remains owner-test-selected until activation.

Candidate complete acceptance inspection retains the same pending observation
and separately binds the application or abandonment question. A pinned snapshot
checks the existing materialized-state digest/revision, replica epoch/position
and exact terminal identity; exact terminal observations also check the hash
link. `NextPosition` and `ExactTerminal` are observations, not permission to apply,
abandon or clean up. Historical route admission does not override a stale replica
epoch. Missing old-position receipts, opposite terminal outcomes and corrupt or
crossed evidence fail closed. The digest check can recompute metadata on revision
change; the API does not claim constant work or full-prefix authentication. Only
owner tests select the complete RPC handlers before coordinated activation.

The candidate complete publication-start store boundary requires an opaque
current Active-primary or exact map-authorized historical-primary authority,
bound to the complete command SHA-256, topology,
locked store, exact primary serialization section, captured route lease and work
deadline. Minting validates both the permit's exact admission-domain identity and
its `Active` class; a foreign or retained-cleanup permit cannot supply authority.
In one transaction it validates the exact slot/scope/protection indexes,
trials complete application under a savepoint, rolls back those effects, and marks
the exact slot publication-started. Exact-applied replay authenticates terminal
evidence without repeating application. Authority is checked at the effect and
commit boundaries. Unconfirmed rollback retires the connection. An error never
grants abandonment or cleanup, including a lost outcome after marker commit.
Embedded and Unix/TLS historical admission are implemented on this candidate
path. It remains owner-test-selected: live publisher selection and frontend
custody are not yet integrated.

Candidate complete replica application now has a closed receiver-side entry.
It requires an Active permit from the exact server admission domain, current
epoch/Active acting-set membership, and PG serialization from that server's
lock registry. An existing primary session must carry the exact current
PG/epoch binding; a historical section cannot substitute. Opaque authority binds
the locked store, target node, complete-command SHA-256, topology, captured route
fence and immutable work cutoff. Both lock waits are bounded. The complete
apply-and-record transaction rechecks authority immediately before fresh mutation
and before commit, including exact replay. Replay performs no owner/session
transfers or log writes. Rollback uncertainty retires the connection; errors,
including post-commit interruption, never authorize cleanup. This complete
apply-and-record entry's raw adapter and mutation callbacks are test-only.
The candidate Unix/TLS receiver additionally admits the exact historical command
reported by the current map. Its PG/epoch/index/CRC64/complete-command SHA-256 must match that report,
whose reporter must be the retained Active route's primary. The target must be
in that historical acting set with a current or retained Peering route; absent,
crossed or unbounded authority fails closed. The reporting primary must retain
its exact historical-recovery session; replicas acquire local serialization and
cannot borrow an unrelated primary section. The bounded current-map fence and
receiver-bound work cutoff cover lock waits and the same transactional effect and
commit checks. Complete-command identity, uncertainty and no-write replay remain
unchanged. This entry supplies receiver admission; historical embedded fanout is
described below and live selection remains gated.
The candidate embedded equivalent borrows its target from a current admitted
primary section. It selects only an embedded member of that admission's Active
acting set and keeps the original shared PG serialization guard alive; it does
not reacquire that guard on a replica. A private node-owned authority binds the
selected runtime/store, target, complete command identity, topology and original
route/section/request cutoffs to the same apply-and-record transaction. Historical
sections require the separately report-authorized selection described below.
Exact replay, expiry at the effect/commit
boundaries, and rollback retirement use the same checks as the remote entry.
The request remains an opaque, deadline-bound view; raw construction and callbacks
are test-only. The candidate owning sender now uses this embedded entry without
encoding/decoding a second wire request: its internal application projection
borrows the retained command and frozen cutoff. For an embedded primary, fanout
accepts only a section derived from its exact original admission, PG and epoch,
and selects each target inside that section. Embedded and remote sends share the
witness-before-primary/primary-before-trailing rule and all-member completion
check. Per-peer uncertainty is recorded before entering the node operation;
unwind, lost post-commit confirmation and later errors cannot erase installation
history or prior application evidence. Dropping/reacquiring serialization cannot
renew the holder's cutoff. Failed terminal handoff returns the same holder;
successful handoff transfers its histories without cleanup on Drop. This does
not activate live frontend custody. Historical embedded fanout is described below.
Quiescent embedded slot
retirement uses the same holder and original admission as described below.
Owner tests now cover an embedded primary with both authenticated Unix and TLS
peers in the same cohort, rotating the witness among the three transports. They
pin all-member state agreement, no-write exact replay, lost witness/trailing
replies, embedded-primary confirmation loss and captured-route expiry. Remote
members cannot substitute local fixture stores. The opposite topology now uses
a closed bridge from the same admitted cohort and exact registered remote-primary
client/session to an embedded nonprimary member. The opaque target borrows both
the route and the session, holding the session mutex through local application;
no concurrent RPC/shutdown or detached runtime/fence can substitute. Lock order is
session mutex then local PG mutex; no network IO runs under the local PG lock.
A busy session is acquired with deadline-aware probes, bounded by the minimum
of the attempt, original frozen work and captured route cutoffs. Timeout before
acquisition preserves application history and does not close the other holder's
session; checking only after a blocking mutex wait is insufficient.
A transport/protocol exchange failure or authenticated peer-declared connection
closure revokes the session, so it cannot later authorize embedded application.
This borrow is not proof that the remote process
cannot fail: exact primary confirmation and all-member completion remain required.
The existing owning sender preserves ordering, original deadlines and per-member
uncertainty across both directions. Reacquiring a remote session does not renew
the holder's work cutoff. Historical application under an embedded primary now
borrows its report-authorized section and selects only historical Active members
with current or retained Peering placement. It checks the primary-reported
complete-command SHA-256 before store acquisition and at effect/commit, not just
against the local pending slot. Targets retain the original admission and shared
serialization; captured route, section and work cutoffs cannot be renewed.
Historical remote-primary/embedded-replica application uses a separate
receiver-only route, not the current fanout capability. It derives the same
report and Active/Peering membership from the authenticated admitted map, and
borrows the exact registered primary client's open historical session through
the local transaction. The route-authorized opener selects the reported epoch
without substituting an old-topology client; raw epoch selection is client-private.
The report's complete SHA-256 is checked before the PG
wait and again at effect/commit, including replay. Session acquisition is
deadline-aware; captured route and target cutoffs can only narrow the original
sender's work cutoff. Failed exchanges revoke further use of that session.
This individual receiver path cannot establish all-member completion or retire
the pending slot.

The historical fanout owner instead requires the **complete** original
Active cohort. All members must have current or retained Peering placement;
it never filters away an unavailable or ineligible member. The cohort can mix
embedded, Unix and TLS endpoints under either primary type. Remote members use
the exact clients registered in the admitted map, never their local shadows.
Binding validates the report's exact ID/CRC64/complete SHA-256, keeps original
custody and cutoffs, and admits only the primary section from that same admission
and report. The canonical witness must confirm before the primary, then trailing
members; lost replies and restart require exact confirmation without replaying
effects. Consuming all-member completion permits historical handoff for quiescent
work or independently releasable command/source reservation work. Other
non-quiescent handoffs still reject and return the same owner. Proof release
retains the exact command, any abandoned source and all custody; by itself it does
not permit physical cleanup or non-quiescent slot retirement. UploadPart creation
additionally uses the exact durable-custody check described below, including on
historical retirement. The owning sender selects
the historical primary session epoch through its current registered client;
only the client owner can invoke raw epoch selection. Opening/reopening cannot
extend the original work/route cutoff. Historical replica sends similarly require
the opaque cohort and exact reported command digest before transport; ordinary
sends retain their client-epoch check. Remote-primary embedded targets retain
the exact report and check its SHA-256 before store access and at effect/commit.
Both session borrowing and deadline-bearing RPC sends use bounded mutex waits.
The immutable PG identity is stored outside the mutex so subject validation
cannot block before the deadline-aware wait. A pre-send timeout preserves the
holder's history and leaves the other session user undisturbed.

Historical quiescent retirement borrows that same terminal holder and its original
primary section. The remote receiver independently requires Active admission in
its own gate, the exact historical primary lock/domain and the control-plane
report's ID/CRC64/complete SHA-256. Its current route lease bounds historical
authority; the fence captured before the PG wait survives same-epoch renewal and
is checked immediately before DELETE and COMMIT. Embedded retirement retains its
original report-bound section through the same transaction. Both paths require
exact Applied evidence and all pending indexes/scope, preserve materialized owners
and log evidence, and confirm exact absence without rewriting state. A lost reply
keeps terminal custody unresolved; retry cannot refresh its cutoff, and confirmed
retirement never redispatches against a newer slot. This retires only the slot,
not owner/resource obligations. Historical command-proof release and the separate
source-bearing cleanup finalizer release use each proof's exact original bucket PG/epoch,
not the current bucket primary or the command's object PG. Durable discovery must
report that exact route for node refresh. Lost replies retain uncertainty and
exact absence confirms retry, including after restart; the original cutoff still
bounds the SQL deletion. This does not erase the retained resource evidence or
complete the command's remaining work. PUT stream creation retains its live proof;
ordinary UploadPart abort has no command proof to release. A successful release
does not change the original non-quiescent retirement classification. Physical
cleanup and other non-quiescent historical work,
reissue and remote-replica/follow-up abandonment handoffs and live dispatch remain gated.

Candidate embedded and Unix/TLS unstarted abandonment consumes the original fanout holder,
retaining its complete context/source and installation uncertainty. It rejects
observed or uncertain publication/application history. The same admission and
primary serialization reach the transaction, which independently requires exact
pending bytes/indexes/scope, no publication marker and no applied evidence.
Current and report-authorized historical primaries can record only the exact
SHA-256-bound tombstone. Route and work cutoffs are rechecked before the first
write and COMMIT; retries cannot extend them. Lost confirmation retains custody;
an existing exact tombstone confirms without writes, while failed rollback
retires the connection. This operation leaves all pending protection, resources,
owners and physical payload untouched. Its confirmation is primary-only, not
permission to retire a slot, release a proof, abandon replicas or begin cleanup.
The remote receiver admits its own Active route permit and exact held primary
lock/domain, with the complete report digest required for historical commands.
It binds the mandatory conservative absolute wire cutoff before decoding and
intersects the server route fence captured before waiting for the PG. Both reach
the common transaction's INSERT/COMMIT checks. The owning sender preserves its
history across retries/reopened sessions and accepts only an exact
purpose/node/command-ID/SHA-256 confirmation. Live dispatch remains unselected.

The owning holder can subsequently lend a separate, private replica-abandonment
permit for an embedded member of the captured cohort. Both an embedded primary
section and an exact Unix/TLS primary session can supply the retained
serialization, including report-authorized historical routes. The permit and
store-bound receiver authority remain distinct from application/publication
authority. Only their closed operation can record the replica tombstone; no raw
store, target or mutation callback can replace them in production. Absent
pending state requires absent sidecars, and a present slot requires exact
context/index/scope and an unset publication marker. Applied or crossed terminal
evidence fails closed. Both immutable cutoffs reach the final write and COMMIT;
failed rollback retires the replica connection. Confirmation is per member and
does not retire pending custody or release owners/resources. The consuming
follow-up transition and live selection remain gated.

The candidate remote replica receiver is independently admitted, not an adapter
to primary abandonment. Its `RABD` request names the exact configured primary
and a distinct replica. Historical admission additionally requires the reported
complete-command SHA-256. Active admission and locally owned serialization bind
the receiver authority to its PG/store; a preheld primary section is rejected.
The wire cutoff is rebound before context decoding and intersected with the
server fence captured before either lock wait. Both are checked at tombstone
INSERT and COMMIT, including exact replay. The receiver preserves all pending
protection and materialized state and confirms only its own exact tombstone.
Primary-confirmed custody belongs to the closed owning sender; it is not
proved by an arbitrary request naming a primary. Installed Unix/TLS tests select
the receiver through a test-only switch; live dispatch rejects its grammar.

That owning sender now supports remote replicas without exporting command or
primary-confirmation custody. The private attempt retains the original holder
and exact primary serialization through its exchange. Acquiring a remote primary
session borrow is bounded before waiting, including the original work/route
cutoffs; another RPC cannot extend them by holding the mutex. The attempt binds
encoding to its own transport and validates the exact registered replica client.
Ambiguous replies preserve per-member uncertainty, later pre-send failures do not
erase it, and exact confirmations bind both nodes and the complete identity.
No confirmed member is redispatched. Reopening a session does not reset holder
history; restart creates observations only by reauthenticating/reinspecting
durable primary/replica tombstones. None of these observations independently
authorizes slot retirement, source extraction, release or physical cleanup.

The consuming abandonment handoff requires exact confirmation from the primary
and every captured replica while the original authority is valid. It preserves
the entire inspected/submitted holder, including any installation uncertainty,
source-bearing context, historical cohort and frozen cutoff. Incomplete or
expired conversion returns that same holder; neither failure recovery nor a
successful conversion resets its deadline. The private child-owned result has
no reverse conversion, raw context/submission projection or cleanup-on-Drop.
It is retained custody only. Current embedded-primary UploadPart follow-up
preparation consumes it under the exact original admission and primary lock,
rechecks complete source/slot/index/tombstone identity in one read snapshot,
and keeps the original holder beside the unsent source-bearing cleanup. The
successor position comes from that snapshot's current prefix, not a caller's
command ID. Authority is rechecked after snapshot completion; failure returns
the original custody. No SQL mutation or new fsync is introduced. Historical
source admission cannot mint a current successor. The prepared pair now lends a
private, exclusive installation attempt to that same current embedded-primary
section. Atomic replacement revalidates the source tombstone, scope/protection
indexes, captured prefix and freshly loaded cleanup closure in its write
transaction. The original authority reaches the final pre-write and pre-COMMIT
checks. Entered errors and unwind preserve uncertainty beside both complete
commands; unconfirmed exact retry validates indexes without resetting publication
state. Confirmed installation never regresses: retry checks supplied authority
but does not redispatch. Confirmation records the earlier installation, not
current slot ownership or permission for a later transition.
Rollback failure retires the connection. This is pending custody only, with no
resource release or physical cleanup, and adds no ordinary-write command/fsync.
The same installed pair can now publish under its current embedded-primary
section using the shared trial-apply/marker transaction. The closed request
projection borrows the original deadline, and the deepest boundary rejects a
byte-identical request from a different deadline owner. Only the marker commits;
the trial's session/owner/proof mutations roll back. Publication uncertainty is
independent of confirmed installation and original source history; no error,
unwind or retry detaches those responsibilities. Confirmed publication never
regresses or redispatches. The pair can then apply its cleanup through closed
embedded or authenticated Unix/TLS targets from that exact cohort, in
witness/primary/trailing-member order. It retains both commands and the original
deadline while tracking each
member independently; an entered error/unwind cannot erase uncertainty, and a
failed confirmed retry cannot erase application confirmation. Replica apply uses
the existing complete-command transaction and exact replay, not a rebuilt
ordinary sender. Source and resource custody transfer atomically on each member;
the primary pending slot is not retired and no physical cleanup is authorized.
Remote targets require the exact registered client identity and cannot substitute
an embedded shadow. The closed send retains the original pair and cutoff through
an exact authenticated response; unsent and ambiguous failures preserve the
appropriate previous custody observation without automatic transport retries.
The all-member handoff consumes the entire source/cleanup pair, requiring the
original live authority and confirmed installation, publication and application
on every original member. Failure returns that same owner; success retains its
exact contexts, histories and cutoff in a private terminal representation. This
move performs no SQL, RPC, release, deletion or slot retirement. Scalar terminal
work is diagnostic only, including the retained finalizer proof for empty
aborts. The terminal owner cannot be reversed, cloned or converted to an ordinary
sender. Its closed retained-finalizer reservation release borrows the whole pair
and reuses the exact-proof release protocol with the same admitted map and frozen
local/wire cutoff. Entered failure/unwind remains unconfirmed, including a lost
reply after deletion; exact absence retry can confirm it. Confirmed release is
non-regressing and is not resent. Release touches only the exact bucket-primary
reservation, leaving owner-resource obligations, payload and pending-slot custody
intact. Non-quiescent retirement for this pair, and historical/remote-primary
preparation, replacement and publication, remain unresolved activation gates.

Fresh terminal custody diagnostics reload the exact source-bearing cleanup under
the original embedded-primary admission and a private read snapshot. Both complete
terminal identities, the owner/window structure, generation fence and retained
finalizer proof must validate; absence of live session rows alone is insufficient.
An unpublished owner's window count must equal its abort segment count plus one
lookahead, even for an empty manifest.
Published-owner windows and retained payload references must also agree with the
authenticated source finalizer's full segment manifest, not only with each other.
Completion/reclaim destination transfers preserve that exact payload binding.
No durable row is repaired or removed. Returned counts are diagnostic, not a
retirement certificate: later mutation must establish its own authority, reference
and physical-exclusion conditions in the applicable transaction. Successful
bucket release does not erase the owner's durable release obligation.

Owner tests also select the individual receiver through authenticated Unix/TLS RPC. The
application-purpose-tagged request carries the complete context and a mandatory
absolute conservative wall cutoff, shortened by the sender's skew budget and
bound once to receiver monotonic time before context decoding. Projection cannot
renew it; the server work budget and route lease can only shorten it. Success
binds node, exact command ID, complete SHA-256 and replica state. Lost replies,
bad authentication and crossed confirmations do not license cleanup; exact retry
checks the same terminal identity without replaying effects, including after
restart. Production dispatch still uses the preceding grammar and rejects the
candidate. The owning publication holder now lends closed current-replica
application attempts, including on the held primary session, only after exact
publication confirmation. It retains the original context and installation
history, reuses the first publication cutoff, and records each peer's application
history independently. Unsent retries cannot erase earlier uncertainty; exact
confirmations survive later errors. Per-peer observations grant neither
acting-set completion nor cleanup/retirement authority. A consuming, pre-attempt
bind now retains the exact admitted Active cohort and registered client identities,
including the originating primary-session client. It enforces witness-before-primary
and primary-before-trailing confirmation, narrows the frozen route/work cutoff,
and lends all-member completion evidence only from the original holder. The
evidence borrows admission and must still pass its captured fence; renewed raw
routes cannot revive it. A consuming terminal handoff retains the original
holder, context and histories; failed handoff returns that same holder. Its
scalar work observation preserves the source-bound finalizer obligation and
separates immediate command work from durable reclaim. Scalar observations grant
no resource authority, including when command work is empty. The holder now
provides a closed command-proof release operation: it derives the exact proof
from the retained complete context, validates its command subject, and selects
the proof's historical bucket-PG primary from the original admitted map. It
retains the first local and absolute wire cutoff across retries, including wall
clock changes, and records uncertainty before entering dispatch. Only a successful
exact release/absence response confirms this operation; conflicts and lost
responses remain unconfirmed. A live PUT stream keeps its creation proof.
Neither handoff nor Drop performs effects. A separate closed operation now releases
the abandoned finalizer proof carried by a source-bound UploadPart abort. It is
eligible only after the complete source-bearing cleanup has converged on all
members, using the same original deadline and full-context identity. Its release
history is independent of the abort's own proof disposition; an ordinary abort
cannot infer a finalizer from session absence or owner rows. The original source's
epoch selects the bucket release route even after cleanup reissue. Both operations
leave conservative durable release evidence intact, with no extra bookkeeping
transaction. Neither observation proves all owner resources are released:
described Direct PUT and other retained obligations still need their owner protocol.
A local exact-slot retirement transaction now requires both the terminal holder's
all-member permit and admitted primary authority, rechecking complete Applied
evidence, bucket scope, marker, indexes and the original cutoff. Direct local
invocation remains test-only; commands without immediate
payload/release/reclaim work qualify, plus the narrowly checked UploadPart
creation handoff described below. It removes only pending custody and never
owner records or resources. Errors/unwind retain uncertainty, exact absence
confirms lost outcomes, and unconfirmed rollback retires the connection. The
terminal holder also lends a closed retirement transport attempt, bound to the
original primary client/session and frozen deadline. Purpose-tagged acknowledgement
validation binds the complete SHA-256 and exact node/position; unsent attempts
preserve prior uncertainty and confirmed retirement does not dispatch again. The
candidate Unix/TLS receiver now independently requires its exact Active admission
domain, current-primary session and lock registry, eligible complete context and
local Applied evidence. Wire framing is not proof of peer completion: that remains
the owning sender's all-member handoff guarantee. Decoding cannot mint the
receiver's private mutation authority. Its pre-lock captured server route fence
and receiver-bound wire/work cutoff are checked before DELETE and COMMIT. Exact
absence confirms lost outcomes without repeated writes, including after restart
and compaction; orphan indexes fail closed. The closed embedded operation now
borrows that same terminal holder's stack-only permit and requires a current-primary
section from its exact original admission. The node privately selects and locks
the store; publication authority alone cannot retire a slot. Both terminal and
node authority are checked at the same transaction boundaries, including absence
confirmation. The original local/absolute cutoff, shorter attempt/section deadline
and captured route fence cannot be renewed. Uncertainty is recorded before lock
acquisition, retained through failure/unwind or lost post-commit confirmation,
and resolved only by exact retry. Confirmed retirement never redispatches, even
if another slot appears; Drop does nothing. No extra transaction, fsync, wire
encoding or detached command is introduced.

UploadPart creation is the sole additional reservation-bearing retirement case.
The sender requires exact creation-reservation release confirmation; missing or
ambiguous release cannot dispatch retirement. The receiver does not trust a wire
claim that release succeeded. In the existing slot-deletion transaction it checks
the exact immutable owner subject, initial window and participants, creation
`ReleaseDue` proof (including historical bucket release PG/epoch), and generation
high-water fence. Those records remain durable retry responsibility after the
slot disappears. Validation also runs on absent-slot retries, uses bounded indexed
reads, and allows subsequent owner progress without mistaking it for loss of the
immutable introduction. Neither retirement nor confirmed release removes the
owner/resource row, grants physical access, or preserves a client session across
failure. All other reservation/payload-bearing terminal cases remain gated.

Only owner tests select these paths,
and live dispatch still rejects this grammar. Physical cleanup, other non-quiescent/owner
retirement, remaining non-quiescent historical terminal integration,
frontend payload/proof custody and live selection remain activation gates.

The existing retained bucket-proof release service carries the operation's
monotonic cutoff through SQLite writer admission, exact reservation inspection
and statement preparation, then rechecks it immediately before DELETE. Unix/TLS
uses the receiver-bound absolute wire cutoff at that same boundary. Expiry also
rejects an absent-row confirmation. Proof identity remains stable across lease
renewal; crossed proofs are conflicts and exact absence is idempotent. This
same service is used by the candidate terminal holder without changing the
transaction or authorizing slot/owner retirement.

Candidate publication payload preparation freezes an absolute conservative wall
cutoff once, never a remaining duration. The pending-evidence holder retains the
first local/portable bound: recreating request views or preparing another retry
cannot renew it, and a shorter attempt budget only intersects it. Receiver decoding
binds it once to local monotonic time; the borrowed request retains that cutoff
and authority can only shorten it. Forwarding also retains the original absolute
wire bound rather than renewing it from later clock samples. A successful marker
acknowledgement binds node, exact command ID and complete SHA-256/context, with no
apply or cleanup authority. The owner-local complete-protocol selector exercises
the versioned request/response and framing admission through authenticated Unix
and TLS/TCP;
production dispatch is not switched. That handler requires an Active permit from
its own admission domain and the exact admitted primary section, bounds the PG
lock wait, then retains the same cutoff through marker commit. Installed tests
close the connection after marker commit, classify the lost reply as
`MayHaveApplied`, and confirm the exact retained context on a fresh session.
Bad response authentication and authenticated crossed confirmations likewise do
not establish success. The candidate owning sender consumes the inspected
complete context, fixes its target node and first deadline, and exposes only
scalar observations and closed send operations. It records uncertainty before
dispatch (including unwind), preserves it across later `NotSent`, authenticated
remote errors and invalid confirmations, and records marker confirmation only
after exact acknowledgement validation. A confirmed historical marker remains
observed across later failures; it is not current apply or cleanup authority.
There is no hidden reconnect/retry or detachable context/reset API. This sender
owns inspection evidence, not frontend payload custody. Candidate embedded
historical admission is implemented; live publisher selection and frontend
custody remain activation gates.

Historical candidate publication requires the current runtime map to authorize
the exact reporting node, PG, command epoch, log position and **complete carrier**
CRC64, not merely another command in the same PG/epoch. It holds the historical
recovery-primary section and an immutable bounded current-map lease; an ordinary
current-primary section cannot substitute. The map report alone is insufficient:
the shared transaction still verifies exact durable complete-command bytes/context,
scope and protection indexes, or exact applied evidence. A map-authorized command
that differs from the durable slot fails closed. The retained historical fence
is rechecked before trial, marker mutation and commit, as is the receiver-bound
work cutoff. Lost replies retain sender uncertainty and retry only the same
context. This introduces no additional command, transaction or fsync and does
not select historical publication in the live frontend.

A complete installation holder can also be consumed directly into that sender.
The transfer moves the original context, captured topology and installation
observation without reinspection, independently supplied descriptor/topology, or
an encode/decode reconstruction. The sender retains installation and publication
history separately: a confirmed marker does not retrospectively turn an
unconfirmed installation attempt into an observed installation. Inspection-only
origin is reported separately, never as evidence of installation absence. The
handoff and Drop perform no durable work or cleanup. Returning a raw submission
after a failed publication would discard history and is not supported. This is
the command-holder bridge, not yet live frontend generation/proof ownership.

The candidate embedded equivalent derives its primary, embedded runtime and
shared metadata-command serialization lock from a borrowed route admission.
Current publication requires the Active primary. Historical publication verifies
the supplied recovery-report snapshot against the dynamic admission's full
map-content identity, then derives the exact reported command and retained
Active primary under the current Peering map. A private section retains that
lock and a captured, immutable route fence; raw runtime-map renewal cannot extend
it. The node owner selects and locks
the actual PG, then binds opaque authority to that exact store, node, epoch,
topology and complete-command SHA-256. Both embedded and remote authority enter
the same trial/marker transaction and final pre-commit checks. Serialization and
PG waits are work-deadline bounded; the serialization scope also bounds them by
the captured route cutoff. Sibling callers cannot mint the authority or inject a
production mutation callback. Current and historical admission are implemented
on this candidate path; live embedded publisher selection and frontend custody
remain gated.

Recovery is a distinct pass that runs after `PgStore::open`. The recovery epoch
is the store's own `metadata_command_replica_state.cluster_epoch`, read
internally; an external authority or config epoch must never be supplied as the
recovery epoch, since orphan detection compares the slot's epoch against the
stored replica epoch and a mismatched external value would defeat the check.

Recovery classifies anomalies into two groups and treats them differently.

Fail closed (possible corruption). Recovery returns a typed error and the store
does not serve:

- command-log hash-chain break (`MetadataCommandLogHashMismatch`);
- materialised digest mismatch: `metadata_command_replica_state.state_digest`
  disagrees with a full recomputation from rows
  (`MetadataStateDigestMismatch`);
- a forked or out-of-order command log.

Reconcile (benign crash leftovers). Recovery repairs the state and continues:

- an older-epoch pending command slot may be removed only after cluster startup
  validates its exact command and old-epoch hash chain from a checkpoint base
  on every actor. Every terminal disposition, applied or abandoned, includes
  the command entry's exact previous and resulting hashes. That evidence must
  agree across the acting set before any resource or slot cleanup. Zero applied
  evidence permits unpublished cleanup only when no durable publication marker
  exists. Mixed, divergent, or invalid evidence fails closed. Future-epoch
  pending slots remain durable until a cluster-level convergence path has
  acting-set evidence.
- cache-only per-table digest drift: after replay validation proves
  `metadata_command_replica_state.state_digest` still matches the materialised
  rows, recovery refreshes `metadata_table_digests` from those rows so stale
  cached table digests cannot poison the next mutation.

Terminal pending command slots are different: a terminal slot proves this
replica recorded the command, but does not by itself prove that the acting set
converged. Local node recovery and heartbeat must therefore preserve terminal
slots. Cluster-level recovery, or an explicit command path that owns the
pending command, may remove a terminal slot only after it has acting-set
evidence or equivalent command-specific convergence proof.

Older-epoch classification is order-dependent: local open preserves the slot
while validating the current epoch, then performs acting-set classification
before releasing any dependency or removing the exact old slot.

Heartbeat and other serving-time observation paths must not delete or advance
pending slots: a legitimate command can install a future-epoch pending slot
before apply/record advances the durable replica state to that epoch, and a
terminal same-epoch slot still needs acting-set cleanup evidence. Heartbeat
therefore reports the durable metadata proof plus whether any pending slot is
present. The distinction between fail-closed, local reconcile, and
cluster-level terminal cleanup is deliberate: corruption is surfaced, local
crash leftovers that are provably orphaned are healed idempotently before
serving, and ambiguous terminal evidence remains visible until a convergence
path consumes it. The full recovery contract and its implementation slices live in
[multihost-phase-11-stabilisation-plan.md](../plans/completed/multihost-phase-11-stabilisation-plan.md).

## Route Transition And Primary-Lease Fencing

Every storage-node RPC frame acquires a route-admission permit before it reads
the current process config and retains that permit until its response has been
written. Runtime-config installation first closes ordinary admission, drains
all admitted frames, persists and publishes the replacement config
exclusively, and then reopens admission. Metadata PG-lock release is the only
frame admitted during the drain: it is cleanup needed to let an already
admitted waiter finish, and it cannot create new PG state.

Metadata mutations recheck route expiry after acquiring the per-PG lock.
Visible metadata command application additionally checks its request-bound
route fence inside the SQLite transaction immediately before commit. A current
command uses the installed map validity. A historical Active command requires
the current Peering runtime map to authorize its exact PG, command epoch, log
index, CRC64, and complete-command SHA-256; retained topology alone grants no mutation authority.
That authorization is bounded by the current map's process-bound monotonic
lease and disappears when the authority no longer reports the pending command.
The route-admission permit means a newer runtime config cannot publish between
the check and commit; expiry or guard failure rolls the transaction back,
including command-log, pending, and materialized metadata changes.

Read-only metadata access uses a separate capability. In `Active`, it resolves
to the current primary. In `Peering`, it resolves only to the replica selected
by the current `PgMetadataReadRoute`; storage atomically verifies the certified
proof and absence of a pending command before reading. Route admission still
drains across runtime-config replacement, so neither the control-plane
certificate nor its lease can change between validation and response
construction. This availability path does not weaken the mutation fence.

Retained-route payload reads do not depend on the historical data-PG primary
remaining available. Each exact historical shard owner returns the stored
payload together with a size and checksum derived from that same read; the
authenticated response codec verifies that binding before exposing either
value. The frontend accepts only the expected shard size, reconstructs from any
`k` valid surviving shards, and verifies the complete segment checksum before
returning bytes. If that checksum fails and another shard is available, it
performs at most `k` single-shard-exclusion reconstructions to retain the
existing one-corrupt-shard recovery contract and identify the suspect repair
target without an unbounded combination search. Failure to record that optional
repair while the PG has no Active mutation route does not invalidate bytes that
passed the segment checksum. A connection failure to one owner is temporary
unavailability, not proof that its shard needs repair, and must not enqueue
destructive repair work. Maintenance and backfill paths may still compare a
shard against the historical primary's durable acknowledgement catalogue when
that catalogue is part of their stronger repair proof.

The control plane independently fences successor activation. Whenever a PG
leaves `Active`, it persists the previous primary's node ID, incarnation,
endpoint, and last lease deadline in the PG record before any node lease is
cleared or replaced. Peering readiness and completion remain blocked while
that deadline is in the future if the proposed primary is a different process
identity. The exact same node/incarnation/endpoint may reactivate before its
own old deadline once the ordinary peering proof checks pass because there is
no deposed storage process to fence. While that old deadline remains live,
recovery-driven peering selection prefers the exact previous primary when it
is still serving; recovery of an earlier acting-set member must not cause an
unnecessary fenced failback. Explicit acting-set, PG-state, and metadata
transfer transitions retain the safety fence but disable this preference so
the requested administrative primary selection can take effect after the old
lease expires. Successful activation clears the previous-primary identity.

The route-transition effect audit classifies the remaining storage RPC
families as follows:

- Shard writes, repair/backfill writes, and shard-ack records may create
  durable payload or local ack state, but they do not independently publish an
  S3-visible object. Visibility still requires the fenced metadata-command
  commit. A route transition drains these frames before publishing Peering, so
  transfer/backfill observes their completed state; an abandoned file or ack
  is permitted orphan state for scavenging.
- Bucket write reservations, drains, generation reservations, and stream
  staging rows are coordination state, not independent object visibility.
  Frames admitted under the old route finish before Peering publication.
  Commands that depend on a reservation carry its durable proof, while exact
  release/reap operations remain cleanup and may run after the serving route
  changes.
- Reclaim claims and physical cleanup are rooted in durable reclaim metadata.
  Physical deletion is additionally fenced by storage-node read handles and
  exact claim/shard identity. Claim release and drain/reservation cleanup must
  remain available through retained routes because refusing cleanup after an
  epoch transition would leak authority records. A frame already in progress
  is drained before config publication.
- Passing a route-map deadline does not by itself publish a successor route.
  New ordinary work fails route validation after the deadline; already
  admitted work may finish, but a later Peering config cannot publish until it
  drains, and control-plane activation cannot precede the persisted previous
  primary deadline. This is the completion guard for non-metadata effects.

This closes the DCC-1 validate-then-transition race by effect. It does not
close RPC4: retained cleanup locations from different epochs can still name
the same physical shard file, so physical shard identity must be fenced across
epochs separately. The socket-level
`storage_node_route_transition_orders_old_command_before_successor_activation`
regression pins an admitted old command behind the session PG lock, starts the
Peering transition, releases the lock through the drain-safe cleanup frame,
and proves that the command is included before Peering while a later old-epoch
command cannot change the metadata proof after deadline and successor
activation.

## Control-plane administration boundary

Process code administers PGs through the bounded control-plane bootstrap and
logical admin clients. Metadata-transfer artifacts, retained transfer routes,
retry state, RPC frames and workers, Raft-peer transport/authentication, and
Raft restart/WAL formats remain private to storage. The public transfer
operation accepts logical PG/node/proof inputs and returns an opaque result;
it does not expose a runtime-map source or a raw transfer client. Its
authority-bearing client and operation are linear, non-debuggable
capabilities with private fields; compile-fail coverage and a focused semantic
boundary check protect those properties because public Rust types can
otherwise acquire new public fields or trait implementations.

Raw RPC and Raft protocol tests are storage-owned. Cross-crate startup and
administration tests use the same opaque clients as production, so they test
the supported boundary rather than minting wire requests or interpreting
private durable representations.

## Node-runtime compiler boundary

The raw embedded node, PG stores, and aggregate local node client are
descendants of the private `node_runtime` module. `SharedStorageNode` is
available to cluster code only through the test-hook facade, and
`LocalStorageNodeClient` uses descendant-only visibility. Production cluster
routing therefore receives only the role-specific node-client traits exposed
by `LocalNodeStore`. Stateful operations live on subject-bound route traits
returned by the matching role opener, so the compiler rejects an operation on
the wrong role or without its scoped subject/PG authority. Embedded and RPC
implementations satisfy the same traits; the boundary does not depend on a
source-level receiver-name inventory.

Command minting is audited separately from method visibility. CreateBucket is
the unique bucket-root command with no existing durable subject from which its
timestamp and execution generation can be reconstructed. Its record is
private and its constructor consumes an unforgeable authority whose mint is
visible only inside `node_runtime`. Canonical bytes may be validated without
minting a command, but returning a decoded `MetadataCommandEnvelope` requires
`MetadataCommandDecodeAuthority`. Its constructor is likewise confined to the
private node runtime, and every envelope-producing storage-RPC decoder must
receive that authority, so cluster publishers cannot bypass a typed factory
through either the raw decoder or an indirect response codec. The remaining
focused semantic rule protects CreateMultipartUpload, the object-root
exception, by rejecting cluster-side direct construction outside its
subject-scoped node-runtime builder. Other migrated object command builders
are backed by apply-time validation of their exact reservation, durable
preimage, generation, or cleanup snapshot; those invariants remain covered by
recovery and malformed-envelope tests rather than route-method name scans.

## Residual boundary-check role

The storage boundary checker is no longer a parallel inventory of production
methods, test files, or physical payload callers. Rust privacy and scoped
capabilities enforce those boundaries. Its retained rules cover only semantic
properties which the compiler does not otherwise reject: resolved Cargo
feature graphs, canonical metadata-command publisher/recovery classification,
selected same-crate conversion and retry-budget rules, process/HTTP dependency
direction, opacity of intentionally public transfer and administration
facades, nested-codec ownership, route-publication ownership, and the curated
cross-crate test-support surface.

Recovery is the one deliberately exact storage-internal authority inventory.
Its leader proof, derivative command chain, and lower mutation entry points
share crate/module visibility, so adding or moving an authority-bearing use is
an architectural review event. All former temporary shard-read, shard-delete,
payload-lease, discovered-file, and historical method-name inventories are
gone.

## Storage operation-class matrix

The table below records the storage operation classes and representative
owner-side entry points used by the architecture. It is not an exhaustive
public-API or source-file inventory: an entry may be a private implementation
detail behind a typed route or opaque operation, and moving or renaming one
does not require a boundary-check allowlist update. Rust visibility, scoped
capability types, and the focused semantic checks described below define the
actual boundary.

| Methods | Class |
|---|---|
| `prepare_standalone_embedded_topology` plus `PreparedStandaloneEmbeddedTopology::open`, `open_static_local_nodes`, `StorageClusterRouteHandle::from_static_cluster`, `StorageClusterRuntimeMapHandle::new` | Authority-checked static/dynamic construction |
| `cluster_epoch`, `operation_epoch`, `metadata_node_id`, `local_node_count`, `local_node_ids`, `local_pg_route`, `local_pg_routes`, `process_local_registry_key`, `default_payload_ec_shape` | Read-only topology/config |
| `place_payload_shards`, `payload_shard_node`, `write_payload_shard`, `read_payload_shard`, `read_payload_shard_into`, `delete_payload_shard`, `write_direct_put_segment_payload_shards`, `write_stream_segment_payload_shards`, `read_segment_payload_stored_bytes_into` | Payload placement/read/write/delete |
| `delete_direct_put_segment_payload_shards` | Best-effort cleanup/worker queue |
| `reserve_put_object_generation`, `release_object_generation_reservation`, `commit_direct_put_object_from_payload_shards` | Epoch-fenced routed metadata PG command apply |
| `create_put_object_stream_session_record`, `create_put_object_stream_session`, `commit_stream_segment_append`, `abort_stream_upload_session`, `finalize_put_object_stream` | Epoch-fenced routed metadata PG command apply for stream session creation, segment append, abort, object-generation reservation, and standard-object publication. The shared append/abort methods use the command path for both PutObject and UploadPart stream sessions |
| `load_stream_upload_session`, `prepare_stream_segment_append` | Epoch-fenced routed metadata PG; prepare drains pending object command state before allocating the next staged segment generation |
| `list_stream_upload_sessions_best_effort` | Best-effort routed metadata PG fanout |
| `create_bucket_with_config_and_load_info`, `put_bucket_versioning_and_load_info`, `put_bucket_acl_and_load_info`, `put_bucket_object_lock_and_load_info`, `put_bucket_encryption_and_load_info`, `put_bucket_public_access_block_and_load_info`, `delete_bucket_public_access_block_and_load_info`, `put_bucket_ownership_controls_and_load_info`, `delete_bucket_ownership_controls_and_load_info`, `put_bucket_abac_enabled_and_load_info`, `put_bucket_subresource_and_load_info`, `delete_bucket_subresource_and_load_info` | Epoch-fenced routed metadata PG command apply |
| `try_probe_bucket_pg_available`, `load_bucket_snapshot`, `head_bucket_info`, `get_bucket_subresource` | Epoch-fenced routed metadata PG |
| `with_bucket_write_snapshot` | Epoch-fenced routed metadata PG plus durable bucket-PG-primary write reservation acquire/release |
| `list_buckets_for_owner`, `list_lifecycle_sweep_buckets` | Epoch-fenced routed metadata PG fanout |
| `begin_bucket_delete` | Epoch-fenced routed metadata PG fanout |
| `try_finalize_bucket_delete` | Epoch-fenced routed metadata PG fanout plus storage-node object-payload read-handle checks and local runtime worker queue |
| `load_available_bucket_execution_generation_batches` | Best-effort routed metadata PG |
| `try_probe_object_pg_available`, `load_object_if`, `load_existing_live_object`, `load_object_read_snapshot_if`, `load_leased_object_read_snapshot_if`, `payload_reclaim_exists`, `get_object_tags_if`, `get_object_legal_hold_if`, `get_object_retention_if` | Epoch-fenced routed metadata PG. Admitted object-subresource reads load their exact authorization subject through `ActiveObjectReadRoute`, which binds bucket/key/version/PG and rechecks the immutable request deadline immediately before node access. The leased snapshot helper additionally acquires a broad payload-generation lease before exact snapshot validation, retries if the authorization subject changed, and returns an opaque non-cloneable handoff binding that snapshot, its route provenance, and the lease |
| `put_object_tags_if`, `delete_object_tags_if`, `put_object_retention_if`, `put_object_legal_hold_if`, `put_object_acl_if`, `delete_specific_object_version_if`, `delete_current_object_if`, `insert_current_delete_marker_if`, `expire_current_object_if_due`, `delete_noncurrent_live_versions_if_due`, `delete_expired_delete_marker_if_due` | Epoch-fenced routed metadata PG command apply. Production object-subresource mutations use `ActiveObjectMetadataMutationRoute`; the corresponding raw cluster entry points are test-only |
| `list_all_objects_for_bucket`, `list_all_object_versions_for_bucket`, `list_all_multipart_uploads_for_bucket`, `list_objects_for_bucket`, `list_object_versions_for_bucket` | Epoch-fenced routed metadata PG fanout |
| `acquire_object_payload_lease`, `acquire_object_payload_lease_for_shard_locations` | Epoch-fenced routed metadata PG plus volatile storage-node-owned object-payload read handles. Copy-source and response-body snapshot loading acquire the coarse generation lease through the actual storage-node clients, so it remains visible to refreshed runtime maps and other frontends, then hand off without a gap to the shard-location helper through an opaque leased-snapshot token. Payload reads acquire all-or-release handles for every shard-owner node of the selected segments, including parity/recovery candidates. Every recorded placement epoch is validated and read through its exact retained route while acquisition remains authorized by the current admitted request epoch; the boundary script rejects direct payload-byte read bypasses |
| `enqueue_object_payload_reclaim`, `enqueue_bucket_delete_finalize`, `wait_for_reclaim_work`, `wake_reclaim_workers` | Best-effort local runtime worker queue |
| `reclaim_object_payload_if_unleased` | Epoch-fenced routed object metadata PG command apply for reclaim-row deletion, storage-node-owned read-handle/delete fencing, and placed payload cleanup |
| `complete_multipart_upload_commit_serialized_with_route_validation` | Admitted and deadline-fenced bucket-PG-primary multipart completion barrier, then routed object metadata PG command apply for completed-object publication, deterministic object write sequencing, selected streamed part segment metadata, object-version-scoped replay metadata, and omitted staging cleanup. The barrier advances one fixed-size bucket scalar and retains no upload history |
| `create_multipart_upload` | Epoch-fenced routed object metadata PG command apply for multipart upload row creation and upload generation reservation |
| `abort_multipart_upload` | Epoch-fenced routed object metadata PG command apply for multipart upload row deletion, upload generation reservation release, and part staging metadata removal; the command carries the abort-preparation cleanup snapshot, and cluster-owned best-effort payload cleanup uses those retryable refs |
| `abort_multipart_upload_if_due` | Bucket-lifecycle recheck under bucket-PG-primary lock, then epoch-fenced routed object metadata PG command apply through `abort_multipart_upload` |
| `create_upload_part_stream_session_with_route_validation`, `prepare_stream_segment_append_with_route_validation`, `finalize_upload_part_stream_with_route_validation` | Epoch-fenced routed object metadata PG effects for streamed UploadPart staging creation, segment-VID allocation, and finalization. Ordinary UploadPart and UploadPartCopy carry their immutable admitted effect fence to reservation, allocator, shard-write, and pending-command boundaries. `CreateStreamUpload` apply revalidates UploadPart targets against the current in-progress MPU row on each acting node |
| `load_multipart_upload`, `load_in_progress_multipart_upload`, `try_load_in_progress_multipart_upload`, `load_in_progress_multipart_upload_for_listing`, `load_multipart_completion_snapshot`, `load_multipart_completion_preflight`, `list_multipart_parts_for_upload`, `lookup_abort_multipart_upload` | Epoch-fenced routed metadata PG |
| `list_multipart_uploads_for_bucket` | Epoch-fenced routed metadata PG fanout |

## Associated Token Types

`ActiveObjectReadRoute` is the non-cloneable active authority for one
bucket/key/requested-version/object-metadata-PG tuple. Metadata-only subject
loads power GetObjectTagging, GetObjectAcl, GetObjectRetention, and
GetObjectLegalHold without granting mutation authority; snapshot and leased
snapshot loads use the same route for HEAD, object attributes, response-body
GET, and copy-source reads. Every node access rechecks the admission's
immutable deadline, and the coordinator accepts the route only from its own
publication domain.

`ActiveObjectMetadataMutationRoute` is the non-cloneable active mutation
authority for one bucket/key/requested-version/object-metadata-PG tuple.
PutObjectTagging, DeleteObjectTagging, PutObjectAcl, PutObjectRetention, and
PutObjectLegalHold use the buffered request's existing admission for both
policy context and the mutation command. The route rechecks that admission's
generation-gated effective deadline before snapshot loading and command
construction. Same-generation renewal may extend it only while the route
remains openly serving; replacement publication freezes it at the original
admission deadline. It also
carries an `AdmittedRouteEffectFence` through bucket-write reservation
acquisition and pending-command installation, where the embedded or RPC
storage node revalidates the request's original epoch and clock health
immediately before inserting the durable row. Embedded calls retain the exact
conservatively bound monotonic deadline. RPC requests contain no monotonic
timestamp: they carry a portable wall-clock upper bound which Unix and TLS/TCP
clients derive with the cross-process skew allowance removed. Every receiving
process binds that same representation to its own monotonic clock.
The authority timestamp remains available for diagnostics, but is not used as
the later cutoff. Once the
authorized command is installed, applying that exact command is convergence
and may complete while a successor map waits to publish. Raw cluster
metadata-mutation entry points are available only to storage unit tests.

`ActivePutObjectRoute` is the non-cloneable authority for one complete direct
or promoted-stream PutObject workflow, including the destination side of
CopyObject. It fixes the bucket, key,
bucket-metadata PG, object-metadata PG, publication domain, request identity,
and original admission deadline before
authorization. The same route then owns current-object loading, the durable
bucket-write snapshot, object-generation reservation, staged data-PG shard
writes, stream-session creation, segment-append publication, finalization, and
final object metadata publication. Reservation acquisition, shard writes, and
every create/append/finalize pending-command insertion carry the route's
`AdmittedRouteEffectFence` to the actual storage-node effect. The shard-write
fence samples the route's effective deadline: healthy same-generation renewal
may extend it while publication remains open, while pending publication
restores the original admission deadline. Streaming body waits are notified of that
transition so a wait already using a renewed deadline cannot delay publication.
The shard-write RPC delegates only portable wall-clock authority and the receiver
conservatively binds it to its own
monotonic clock; repair writes cannot carry that frontend authority. Expiry at
any pre-publication boundary releases transient reservations and removes only
payload known to belong to the rejected attempt, including shards written
earlier in a partially completed direct placement.

Stream heartbeats use that same route rather than a renewable raw cluster
handle. Both durable effects—the bucket reservation lease renewal and the
stream session's persisted proof update—receive and revalidate the original
effect fence immediately beside mutation. The route epoch binds the new
effect; the reservation proof may retain its older acquisition epoch across a
route transition. The node-client heartbeat interfaces derive that authorizing
route epoch from the effect fence itself; callers cannot provide a second epoch
which disagrees with the fence. Unix and TLS/TCP carry only the portable wall
deadline and rebind it to storage-local monotonic time. The raw unbounded
heartbeat adapter is test/test-hook-only.

`ActiveMultipartObjectRoute` is the corresponding non-cloneable authority for
one multipart bucket/key/object-metadata-PG tuple. In addition to multipart
control and completion, UploadPartCopy uses it for destination session
creation, encrypted shard placement, segment append, and part finalization.
Multipart authorization candidates and the operation-specific capabilities
derived from them carry a private non-`Clone` marker, so consuming a candidate
cannot accidentally become reusable merely because its retained durable
record is cloneable. The multipart-upload-ID authority is intentionally
cloneable for bucket snapshot/response sharing, but carries a separate
non-comparable marker so equality cannot expose whether two authorities retain
the same hidden signing key.
The route binds the authorized upload row to the destination subject and
carries the immutable request effect fence through every fresh durable
reservation, segment-VID allocation, shard-file, and pending-command effect.
The cleanup deadline is derived from the admission inside the capability and
cannot be omitted by its caller, so recovery can remove abandoned state after
prompt retained cleanup releases frontend admission. Unbounded create and
finalize wrappers exist only for test support.

`LeasedObjectReadSnapshot` is the non-cloneable broad-to-narrow handoff token.
Its private fields bind the exact object snapshot, requested version, metadata
route, originating cluster, and broad generation lease; callers may inspect the
snapshot but cannot pair another snapshot with that lease. An
`ActiveObjectReadRoute` can consume it only when all provenance matches.
Broad-lease acquisition and exact snapshot loading are structurally colocated
in one child module. The local-map acquisition primitive requires an opaque
authority that binds the exact bucket, key, and generation loaded from the
live subject. Its constructor is private to that complete workflow, so sibling
request implementations cannot acquire or retain a coarse lease independently
of the snapshot token. The token's representation and the consuming
broad-to-narrow lease transition are defined in that same child module, rather
than an ancestor module whose private fields would remain visible to its
descendants. Surrounding cluster code can only inspect the immutable snapshot
or call the opaque operations exposed on `ActiveObjectReadRoute`. Those
operations are implemented inside the child module and derive the repair fence
and route-validity checks directly from that route's admission; no production
cluster helper accepts caller-supplied repair authority.

CopyObject and UploadPartCopy reconstruct that exact admitted source route
before consuming the handoff. Both forms derive every shard owner from the
token's recorded placement epochs,
acquire the narrow storage-node leases, and only then release the broad lease.
Copy-source snapshot loading itself uses the request admission, so its retained
repair fence captures the publication generation and immutable request
deadline. The authorization result and token share one immutable snapshot
allocation, so large multipart part and segment vectors are not cloned during
handoff.

`RetainedObjectPayloadRead` is the resulting response-body or copy-source
authority. It carries no long-lived publication admission and cannot perform
object metadata reads. It binds one bucket/key/generation and a private
allowlist of complete segment descriptors; range, part, ordinary GET, and copy
bodies can read only those descriptors. Every payload read uses exact
retained-route inspection, including when the segment's placement epoch equals
the originating frontend's epoch. If recovery discovers a corrupt shard, it
may reacquire a short publication permit solely to record repair work, but
only if the originating publication generation remains current and the
admission's effective deadline is still valid. Same-generation renewal may
extend that deadline while publication is open; a pending publication freezes
it at the admission-time deadline. After map publication or deadline expiry it
skips that stale active-route mutation. The payload read
therefore remains valid if the storage nodes install a successor map before
the body's first read. Dropping the final body reference releases the captured
node sessions and performs the ordinary reclaim follow-up.

`ActiveObjectPayloadRead` is the corresponding capability for cluster-backed
body reads that do not retain an admitted snapshot across publication. Storage
constructs it from the exact bucket, key, generation, and selected opaque
segments, expands their historical placements, and acquires the required
deletion-exclusion leases before returning it. The capability owns those
leases and is the only public surface that can read its selected segment
bytes; the raw cluster segment reader is private. Dropping the final shared
capability releases its leases and schedules reclaim when required. Thus
server-core cannot retain one lease while reading bytes through an unrelated
cluster handle, and no source-level caller inventory is needed for this path.

Complete Cobblestone reads use a second opaque state nested under either
payload capability. The state accepts only contiguous ciphertext ranges from
offset zero and privately maps those bytes back to the systematic shards whose
deletion-exclusion leases are held. It updates each shard's CRC as bytes are
read, consumes the actual EC-padding suffix from the final systematic shard,
and completes only when every resulting size and CRC matches the segment's
durable integrity witness and their systematic composition matches the stored
segment checksum. EC geometry, shard identities, hash state, and completion
authority never cross into server-core.

Server-core couples that state to the subject-bound incremental Cobblestone
opener. It may expose each successfully authenticated non-final record while
later records are still unread. The final plaintext-bearing record is held
until physical completion; for exact chunk multiples this also means consuming
the authenticated empty terminal record first. If a later systematic range or
physical witness check fails, the existing complete reconstruction path
resumes after the exact plaintext prefix already emitted. An unrecoverable
failure exposes no bytes from the failing record and terminates an already
committed response. Returning an authenticated frame from the HTTP body does
not acknowledge delivery to the client: when a later body error aborts the
transport, the socket may deliver none, part, or all of that earlier frame.
Hyper may also encounter the body error before flushing any response headers,
so a committed response can produce an entirely empty wire response.
Every delivered byte must still be a prefix of records that authenticated
before the failure, and the declared response must remain incomplete.
Historical whole-segment codec bytes are rejected at the current format
boundary; there is no compatibility reader.

### Degraded range-read reconstruction policy

The complete-segment fallback is deliberate, not unfinished correctness work.
A healthy range reads only its intersecting Cobblestone records, but any
missing, unavailable, malformed, unauthenticated, or witness-inconsistent
required range enters complete witness-backed segment reconstruction. The
fallback supplies one uniform diagnostic boundary: it validates every physical
shard against durable evidence, identifies known erasures, reconstructs the
complete ciphertext when at least `k` valid shards remain, authenticates it,
and schedules repair only from complete evidence.

Partial EC reconstruction is deferred because its benefit depends on both the
workload and the fault shape. It is most useful when requests repeatedly touch
small, sparse ranges and the failed bytes are already attributable to a narrow
known erasure. An isolated small range can still benefit while a complete shard
is unavailable, but repeated partial reconstruction can do more total work when
that failure persists and callers eventually cover most of a segment through
many requests. The same is true for individually large ranges or broadly
corrupt extents. Suspect bytes discovered by AEAD or witness failure
additionally require the complete diagnostic path before storage may identify
a corrupt shard or authorize repair; treating them as an assumed erasure would
weaken that boundary.

Do not add partial reconstruction merely because the EC library supports it.
Revisit it only if production-like measurements show that full fallback is a
material cost for the observed combination of range sizes, cumulative segment
coverage, fallback frequency, fault cause and extent, and fault duration. Any
future optimization must remain restricted to already-known erasures, hold
deletion-exclusion leases for every selected survivor, authenticate all
reconstructed records before exposure, and fall back to complete validation on
short, malformed, suspect, or unauthenticated results. It is an IO optimization
and must not change durable encryption bytes or the recovery guarantee. The
current measured healthy and degraded costs are recorded in the
[performance guide](performance.md#multihost-and-degraded-payload-reads).

`ObjectPayloadLease::release` is the current active token release path. It must
release against the actual storage-node sessions captured at acquisition time
and must not depend on the frontend's current cluster epoch or runtime-map
generation. Unix session disconnect releases the server-side generation lease.
Long-lived Unix generation-lease sessions count against both the aggregate RPC
admission limit and its shared non-control budget. All lease acquisition stops
one slot short of that shared budget, preserving narrow-lease to read-handle
progress. Broad acquisition stops one additional slot earlier, preventing new
broad leases from consuming the broad-to-narrow transition slot during
one-at-a-time handoff to shard-scoped leases. Releasing each broad lease after
its successor is acquired preserves the same slot for the next handoff. The
remaining reserved capacity allows short reclaim/control operations.
Phase 9.5 keeps these handles volatile: if the process/node serving the read
fails, the client retries from a fresh snapshot.

Storage-node read-handle and reclaim-fence primitives are crate-local. The
production surface is `StorageCluster` read-handle acquisition plus a
storage-owned placed-segment deletion capability; Rust visibility prevents
downstream use of the low-level payload-read, current-route delete,
retained-route delete, and storage-node handle/fence APIs. The deletion
capability derives every location and shard key from the typed data PG, EC
shape, segment identity, and payload generation. Current deletion uses the
installed route; historical cleanup uses the exact reconstructed route at the
payload's placement epoch. Both accept only a matching derived key from a
staged-write owner or durable reclaim record before reaching the corresponding
storage-node deletion fence, so a caller cannot combine a location from one
placement with another segment's key. No temporary semantic caller inventory
remains: coarse generation-lease acquisition is enforced by the private
broad-lease authority, and physical deletion is enforced by the placed-segment
capability.

Current-route segment reads use a separate storage-private placed-segment
reader over the same exact derived subject. Callers select only a shard index,
so they cannot combine a location from one placement with a key from another
segment. A batched reader additionally retains the exact shard-index subset
for which read handles were acquired and rejects access to every other index.
The write-side publish validator uses the same reader to prove acknowledged
shard files still match their `WriteAck` before publishing metadata.

`ReleasedObjectPayloadLease::remaining`, `ReleasedObjectPayloadLease::payload_reclaim_exists`,
and `ReleasedObjectPayloadLease::enqueue_object_payload_reclaim` are release
follow-up helpers for the already-acquired token. `payload_reclaim_exists`
routes through the object PG primary for the handle epoch captured at acquire
time; stale or unavailable routing is treated by callers as a conservative
reason to enqueue. `enqueue_object_payload_reclaim` uses the captured
local-cluster runtime state because enqueueing after the final release is part
of the already-acquired token workflow, not new metadata work. Durable reclaim
metadata remains the retry source, and read handles only decide whether storage
nodes may physically delete local shard files now.

Maintenance queue rows, scan cursors, claims, physical repair/backfill plans,
scavenger observations, and stream-session cleanup transitions are
storage-private. Other crates can schedule these workflows only through the
opaque `StorageReclaimSweeper`, `StorageShardScavengerSweeper`,
`StorageShardRepairSweeper`, `StorageShardBackfillSweeper`, and
`StorageStreamSessionSweeper` facades. `StorageMaintenanceAdmission` is the
deliberate shared admission interface for lifecycle cleanup and the opaque
sweepers; it does not expose their durable records or physical operations.
